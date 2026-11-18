"""SQLite 登记 + 下游目录原子发布 + 启动一致性恢复。

设计要点
--------
* SQLite 是版本/行数/标样/校正登记的唯一真相源；下游目录中的 CSV 只是它的镜像。
* 发布在单个 ``BEGIN IMMEDIATE`` 事务内完成：先写同目录临时文件并 fsync，
  再 ``os.replace`` 原子改名（下游只会看到完整的旧文件或新文件），
  最后提交事务登记新版本。
* 校正数据（``calibrated_results.csv``）与原始发布 CSV 完全独立：确认校正时
  同样原子写入下游目录，并在 ``data/calibration_archive/`` 留存归档副本；
  新批次发布或标样修改会在同一事务内使旧校正失效并撤下供数文件，
  旧校正只能按原版本从归档追溯下载。
* 提交后进程若在改名完成前崩溃：重启时按数据库登记重新生成 CSV，
  并清理遗留临时文件；登记里不存在的游离文件会被移入 quarantine 隔离。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from .calibration import calibrate_rows, render_calibrated_csv
from .validation import format_concentration, render_csv

LIVE_FILENAME = "published_results.csv"
CALIBRATED_FILENAME = "calibrated_results.csv"


class PublishConflictError(Exception):
    """确认发布时基线版本已过期，需要重新预览。"""


class CalibrationConflictError(Exception):
    """确认校正时发布版本或标样集已变化，需要重新换算预览。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Storage:
    def __init__(self, data_dir: str, downstream_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.downstream_dir = os.path.abspath(downstream_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.downstream_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "wq_publish.db")
        self.live_path = os.path.join(self.downstream_dir, LIVE_FILENAME)
        self.calibrated_path = os.path.join(self.downstream_dir, CALIBRATED_FILENAME)
        self.quarantine_dir = os.path.join(self.data_dir, "quarantine")
        self.calibration_archive_dir = os.path.join(
            self.data_dir, "calibration_archive"
        )
        os.makedirs(self.quarantine_dir, exist_ok=True)
        os.makedirs(self.calibration_archive_dir, exist_ok=True)
        self._init_db()
        self.recover()

    # ---------- 基础 ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS registry (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    published_at TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    sample_no TEXT NOT NULL,
                    analyte TEXT NOT NULL,
                    concentration REAL NOT NULL CHECK (concentration >= 0),
                    UNIQUE (version, sample_no, analyte)
                );
                CREATE INDEX IF NOT EXISTS idx_results_version
                    ON results(version);
                CREATE TABLE IF NOT EXISTS revision_sessions (
                    token TEXT PRIMARY KEY,
                    batch_no TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','published','obsolete','cancelled'))
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS standard_sets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    point_count INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS standard_points (
                    set_id INTEGER NOT NULL REFERENCES standard_sets(id),
                    analyte TEXT NOT NULL,
                    x1 REAL NOT NULL CHECK (x1 >= 0),
                    y1 REAL NOT NULL CHECK (y1 >= 0),
                    x2 REAL NOT NULL CHECK (x2 >= 0),
                    y2 REAL NOT NULL CHECK (y2 >= 0),
                    UNIQUE (set_id, analyte)
                );
                CREATE TABLE IF NOT EXISTS calibration_sessions (
                    token TEXT PRIMARY KEY,
                    publish_version INTEGER NOT NULL,
                    standards_id INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','confirmed','obsolete','cancelled'))
                );
                CREATE TABLE IF NOT EXISTS calibrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_version INTEGER NOT NULL,
                    standards_id INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('current','superseded'))
                );
                """
            )

    def _log(self, conn: sqlite3.Connection, kind: str, detail: str) -> None:
        conn.execute(
            "INSERT INTO event_log(ts, kind, detail) VALUES (?,?,?)",
            (_now(), kind, detail),
        )

    def log(self, kind: str, detail: str) -> None:
        with self._connect() as conn:
            self._log(conn, kind, detail)

    # ---------- 启动恢复 ----------
    def recover(self) -> list[str]:
        """重启后核对下游文件与登记的一致性，返回本次恢复动作说明。"""
        actions: list[str] = []
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

            # 1) 清理崩溃遗留的临时文件（可能只写了一半，绝不能被下游读到）
            for name in os.listdir(self.downstream_dir):
                if name.startswith((".tmp-publish-", ".tmp-calibrate-")):
                    path = os.path.join(self.downstream_dir, name)
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                    msg = f"清理遗留临时文件 downstream/{name}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)
            for name in os.listdir(self.calibration_archive_dir):
                if name.startswith(".tmp-calib-archive-"):
                    try:
                        os.unlink(
                            os.path.join(self.calibration_archive_dir, name)
                        )
                    except OSError:
                        pass

            # 2) 登记为空：正式文件（若存在）属于无登记游离文件，隔离并保留
            if reg is None:
                if os.path.exists(self.live_path):
                    dest = self._quarantine(self.downstream_dir, LIVE_FILENAME)
                    msg = f"数据库无任何发布登记，将下游游离文件移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)
            else:
                expected_csv = self._render_version(conn, reg["current_version"])
                expected_hash = hashlib.sha256(expected_csv.encode("utf-8")).hexdigest()

                # 3) 文件缺失/哈希不符：以登记为准原子重建
                need_rewrite = True
                if os.path.exists(self.live_path):
                    with open(self.live_path, "rb") as fh:
                        actual_hash = hashlib.sha256(fh.read()).hexdigest()
                    need_rewrite = actual_hash != expected_hash

                if need_rewrite:
                    self._atomic_write(self.live_path, ".tmp-publish-", expected_csv)
                    msg = (
                        f"下游 CSV 与登记 v{reg['current_version']} 不一致，"
                        "已按数据库登记重建（恢复为完整新版本）"
                    )
                    actions.append(msg)
                    self._log(conn, "recover", msg)
                else:
                    self._log(
                        conn,
                        "recover",
                        f"启动核对通过：下游 CSV 与登记 v{reg['current_version']} 一致",
                    )

            # 4) 校正供数文件核对：仅当登记中存在生效校正时才允许存在
            cal = conn.execute(
                "SELECT * FROM calibrations WHERE status='current' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if cal is None:
                if os.path.exists(self.calibrated_path):
                    dest = self._quarantine(self.downstream_dir, CALIBRATED_FILENAME)
                    msg = (
                        "无生效校正登记（校正已失效或从未确认），下游校正文件 "
                        f"已移入 {os.path.basename(dest)}，不作为当前数据供数"
                    )
                    actions.append(msg)
                    self._log(conn, "recover", msg)
            else:
                try:
                    expected_cal = self._render_calibration(conn, cal)
                except Exception as exc:  # 登记数据被外部破坏时宁可不供数也不崩溃
                    msg = f"生效校正 #{cal['id']} 重算失败（{exc}），请重新换算确认"
                    actions.append(msg)
                    self._log(conn, "recover", msg)
                    if os.path.exists(self.calibrated_path):
                        dest = self._quarantine(
                            self.downstream_dir, CALIBRATED_FILENAME
                        )
                        msg = (
                            "校正登记无法验证，下游校正文件已移入 "
                            f"{os.path.basename(dest)}，不作为当前数据供数"
                        )
                        actions.append(msg)
                        self._log(conn, "recover", msg)
                else:
                    expected_cal_hash = hashlib.sha256(
                        expected_cal.encode("utf-8")
                    ).hexdigest()
                    need_cal_rewrite = True
                    if os.path.exists(self.calibrated_path):
                        with open(self.calibrated_path, "rb") as fh:
                            actual_cal_hash = hashlib.sha256(fh.read()).hexdigest()
                        need_cal_rewrite = actual_cal_hash != expected_cal_hash
                    if need_cal_rewrite:
                        self._atomic_write(
                            self.calibrated_path, ".tmp-calibrate-", expected_cal
                        )
                        msg = (
                            f"下游校正 CSV 与登记校正 #{cal['id']} 不一致，"
                            "已按数据库登记重建"
                        )
                        actions.append(msg)
                        self._log(conn, "recover", msg)
                    archive_path = self._archive_path(cal)
                    if not os.path.exists(archive_path):
                        self._atomic_write(
                            archive_path, ".tmp-calib-archive-", expected_cal
                        )
                        msg = f"校正 #{cal['id']} 归档缺失，已按登记补写"
                        actions.append(msg)
                        self._log(conn, "recover", msg)

            # 5) 归档目录白名单：未登记的归档文件（如崩溃残留）移入隔离
            valid_archives = {
                os.path.basename(self._archive_path(row))
                for row in conn.execute("SELECT * FROM calibrations").fetchall()
            }
            for name in os.listdir(self.calibration_archive_dir):
                full = os.path.join(self.calibration_archive_dir, name)
                if (
                    os.path.isfile(full)
                    and not name.startswith(".tmp-")
                    and name not in valid_archives
                ):
                    dest = self._quarantine(self.calibration_archive_dir, name)
                    msg = f"校正归档目录存在未登记文件 {name}，已移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 6) 同目录下游文件白名单：非登记文件一律隔离
            for name in os.listdir(self.downstream_dir):
                full = os.path.join(self.downstream_dir, name)
                if os.path.isfile(full) and name not in (
                    LIVE_FILENAME,
                    CALIBRATED_FILENAME,
                ):
                    dest = self._quarantine(self.downstream_dir, name)
                    msg = f"下游目录存在未登记文件 {name}，已移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 7) 超过 24 小时仍未确认的预览/校正会话作废，避免无限期堆积
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(
                timespec="seconds"
            )
            stale = conn.execute(
                "SELECT token FROM revision_sessions "
                "WHERE status='pending' AND created_at < ?",
                (cutoff,),
            ).fetchall()
            for s in stale:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete' WHERE token=?",
                    (s["token"],),
                )
                self._log(
                    conn, "recover",
                    f"预览会话 {s['token'][:8]} 超过 24 小时未确认，已作废",
                )
            stale_cal = conn.execute(
                "SELECT token FROM calibration_sessions "
                "WHERE status='pending' AND created_at < ?",
                (cutoff,),
            ).fetchall()
            for s in stale_cal:
                conn.execute(
                    "UPDATE calibration_sessions SET status='obsolete' WHERE token=?",
                    (s["token"],),
                )
                self._log(
                    conn, "recover",
                    f"校正会话 {s['token'][:8]} 超过 24 小时未确认，已作废",
                )
        return actions

    def _quarantine(self, src_dir: str, name: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = os.path.join(self.quarantine_dir, f"{stamp}-{name}")
        os.replace(os.path.join(src_dir, name), dest)
        return dest

    def _atomic_write(self, dest_path: str, tmp_prefix: str, csv_text: str) -> None:
        """同目录临时文件 + fsync + 原子改名，保证下游只看到完整文件。"""
        directory = os.path.dirname(dest_path)
        tmp_path = os.path.join(directory, f"{tmp_prefix}{uuid.uuid4().hex}.csv")
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
                fh.write(csv_text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, dest_path)
            self._fsync_dir(directory)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # ---------- 读 ----------
    def current_registry(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

    def current_rows(self) -> list[dict]:
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()
            if reg is None:
                return []
            return self._version_rows(conn, reg["current_version"])

    def _version_rows(self, conn: sqlite3.Connection, version: int) -> list[dict]:
        cur = conn.execute(
            "SELECT sample_no, analyte, concentration FROM results "
            "WHERE version = ? ORDER BY sample_no, analyte",
            (version,),
        )
        return [dict(r) for r in cur.fetchall()]

    def _render_version(self, conn: sqlite3.Connection, version: int) -> str:
        return render_csv(self._version_rows(conn, version))

    def recent_events(self, limit: int = 20):
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM event_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def get_session(self, token: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM revision_sessions WHERE token = ?", (token,)
            ).fetchone()
            if row is None:
                return None
            data = dict(row)
            data["rows"] = json.loads(data["payload"])
            return data

    def create_session(self, batch_no: str, base_version: int, rows: list[dict]) -> str:
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO revision_sessions(token, batch_no, base_version, payload,"
                " created_at, status) VALUES (?,?,?,?,?,?)",
                (
                    token,
                    batch_no,
                    base_version,
                    json.dumps(rows, ensure_ascii=False),
                    _now(),
                    "pending",
                ),
            )
        return token

    def mark_obsolete(self, token: str, current_version: int) -> None:
        """预览在确认前已被其他发布超越：作废该会话，强制重新预览。"""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='obsolete' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 已过期（当前 v{current_version}），"
                    "拒绝覆盖，需重新预览",
                )

    def cancel_session(self, token: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='cancelled' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(conn, "cancel", f"放弃修订会话 {token[:8]}")
                return cur.rowcount > 0
        return False

    # ---------- 预览差异 ----------
    def preview_diff(self, new_rows: list[dict]) -> dict:
        """与当前已发布版本比对，返回 增/改/删/不变 四类差异。"""
        old = {(r["sample_no"], r["analyte"]): r["concentration"]
               for r in self.current_rows()}
        new = {(r["sample_no"], r["analyte"]): r["concentration"] for r in new_rows}
        inserts, updates, deletes, unchanged = [], [], [], []
        for key, val in new.items():
            if key not in old:
                inserts.append({"sample_no": key[0], "analyte": key[1], "new": val})
            elif old[key] != val:
                updates.append(
                    {"sample_no": key[0], "analyte": key[1],
                     "old": old[key], "new": val}
                )
            else:
                unchanged.append({"sample_no": key[0], "analyte": key[1], "value": val})
        for key, val in old.items():
            if key not in new:
                deletes.append({"sample_no": key[0], "analyte": key[1], "old": val})
        inserts.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        updates.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        deletes.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        unchanged.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        return {
            "inserts": inserts, "updates": updates,
            "deletes": deletes, "unchanged": unchanged,
        }

    # ---------- 确认发布（核心事务） ----------
    def publish(self, token: str) -> tuple[int, int]:
        """确认整批替换。返回 (新版本号, 行数)。

        并发控制：BEGIN IMMEDIATE 立即拿写锁，事务内再次核对会话基线版本，
        若期间已有别人发布（current_version > base_version），本会话作废，
        抛出 PublishConflictError，调用方须重新预览。
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM revision_sessions WHERE token=?", (token,)
            ).fetchone()
            if session is None:
                raise PublishConflictError("修订会话不存在或已失效，请重新上传预览")
            if session["status"] != "pending":
                label = {"published": "已发布", "obsolete": "已过期",
                         "cancelled": "已取消"}.get(session["status"], session["status"])
                raise PublishConflictError(f"该修订会话{label}，请重新上传预览")

            reg = conn.execute("SELECT * FROM registry WHERE id=1").fetchone()
            current_version = reg["current_version"] if reg else 0
            if session["base_version"] != current_version:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete' WHERE token=?",
                    (token,),
                )
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 基于 v{session['base_version']} 发起，"
                    f"当前已为 v{current_version}，拒绝覆盖，需重新预览",
                )
                raise PublishConflictError(
                    f"版本已变化：您基于 v{session['base_version']} 预览，"
                    f"当前已是 v{current_version}，请重新预览后再发布"
                )

            rows = json.loads(session["payload"])
            new_version = current_version + 1
            csv_text = render_csv(rows)
            content_hash = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()

            # 先原子替换文件（旧文件在 replace 前始终完整可用）
            self._atomic_write(self.live_path, ".tmp-publish-", csv_text)

            # 再落库登记；若此后进程崩溃，重启 recover() 以登记为准重建/核对
            conn.executemany(
                "INSERT INTO results(version, batch_no, sample_no, analyte, concentration)"
                " VALUES (?,?,?,?,?)",
                [
                    (new_version, session["batch_no"], r["sample_no"],
                     r["analyte"], r["concentration"])
                    for r in rows
                ],
            )
            conn.execute(
                "INSERT INTO registry(id, current_version, batch_no, row_count,"
                " published_at, content_hash) VALUES (1,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET current_version=excluded.current_version,"
                " batch_no=excluded.batch_no, row_count=excluded.row_count,"
                " published_at=excluded.published_at, content_hash=excluded.content_hash",
                (new_version, session["batch_no"], len(rows), _now(), content_hash),
            )
            conn.execute(
                "UPDATE revision_sessions SET status='published' WHERE token=?",
                (token,),
            )
            self._log(
                conn, "publish",
                f"发布 v{new_version}：批次 {session['batch_no']}，{len(rows)} 行，"
                f"sha256={content_hash[:12]}…",
            )
            # 新批次发布后，基于旧版本的校正数据不得再作为当前数据供数：
            # 同事务标记失效并撤下下游校正文件（归档保留，可按原版本追溯）
            self._supersede_current_calibration(
                conn, f"发布 v{new_version} 后原校正数据已失效"
            )
            return new_version, len(rows)

    def _supersede_current_calibration(self, conn: sqlite3.Connection, reason: str) -> None:
        """在调用方事务内使当前生效校正失效，并撤下下游校正供数文件。"""
        cur = conn.execute(
            "UPDATE calibrations SET status='superseded' WHERE status='current'"
        )
        if cur.rowcount:
            try:
                os.unlink(self.calibrated_path)
            except FileNotFoundError:
                pass
            self._log(
                conn, "calibration",
                f"{reason}：校正供数文件已撤下，旧校正仅可按原版本追溯下载",
            )

    # ---------- 页面辅助 ----------
    @staticmethod
    def display_concentration(value: float) -> str:
        return format_concentration(value)

    # ---------- 标样集 ----------
    def current_standard_set(self):
        """当前标样集：{"set": 行, "points": [点...]}；从未录入时返回 None。"""
        with self._connect() as conn:
            s = conn.execute(
                "SELECT * FROM standard_sets ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if s is None:
                return None
            points = [
                dict(r)
                for r in conn.execute(
                    "SELECT analyte, x1, y1, x2, y2 FROM standard_points "
                    "WHERE set_id = ? ORDER BY analyte",
                    (s["id"],),
                ).fetchall()
            ]
            return {"set": dict(s), "points": points}

    @staticmethod
    def _points_map(points: list[dict]) -> dict:
        return {
            p["analyte"]: (p["x1"], p["y1"], p["x2"], p["y2"]) for p in points
        }

    def save_standards(self, points: list[dict]) -> tuple[int, bool]:
        """保存标样集。返回 (set_id, 是否新建版本)。

        内容与当前标样集完全一致时复用原版本（不影响已生效校正）；
        否则创建新标样集版本，并在同一事务内使当前生效校正失效、
        撤下下游校正供数文件（标样修改后旧校正不得作为当前数据下载）。
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM standard_sets ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if current is not None:
                old_points = [
                    dict(r)
                    for r in conn.execute(
                        "SELECT analyte, x1, y1, x2, y2 FROM standard_points "
                        "WHERE set_id = ?",
                        (current["id"],),
                    ).fetchall()
                ]
                if self._points_map(old_points) == self._points_map(points):
                    return current["id"], False

            cur = conn.execute(
                "INSERT INTO standard_sets(created_at, point_count) VALUES (?,?)",
                (_now(), len(points)),
            )
            set_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO standard_points(set_id, analyte, x1, y1, x2, y2)"
                " VALUES (?,?,?,?,?,?)",
                [
                    (set_id, p["analyte"], p["x1"], p["y1"], p["x2"], p["y2"])
                    for p in points
                ],
            )
            self._log(
                conn, "standards",
                f"保存标样集 #{set_id}：{len(points)} 个项目（"
                + "、".join(sorted(p["analyte"] for p in points)) + "）",
            )
            self._supersede_current_calibration(
                conn, f"标样集已更新为 #{set_id}，原校正数据已失效"
            )
            return set_id, True

    # ---------- 校正会话 ----------
    def create_calibration_session(
        self, publish_version: int, standards_id: int, rows: list[dict]
    ) -> str:
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO calibration_sessions(token, publish_version,"
                " standards_id, payload, created_at, status)"
                " VALUES (?,?,?,?,?,?)",
                (
                    token,
                    publish_version,
                    standards_id,
                    json.dumps(rows, ensure_ascii=False),
                    _now(),
                    "pending",
                ),
            )
        return token

    def get_calibration_session(self, token: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM calibration_sessions WHERE token = ?", (token,)
            ).fetchone()
            if row is None:
                return None
            data = dict(row)
            data["rows"] = json.loads(data["payload"])
            return data

    def mark_calibration_obsolete(self, token: str) -> None:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE calibration_sessions SET status='obsolete' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(
                    conn, "calibration_conflict",
                    f"校正会话 {token[:8]} 已过期（发布版本或标样集已变化），"
                    "拒绝固化混合状态，需重新换算预览",
                )

    def cancel_calibration_session(self, token: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE calibration_sessions SET status='cancelled' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(conn, "cancel", f"放弃校正会话 {token[:8]}")
                return cur.rowcount > 0
        return False

    # ---------- 校正记录 ----------
    def current_calibration(self):
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM calibrations WHERE status='current' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()

    def list_calibrations(self, limit: int = 50):
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM calibrations ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def _archive_path(self, cal) -> str:
        name = (
            f"calibrated_v{cal['source_version']}_s{cal['standards_id']}"
            f"_c{cal['id']}.csv"
        )
        return os.path.join(self.calibration_archive_dir, name)

    def _render_calibration(self, conn: sqlite3.Connection, cal) -> str:
        """按登记重算校正 CSV（源版本数据与标样集均不可变，结果确定）。"""
        rows = self._version_rows(conn, cal["source_version"])
        points = [
            dict(r)
            for r in conn.execute(
                "SELECT analyte, x1, y1, x2, y2 FROM standard_points WHERE set_id=?",
                (cal["standards_id"],),
            ).fetchall()
        ]
        cal_rows, errors = calibrate_rows(rows, points)
        if cal_rows is None:
            raise ValueError("按登记重算校正失败：" + "；".join(errors))
        batch_row = conn.execute(
            "SELECT batch_no FROM results WHERE version=? LIMIT 1",
            (cal["source_version"],),
        ).fetchone()
        return render_calibrated_csv(
            cal_rows,
            source_version=cal["source_version"],
            standards_id=cal["standards_id"],
            batch_no=batch_row["batch_no"] if batch_row else "",
            calibrated_at=cal["created_at"],
        )

    def calibration_archive_path(self, calib_id: int) -> str | None:
        """历史校正归档文件路径（缺失时按登记重算补写）；记录不存在返回 None。"""
        with self._connect() as conn:
            cal = conn.execute(
                "SELECT * FROM calibrations WHERE id=?", (calib_id,)
            ).fetchone()
            if cal is None:
                return None
            path = self._archive_path(cal)
            if not os.path.exists(path):
                csv_text = self._render_calibration(conn, cal)
                self._atomic_write(path, ".tmp-calib-archive-", csv_text)
                self._log(conn, "recover", f"校正 #{calib_id} 归档缺失，已按登记补写")
            return path

    # ---------- 确认校正（核心事务） ----------
    def confirm_calibration(self, token: str) -> tuple[int, int]:
        """确认生成校正 CSV。返回 (校正记录 id, 行数)。

        并发控制：BEGIN IMMEDIATE 写事务内复核「发布版本 + 标样集」与预览时
        完全一致；任一已变化（确认与发布、标样修改交错）则会话作废并抛出
        CalibrationConflictError，绝不固化混合状态。换算失败（理论上前置
        预览已拦截）时不写任何文件，上次完整校正结果原样保留。
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM calibration_sessions WHERE token=?", (token,)
            ).fetchone()
            if session is None:
                raise CalibrationConflictError("校正会话不存在或已失效，请重新换算预览")
            if session["status"] != "pending":
                label = {"confirmed": "已确认", "obsolete": "已过期",
                         "cancelled": "已取消"}.get(session["status"], session["status"])
                raise CalibrationConflictError(f"该校正会话{label}，请重新换算预览")

            reg = conn.execute("SELECT * FROM registry WHERE id=1").fetchone()
            current_version = reg["current_version"] if reg else 0
            std_row = conn.execute(
                "SELECT MAX(id) AS id FROM standard_sets"
            ).fetchone()
            current_standards = std_row["id"]

            stale_reasons = []
            if session["publish_version"] != current_version:
                stale_reasons.append(
                    f"发布版本已从 v{session['publish_version']} 变为 "
                    f"v{current_version}"
                )
            if session["standards_id"] != current_standards:
                stale_reasons.append(
                    f"标样集已从 #{session['standards_id']} 变为 "
                    f"#{current_standards}"
                )
            if stale_reasons:
                conn.execute(
                    "UPDATE calibration_sessions SET status='obsolete' WHERE token=?",
                    (token,),
                )
                reason = "；".join(stale_reasons)
                self._log(
                    conn, "calibration_conflict",
                    f"校正会话 {token[:8]} 确认被拒：{reason}，"
                    "拒绝固化混合状态，需重新换算预览",
                )
                raise CalibrationConflictError(reason + "，请重新换算预览后再确认")

            rows = json.loads(session["payload"])
            batch_row = conn.execute(
                "SELECT batch_no FROM results WHERE version=? LIMIT 1",
                (session["publish_version"],),
            ).fetchone()
            created_at = _now()
            csv_text = render_calibrated_csv(
                rows,
                source_version=session["publish_version"],
                standards_id=session["standards_id"],
                batch_no=batch_row["batch_no"] if batch_row else "",
                calibrated_at=created_at,
            )
            content_hash = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()

            cur = conn.execute(
                "INSERT INTO calibrations(source_version, standards_id, row_count,"
                " content_hash, created_at, status) VALUES (?,?,?,?,?,?)",
                (
                    session["publish_version"],
                    session["standards_id"],
                    len(rows),
                    content_hash,
                    created_at,
                    "current",
                ),
            )
            calib_id = cur.lastrowid

            # 先写归档与下游供数文件（均为原子替换），再提交登记；
            # 崩溃时事务回滚，游离文件由 recover() 隔离，旧校正不受影响
            archive_path = self._archive_path(
                {
                    "id": calib_id,
                    "source_version": session["publish_version"],
                    "standards_id": session["standards_id"],
                }
            )
            self._atomic_write(archive_path, ".tmp-calib-archive-", csv_text)
            self._atomic_write(self.calibrated_path, ".tmp-calibrate-", csv_text)

            conn.execute(
                "UPDATE calibrations SET status='superseded' "
                "WHERE status='current' AND id != ?",
                (calib_id,),
            )
            conn.execute(
                "UPDATE calibration_sessions SET status='confirmed' WHERE token=?",
                (token,),
            )
            self._log(
                conn, "calibration",
                f"校正 #{calib_id} 生效：源版本 v{session['publish_version']}，"
                f"标样集 #{session['standards_id']}，{len(rows)} 行，"
                f"sha256={content_hash[:12]}…",
            )
            return calib_id, len(rows)
