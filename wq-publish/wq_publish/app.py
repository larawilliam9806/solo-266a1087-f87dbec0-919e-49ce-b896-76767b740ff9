"""Flask 路由：上传校验 → 预览增删改 → 确认整批替换；标样录入 → 换算预览 → 确认校正。"""
from __future__ import annotations

import os
import secrets
import sqlite3

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from .calibration import calibrate_rows, linear_params, validate_standards_form
from .storage import (
    CALIBRATED_FILENAME,
    LIVE_FILENAME,
    CalibrationConflictError,
    PublishConflictError,
    Storage,
)
from .validation import format_concentration, validate_csv

MAX_UPLOAD_BYTES = 16 * 1024 * 1024


def create_app(data_dir: str | None = None, downstream_dir: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    data_dir = data_dir or os.environ.get("WQ_DATA_DIR") or os.path.join(base, "data")
    downstream_dir = (
        downstream_dir
        or os.environ.get("WQ_DOWNSTREAM_DIR")
        or os.path.join(base, "downstream")
    )

    secret_path = os.path.join(data_dir, "flask_secret")
    os.makedirs(data_dir, exist_ok=True)
    if os.path.exists(secret_path):
        with open(secret_path, "rb") as fh:
            app.secret_key = fh.read()
    else:
        key = secrets.token_bytes(32)
        with open(secret_path, "wb") as fh:
            fh.write(key)
        os.chmod(secret_path, 0o600)
        app.secret_key = key

    storage = Storage(data_dir, downstream_dir)
    app.storage = storage  # 供自测脚本直接访问

    @app.template_filter("conc")
    def _conc(value):
        return format_concentration(value)

    @app.get("/")
    def index():
        reg = storage.current_registry()
        rows = storage.current_rows()
        events = storage.recent_events()
        return render_template(
            "index.html", reg=reg, rows=rows, events=events,
            live_filename=LIVE_FILENAME,
            calibrated_filename=CALIBRATED_FILENAME,
            current_cal=storage.current_calibration(),
            standards=storage.current_standard_set(),
        )

    @app.post("/preview")
    def preview():
        upload = request.files.get("csv_file")
        batch_no = request.form.get("batch_no", "")
        if upload is None or not upload.filename:
            flash("请选择仪器导出的 CSV 文件", "error")
            return redirect(url_for("index"))
        raw = upload.read()
        if len(raw) > MAX_UPLOAD_BYTES:
            flash("CSV 文件超过 16MB 上限", "error")
            return redirect(url_for("index"))

        rows, errors = validate_csv(raw, batch_no)
        if errors:
            for msg in errors:
                flash(msg, "error")
            return redirect(url_for("index"))

        reg = storage.current_registry()
        base_version = reg["current_version"] if reg else 0
        token = storage.create_session(batch_no.strip(), base_version, rows)
        diff = storage.preview_diff(rows)
        storage.log(
            "preview",
            f"会话 {token[:8]} 基于 v{base_version} 预览批次 {batch_no.strip()}："
            f"新增 {len(diff['inserts'])} / 修改 {len(diff['updates'])} / "
            f"撤回 {len(diff['deletes'])} / 不变 {len(diff['unchanged'])}",
        )
        return render_template(
            "preview.html",
            token=token,
            batch_no=batch_no.strip(),
            base_version=base_version,
            current_version=base_version,
            rows=rows,
            diff=diff,
            live_filename=LIVE_FILENAME,
        )

    @app.post("/publish/<token>")
    def publish(token):
        session = storage.get_session(token)
        if session is None:
            flash("修订会话不存在或已过期，请重新上传 CSV", "error")
            return redirect(url_for("index"))

        reg = storage.current_registry()
        current_version = reg["current_version"] if reg else 0
        if session["status"] != "pending" or session["base_version"] != current_version:
            # 会话在预览期间被别的发布超越：作废会话，展示基于旧版的差异，要求重新预览
            if session["status"] == "pending":
                storage.mark_obsolete(token, current_version)
                stale_reason = (
                    f"您基于 v{session['base_version']} 发起修订，"
                    f"但当前版本已变为 v{current_version}（另一页面已成功发布）"
                )
            else:
                reason_map = {
                    "published": "该修订已经发布，不能重复发布",
                    "obsolete": "该预览已被更新的发布超越而作废",
                    "cancelled": "该修订已被放弃（取消）",
                }
                stale_reason = reason_map.get(session["status"], "该修订会话已失效")
            diff = storage.preview_diff(session["rows"])
            storage.log(
                "conflict",
                f"会话 {token[:8]} 发布被拒（基线 v{session['base_version']}，"
                f"当前 v{current_version}，状态 {session['status']}）",
            )
            return render_template(
                "preview.html",
                token=token,
                batch_no=session["batch_no"],
                base_version=session["base_version"],
                current_version=current_version,
                rows=session["rows"],
                diff=diff,
                stale=True,
                stale_reason=stale_reason,
                live_filename=LIVE_FILENAME,
            ), 409

        try:
            new_version, row_count = storage.publish(token)
        except PublishConflictError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))
        except sqlite3.OperationalError as exc:  # 极端并发下写锁等待超时
            app.logger.warning("publish lock contention: %s", exc)
            flash("当前有其他发布正在写入，请稍后重新预览再发布", "error")
            return redirect(url_for("index"))
        flash(f"发布成功：v{new_version} 已生效，共 {row_count} 行", "success")
        return redirect(url_for("index"))

    @app.post("/cancel/<token>")
    def cancel(token):
        if storage.cancel_session(token):
            flash("已放弃本次修订，当前发布版本未受影响", "info")
        return redirect(url_for("index"))

    @app.get("/published.csv")
    def published_csv():
        if not os.path.exists(storage.live_path):
            abort(404, description="尚无已发布的 CSV")
        return send_file(
            storage.live_path, mimetype="text/csv",
            download_name=LIVE_FILENAME, as_attachment=False,
        )

    # ---------- 标样校正 ----------
    @app.get("/calibration")
    def calibration():
        reg = storage.current_registry()
        standards = storage.current_standard_set()
        points_view = []
        if standards:
            for p in standards["points"]:
                k, b, err = linear_params(p)
                points_view.append({**p, "k": k, "b": b, "err": err})
        # 表单预填：已有标样按现值预填；?prefill=current 时按当前发布项目列空行
        form_rows = []
        if standards:
            form_rows = [
                {
                    "analyte": p["analyte"],
                    "x1": format_concentration(p["x1"]),
                    "y1": format_concentration(p["y1"]),
                    "x2": format_concentration(p["x2"]),
                    "y2": format_concentration(p["y2"]),
                }
                for p in standards["points"]
            ]
        elif request.args.get("prefill") == "current" and reg:
            seen = sorted({r["analyte"] for r in storage.current_rows()})
            form_rows = [
                {"analyte": a, "x1": "", "y1": "", "x2": "", "y2": ""} for a in seen
            ]
        if not form_rows:
            form_rows = [{"analyte": "", "x1": "", "y1": "", "x2": "", "y2": ""}]
        return render_template(
            "calibration.html",
            reg=reg,
            standards=standards,
            points_view=points_view,
            form_rows=form_rows,
            current_cal=storage.current_calibration(),
            history=storage.list_calibrations(),
            calibrated_filename=CALIBRATED_FILENAME,
        )

    @app.post("/calibration/standards")
    def save_standards():
        points, errors = validate_standards_form(
            request.form.getlist("analyte"),
            request.form.getlist("x1"),
            request.form.getlist("y1"),
            request.form.getlist("x2"),
            request.form.getlist("y2"),
        )
        if errors:
            for msg in errors:
                flash(msg, "error")
            return redirect(url_for("calibration"))
        set_id, created = storage.save_standards(points)
        if created:
            flash(
                f"标样集 #{set_id} 已保存（{len(points)} 个项目）。"
                "原校正数据已失效，请重新换算并确认后再供数",
                "success",
            )
        else:
            flash(f"标样内容与当前标样集 #{set_id} 完全一致，未产生新版本", "info")
        return redirect(url_for("calibration"))

    @app.post("/calibration/preview")
    def calibration_preview():
        reg = storage.current_registry()
        if reg is None:
            flash("尚未发布任何批次，无法换算校正数据", "error")
            return redirect(url_for("calibration"))
        standards = storage.current_standard_set()
        if standards is None:
            flash("尚未录入标样，请先在下方录入各检测项目的两组标样读数", "error")
            return redirect(url_for("calibration"))

        rows = storage.current_rows()
        cal_rows, errors = calibrate_rows(rows, standards["points"])
        if cal_rows is None:
            storage.log(
                "calibration",
                f"换算失败（发布 v{reg['current_version']} + 标样集 "
                f"#{standards['set']['id']}）：整版校正数据不可供数，"
                f"共 {len(errors)} 项原因",
            )
            return render_template(
                "calibration_preview.html",
                mode="failed",
                errors=errors,
                source_version=reg["current_version"],
                standards_id=standards["set"]["id"],
            )

        token = storage.create_calibration_session(
            reg["current_version"], standards["set"]["id"], cal_rows
        )
        storage.log(
            "calibration",
            f"校正会话 {token[:8]} 基于发布 v{reg['current_version']} + "
            f"标样集 #{standards['set']['id']} 预览换算，{len(cal_rows)} 行",
        )
        return render_template(
            "calibration_preview.html",
            mode="ok",
            token=token,
            rows=cal_rows,
            source_version=reg["current_version"],
            standards_id=standards["set"]["id"],
            batch_no=reg["batch_no"],
        )

    @app.post("/calibration/confirm/<token>")
    def calibration_confirm(token):
        session = storage.get_calibration_session(token)
        if session is None:
            flash("校正会话不存在或已过期，请重新换算预览", "error")
            return redirect(url_for("calibration"))

        reg = storage.current_registry()
        current_version = reg["current_version"] if reg else 0
        standards = storage.current_standard_set()
        current_standards = standards["set"]["id"] if standards else None
        stale_reasons = []
        if session["status"] == "pending":
            if session["publish_version"] != current_version:
                stale_reasons.append(
                    f"发布版本已从 v{session['publish_version']} 变为 v{current_version}"
                    "（期间有新批次发布）"
                )
            if session["standards_id"] != current_standards:
                stale_reasons.append(
                    f"标样集已从 #{session['standards_id']} 变为 #{current_standards}"
                    "（期间标样被修改）"
                )
        if session["status"] != "pending" or stale_reasons:
            if session["status"] == "pending":
                storage.mark_calibration_obsolete(token)
            else:
                reason_map = {
                    "confirmed": "该校正预览已经确认，不能重复确认",
                    "obsolete": "该校正预览已被更新的发布/标样超越而作废",
                    "cancelled": "该校正预览已被放弃（取消）",
                }
                stale_reasons.append(
                    reason_map.get(session["status"], "该校正会话已失效")
                )
            return render_template(
                "calibration_preview.html",
                mode="stale",
                stale_reason="；".join(stale_reasons),
                source_version=session["publish_version"],
                standards_id=session["standards_id"],
            ), 409

        try:
            calib_id, row_count = storage.confirm_calibration(token)
        except CalibrationConflictError as exc:
            # 事务内复核仍可能遇到并发变化：同样拒绝，绝不固化混合状态
            return render_template(
                "calibration_preview.html",
                mode="stale",
                stale_reason=str(exc),
                source_version=session["publish_version"],
                standards_id=session["standards_id"],
            ), 409
        except sqlite3.OperationalError as exc:  # 极端并发下写锁等待超时
            app.logger.warning("calibration lock contention: %s", exc)
            flash("当前有其他写入正在进行，请稍后重新换算预览再确认", "error")
            return redirect(url_for("calibration"))
        flash(
            f"校正数据已生效：校正 #{calib_id}（源版本 "
            f"v{session['publish_version']} + 标样集 #{session['standards_id']}），"
            f"共 {row_count} 行，校正 CSV 已生成",
            "success",
        )
        return redirect(url_for("calibration"))

    @app.post("/calibration/cancel/<token>")
    def calibration_cancel(token):
        if storage.cancel_calibration_session(token):
            flash("已放弃本次校正预览，未生成任何校正数据", "info")
        return redirect(url_for("calibration"))

    @app.get("/calibrated.csv")
    def calibrated_csv():
        cal = storage.current_calibration()
        if cal is None or not os.path.exists(storage.calibrated_path):
            abort(
                404,
                description=(
                    "当前无生效的校正数据（尚未确认，或已因新批次发布/标样修改"
                    "而失效），请先到校正页重新换算并确认"
                ),
            )
        return send_file(
            storage.calibrated_path, mimetype="text/csv",
            download_name=CALIBRATED_FILENAME, as_attachment=False,
        )

    @app.get("/calibrations/<int:cid>/download")
    def calibration_download(cid):
        path = storage.calibration_archive_path(cid)
        if path is None:
            abort(404, description="校正记录不存在")
        return send_file(
            path, mimetype="text/csv",
            download_name=os.path.basename(path), as_attachment=True,
        )

    @app.errorhandler(413)
    def too_large(_):
        flash("上传文件超过 16MB 上限", "error")
        return redirect(url_for("index"))

    return app
