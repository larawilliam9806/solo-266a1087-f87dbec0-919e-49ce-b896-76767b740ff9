"""关键流程自测（不依赖 pytest，直接 python -I tests/selftest.py 运行）。

覆盖：
1. CSV 校验：中文表头、重复 (样本,项目)、负数/非数字/空值/缺列/缺批次号
2. 端到端：上传 → 预览(增/改/删/不变分类) → 发布 → 登记版本/行数 → 下游文件哈希一致
3. 并发：两个页面从同一版本发起，仅一个发布成功，另一个 409 须重新预览
4. 崩溃恢复：文件缺失、文件被改坏、遗留临时文件、游离文件、改名后未提交
5. 撤回语义：整批替换后旧行消失；会话取消/重复发布防护
6. 标样校正：录入校验、两点线性换算、缺标样/斜率非正/负值/非有限整版失败、
   确认与发布/标样修改交错拒绝固化混合状态、失效校正追溯、校正文件崩溃恢复
"""
import hashlib
import io
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wq_publish.app import create_app
from wq_publish.storage import CALIBRATED_FILENAME, LIVE_FILENAME
from wq_publish.validation import render_csv, validate_csv

PASS, FAIL = 0, 0
EXAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "examples")


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def read_example(fname):
    with open(os.path.join(EXAMPLE_DIR, fname), "rb") as fh:
        return fh.read()


def upload(client, fname, batch_no="B1"):
    return client.post(
        "/preview",
        data={"batch_no": batch_no,
              "csv_file": (io.BytesIO(read_example(fname)), fname)},
        content_type="multipart/form-data",
    )


# ---------------- 1. 校验单元测试 ----------------
print("== 1. CSV 校验 ==")
rows, errs = validate_csv(read_example("batch_v1.csv"), "B1")
check("合法 CSV 通过并解析出 5 行", errs == [] and len(rows) == 5, str(errs))

rows, errs = validate_csv(read_example("batch_invalid.csv"), "B1")
check("重复(样本,项目)被拒", any("重复" in e for e in errs), str(errs))
check("负浓度被拒", any("负数" in e for e in errs), str(errs))
check("空浓度被拒", any("浓度为空" in e for e in errs), str(errs))
check("空项目被拒", any("检测项目为空" in e for e in errs), str(errs))

bad = "样本号,浓度\nS1,1.0\n".encode()
rows, errs = validate_csv(bad, "B1")
check("缺少检测项目列被拒", rows is None and any("表头" in e for e in errs), str(errs))

rows, errs = validate_csv(read_example("batch_v1.csv"), " ")
check("空批次号被拒", any("批次号" in e for e in errs), str(errs))

for text in ["S1,氨氮,nan", "S1,氨氮,inf", "S1,氨氮,abc"]:
    raw = f"样本号,检测项目,浓度\n{text}\n".encode()
    rows, errs = validate_csv(raw, "B1")
    check(f"非法浓度 {text!r} 被拒", rows is None and len(errs) > 0, str(errs))

raw_zero = "样本号,检测项目,浓度\nS1,氨氮,0\n".encode()
rows, errs = validate_csv(raw_zero, "B1")
check("零浓度（非负边界）通过", errs == [] and rows[0]["concentration"] == 0.0, str(errs))

# ---------------- 2-5. 端到端（隔离临时目录） ----------------
tmp = tempfile.mkdtemp(prefix="wq-selftest-")
data_dir = os.path.join(tmp, "data")
down_dir = os.path.join(tmp, "downstream")
app = create_app(data_dir=data_dir, downstream_dir=down_dir)
client = app.test_client()

print("== 2. 首次发布 v1 ==")
resp = upload(client, "batch_v1.csv", "BATCH-001")
check("预览页 200 且显示增删改", resp.status_code == 200
      and "新增".encode() in resp.data and "撤回".encode() in resp.data)
import re
m = re.search(rb'/publish/([0-9a-f]+)', resp.data)
check("预览页含发布令牌", m is not None)
token1 = m.group(1).decode()

resp = client.post(f"/publish/{token1}")
check("确认发布后 302 回首页", resp.status_code == 302)
reg = app.storage.current_registry()
check("登记为 v1、5 行", reg["current_version"] == 1 and reg["row_count"] == 5,
      f"got {dict(reg) if reg else None}")
live = os.path.join(down_dir, LIVE_FILENAME)
check("下游 CSV 已生成", os.path.exists(live))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("文件哈希与登记一致", file_hash == reg["content_hash"])
check("下游目录无临时文件残留",
      not [n for n in os.listdir(down_dir) if n.startswith(".tmp-")])
resp = client.get("/published.csv")
check("已发布 CSV 可下载", resp.status_code == 200 and b"S001" in resp.data)
resp = client.post(f"/publish/{token1}")
check("同一会话不可重复发布（返回 409 过期页）", resp.status_code == 409
      and app.storage.get_session(token1)["status"] == "published")

print("== 3. 修订预览的增/改/删/不变分类 ==")
resp = upload(client, "batch_v2_revision.csv", "BATCH-002")
body = resp.data.decode()
# v2 相对 v1：S004氨氮新增；S001氨氮、S002COD 修改；S002氨氮 撤回；S001总磷/S003总磷 不变
check("新增 1 行", "+1" in body and "S004" in body)
check("修改 2 行", "~2" in body and "1.25" in body and "35.0" in body)
check("撤回 1 行（S002 氨氮）", "−1" in body and "S002" in body)
check("不变 2 行", "不变项 2" in body)
token2 = re.search(rb'/publish/([0-9a-f]+)', resp.data).group(1).decode()
client.post(f"/publish/{token2}")
reg = app.storage.current_registry()
check("发布后登记为 v2、5 行", reg["current_version"] == 2 and reg["row_count"] == 5)
current = app.storage.current_rows()
check("撤回生效：S002 氨氮已不存在",
      not any(r["sample_no"] == "S002" and r["analyte"] == "氨氮" for r in current))
check("修改生效：S001 氨氮=1.3",
      any(r["sample_no"] == "S001" and r["analyte"] == "氨氮"
          and r["concentration"] == 1.3 for r in current))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("v2 文件哈希与登记一致", file_hash == reg["content_hash"])

print("== 4. 同版本并发修订：只允许一个成功 ==")
r_a = upload(client, "batch_v1.csv", "CONCUR-A")
r_b = upload(client, "batch_v2_revision.csv", "CONCUR-B")
tok_a = re.search(rb'/publish/([0-9a-f]+)', r_a.data).group(1).decode()
tok_b = re.search(rb'/publish/([0-9a-f]+)', r_b.data).group(1).decode()
ok = client.post(f"/publish/{tok_a}")
check("A 发布成功 → v3", ok.status_code == 302
      and app.storage.current_registry()["current_version"] == 3)
conflict = client.post(f"/publish/{tok_b}")
check("B 发布被拒并返回 409 过期预览页", conflict.status_code == 409
      and "该预览已过期".encode() in conflict.data)
sess_b = app.storage.get_session(tok_b)
check("B 会话被标记 obsolete", sess_b["status"] == "obsolete")
reg_before = app.storage.current_registry()["current_version"]
retry = upload(client, "batch_v2_revision.csv", "CONCUR-B-RETRY")
tok_b2 = re.search(rb'/publish/([0-9a-f]+)', retry.data).group(1).decode()
client.post(f"/publish/{tok_b2}")
check("B 重新预览后可发布 → v4",
      app.storage.current_registry()["current_version"] == reg_before + 1)
check("A 的发布未被覆盖：v4 文件行数=5",
      app.storage.current_registry()["row_count"] == 5)

print("== 5. 崩溃后重启自动恢复 ==")
# 5a 下游文件被删除
os.unlink(live)
app2 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app2.storage.current_registry()
check("5a 文件缺失后重建且哈希与登记一致",
      os.path.exists(live)
      and hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5b 文件被外部改坏
with open(live, "ab") as fh:
    fh.write(b"CORRUPTED\n")
app3 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app3.storage.current_registry()
check("5b 损坏文件被按登记重写",
      hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5c 遗留半写临时文件
tmp_orphan = os.path.join(down_dir, ".tmp-publish-deadbeef.csv")
open(tmp_orphan, "wb").write(b"partial")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5c 遗留临时文件被清理", not os.path.exists(tmp_orphan))

# 5d 下游目录出现未登记游离文件
stranger = os.path.join(down_dir, "stranger.csv")
open(stranger, "wb").write(b"x")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5d 游离文件被移入 quarantine",
      not os.path.exists(stranger)
      and any(f.endswith("stranger.csv")
              for f in os.listdir(os.path.join(data_dir, "quarantine"))))

# 5e 模拟“文件已改名但事务未提交”：文件内容比登记新，重启应回滚到登记版本
with app3.storage._connect() as conn:
    old_csv = app3.storage._render_version(conn, reg["current_version"])
newer = render_csv([{"sample_no": "ZX", "analyte": "氨氮", "concentration": 9.9}])
app3.storage._atomic_write(app3.storage.live_path, ".tmp-publish-", newer)
check("预置：文件已被换成未登记的新版本",
      open(live, "rb").read().decode() != old_csv)
app4 = create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5e 改名后未提交 → 恢复为登记的完整旧版",
      open(live, "rb").read().decode() == old_csv)
events = app4.storage.recent_events(50)
check("恢复动作写入事件日志", any(e["kind"] == "recover" for e in events))

print("== 6. 会话取消 ==")
r = upload(client, "batch_v1.csv", "CANCEL-ME")
tok = re.search(rb'/publish/([0-9a-f]+)', r.data).group(1).decode()
v_before = app.storage.current_registry()["current_version"]
client.post(f"/cancel/{tok}")
check("取消后发布被拒绝（409 且会话 cancelled）",
      client.post(f"/publish/{tok}").status_code == 409
      and app.storage.get_session(tok)["status"] == "cancelled")
check("取消不影响当前版本",
      app.storage.current_registry()["current_version"] == v_before)

# ---------------- 7-13. 标样校正 ----------------
calibrated_live = os.path.join(down_dir, CALIBRATED_FILENAME)
archive_dir = os.path.join(data_dir, "calibration_archive")


def post_standards(rows, follow=True):
    """rows: [(analyte, x1, y1, x2, y2), ...]（字符串原样提交）"""
    data = {"analyte": [], "x1": [], "y1": [], "x2": [], "y2": []}
    for a, x1, y1, x2, y2 in rows:
        data["analyte"].append(a)
        data["x1"].append(x1)
        data["y1"].append(y1)
        data["x2"].append(x2)
        data["y2"].append(y2)
    return client.post("/calibration/standards", data=data,
                       follow_redirects=follow)


def publish_file(fname, batch_no):
    r = upload(client, fname, batch_no)
    tok = re.search(rb'/publish/([0-9a-f]+)', r.data).group(1).decode()
    return client.post(f"/publish/{tok}")


def cal_preview_token(resp):
    m = re.search(rb'/calibration/confirm/([0-9a-f]+)', resp.data)
    return m.group(1).decode() if m else None


def current_cal_hash():
    cal = app.storage.current_calibration()
    return (cal["id"], cal["content_hash"]) if cal else (None, None)


# 当前状态：发布 v4（batch_v2_revision 内容），项目：氨氮/总磷/COD
print("== 7. 标样录入校验 ==")
resp = post_standards([("氨氮", "1", "1.1", "1", "2.1")])
check("两组读数相同被拒", "无法确定" in resp.data.decode() or "相同" in resp.data.decode())
resp = post_standards([("氨氮", "-1", "1.1", "2", "2.1")])
check("负读数被拒", "负数" in resp.data.decode() or "≥ 0" in resp.data.decode())
resp = post_standards([("氨氮", "nan", "1.1", "2", "2.1")])
check("NaN 读数被拒", "有限" in resp.data.decode())
resp = post_standards([("氨氮", "abc", "1.1", "2", "2.1")])
check("非数字读数被拒", "不是合法数字" in resp.data.decode())
resp = post_standards([("氨氮", "0", "0.1", "1", "1.1"),
                       ("氨氮", "0", "0.2", "1", "1.2")])
check("重复项目被拒", "重复" in resp.data.decode())
resp = post_standards([("", "", "", "", "")])
check("空表单被拒", "至少需要录入一个" in resp.data.decode())
check("非法录入均未产生标样集", app.storage.current_standard_set() is None)

# 合法标样：三个项目 k=2, b=0
GOOD_STANDARDS = [("氨氮", "0", "0", "1", "2"),
                  ("总磷", "0", "0", "0.5", "1"),
                  ("COD", "0", "0", "10", "20")]
resp = post_standards(GOOD_STANDARDS)
check("合法标样保存成功", "标样集 #1 已保存" in resp.data.decode(), resp.data.decode())
check("当前标样集为 #1", app.storage.current_standard_set()["set"]["id"] == 1)
resp = post_standards(GOOD_STANDARDS)
check("相同内容复用原版本（不产生新版本）",
      "完全一致" in resp.data.decode()
      and app.storage.current_standard_set()["set"]["id"] == 1)

print("== 8. 换算预览 → 确认 → 校正 CSV 供数 ==")
check("确认前无生效校正，/calibrated.csv 404",
      client.get("/calibrated.csv").status_code == 404)
resp = client.post("/calibration/preview")
body = resp.data.decode()
check("换算预览成功且含确认令牌", resp.status_code == 200
      and "校正预览" in body and cal_preview_token(resp) is not None)
check("预览含换算值（S001 氨氮 1.3→2.6，S002 COD 32.5→65.0）",
      "2.6" in body and "65.0" in body)
check("预览未确认前仍未生成校正文件", not os.path.exists(calibrated_live)
      and client.get("/calibrated.csv").status_code == 404)
ctok1 = cal_preview_token(resp)
resp = client.post(f"/calibration/confirm/{ctok1}")
check("确认后 302 回校正页", resp.status_code == 302)
cal_id, cal_hash = current_cal_hash()
check("校正 #1 生效", cal_id == 1)
check("下游校正 CSV 已生成且哈希与登记一致",
      os.path.exists(calibrated_live)
      and hashlib.sha256(open(calibrated_live, "rb").read()).hexdigest() == cal_hash)
resp = client.get("/calibrated.csv")
check("当前校正 CSV 可下载", resp.status_code == 200)
check("校正 CSV 注释头记录源版本与标样集",
      b"source_version=4" in resp.data and b"standards_set=1" in resp.data)
check("校正 CSV 含原始读数与校正浓度",
      b"raw_reading" in resp.data and b"2.6" in resp.data and b"65.0" in resp.data)
check("归档文件已生成",
      any(f"calibrated_v4_s1_c1" in n for n in os.listdir(archive_dir)))
check("同一会话重复确认被拒（409）",
      client.post(f"/calibration/confirm/{ctok1}").status_code == 409)
reg = app.storage.current_registry()
check("原始 CSV 供数不受影响",
      client.get("/published.csv").status_code == 200
      and hashlib.sha256(open(os.path.join(down_dir, LIVE_FILENAME), "rb").read()).hexdigest()
      == reg["content_hash"])

print("== 9. 交错：预览后先发布新批次，确认被拒且不固化混合状态 ==")
resp = client.post("/calibration/preview")
ctok2 = cal_preview_token(resp)
check("基于 v4+#1 的校正预览生成", ctok2 is not None)
publish_file("batch_v1.csv", "BATCH-101")
check("新批次发布为 v5", app.storage.current_registry()["current_version"] == 5)
resp = client.post(f"/calibration/confirm/{ctok2}")
check("确认被拒（409 过期页，提示不得固化混合状态）",
      resp.status_code == 409 and "不得固化混合状态" in resp.data.decode())
check("过期会话标记 obsolete",
      app.storage.get_calibration_session(ctok2)["status"] == "obsolete")
check("发布后旧校正已失效，当前无生效校正",
      app.storage.current_calibration() is None)
check("旧校正文件已撤下，不作为当前数据下载",
      not os.path.exists(calibrated_live)
      and client.get("/calibrated.csv").status_code == 404)
resp = client.get("/calibrations/1/download")
check("旧校正可按原版本追溯下载（source_version=4）",
      resp.status_code == 200 and b"source_version=4" in resp.data)

print("== 10. 交错：预览后标样被修改，确认被拒 ==")
resp = client.post("/calibration/preview")
ctok3 = cal_preview_token(resp)
check("基于 v5+#1 的校正预览生成", ctok3 is not None)
resp = post_standards([("氨氮", "0", "0", "1", "2"), ("总磷", "0", "0", "0.5", "1")])
check("标样修改保存为 #2（去掉 COD 项目）",
      app.storage.current_standard_set()["set"]["id"] == 2)
resp = client.post(f"/calibration/confirm/{ctok3}")
check("确认被拒（409，标样已变化）",
      resp.status_code == 409 and "标样" in resp.data.decode())
check("仍无生效校正", app.storage.current_calibration() is None)

print("== 11. 换算失败：整版不可供数并显示原因 ==")
resp = client.post("/calibration/preview")
body = resp.data.decode()
check("缺少项目标样 → 整版失败并显示原因",
      "整版校正数据不可供数" in body and "缺少标样" in body and "COD" in body)
check("失败预览不提供确认令牌", cal_preview_token(resp) is None)
check("失败后不产生校正文件", not os.path.exists(calibrated_live)
      and client.get("/calibrated.csv").status_code == 404)

resp = post_standards([("氨氮", "0", "0", "1", "2"),
                       ("总磷", "0", "0", "0.5", "1"),
                       ("COD", "10", "20", "20", "10")])  # k=-1
resp = client.post("/calibration/preview")
check("斜率非正 → 整版失败并显示原因", "斜率" in resp.data.decode()
      and "整版校正数据不可供数" in resp.data.decode())

resp = post_standards([("氨氮", "5", "0", "10", "1"),   # k=0.2, b=-1 → 负值
                       ("总磷", "0", "0", "0.5", "1"),
                       ("COD", "0", "0", "10", "20")])
resp = client.post("/calibration/preview")
body = resp.data.decode()
check("换算得负值 → 整版失败并显示原因",
      "负值" in body and "整版校正数据不可供数" in body, body[:2000])

resp = post_standards([("氨氮", "0", "0", "1e-300", "1e300"),  # k 溢出 inf
                       ("总磷", "0", "0", "0.5", "1"),
                       ("COD", "0", "0", "10", "20")])
resp = client.post("/calibration/preview")
check("斜率非有限 → 整版失败并显示原因", "非有限" in resp.data.decode())

check("多次失败均未产生校正文件", not os.path.exists(calibrated_live))
resp = client.get("/calibrations/1/download")
check("上次完整校正结果（#1）仍完整可追溯",
      resp.status_code == 200 and b"source_version=4" in resp.data)

print("== 12. 重新换算确认 + 校正文件崩溃恢复 ==")
resp = post_standards(GOOD_STANDARDS)  # 内容同 #1，但相对当前 #5 已变化 → 新版本
sid = app.storage.current_standard_set()["set"]["id"]
check("标样恢复为合法集（新版本）", sid == 6, f"got #{sid}")
resp = client.post("/calibration/preview")
ctok4 = cal_preview_token(resp)
resp = client.post(f"/calibration/confirm/{ctok4}")
check("校正 #2 确认生效", resp.status_code == 302
      and app.storage.current_calibration()["id"] == 2
      and app.storage.current_calibration()["source_version"] == 5)
resp = client.get("/calibrated.csv")
check("新校正 CSV 记录源版本 v5 与标样集 #6",
      b"source_version=5" in resp.data and f"standards_set={sid}".encode() in resp.data)
check("校正 #1 已失效但可追溯",
      b"source_version=4" in client.get("/calibrations/1/download").data)

# 12a 校正文件被改坏 → 重启按登记重建
with open(calibrated_live, "ab") as fh:
    fh.write(b"CORRUPTED\n")
app_c1 = create_app(data_dir=data_dir, downstream_dir=down_dir)
cal = app_c1.storage.current_calibration()
check("12a 损坏校正文件按登记重建",
      hashlib.sha256(open(calibrated_live, "rb").read()).hexdigest()
      == cal["content_hash"])

# 12b 校正文件被删除 → 重启重建
os.unlink(calibrated_live)
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("12b 缺失校正文件按登记重建", os.path.exists(calibrated_live))

# 12c 归档文件被删除 → 重启补写
os.unlink(os.path.join(archive_dir, f"calibrated_v5_s{sid}_c2.csv"))
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("12c 缺失归档按登记补写",
      os.path.exists(os.path.join(archive_dir, f"calibrated_v5_s{sid}_c2.csv")))

# 12d 模拟“文件已写但登记未提交”：DB 无生效校正而文件存在 → 隔离不供数
with app.storage._connect() as conn:
    conn.execute("UPDATE calibrations SET status='superseded' WHERE status='current'")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("12d 无生效登记时校正文件被隔离（不作为当前数据）",
      not os.path.exists(calibrated_live)
      and any(f.endswith(CALIBRATED_FILENAME)
              for f in os.listdir(os.path.join(data_dir, "quarantine"))))

# 12e 归档目录游离文件 → 隔离
stranger_arc = os.path.join(archive_dir, "stranger-calib.csv")
open(stranger_arc, "wb").write(b"x")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("12e 归档目录游离文件被隔离", not os.path.exists(stranger_arc))

reg = app.storage.current_registry()
check("原始发布 CSV 全程不受影响",
      client.get("/published.csv").status_code == 200
      and hashlib.sha256(open(os.path.join(down_dir, LIVE_FILENAME), "rb").read()).hexdigest()
      == reg["content_hash"])

print("== 13. 校正会话取消 ==")
resp = client.post("/calibration/preview")
ctok5 = cal_preview_token(resp)
client.post(f"/calibration/cancel/{ctok5}")
check("取消后确认被拒（409 且会话 cancelled）",
      client.post(f"/calibration/confirm/{ctok5}").status_code == 409
      and app.storage.get_calibration_session(ctok5)["status"] == "cancelled")
check("取消后不生成校正文件", not os.path.exists(calibrated_live))

shutil.rmtree(tmp, ignore_errors=True)
print(f"\n结果：{PASS} 通过，{FAIL} 失败")
sys.exit(1 if FAIL else 0)
