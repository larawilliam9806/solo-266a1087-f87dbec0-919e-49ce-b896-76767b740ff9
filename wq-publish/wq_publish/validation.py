"""仪器导出 CSV 的解析与业务校验。

校验规则：
1. 必须包含样本号、检测项目、浓度三列（兼容中英文表头）；
2. 同一样本同一检测项目不得重复；
3. 浓度必须是数字且为非负数（拒绝 NaN/Inf）。
"""
from __future__ import annotations

import csv
import io
import math

CANONICAL_HEADER = ["sample_no", "analyte", "concentration"]

# 兼容仪器导出的中英文表头（全部转小写后匹配）
_HEADER_ALIASES = {
    "样本号": "sample_no",
    "样本编号": "sample_no",
    "sample_no": "sample_no",
    "sampleno": "sample_no",
    "sample": "sample_no",
    "检测项目": "analyte",
    "项目": "analyte",
    "analyte": "analyte",
    "item": "analyte",
    "浓度": "concentration",
    "浓度(mg/l)": "concentration",
    "浓度（mg/l）": "concentration",
    "concentration": "concentration",
    "conc": "concentration",
}

_MAX_ERRORS = 50


def validate_csv(raw_bytes: bytes, batch_no: str):
    """校验上传内容。

    返回 (rows, errors)：成功时 rows 为 dict 列表、errors 为空列表；
    失败时 rows 为 None、errors 为面向数据员的中文错误信息（含行号）。
    """
    errors: list[str] = []
    batch_no = (batch_no or "").strip()
    if not batch_no:
        errors.append("批次号不能为空")
    elif len(batch_no) > 64 or any(ch in batch_no for ch in '/\\:*?"<>|'):
        errors.append("批次号长度需在 1-64 之间，且不能包含 / \\ : * ? \" < > | 字符")

    try:
        text = raw_bytes.decode("utf-8-sig")  # 兼容带 BOM 的仪器导出文件
    except UnicodeDecodeError:
        return None, errors + ["CSV 文件必须为 UTF-8 编码"]
    if not text.strip():
        return None, errors + ["CSV 文件内容为空"]

    reader = csv.reader(io.StringIO(text))
    try:
        table = list(reader)
    except csv.Error as exc:
        return None, errors + [f"CSV 解析失败：{exc}"]
    if not table:
        return None, errors + ["CSV 文件内容为空"]

    header = [cell.strip().lower() for cell in table[0]]
    col_index: dict[str, int] = {}
    for idx, name in enumerate(header):
        canon = _HEADER_ALIASES.get(name)
        if canon and canon not in col_index:
            col_index[canon] = idx
    for col in CANONICAL_HEADER:
        if col not in col_index:
            errors.append(
                "表头缺少必要列：样本号 / 检测项目 / 浓度"
                f"（当前表头：{', '.join(table[0])}）"
            )
            break
    if errors:
        return None, errors

    seen: dict[tuple[str, str], int] = {}
    rows: list[dict] = []
    for line_no, record in enumerate(table[1:], start=2):
        if not any(cell.strip() for cell in record):
            continue  # 跳过整行空白
        if len(record) != len(header):
            errors.append(f"第 {line_no} 行：列数为 {len(record)}，应为 {len(header)}")
            if len(errors) >= _MAX_ERRORS:
                break
            continue

        sample_no = record[col_index["sample_no"]].strip()
        analyte = record[col_index["analyte"]].strip()
        conc_text = record[col_index["concentration"]].strip()
        row_ok = True

        if not sample_no:
            errors.append(f"第 {line_no} 行：样本号为空")
            row_ok = False
        if not analyte:
            errors.append(f"第 {line_no} 行：检测项目为空")
            row_ok = False
        if not conc_text:
            errors.append(f"第 {line_no} 行：浓度为空")
            row_ok = False

        value = None
        if conc_text:
            try:
                value = float(conc_text)
            except ValueError:
                errors.append(f"第 {line_no} 行：浓度“{conc_text}”不是合法数字")
                row_ok = False
            else:
                if math.isnan(value) or math.isinf(value):
                    errors.append(f"第 {line_no} 行：浓度不能为 NaN 或无穷大")
                    row_ok = False
                elif value < 0:
                    errors.append(f"第 {line_no} 行：浓度 {value} 为负数，必须 ≥ 0")
                    row_ok = False

        key = (sample_no, analyte)
        if sample_no and analyte:
            if key in seen:
                errors.append(
                    f"第 {line_no} 行：样本 {sample_no} 的项目 {analyte} "
                    f"与第 {seen[key]} 行重复，同一样本同一项目不得重复"
                )
                row_ok = False
            else:
                seen[key] = line_no

        if row_ok:
            rows.append(
                {"sample_no": sample_no, "analyte": analyte, "concentration": value}
            )
        if len(errors) >= _MAX_ERRORS:
            errors.append(f"错误过多，仅展示前 {_MAX_ERRORS} 条，请修正后重新上传")
            break

    if errors:
        return None, errors
    if not rows:
        return None, ["CSV 中没有任何有效数据行"]
    return rows, []


def format_concentration(value: float) -> str:
    """规范浓度文本：float 的最短往返表示，保证重新解析结果一致。"""
    return repr(float(value))


def render_csv(rows: list[dict]) -> str:
    """把校验通过的行渲染为规范 CSV（按样本号、项目排序，输出稳定）。"""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(CANONICAL_HEADER)
    for row in sorted(rows, key=lambda r: (r["sample_no"], r["analyte"])):
        writer.writerow(
            [
                row["sample_no"],
                row["analyte"],
                format_concentration(row["concentration"]),
            ]
        )
    return buf.getvalue()
