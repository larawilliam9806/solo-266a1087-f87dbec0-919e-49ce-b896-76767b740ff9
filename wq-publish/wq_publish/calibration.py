"""标样（校正点）解析校验与两点线性校正换算。

录入规则（保存标样集时校验，任一失败则整组拒绝保存）：
1. 每个检测项目一行：项目名 + 两组「仪器读数 / 真实浓度」；
2. 四个数值均须为有限数字且 ≥ 0（拒绝 NaN/Inf/负数/非数字/空值）；
3. 同一项目的两组仪器读数必须不同（否则两点无法确定一条直线）；
4. 同一标样集内项目名不得重复，且至少录入一个项目。

换算规则（生成校正数据时校验，任一失败则整版校正数据不可供数）：
1. 发布数据中出现的每个检测项目都必须有标样；
2. 每个项目的校正斜率 k=(y2-y1)/(x2-x1) 必须为有限值且 > 0；
3. 每一行换算结果 y=k*x+b 必须为有限值且 ≥ 0。
"""
from __future__ import annotations

import csv
import io
import math

from .validation import format_concentration

CALIBRATED_HEADER = ["sample_no", "analyte", "raw_reading", "concentration"]

_MAX_ERRORS = 50


def parse_finite_nonnegative(text: str):
    """解析「有限且非负」的数字。返回 (value, error)，二者必居其一。"""
    text = (text or "").strip()
    if not text:
        return None, "数值不能为空"
    try:
        value = float(text)
    except ValueError:
        return None, f"“{text}”不是合法数字"
    if math.isnan(value) or math.isinf(value):
        return None, f"“{text}”不是有限数值（拒绝 NaN/Inf）"
    if value < 0:
        return None, f"数值 {format_concentration(value)} 为负数，必须 ≥ 0"
    return value, None


def validate_standards_form(analytes, x1s, y1s, x2s, y2s):
    """校验浏览器提交的标样表格（五个等长列表，逐行对应）。

    返回 (points, errors)：points 为
    ``[{"analyte", "x1", "y1", "x2", "y2"}]`` 列表；失败时 points 为 None。
    整行空白（项目与四个数值全空）的行直接跳过。
    """
    errors: list[str] = []
    counts = {len(analytes), len(x1s), len(y1s), len(x2s), len(y2s)}
    if len(counts) != 1:
        return None, ["表单数据不完整：各列行数不一致，请重新提交"]

    points: list[dict] = []
    seen: dict[str, int] = {}
    for idx, (analyte, x1t, y1t, x2t, y2t) in enumerate(
        zip(analytes, x1s, y1s, x2s, y2s), start=1
    ):
        analyte = (analyte or "").strip()
        raw_cells = [x1t, y1t, x2t, y2t]
        if not analyte and not any((c or "").strip() for c in raw_cells):
            continue  # 整行空白，跳过
        row_ok = True
        if not analyte:
            errors.append(f"第 {idx} 行：检测项目为空")
            row_ok = False
        elif len(analyte) > 64:
            errors.append(f"第 {idx} 行：检测项目「{analyte[:20]}…」超长（≤64 字符）")
            row_ok = False
        elif analyte in seen:
            errors.append(
                f"第 {idx} 行：项目「{analyte}」与第 {seen[analyte]} 行重复，"
                "同一项目只能录入一组标样"
            )
            row_ok = False

        values: dict[str, float] = {}
        for label, raw in zip(("x1", "y1", "x2", "y2"), raw_cells):
            value, err = parse_finite_nonnegative(raw)
            if err:
                errors.append(f"第 {idx} 行（{analyte or '未命名项目'}）{label}：{err}")
                row_ok = False
            else:
                values[label] = value

        if row_ok:
            if values["x1"] == values["x2"]:
                errors.append(
                    f"第 {idx} 行（{analyte}）：两组仪器读数相同"
                    f"（均为 {format_concentration(values['x1'])}），无法确定校正直线"
                )
                row_ok = False
        if row_ok:
            seen[analyte] = idx
            points.append({"analyte": analyte, **values})
        if len(errors) >= _MAX_ERRORS:
            errors.append(f"错误过多，仅展示前 {_MAX_ERRORS} 条，请修正后重新提交")
            break

    if errors:
        return None, errors
    if not points:
        return None, ["至少需要录入一个检测项目的标样（两组读数与真实浓度）"]
    return points, []


def linear_params(point: dict):
    """由两点求线性校正参数。返回 (k, b, error)，出错时 k、b 为 None。"""
    x1, y1, x2, y2 = point["x1"], point["y1"], point["x2"], point["y2"]
    if x1 == x2:  # 保存时已拦截，此处为防御性复核
        return None, None, "两组仪器读数相同，无法确定校正直线"
    k = (y2 - y1) / (x2 - x1)
    if not math.isfinite(k):
        return None, None, (
            f"校正斜率非有限值（标样点 ({format_concentration(x1)}, "
            f"{format_concentration(y1)}) 与 ({format_concentration(x2)}, "
            f"{format_concentration(y2)})）"
        )
    if k <= 0:
        return None, None, f"校正斜率 k={format_concentration(k)} 不为正数"
    b = y1 - k * x1
    if not math.isfinite(b):
        return None, None, "校正截距非有限值"
    return k, b, None


def calibrate_rows(rows: list[dict], points: list[dict]):
    """用标样集换算发布批次的全部结果。

    返回 (calibrated_rows, errors)：成功时每行含
    ``sample_no / analyte / raw（原读数）/ concentration（校正后）``；
    任一项目缺标样、斜率非正或任一行算出负值/非有限值时，
    返回 (None, errors)，整版校正数据不可供数。
    """
    by_analyte = {p["analyte"]: p for p in points}
    errors: list[str] = []

    # 1) 先逐项目求斜率/截距，斜率非正或非有限的项目整项不可用
    params: dict[str, tuple[float, float]] = {}
    for analyte in sorted(by_analyte):
        k, b, err = linear_params(by_analyte[analyte])
        if err:
            errors.append(f"项目「{analyte}」：{err}")
        else:
            params[analyte] = (k, b)

    # 2) 逐行换算
    missing: dict[str, int] = {}
    out: list[dict] = []
    for row in rows:
        analyte = row["analyte"]
        if analyte not in by_analyte:
            missing[analyte] = missing.get(analyte, 0) + 1
            continue
        if analyte not in params:
            continue  # 该项目斜率错误已记录
        k, b = params[analyte]
        raw = row["concentration"]
        value = k * raw + b
        if not math.isfinite(value):
            errors.append(
                f"样本 {row['sample_no']} 项目「{analyte}」：读数 "
                f"{format_concentration(raw)} 换算结果为非有限值"
            )
            continue
        if value < 0:
            errors.append(
                f"样本 {row['sample_no']} 项目「{analyte}」：读数 "
                f"{format_concentration(raw)} 换算得 "
                f"{format_concentration(value)}（负值）"
            )
            continue
        out.append(
            {
                "sample_no": row["sample_no"],
                "analyte": analyte,
                "raw": raw,
                "concentration": value,
            }
        )
        if len(errors) >= _MAX_ERRORS:
            break

    for analyte in sorted(missing):
        errors.append(
            f"项目「{analyte}」缺少标样（{missing[analyte]} 行受影响），无法换算"
        )

    if errors:
        return None, errors[:_MAX_ERRORS] + (
            [f"错误过多，仅展示前 {_MAX_ERRORS} 条"] if len(errors) > _MAX_ERRORS else []
        )
    return out, []


def render_calibrated_csv(
    rows: list[dict],
    *,
    source_version: int,
    standards_id: int,
    batch_no: str,
    calibrated_at: str,
) -> str:
    """渲染校正 CSV（输出稳定，按样本号、项目排序）。

    首行为 ``#`` 注释，记录原始发布版本、所用标样集、批次号与换算时间，
    供统计人员追溯；数据列含原始读数与校正后浓度。
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(
        [
            f"# source_version={source_version}; standards_set={standards_id}; "
            f"batch_no={batch_no}; calibrated_at={calibrated_at}"
        ]
    )
    writer.writerow(CALIBRATED_HEADER)
    for row in sorted(rows, key=lambda r: (r["sample_no"], r["analyte"])):
        writer.writerow(
            [
                row["sample_no"],
                row["analyte"],
                format_concentration(row["raw"]),
                format_concentration(row["concentration"]),
            ]
        )
    return buf.getvalue()
