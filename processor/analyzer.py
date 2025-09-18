import math
import os
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from difflib import get_close_matches, SequenceMatcher
from typing import List, Dict, Tuple, Optional
import numpy as np
import pandas as pd

# Cố gắng dùng _parse_dt đã định nghĩa ở module preprocess (nếu có) để thống nhất cách parse timestamp.
# Nếu import thất bại (khác môi trường/thư mục), fallback sang hàm nội bộ bên dưới.
try:
    from processor.preprocess import _parse_dt as _parse_dt_ext
except Exception:
    _parse_dt_ext = None

def _parse_dt(date_str: str, time_str: str) -> datetime:
    # Wrapper: ưu tiên dùng _parse_dt_ext nếu có; nếu không thì parse theo định dạng mặc định "YYYY-mm-dd HHMMSS".
    return _parse_dt_ext(date_str, time_str) if _parse_dt_ext else datetime.strptime(
        f"{date_str} {time_str}", "%Y-%m-%d %H%M%S"
    )

# ==== xác định project root theo vị trí file này ====
# analyzer.py nằm ở: <root>/processor/analyzer.py → parents[1] là thư mục root dự án.
ROOT = Path(__file__).resolve().parents[1]

# ==== đường dẫn tuyệt đối ====
# Thư mục đầu vào (sản phẩm của preprocess) và đầu ra (kết quả phân tích).
PREPROCESS_DIR = ROOT / "output" / "preprocess"
ANALYZER_DIR   = ROOT / "output" / "analyzer"

# ==== regex tên file (match cả processed & preprocessed) ====
# Mẫu nhận diện file hợp lệ để đưa vào phân tích:
#   job_detail_output_<slug>_g<gid>_<loc>_<YYYY-mm-dd>_<HHMMSS>[_processed|_preprocessed].xlsx
FNAME_RE = re.compile(
    r"^job_detail_output_(?P<slug>.+?)_g(?P<gid>\d+)_(?P<loc>\d{4})_"
    r"(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{6})"
    r"(?:_(?P<suffix>(?:pre)?processed))?\.xlsx$",
    re.IGNORECASE,
)

DEBUG = True  # Bật LOG debug chi tiết trong quá trình quét file mới nhất.

def _make_analyzer_path(src: Path) -> Path:
    # Tạo đường dẫn file đầu ra cho một file nguồn:
    # - Loại bỏ hậu tố _processed/_preprocessed khỏi stem.
    # - Ghi ra thư mục analyzer, tên đuôi _analyzed.xlsx
    stem = src.stem
    stem = re.sub(r"_(?:pre)?processed$", "", stem, flags=re.IGNORECASE)
    out_name = f"{stem}_analyzed.xlsx"
    ANALYZER_DIR.mkdir(parents=True, exist_ok=True)
    return ANALYZER_DIR / out_name

def get_latest_detail_files(base_dir: Path) -> List[Path]:
    # Duyệt base_dir, group theo (slug, gid, loc) và chọn phiên bản MỚI NHẤT dựa trên timestamp trong tên file.
    latest: Dict[Tuple[str, str, str], Tuple[datetime, Path]] = {}

    if DEBUG:
        print(f"[DEBUG] CWD       : {Path.cwd()}")
        print(f"[DEBUG] base_dir  : {base_dir} (exists={base_dir.exists()})")

    if not base_dir.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục: {base_dir}")

    for p in base_dir.iterdir():
        if not p.is_file():
            continue
        m = FNAME_RE.match(p.name)
        if DEBUG:
            print(f"[DEBUG] {p.name} -> {'MATCH' if m else 'NO MATCH'}")
        if not m:
            continue

        slug = m.group("slug")
        gid  = m.group("gid")
        loc  = m.group("loc")
        dt   = _parse_dt(m.group("date"), m.group("time"))
        key  = (slug, gid, loc)
        cur  = latest.get(key)
        if cur is None or dt > cur[0]:
            latest[key] = (dt, p)

    return [info[1] for info in latest.values()]

def save_combined_with_timestamp(
    df: pd.DataFrame,
    out_dir: Path = ANALYZER_DIR,
    prefix: str = "job_detail_output__combined"
) -> Path:
    """
    Ghi file tổng hợp kết quả phân tích với timestamp vào tên:
    output/analyzer/job_detail_output__combined_{YYYY-mm-dd_HHMMSS}_analyzed.xlsx
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_path = out_dir / f"{prefix}_{ts}_analyzed.xlsx"
    df.to_excel(out_path, index=False)
    return out_path

def _norm_text_no_accent(s) -> str:
    """Chuẩn hoá văn bản: strip/ lower/ bỏ dấu (NFD – loại Mn). Dùng để nhận diện 'không yêu cầu' / biến thể."""
    if pd.isna(s):
        return ""
    t = str(s).strip().lower()
    t = "".join(c for c in unicodedata.normalize("NFD", t) if unicodedata.category(c) != "Mn")
    return t

def _to_safe_str(x):
    # Ép về chuỗi an toàn (xử lý None/NaN). Tránh lỗi khi đưa vào so khớp/ so sánh.
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def safe_similarity(a, b):
    # So sánh độ tương đồng 2 chuỗi (0..1) với SequenceMatcher, có phòng ngừa lỗi & ép chuỗi an toàn.
    a = _to_safe_str(a)
    b = _to_safe_str(b)
    try:
        # autojunk=False để ổn định hơn với chuỗi ngắn/ít ký tự lặp.
        return SequenceMatcher(None, a, b, autojunk=False).ratio()
    except Exception:
        return 0.0

def safe_get_close_matches(word, possibilities, n=3, cutoff=0.6):
    # Gợi ý cột gần đúng khi thiếu cột mục tiêu (ví dụ: 'nganh' bị viết nhầm).
    word = _to_safe_str(word)
    poss = [_to_safe_str(p) for p in possibilities if _to_safe_str(p)]
    try:
        return get_close_matches(word, poss, n=n, cutoff=cutoff)
    except Exception:
        return []

def analyze_one_file(file_path: Path) -> Path:
    """Phân tích một file đã preprocess và ghi ra output/analyzer/<tên_gốc>_analyzed.xlsx"""
    if not file_path.exists():
        raise SystemExit(f"❌ Không tìm thấy file: {file_path}")

    out_phantich = _make_analyzer_path(file_path)

    # ==== ĐỌC DỮ LIỆU ====
    df = pd.read_excel(file_path, engine="openpyxl")
    print(f"✅ Đọc: {file_path.name} — {df.shape[0]} dòng × {df.shape[1]} cột")

    # === 1) In số dòng/cột ===
    print(f"📊 Kích thước ban đầu: {df.shape[0]} dòng × {df.shape[1]} cột")

    # === 2) Xóa dòng trùng hoàn toàn ===
    # Dùng duplicated() để phát hiện hàng giống 100% trên mọi cột; chỉ giữ bản ghi đầu tiên.
    dup_exact = df.duplicated(keep="first").sum()
    if dup_exact > 0:
        df = df.drop_duplicates(keep="first").reset_index(drop=True)
        print(f"🗑️  Đã xóa {dup_exact} dòng trùng hoàn toàn. Còn lại: {len(df)} dòng")
    else:
        print("✅ Không có dòng trùng hoàn toàn.")

    # === 3) Xóa dòng trùng xấp xỉ (≥ 90%) ===
    # Ý tưởng:
    #  - Chuẩn hoá text từng ô (lower/strip/loại 'nan'/'no_info') → ghép toàn hàng thành 1 chuỗi.
    #  - Bucket theo độ dài (len // 20) để giảm số cặp so sánh O(n²).
    #  - Dùng SequenceMatcher.ratio() cho từng cặp trong cùng bucket, ngưỡng 0.90.
    limit = min(len(df), 5000)  # giới hạn để tránh O(n²) quá nặng

    # Chuẩn hóa từng ô -> chuỗi gọn gàng để so sánh
    df_cmp = df.iloc[:limit].copy()
    for c in df_cmp.columns:
        s = df_cmp[c].astype(str).str.strip().str.lower()
        s = s.mask(s.isin(["nan", "no_info"]), "")
        s = s.str.replace(r"\s+", " ", regex=True)
        df_cmp[c] = s

    # Ghép mỗi dòng thành một chuỗi duy nhất (loại ô rỗng để giảm nhiễu).
    row_texts = df_cmp.apply(lambda r: " | ".join([v for v in r.tolist() if v]), axis=1).tolist()

    # Nhóm thô theo độ dài chuỗi để giảm số cặp so sánh (bucketization).
    bucket_map: Dict[int, list] = {}
    for i, t in enumerate(row_texts):
        L = len(t) // 20
        bucket_map.setdefault(L, []).append(i)

    # So sánh trong từng bucket, đánh dấu các vị trí cần xoá (giữ dòng đầu tiên).
    to_drop_pos = set()
    for _, idxs in bucket_map.items():
        m = len(idxs)
        for a in range(m):
            i = idxs[a]
            if i in to_drop_pos:
                continue
            ti = row_texts[i]
            if not ti:
                continue
            for b in range(a + 1, m):
                j = idxs[b]
                if j in to_drop_pos:
                    continue
                tj = row_texts[j]
                if not tj:
                    continue
                sim = SequenceMatcher(None, ti, tj).ratio()
                if sim >= 0.90:
                    to_drop_pos.add(j)  # bỏ dòng sau, giữ dòng đầu

    # Map từ vị trí so sánh (iloc) sang index thật rồi drop.
    approx_drop_idx = df.iloc[list(to_drop_pos)].index.tolist()
    if approx_drop_idx:
        print(f"🗑️  Phát hiện {len(approx_drop_idx)} dòng trùng xấp xỉ (≥90%), tiến hành xóa.")
        df = df.drop(index=approx_drop_idx).reset_index(drop=True)
        print(f"✅ Sau khi xóa trùng xấp xỉ: {len(df)} dòng")
    else:
        print("✅ Không phát hiện dòng trùng xấp xỉ.")

    # === 4) Kết quả cuối cùng ===
    print(f"📊 Kích thước cuối cùng: {df.shape[0]} dòng × {df.shape[1]} cột")

    # Chuẩn bị thống kê theo 'nganh'
    TARGET_COL = "nganh"
    if TARGET_COL not in df.columns:
        # Nếu thiếu cột, gợi ý các tên gần đúng để dễ sửa pipeline/đầu vào.
        suggestion = get_close_matches(TARGET_COL, df.columns.tolist(), n=3, cutoff=0.6)
        raise SystemExit(f"❌ Không tìm thấy cột '{TARGET_COL}'. Gợi ý: {', '.join(suggestion)}")

    # Cột ngành gốc (giữ dấu/hoa-thường như file), thay ô rỗng bằng "no_info"
    s_nganh_raw = df[TARGET_COL].astype(str).str.strip()
    s_nganh_raw = s_nganh_raw.replace(r"^\s*$", "no_info", regex=True)

    # Cột ngành chuẩn hoá (lower) để gom nhóm khi value không đồng nhất kiểu chữ.
    s_nganh_norm = s_nganh_raw.str.lower()

    # Ánh xạ "dạng chuẩn hoá" → "bản gốc đầu tiên" để sau gộp nhóm vẫn hiển thị tên đẹp có dấu.
    map_norm_to_raw = {}
    for norm, raw in zip(s_nganh_norm, s_nganh_raw):
        if norm not in map_norm_to_raw:
            map_norm_to_raw[norm] = raw

    # Chuẩn hoá cờ 'check_luong' về bool; nếu thiếu cột thì coi như False toàn bộ.
    has_check = "check_luong" in df.columns
    if has_check:
        def _to_bool(x):
            if isinstance(x, bool):
                return x
            s = str(x).strip().lower()
            if s in {"true", "1", "yes", "y"}:
                return True
            if s in {"false", "0", "no", "n"}:
                return False
            return False

        s_check = df["check_luong"].map(_to_bool)
    else:
        s_check = pd.Series([False] * len(df))

    # Đếm số tin theo ngành
    counts = s_nganh_norm.value_counts(dropna=False)
    total = int(counts.sum())

    # Đếm theo ngành tách theo có/không có lương (dựa trên check_luong)
    by_nganh_check = (
        pd.DataFrame({TARGET_COL: s_nganh_norm, "_check": s_check})
        .groupby(TARGET_COL, dropna=False)["_check"]
        .agg(
            so_tin_co_luong=lambda x: int(x.sum()),
            so_tin_co_thuong_luong=lambda x: int((~x).sum())
        )
        .reset_index()
    )

    # Gộp 2 bảng lại thành thống kê chính
    stats_df = (
        counts.rename("so_tin").to_frame()
        .reset_index()
        .rename(columns={"index": TARGET_COL})
        .merge(by_nganh_check, how="left", on=TARGET_COL)
    )

    # Đổi khoá chuẩn hoá về nhãn gốc có dấu để hiển thị đẹp
    stats_df[TARGET_COL] = stats_df[TARGET_COL].map(map_norm_to_raw)

    # Điền 0 cho cột đếm nếu thiếu (an toàn khi merge)
    for c in ["so_tin_co_luong", "so_tin_co_thuong_luong"]:
        if c in stats_df.columns:
            stats_df[c] = stats_df[c].fillna(0).astype("Int64")
        else:
            stats_df[c] = pd.Series([0] * len(stats_df), dtype="Int64")

    # Tính tỷ lệ %
    stats_df["ty_le"] = (stats_df["so_tin"] / total * 100).round(2)

    # Sắp xếp ngành theo số tin giảm dần
    stats_df = stats_df.sort_values("so_tin", ascending=False, ignore_index=True)

    # Thêm dòng tổng cộng cuối bảng
    row_total = pd.DataFrame([{
        TARGET_COL: "Tổng cộng",
        "so_tin": stats_df["so_tin"].sum(),
        "ty_le": 100.0,
        "so_tin_co_luong": stats_df["so_tin_co_luong"].sum(skipna=True),
        "so_tin_co_thuong_luong": stats_df["so_tin_co_thuong_luong"].sum(skipna=True)
    }])

    stats_df_out = pd.concat([stats_df, row_total], ignore_index=True)

    # Ghi kết quả ra một sheet duy nhất (tin_theo_nganh) trong file analyzed
    with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="w") as writer:
        stats_df_out.to_excel(writer, sheet_name="tin_theo_nganh", index=False)

    print(f"📄 Đã lưu {len(stats_df_out)} dòng (sheet: 'tin_theo_nganh').")

    # ==== 5) Lương trung bình & min/max theo ngành ====
    SALARY_COL = "med_luong"  # Cột lương đã được chuẩn hoá về VND/tháng (giá trị "trung vị/median" cho mỗi bản ghi)
    MIN_COL = "min_luong"  # Cột lương tối thiểu (tuỳ chọn, có thể không tồn tại)
    MAX_COL = "max_luong"  # Cột lương tối đa (tuỳ chọn, có thể không tồn tại)

    # Kiểm tra cột bắt buộc (phải có cột ngành + cột lương chuẩn hoá)
    for col_need in [TARGET_COL, SALARY_COL]:
        if col_need not in df.columns:
            sug = get_close_matches(col_need, df.columns.tolist(), n=3, cutoff=0.6)
            raise SystemExit(f"❌ Thiếu cột '{col_need}'. Gợi ý: {', '.join(sug) if sug else 'không có'}")

    # Ép cột lương về kiểu số (nan nếu không chuyển được)
    s_med = pd.to_numeric(df[SALARY_COL], errors="coerce")

    # Hai cột min/max có thể không tồn tại -> tạo Series mềm để xử lý chung
    has_min = MIN_COL in df.columns
    has_max = MAX_COL in df.columns
    s_min = pd.to_numeric(df[MIN_COL], errors="coerce") if has_min else pd.Series([pd.NA] * len(df))
    s_max = pd.to_numeric(df[MAX_COL], errors="coerce") if has_max else pd.Series([pd.NA] * len(df))

    # Tính trung bình toàn bộ (round đến nghìn cho dễ đọc); nếu không có dữ liệu thì gán NaN
    overall_mean = float(s_med.mean()) if s_med.notna().any() else np.nan
    tb_chung = None if not np.isfinite(overall_mean) else round(overall_mean, -3)

    # Thêm: min toàn tập và max toàn tập (nếu có dữ liệu min/max)
    global_min_min = None
    global_max_max = None
    if has_min and s_min.notna().any():
        global_min_min = int(round(float(s_min.min(skipna=True)), -3))
    if has_max and s_max.notna().any():
        global_max_max = int(round(float(s_max.max(skipna=True)), -3))

    # Cờ lương bất thường: đánh dấu các dòng có med_luong == "bat_thuong" (xử lý an toàn dù cột là số/chuỗi)
    s_bat = df[SALARY_COL].astype(str).str.strip().str.lower().eq("bat_thuong")

    # Chuẩn bị trường "chi tiết bất thường" (gộp luong | href nếu có)
    has_luong = "luong" in df.columns
    has_href = "href" in df.columns

    def _fmt_detail(idx, row):
        if not s_bat.iat[idx]:
            return None
        parts = []
        if has_luong:
            v = str(row["luong"])
            if v and v.strip() and v.strip().lower() != "nan":
                parts.append(v.strip())
        if has_href:
            v = str(row["href"])
            if v and v.strip() and v.strip().lower() != "nan":
                parts.append(v.strip())
        return " | ".join(parts) if parts else None

    detail_series = df.apply(lambda r: _fmt_detail(r.name, r), axis=1)

    # Đếm số tin bất thường + gom mô tả chi tiết theo ngành
    bat_detail_df = (
        df.assign(_bat=s_bat, _detail=detail_series)
        .groupby(TARGET_COL, dropna=False)
        .agg(
            so_tin_luong_bat_thuong=("_bat", lambda x: int(x.sum())),
            chi_tiet_bat_thuong=("_detail", lambda col: "khong_phat_hien"
            if col.dropna().empty else "; ".join(map(str, col.dropna().tolist())))
        )
        .reset_index()
    )

    # Tính TB/min/max theo ngành (dựa trên các cột *_num đã ép kiểu)
    agg_df = (
        df.assign(
            med_luong_num=s_med,
            min_luong_num=s_min,
            max_luong_num=s_max
        )
        .groupby(TARGET_COL, dropna=False)
        .agg(
            med_luong_num=('med_luong_num', 'mean'),
            min_luong=('min_luong_num', 'min') if has_min else ('med_luong_num', lambda x: pd.NA),
            max_luong=('max_luong_num', 'max') if has_max else ('med_luong_num', lambda x: pd.NA),
        )
        .dropna(subset=['med_luong_num'])
        .reset_index()
    )

    # Làm tròn đến nghìn & chuẩn hoá kiểu Int64 (có thể chứa NA)
    agg_df["med_luong_tb"] = agg_df["med_luong_num"].round(-3).astype("Int64")
    if has_min:
        agg_df["min_luong"] = agg_df["min_luong"].round(-3).astype("Int64")
    else:
        agg_df["min_luong"] = pd.Series([pd.NA] * len(agg_df), dtype="Int64")

    if has_max:
        agg_df["max_luong"] = agg_df["max_luong"].round(-3).astype("Int64")
    else:
        agg_df["max_luong"] = pd.Series([pd.NA] * len(agg_df), dtype="Int64")

    # So sánh với TB chung (đơn vị: %); nếu không có TB chung thì gán 0.0
    if np.isfinite(overall_mean) and overall_mean > 0:
        agg_df["so_voi_tb_chung"] = ((agg_df["med_luong_num"] - overall_mean) / overall_mean * 100).round(1)
    else:
        agg_df["so_voi_tb_chung"] = 0.0

    # Gộp thêm số tin & chi tiết bất thường theo ngành
    agg_merged = agg_df.merge(bat_detail_df, how="left", on=TARGET_COL)
    agg_merged["so_tin_luong_bat_thuong"] = agg_merged["so_tin_luong_bat_thuong"].fillna(0).astype("Int64")
    agg_merged["chi_tiet_bat_thuong"] = agg_merged["chi_tiet_bat_thuong"].fillna("khong_phat_hien")

    # Bảng cuối cùng cho sheet lương
    # (KHÔNG thêm cột đếm "so_tin_co_luong" / "so_tin_co_thuong_luong")
    salary_df = (
        agg_merged.sort_values("med_luong_tb", ascending=False)
        .rename(columns={TARGET_COL: "nganh"})
        [["nganh", "med_luong_tb", "min_luong", "max_luong", "so_voi_tb_chung",
          "so_tin_luong_bat_thuong", "chi_tiet_bat_thuong"]]
        .reset_index(drop=True)
    )

    # Thêm dòng "Tổng quan (TB)" nếu tính được TB chung
    # (kèm min/max toàn tập, tổng số dòng bất thường; "chi_tiet_bat_thuong" để NA)
    if tb_chung is not None:
        total_bat = int(s_bat.sum(skipna=True))
        row_tong = {
            "nganh": "Tổng quan (TB)",
            "med_luong_tb": int(tb_chung),
            "min_luong": (pd.NA if global_min_min is None else pd.array([global_min_min], dtype="Int64")[0]),
            "max_luong": (pd.NA if global_max_max is None else pd.array([global_max_max], dtype="Int64")[0]),
            "so_voi_tb_chung": 0.0,
            "so_tin_luong_bat_thuong": pd.NA if total_bat is None else total_bat,
            "chi_tiet_bat_thuong": pd.NA
        }
        salary_df = pd.concat([salary_df, pd.DataFrame([row_tong])], ignore_index=True)

        # Ghi sheet Excel "phan_tich_luong" (append nếu file đã tồn tại, thay thế sheet nếu có sẵn)
        sheet_salary = "phan_tich_luong"
        try:
            if out_phantich.exists():
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    salary_df.to_excel(writer, sheet_name=sheet_salary, index=False)
            else:
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="w") as writer:
                    salary_df.to_excel(writer, sheet_name=sheet_salary, index=False)

            # Log tóm tắt sau khi ghi
            print(f"📄 Đã lưu {len(salary_df)} dòng (sheet: '{sheet_salary}').")
            if tb_chung is not None:
                print(f"• Lương TB toàn bộ: {tb_chung:,.0f} VND/tháng")
            if has_min and (global_min_min is not None):
                print(f"• Min nhỏ nhất toàn tập: {global_min_min:,.0f} VND/tháng")
            if has_max and (global_max_max is not None):
                print(f"• Max lớn nhất toàn tập: {global_max_max:,.0f} VND/tháng")
            if not has_min:
                print("⚠️ Không thấy cột 'min_luong' → cột 'min_luong' trong sheet sẽ là NA.")
            if not has_max:
                print("⚠️ Không thấy cột 'max_luong' → cột 'max_luong' trong sheet sẽ là NA.")
        except Exception as e:
            # Bắt lỗi khi ghi file Excel
            print(f"❌ Lỗi khi ghi Excel (sheet lương): {e}")

        # ==== 6) Thống kê yêu cầu kinh nghiệm theo ngành ====
        EXP_COL = "so_nam_kinh_nghiem"
        if EXP_COL not in df.columns or TARGET_COL not in df.columns:
            print("⚠️ Bỏ qua thống kê kinh nghiệm (thiếu cột).")
            return out_phantich

        # --- Helpers ---
        def _safe_str(x):
            return "" if pd.isna(x) else str(x)

        def _to_num_years(x):
            s = _safe_str(x)
            if not s:
                return None
            s_norm = _norm_text_no_accent(s)  # Hàm chuẩn hoá không dấu/spacing (đã định nghĩa ở phần trước)
            s_norm = s_norm.replace(",", ".")  # Chuẩn hoá phân số kiểu '1,5' -> '1.5'

            # Bắt nhanh một vài cụm phổ biến (diễn giải thô → số)
            if "duoi" in s_norm and "1" in s_norm:
                return 0.5
            if "tren" in s_norm and "7" in s_norm:
                return 7.0
            if "khong yeu cau" in s_norm:
                return None  # Trả None: sẽ được phân loại bằng cờ "không yêu cầu" riêng

            # Trích số (hỗ trợ khoảng 1-3, 4–6, …)
            nums = re.findall(r"\d+(?:\.\d+)?", s_norm)
            if not nums:
                # Trường hợp dạng '7+' hoặc '>=7' hoặc 'lon hon/ tren 7'
                if re.search(r"(?:\+|>=?|lon hon|tren)\s*7", s_norm):
                    return 7.0
                return None

            vals = [float(n) for n in nums]
            # Nếu có ký hiệu ngưỡng (>=, +) và giá trị lớn, coi như giá trị lớn nhất
            if re.search(r"\+|>=?", s_norm) and max(vals) >= 7:
                return max(vals)

            # Nếu là khoảng (range) → lấy trung bình (ví dụ 1-3 → 2.0)
            return sum(vals) / len(vals)

        # Cờ văn bản: "không yêu cầu" (no-exp) và ngược lại là "có yêu cầu"
        df["_no_exp_flag"] = df[EXP_COL].map(lambda x: 1 if "khong yeu cau" in _norm_text_no_accent(x) else 0)
        df["_has_exp_flag"] = (df["_no_exp_flag"] == 0).astype(int)

        # Suy ra số năm kinh nghiệm chuẩn hoá (float) chỉ cho các bản ghi có yêu cầu
        df["_exp_years"] = df.apply(lambda r: (_to_num_years(r[EXP_COL]) if r["_has_exp_flag"] == 1 else None), axis=1)

        # Đặt các "bucket" theo năm (chỉ khi có giá trị số hợp lệ)
        df["_b_duoi_1"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (r["_exp_years"] <= 1.0)),
            axis=1)
        df["_b_1_3"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (1.0 < r["_exp_years"] <= 3.0)),
            axis=1)
        df["_b_4_6"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (3.0 < r["_exp_years"] <= 6.0)),
            axis=1)
        df["_b_tren_7"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (r["_exp_years"] > 6.0)),
            axis=1)

        # Sử dụng _exp_years (float) để tính mean/min/max (chỉ các bản ghi có yêu cầu)
        s_exp = pd.to_numeric(df["_exp_years"], errors="coerce")

        # Gom theo ngành và tính các thống kê + đếm bucket
        exp_stats = (
            df.assign(exp_num=s_exp)
            .groupby(TARGET_COL, dropna=False)
            .agg(
                mean_exp=("exp_num", lambda x: round(x.mean(skipna=True), 2)),
                min_exp=("exp_num", "min"),
                max_exp=("exp_num", "max"),
                so_tin_no_exp=("_no_exp_flag", "sum"),
                so_tin_co_exp=("_has_exp_flag", "sum"),
                Duoi_1=("_b_duoi_1", "sum"),
                **{"1_3": ("_b_1_3", "sum")},
                **{"4_6": ("_b_4_6", "sum")},
                Tren_7=("_b_tren_7", "sum"),
            )
            .reset_index()
        )

        # Ép kiểu Int64 cho các cột đếm (hỗ trợ NA an toàn hơn int thường)
        for c in ["so_tin_no_exp", "so_tin_co_exp", "Duoi_1", "1_3", "4_6", "Tren_7"]:
            if c in exp_stats.columns:
                exp_stats[c] = exp_stats[c].astype("Int64")

        # Sắp xếp kết quả: ưu tiên ngành có mean_exp cao → sau đó theo tên ngành (ASC)
        exp_stats = exp_stats.sort_values(["mean_exp", TARGET_COL], ascending=[False, True],
                                          na_position="last", ignore_index=True)

        # Thêm dòng "TỔNG QUAN" ở cuối: tổng các cột đếm + mean/min/max tổng thể
        summary_row = {
            TARGET_COL: "TỔNG QUAN",
            "mean_exp": round(exp_stats["mean_exp"].mean(skipna=True), 2),
            "min_exp": exp_stats["min_exp"].min(skipna=True),
            "max_exp": exp_stats["max_exp"].max(skipna=True),
            "so_tin_no_exp": int(exp_stats["so_tin_no_exp"].sum(skipna=True)),
            "so_tin_co_exp": int(exp_stats["so_tin_co_exp"].sum(skipna=True)),
            "Duoi_1": int(exp_stats["Duoi_1"].sum(skipna=True)),
            "1_3": int(exp_stats["1_3"].sum(skipna=True)),
            "4_6": int(exp_stats["4_6"].sum(skipna=True)),
            "Tren_7": int(exp_stats["Tren_7"].sum(skipna=True)),
        }
        exp_stats = pd.concat([exp_stats, pd.DataFrame([summary_row])], ignore_index=True)

        # Ghi ra sheet Excel "nam_kinh_nghiem" (thay thế sheet nếu đã tồn tại)
        try:
            with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                exp_stats.to_excel(writer, sheet_name="nam_kinh_nghiem", index=False)
            print(f"📄 Đã lưu {len(exp_stats)} dòng (sheet: 'nam_kinh_nghiem').")
        except Exception as e:
            print(f"❌ Lỗi khi ghi Excel (sheet kinh nghiệm): {e}")
        # ===============================================================================
        EXP_COL = "so_nam_kinh_nghiem"
        if EXP_COL not in df.columns or TARGET_COL not in df.columns:
            print("⚠️ Bỏ qua thống kê kinh nghiệm (thiếu cột).")
            return out_phantich

        # --- Helpers ---
        def _safe_str(x):
            return "" if pd.isna(x) else str(x)

        def _to_num_years(x):
            s = _safe_str(x)
            if not s:
                return None
            s_norm = _norm_text_no_accent(s)  # Hàm chuẩn hoá không dấu/spacing (đã định nghĩa ở phần trước)
            s_norm = s_norm.replace(",", ".")  # Chuẩn hoá phân số kiểu '1,5' -> '1.5'

            # Bắt nhanh một vài cụm phổ biến (diễn giải thô → số)
            if "duoi" in s_norm and "1" in s_norm:
                return 0.5
            if "tren" in s_norm and "7" in s_norm:
                return 7.0
            if "khong yeu cau" in s_norm:
                return None  # Trả None: sẽ được phân loại bằng cờ "không yêu cầu" riêng

            # Trích số (hỗ trợ khoảng 1-3, 4–6, …)
            nums = re.findall(r"\d+(?:\.\d+)?", s_norm)
            if not nums:
                # Trường hợp dạng '7+' hoặc '>=7' hoặc 'lon hon/ tren 7'
                if re.search(r"(?:\+|>=?|lon hon|tren)\s*7", s_norm):
                    return 7.0
                return None

            vals = [float(n) for n in nums]
            # Nếu có ký hiệu ngưỡng (>=, +) và giá trị lớn, coi như giá trị lớn nhất
            if re.search(r"\+|>=?", s_norm) and max(vals) >= 7:
                return max(vals)

            # Nếu là khoảng (range) → lấy trung bình (ví dụ 1-3 → 2.0)
            return sum(vals) / len(vals)

        # Cờ văn bản: "không yêu cầu" (no-exp) và ngược lại là "có yêu cầu"
        df["_no_exp_flag"] = df[EXP_COL].map(lambda x: 1 if "khong yeu cau" in _norm_text_no_accent(x) else 0)
        df["_has_exp_flag"] = (df["_no_exp_flag"] == 0).astype(int)

        # Suy ra số năm kinh nghiệm chuẩn hoá (float) chỉ cho các bản ghi có yêu cầu
        df["_exp_years"] = df.apply(lambda r: (_to_num_years(r[EXP_COL]) if r["_has_exp_flag"] == 1 else None), axis=1)

        # Đặt các "bucket" theo năm (chỉ khi có giá trị số hợp lệ)
        df["_b_duoi_1"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (r["_exp_years"] <= 1.0)),
            axis=1)
        df["_b_1_3"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (1.0 < r["_exp_years"] <= 3.0)),
            axis=1)
        df["_b_4_6"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (3.0 < r["_exp_years"] <= 6.0)),
            axis=1)
        df["_b_tren_7"] = df.apply(
            lambda r: int((r["_has_exp_flag"] == 1) and (pd.notna(r["_exp_years"])) and (r["_exp_years"] > 6.0)),
            axis=1)

        # Sử dụng _exp_years (float) để tính mean/min/max (chỉ các bản ghi có yêu cầu)
        s_exp = pd.to_numeric(df["_exp_years"], errors="coerce")

        # Gom theo ngành và tính các thống kê + đếm bucket
        exp_stats = (
            df.assign(exp_num=s_exp)
            .groupby(TARGET_COL, dropna=False)
            .agg(
                mean_exp=("exp_num", lambda x: round(x.mean(skipna=True), 2)),
                min_exp=("exp_num", "min"),
                max_exp=("exp_num", "max"),
                so_tin_no_exp=("_no_exp_flag", "sum"),
                so_tin_co_exp=("_has_exp_flag", "sum"),
                Duoi_1=("_b_duoi_1", "sum"),
                **{"1_3": ("_b_1_3", "sum")},
                **{"4_6": ("_b_4_6", "sum")},
                Tren_7=("_b_tren_7", "sum"),
            )
            .reset_index()
        )

        # Ép kiểu Int64 cho các cột đếm (hỗ trợ NA an toàn hơn int thường)
        for c in ["so_tin_no_exp", "so_tin_co_exp", "Duoi_1", "1_3", "4_6", "Tren_7"]:
            if c in exp_stats.columns:
                exp_stats[c] = exp_stats[c].astype("Int64")

        # Sắp xếp kết quả: ưu tiên ngành có mean_exp cao → sau đó theo tên ngành (ASC)
        exp_stats = exp_stats.sort_values(["mean_exp", TARGET_COL], ascending=[False, True],
                                          na_position="last", ignore_index=True)

        # Thêm dòng "TỔNG QUAN" ở cuối: tổng các cột đếm + mean/min/max tổng thể
        summary_row = {
            TARGET_COL: "TỔNG QUAN",
            "mean_exp": round(exp_stats["mean_exp"].mean(skipna=True), 2),
            "min_exp": exp_stats["min_exp"].min(skipna=True),
            "max_exp": exp_stats["max_exp"].max(skipna=True),
            "so_tin_no_exp": int(exp_stats["so_tin_no_exp"].sum(skipna=True)),
            "so_tin_co_exp": int(exp_stats["so_tin_co_exp"].sum(skipna=True)),
            "Duoi_1": int(exp_stats["Duoi_1"].sum(skipna=True)),
            "1_3": int(exp_stats["1_3"].sum(skipna=True)),
            "4_6": int(exp_stats["4_6"].sum(skipna=True)),
            "Tren_7": int(exp_stats["Tren_7"].sum(skipna=True)),
        }
        exp_stats = pd.concat([exp_stats, pd.DataFrame([summary_row])], ignore_index=True)

        # Ghi ra sheet Excel "nam_kinh_nghiem" (thay thế sheet nếu đã tồn tại)
        try:
            with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                exp_stats.to_excel(writer, sheet_name="nam_kinh_nghiem", index=False)
            print(f"📄 Đã lưu {len(exp_stats)} dòng (sheet: 'nam_kinh_nghiem').")
        except Exception as e:
            print(f"❌ Lỗi khi ghi Excel (sheet kinh nghiệm): {e}")

        # ===================================================
        # ==== 7) Thống kê ngôn ngữ CV (chỉ ghi sheet 'ngon_ngu_cv') ====
        LANG_COL = "ngon_ngu_cv"
        TARGET_COL = "nganh"  # Đã được sử dụng ở các bước trước (cột ngành chuẩn)

        import unicodedata as _ud
        from collections import Counter

        def _no_accent(s: str) -> str:
            """
            Chuẩn hoá chuỗi về dạng không dấu, lower-case, trim.
            Trả về "" nếu đầu vào là None.
            """
            if s is None:
                return ""
            s = _ud.normalize("NFD", str(s))
            return "".join(ch for ch in s if _ud.category(ch) != "Mn").lower().strip()

        # Từ điển ngôn ngữ → các alias (bao gồm tên ngôn ngữ & chứng chỉ liên quan)
        # Lưu ý: phát hiện theo "substring contains" sau khi normalize không dấu
        _LANG_ALIASES = {
            "Tiếng Anh": [
                "tieng anh", "anh", "english", "en",
                "ielts", "toefl", "toeic", "cambridge", "sat", "gre", "gmat"
            ],
            "Tiếng Nhật": [
                "tieng nhat", "nhat", "japanese", "nihongo", "jp", "nihon",
                "jlpt", "n1", "n2", "n3", "n4", "n5"
            ],
            "Tiếng Trung": [
                "tieng trung", "trung", "chinese", "mandarin", "zhong", "zh",
                "putonghua", "han ngu", "hoa",
                "hsk", "hsk1", "hsk2", "hsk3", "hsk4", "hsk5", "hsk6"
            ],
            "Tiếng Hàn": [
                "tieng han", "han", "korean", "hangul", "kr", "han quoc",
                "topik", "topik1", "topik2", "topik3", "topik4", "topik5", "topik6"
            ],
            "Tiếng Đức": [
                "tieng duc", "duc", "german", "deutsch", "de",
                "goethe", "testdaf", "dsh", "telc"
            ],
            "Tiếng Pháp": [
                "tieng phap", "phap", "french", "francais", "fr",
                "delf", "dalf", "tef", "tcf"
            ],
            "Tiếng Tây Ban Nha": [
                "tieng tay ban nha", "tay ban nha", "spanish", "espanol", "es",
                "dele"
            ],
            "Tiếng Ý": [
                "tieng y", "italian", "it", "celi", "cils", "plida"
            ],
            "Tiếng Nga": [
                "tieng nga", "nga", "russian", "ru",
                "torfl"
            ],
            "Tiếng Thái": [
                "tieng thai", "thai"
            ],
            "Tiếng Việt": [
                "tieng viet", "viet", "vietnamese", "vi"
            ],
            "Bất kỳ": [
                "bat ky", "batki", "bat-ky", "bat_ky"
            ],  # Không yêu cầu ngôn ngữ cụ thể
        }

        # Bản đồ alias (đã normalize) → tên ngôn ngữ chuẩn (canon)
        _ALIAS2CANON = {_no_accent(a): canon for canon, aliases in _LANG_ALIASES.items() for a in aliases}

        def _detect_langs(text: str) -> set[str]:
            """
            Phát hiện tập ngôn ngữ xuất hiện trong chuỗi:
            - Chuẩn hoá chuỗi về không dấu/lower.
            - Dò theo "substring" với các alias.
            - Trả về set tên ngôn ngữ chuẩn (có thể nhiều hơn 1 nếu text đề cập nhiều ngôn ngữ).
            """
            s = _no_accent(text)
            if not s:
                return set()
            return {canon for alias_norm, canon in _ALIAS2CANON.items() if alias_norm and alias_norm in s}

        # Nếu thiếu cột nguồn ngôn ngữ → bỏ qua bước 7
        if LANG_COL not in df.columns:
            print(f"⚠️ Bỏ qua thống kê ngôn ngữ (thiếu cột '{LANG_COL}').")
        else:
            total_rows = len(df)  # Số dòng gốc (làm mẫu số tính tỷ lệ)
            total_counts = Counter()  # Đếm số lần phát hiện theo từng ngôn ngữ
            pairs_rows = []  # Lưu cặp (ngôn ngữ, ngành) để lấy top ngành theo ngôn ngữ

            # Chuẩn hoá ngành rỗng → 'no_info' để tránh NaN khi groupby
            s_nganh_clean = (
                df[TARGET_COL]
                .astype(str).str.strip()
                .str.replace(r"^\s*$", "no_info", regex=True)
            )

            # Quét từng dòng, phát hiện các ngôn ngữ được nhắc tới và gom cặp (ngôn ngữ, ngành)
            for idx, row in df.iterrows():
                langs = _detect_langs(row.get(LANG_COL, ""))
                if not langs:
                    continue
                nganh_val = s_nganh_clean.iat[idx]
                for lg in langs:
                    total_counts[lg] += 1
                    pairs_rows.append({"ngon_ngu": lg, "nganh": nganh_val})

            # Không phát hiện được ngôn ngữ nào → ghi sheet rỗng với schema chuẩn
            if not total_counts:
                lang_df = pd.DataFrame(columns=["ngon_ngu", "so_tin", "ty_le(%)", "top_nganh", "so_tin_top_nganh"])
            else:
                # Bảng tổng: mỗi ngôn ngữ + số dòng có nhắc đến ngôn ngữ đó
                lang_df = (
                    pd.DataFrame([{"ngon_ngu": k, "so_tin": v} for k, v in total_counts.items()])
                    .sort_values("so_tin", ascending=False, ignore_index=True)
                )
                # Tỷ lệ trên tổng số dòng input (không phải trên số dòng phát hiện)
                lang_df["ty_le(%)"] = (lang_df["so_tin"] / max(total_rows, 1) * 100).round(2)

                # Tính "top ngành" cho từng ngôn ngữ (không ghi sheet chi tiết pairs)
                pairs_df = pd.DataFrame(pairs_rows)
                top_per_lang = (
                    pairs_df.groupby(["ngon_ngu", "nganh"]).size().reset_index(name="so_tin_top_nganh")
                    .sort_values(["ngon_ngu", "so_tin_top_nganh", "nganh"], ascending=[True, False, True])
                    .groupby("ngon_ngu", as_index=False).head(1)
                    .rename(columns={"nganh": "top_nganh"})
                )
                lang_df = lang_df.merge(top_per_lang, on="ngon_ngu", how="left")

                # Dòng tổng quan: tổng số lần phát hiện và tỷ lệ trên tổng mẫu
                total_detected = int(lang_df["so_tin"].sum())
                summary_row = {
                    "ngon_ngu": "tổng quan",
                    "so_tin": total_detected,
                    "ty_le(%)": round(total_detected / max(total_rows, 1) * 100.0, 2),
                    "top_nganh": pd.NA,
                    "so_tin_top_nganh": pd.NA,
                }
                lang_df = pd.concat([lang_df, pd.DataFrame([summary_row])], ignore_index=True)

            # Ghi sheet 'ngon_ngu_cv' (thay thế sheet nếu đã tồn tại; không tạo các sheet khác)
            try:
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    lang_df.to_excel(writer, sheet_name="ngon_ngu_cv", index=False)
                print(f"📄 Đã lưu {len(lang_df)} dòng (sheet: 'ngon_ngu_cv').")
            except Exception as e:
                # Bắt lỗi ghi file Excel của bước ngôn ngữ
                print(f"❌ Lỗi khi ghi Excel (sheet ngon_ngu_cv): {e}")

        # ============================================================================
        # ==== 8) Thống kê trình độ học vấn (không tách alias) ====
        EDU_COL = "trinh_do_hoc_van"
        TARGET_COL = "nganh"  # Đã dùng ở trên làm cột ngành chuẩn để groupby

        if EDU_COL not in df.columns:
            print(f"⚠️ Bỏ qua thống kê học vấn (thiếu cột '{EDU_COL}').")
        else:
            total_rows = len(df)

            # Chuẩn hoá nhẹ: ép str + trim, nếu rỗng thì gán 'no_info' để tránh NaN khi groupby/pivot
            s_edu = (
                df[EDU_COL]
                .astype(str).str.strip()
                .str.replace(r"^\s*$", "no_info", regex=True)
            )
            s_nganh = (
                df[TARGET_COL]
                .astype(str).str.strip()
                .str.replace(r"^\s*$", "no_info", regex=True)
            )

            # Đếm số tin theo trình độ học vấn (mức độ xuất hiện từng giá trị)
            edu_counts = s_edu.value_counts(dropna=False).rename_axis("trinh_do").reset_index(name="so_tin")
            edu_counts = edu_counts.sort_values("so_tin", ascending=False, ignore_index=True)

            # Tỷ lệ % trên tổng số dòng đầu vào
            edu_counts["ty_le(%)"] = (edu_counts["so_tin"] / max(total_rows, 1) * 100).round(2)

            # Tìm "ngành có nhiều tin nhất" cho từng trình độ (không ghi sheet pairs chi tiết)
            pairs_df = (
                pd.DataFrame({"trinh_do": s_edu, "nganh": s_nganh})
                .groupby(["trinh_do", "nganh"]).size().reset_index(name="so_tin_top_nganh")
            )
            top_per_edu = (
                pairs_df.sort_values(["trinh_do", "so_tin_top_nganh", "nganh"], ascending=[True, False, True])
                .groupby("trinh_do", as_index=False).head(1)
                .rename(columns={"nganh": "top_nganh"})
            )

            edu_df = edu_counts.merge(top_per_edu, on="trinh_do", how="left")

            # Dòng TỔNG QUAN: tổng số lần xuất hiện + tỷ lệ trên tổng mẫu (không có top_nganh)
            total_detected = int(edu_df["so_tin"].sum())
            summary_row = {
                "trinh_do": "TỔNG QUAN",
                "so_tin": total_detected,
                "ty_le(%)": round(total_detected / max(total_rows, 1) * 100.0, 2),
                "top_nganh": pd.NA,
                "so_tin_top_nganh": pd.NA,
            }
            edu_df = pd.concat([edu_df, pd.DataFrame([summary_row])], ignore_index=True)

            # Ghi ra sheet Excel 'trinh_do_hoc_van' (replace sheet nếu đã tồn tại)
            try:
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    edu_df.to_excel(writer, sheet_name="trinh_do_hoc_van", index=False)
                print(f"📄 Đã lưu {len(edu_df)} dòng (sheet: 'trinh_do_hoc_van').")
            except Exception as e:
                print(f"❌ Lỗi khi ghi Excel (sheet trinh_do_hoc_van): {e}")
        # ===================================================================================
        # ==== 9) Thống kê loại hình làm việc (sheet: 'loai_hinh_lam_viec') ====
        WORK_COL = "loai_hinh_lam_viec"
        TARGET_COL = "nganh"

        if WORK_COL not in df.columns:
            print(f"⚠️ Bỏ qua thống kê loại hình làm việc (thiếu cột '{WORK_COL}').")
        else:
            total_rows = len(df)

            # Chuẩn hoá nhẹ: ép str + trim, rỗng -> 'no_info'
            s_work = (
                df[WORK_COL]
                .astype(str).str.strip()
                .str.replace(r"^\s*$", "no_info", regex=True)
            )
            s_nganh = (
                df[TARGET_COL]
                .astype(str).str.strip()
                .str.replace(r"^\s*$", "no_info", regex=True)
            )

            # Đếm số tin theo loại hình làm việc
            work_counts = s_work.value_counts(dropna=False).rename_axis("loai_hinh").reset_index(name="so_tin")
            work_counts = work_counts.sort_values("so_tin", ascending=False, ignore_index=True)

            # Tỷ lệ % trên tổng số dòng
            work_counts["ty_le(%)"] = (work_counts["so_tin"] / max(total_rows, 1) * 100).round(2)

            # Tìm ngành phổ biến nhất cho từng loại hình (không ghi sheet pairs)
            pairs_df = (
                pd.DataFrame({"loai_hinh": s_work, "nganh": s_nganh})
                .groupby(["loai_hinh", "nganh"]).size().reset_index(name="so_tin_top_nganh")
            )
            top_per_work = (
                pairs_df.sort_values(["loai_hinh", "so_tin_top_nganh", "nganh"], ascending=[True, False, True])
                .groupby("loai_hinh", as_index=False).head(1)
                .rename(columns={"nganh": "top_nganh"})
            )

            work_df = work_counts.merge(top_per_work, on="loai_hinh", how="left")

            # Dòng TỔNG QUAN cho loại hình làm việc
            total_detected = int(work_df["so_tin"].sum())
            summary_row = {
                "loai_hinh": "TỔNG QUAN",
                "so_tin": total_detected,
                "ty_le(%)": round(total_detected / max(total_rows, 1) * 100.0, 2),
                "top_nganh": pd.NA,
                "so_tin_top_nganh": pd.NA,
            }
            work_df = pd.concat([work_df, pd.DataFrame([summary_row])], ignore_index=True)

            # Ghi ra sheet 'loai_hinh_lam_viec' (replace sheet nếu đã tồn tại)
            try:
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    work_df.to_excel(writer, sheet_name="loai_hinh_lam_viec", index=False)
                print(f"📄 Đã lưu {len(work_df)} dòng (sheet: 'loai_hinh_lam_viec').")
            except Exception as e:
                print(f"❌ Lỗi khi ghi Excel (sheet loai_hinh_lam_viec): {e}")

        # =============================================================================
        # ==== 10) Thống kê độ tuổi theo ngành & nhóm tuổi ====
        TARGET_COL = "nganh"  # Tên cột ngành để group các thống kê theo ngành

        def _no_accent(s: str) -> str:
            # Chuẩn hoá không dấu, lower, trim; None -> ""
            if s is None:
                return ""
            s = unicodedata.normalize("NFD", str(s))
            return "".join(ch for ch in s if unicodedata.category(ch) != "Mn").lower().strip()

        def _to_num(x):
            # Ép kiểu số an toàn (lỗi -> NaN)
            try:
                return pd.to_numeric(x, errors="coerce")
            except Exception:
                return np.nan

        def _extract_ages_from_text(s: str):
            """
            Trích các số (có thể có dấu phẩy làm thập phân, nghìn) từ chuỗi 'độ tuổi'.
            Ví dụ: '18-24', 'Trên 35', '1.000' → chuẩn hoá rồi lấy các số float.
            """
            if not s:
                return []
            s1 = str(s)
            s1 = s1.replace(",", ".")
            s1 = re.sub(r"(?<=\d)\.(?=\d{3}\b)", "", s1)  # Ví dụ '1.000' -> '1000'
            nums = re.findall(r"\d+(?:\.\d+)?", s1)
            try:
                vals = [float(n) for n in nums]
            except Exception:
                vals = []
            return vals

        def _representative_age(row):
            """
            Tuổi đại diện (dùng để bucket hoá):
            - Ưu tiên med_tuoi (nếu có).
            - Nếu có min/max thì lấy trung bình (min+max)/2.
            - Nếu không, parse từ 'do_tuoi' tự do bằng regex số.
            - Không suy được → NaN.
            """
            minv = _to_num(row.get("min_tuoi")) if "min_tuoi" in row else np.nan
            maxv = _to_num(row.get("max_tuoi")) if "max_tuoi" in row else np.nan
            medv = _to_num(row.get("med_tuoi")) if "med_tuoi" in row else np.nan
            if pd.notna(medv):
                return float(medv)
            if pd.notna(minv) or pd.notna(maxv):
                a = minv if pd.notna(minv) else maxv
                b = maxv if pd.notna(maxv) else minv
                if pd.notna(a) and pd.notna(b):
                    return float((a + b) / 2.0)
                return float(a) if pd.notna(a) else float("nan")
            text = str(row.get("do_tuoi")) if "do_tuoi" in row else ""
            vals = _extract_ages_from_text(text)
            if vals:
                return float(np.mean(vals))
            return np.nan

        def _is_no_info_age(row):
            """
            Cờ 'không yêu cầu/không giới hạn tuổi' hoặc không suy được tuổi đại diện.
            Dùng để đẩy vào bucket 'no_info'.
            """
            text = _no_accent(row.get("do_tuoi", ""))
            if "khong yeu cau" in text or "khong gioi han" in text or "khong bat buoc" in text:
                return True
            rep = row.get("_age_rep")
            return not (pd.notna(rep) and np.isfinite(rep))

        def _bucket_age(val):
            # Phân bucket tuổi theo khoảng: 15–24, 25–34, 35–54, 55+, hoặc None (nếu không xếp được)
            if pd.isna(val) or not np.isfinite(val):
                return None
            if 15 <= val <= 24:
                return "15–24"
            if 24 < val <= 34:
                return "25–34"
            if 34 < val <= 54:
                return "35–54"
            if val > 54:
                return "55+"
            return None

        if TARGET_COL not in df.columns:
            print(f"⚠️ Bỏ qua thống kê độ tuổi: thiếu cột '{TARGET_COL}'.")
        else:
            # 1) Chuẩn hoá ngành (rỗng -> 'no_info')
            s_nganh = (
                df[TARGET_COL].astype(str).str.strip()
                .replace(r"^\s*$", "no_info", regex=True)
            )

            # 2) Tính tuổi đại diện cho từng dòng (phục vụ phân bucket)
            df["_age_rep"] = df.apply(_representative_age, axis=1)

            # Ép kiểu số nguồn (nếu có cột) để tính min/max/mean theo ngành
            if "min_tuoi" in df.columns:
                df["_min_src"] = pd.to_numeric(df["min_tuoi"], errors="coerce")
            else:
                df["_min_src"] = np.nan
            if "max_tuoi" in df.columns:
                df["_max_src"] = pd.to_numeric(df["max_tuoi"], errors="coerce")
            else:
                df["_max_src"] = np.nan
            if "med_tuoi" in df.columns:
                df["_med_src"] = pd.to_numeric(df["med_tuoi"], errors="coerce")
            else:
                df["_med_src"] = np.nan

            # Thống kê theo ngành: min/max/mean (mean dựa trên med_tuoi nếu có)
            age_stats = (
                pd.DataFrame({
                    TARGET_COL: s_nganh,
                    "_min_src": df["_min_src"],
                    "_max_src": df["_max_src"],
                    "_med_src": df["_med_src"],
                })
                .groupby(TARGET_COL, dropna=False)
                .agg(
                    min_tuoi=("_min_src", "min"),
                    max_tuoi=("_max_src", "max"),
                    mean_tuoi=("_med_src", "mean"),
                )
                .reset_index()
            )
            if not age_stats.empty:
                age_stats["mean_tuoi"] = age_stats["mean_tuoi"].round(2)

            # 4) Đếm số tin theo nhóm tuổi (bao gồm 'no_info')
            df["_no_info_age"] = df.apply(_is_no_info_age, axis=1)
            df["_age_bucket"] = df["_age_rep"].apply(_bucket_age)
            bucket_series = np.where(df["_no_info_age"], "no_info", df["_age_bucket"].astype(object))
            bucket_series = pd.Series(bucket_series).fillna("no_info")

            buck_tbl = pd.DataFrame({
                TARGET_COL: s_nganh,
                "age_bucket": bucket_series
            })

            bucket_order = ["15–24", "25–34", "35–54", "55+", "no_info"]

            # Bảng đếm dài → pivot rộng theo bucket
            pivot_counts = (
                buck_tbl.groupby([TARGET_COL, "age_bucket"], dropna=False)
                .size()
                .rename("so_tin")
                .reset_index()
            )

            counts_wide = (
                pivot_counts.pivot(index=TARGET_COL, columns="age_bucket", values="so_tin")
                .reindex(columns=bucket_order)
                .fillna(0)
                .astype(int)
                .reset_index()
            )
            counts_wide["tong_so_tin"] = counts_wide[bucket_order].sum(axis=1)

            # 5) Gộp thống kê min/max/mean + bảng đếm bucket → do_tuoi_df
            do_tuoi_df = (
                counts_wide.merge(age_stats, on=TARGET_COL, how="left")
                .loc[:, [TARGET_COL, "min_tuoi", "max_tuoi", "mean_tuoi"] + bucket_order + ["tong_so_tin"]]
            )

            # 6) Dòng TỔNG: tính từ đúng cột nguồn (toàn bộ file)
            total_row = {
                TARGET_COL: "TỔNG",
                "min_tuoi": (pd.to_numeric(df["_min_src"], errors="coerce").dropna().min()
                             if "_min_src" in df.columns else np.nan),
                "max_tuoi": (pd.to_numeric(df["_max_src"], errors="coerce").dropna().max()
                             if "_max_src" in df.columns else np.nan),
                "mean_tuoi": (round(float(pd.to_numeric(df["_med_src"], errors="coerce").dropna().mean()), 2)
                              if "_med_src" in df.columns and pd.to_numeric(df["_med_src"],
                                                                            errors="coerce").notna().any()
                              else np.nan),
            }
            for b in bucket_order + ["tong_so_tin"]:
                total_row[b] = int(do_tuoi_df[b].sum()) if b in do_tuoi_df.columns else 0

            # 7) Dòng TOP NGÀNH: ngành có số tin lớn nhất theo từng bucket
            top_row = {TARGET_COL: "TOP NGÀNH", "min_tuoi": pd.NA, "max_tuoi": pd.NA, "mean_tuoi": pd.NA}
            for b in bucket_order:
                if b in do_tuoi_df.columns and not do_tuoi_df.empty:
                    idx = do_tuoi_df[b].astype(int).idxmax()
                    if pd.notna(idx):
                        top_ind = do_tuoi_df.loc[idx, TARGET_COL]
                        top_val = int(do_tuoi_df.loc[idx, b])
                        top_row[b] = f"{top_ind} ({top_val})"
                    else:
                        top_row[b] = pd.NA
                else:
                    top_row[b] = pd.NA
            top_row["tong_so_tin"] = pd.NA

            # 8) Thêm 2 dòng tổng & top vào cuối, ghi sheet 'do_tuoi'
            add_rows = pd.DataFrame([total_row, top_row]).dropna(axis=1, how="all")
            do_tuoi_out = pd.concat([do_tuoi_df, add_rows], ignore_index=True)

            try:
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    do_tuoi_out.to_excel(writer, sheet_name="do_tuoi", index=False)
                print(f"📄 Đã lưu {len(do_tuoi_out)} dòng (sheet: 'do_tuoi').")
            except Exception as e:
                # Ghi log nếu lỗi khi ghi sheet độ tuổi
                print(f"❌ Lỗi khi ghi Excel (do_tuoi): {e}")

    #===========================================================================
        # ==== 11) Phân tích Ngày làm việc theo ngành (sheet: ngay_lam_viec) ====
        NGAY_COL = "ngay_lam_viec"  # Sửa lỗi chính tả (tên cột chuẩn)
        SONGAY_COL = "so_ngay_lam"

        # Alias chuẩn hoá để tránh lỗi scope/closure:
        # Ưu tiên dùng _norm_text_no_accent; nếu không có thì fallback sang _no_accent; cuối cùng tự định nghĩa.
        try:
            _NORM = _norm_text_no_accent  # Đã có sẵn ở các phần trước
        except NameError:
            try:
                _NORM = _no_accent  # Fallback nếu dự án dùng tên này
            except NameError:
                import unicodedata as _ud
                def _NORM(s: str) -> str:  # Fallback cuối cùng: normalize không dấu + lower + trim
                    if s is None:
                        return ""
                    t = _ud.normalize("NFD", str(s))
                    t = "".join(ch for ch in t if _ud.category(ch) != "Mn")
                    return t.lower().strip()

        def _to_int_safe(x):
            # Ép kiểu số nguyên an toàn; lỗi/NaN -> None
            try:
                v = pd.to_numeric(x, errors="coerce")
                if pd.isna(v):
                    return None
                return int(round(float(v)))
            except Exception:
                return None

        def _is_no_info_text(t: str) -> bool:
            # Xác định chuỗi "không thông tin/không yêu cầu" theo tập các cụm từ phổ biến
            if not t:
                return True
            if t in {"", "na", "no_info"}:
                return True
            return any(pat in t for pat in [
                "khong yeu cau", "khong bat buoc", "khong ro", "khong xac dinh",
                "khong de cap", "khong quy dinh", "bo qua", "khong co thong tin"
            ])

        def _classify_ngay(text_raw: str, so_ngay):
            """
            Phân loại nhóm ngày làm việc:
            - Ưu tiên rule dựa trên text (T2–T6 / T2–T7) sau khi chuẩn hoá.
            - Nếu text mơ hồ → dùng so_ngay_lam (5 → T2–T6; 6/7 → T2–T7).
            - Nếu vẫn không xác định → 'no_info'; còn lại → 'Khac'.
            """
            t = _NORM(text_raw)
            t_compact = t.replace(" ", "").replace("thu", "t")  # "thứ 2" → "t2"

            # 0) Trường hợp không có thông tin
            if _is_no_info_text(t) and (so_ngay is None):
                return "no_info"

            # 1) Nhận dạng trực tiếp qua text
            if ("t2" in t_compact and "t6" in t_compact) and ("t7" not in t_compact):
                return "T2-T6"
            if "t2" in t_compact and "t7" in t_compact:
                return "T2-T7"

            # Alias thường gặp cho từng nhóm
            alias_t2t6 = [
                "thu 2 - thu 6", "thu hai den thu sau", "tu thu 2 den thu 6",
                "t2-t6", "t2 den t6", "lam hanh chinh", "hanh chinh"
            ]
            alias_t2t7 = [
                "thu 2 - thu 7", "thu hai den thu bay", "tu thu 2 den thu 7",
                "t2-t7", "t2 den t7", "6 ngay/tuan", "7 ngay/tuan", "lam ca thu 7"
            ]
            if any(a.replace(" ", "") in t_compact for a in alias_t2t6):
                return "T2-T6"
            if any(a.replace(" ", "") in t_compact for a in alias_t2t7):
                return "T2-T7"

            # 2) Sử dụng số ngày nếu text không rõ
            if so_ngay is not None:
                if so_ngay == 5:
                    return "T2-T6"
                if so_ngay in (6, 7):
                    return "T2-T7"

            # 3) Không rõ ràng → 'no_info'
            if _is_no_info_text(t) or (so_ngay is None and not t):
                return "no_info"

            # 4) Các trường hợp còn lại gán 'Khac'
            return "Khac"

        # Kiểm tra cột nguồn bắt buộc
        if TARGET_COL not in df.columns:
            print(f"⚠️ Bỏ qua phân tích 'ngay_lam_viec': thiếu cột '{TARGET_COL}'.")
        elif NGAY_COL not in df.columns and SONGAY_COL not in df.columns:
            print(f"⚠️ Bỏ qua phân tích 'ngay_lam_viec': thiếu cả '{NGAY_COL}' và '{SONGAY_COL}'.")
        else:
            # 1) Chuẩn hoá ngành (rỗng → 'no_info')
            s_nganh = (
                df[TARGET_COL].astype(str).str.strip()
                .replace(r"^\s*$", "no_info", regex=True)
            )

            # 2) Lấy 2 cột nguồn (broadcast đúng index, điền mặc định khi thiếu)
            s_ngay_raw = df.get(NGAY_COL, pd.Series("", index=df.index)).astype(str).reindex(df.index, fill_value="")
            s_so_ngay = df.get(SONGAY_COL, pd.Series(np.nan, index=df.index)).reindex(df.index).map(_to_int_safe)

            # 3) Phân loại nhóm ngày + lưu text đã chuẩn hoá (phục vụ "Chi_tiet_khac")
            nhom_vals, ngay_raw_norm_vals = [], []
            for txt, sn in zip(s_ngay_raw, s_so_ngay):
                nhom = _classify_ngay(txt, sn)
                nhom_vals.append(nhom)
                ngay_raw_norm_vals.append(_NORM(txt) or ("so_ngay=" + (str(sn) if sn is not None else "na")))

            df["_ngay_nhom"] = pd.Series(nhom_vals, index=df.index)
            df["_ngay_raw_norm"] = pd.Series(ngay_raw_norm_vals, index=df.index)

            # 4) Đếm theo ngành x nhóm (bao gồm 'no_info')
            bucket_order = ["T2-T6", "T2-T7", "Khac", "no_info"]

            grp = (
                pd.DataFrame({TARGET_COL: s_nganh, "_ngay_nhom": df["_ngay_nhom"]})
                .groupby([TARGET_COL, "_ngay_nhom"], dropna=False)
                .size()
                .rename("so_tin")
                .reset_index()
            )

            counts_wide = (
                grp.pivot(index=TARGET_COL, columns="_ngay_nhom", values="so_tin")
                .reindex(columns=bucket_order)
                .fillna(0)
                .astype(int)
                .reset_index()
            )

            # 5) Chi tiết 'Khac': gộp giá trị raw chuẩn hoá kèm danh sách link (nếu có)
            #    Output: "gia_tri_khac | url1; url2, gia_tri_khac_2 | url3"
            def _pick_href_col(_df):
                # Tự động chọn cột link phù hợp nếu tồn tại
                for c in ["href", "link", "url", "job_url", "job_href", "source_url"]:
                    if c in _df.columns:
                        return c
                return None

            href_col = _pick_href_col(df)

            d_khac = pd.DataFrame({
                TARGET_COL: s_nganh,
                "_ngay_nhom": df["_ngay_nhom"],
                "_ngay_raw_norm": df["_ngay_raw_norm"],
                "_href": (df[href_col].astype(str) if href_col else pd.Series("", index=df.index))
            })

            d_khac = d_khac.loc[d_khac["_ngay_nhom"] == "Khac"].copy()
            d_khac["_href"] = d_khac["_href"].fillna("").str.strip()
            d_khac["_ngay_raw_norm"] = d_khac["_ngay_raw_norm"].fillna("").str.strip()
            d_khac = d_khac.loc[
                (d_khac["_ngay_raw_norm"] != "") &
                (~d_khac["_ngay_raw_norm"].isin({"na", "no_info"}))
                ]

            if not d_khac.empty:
                # Gom theo (ngành, giá trị khác) → gộp link (unique) bằng "; "
                agg = (
                    d_khac.groupby([TARGET_COL, "_ngay_raw_norm"])["_href"]
                    .apply(lambda s: "; ".join(sorted(set([x for x in s if x]))))
                    .reset_index()
                )
                agg["pair"] = agg.apply(
                    lambda r: f"{r['_ngay_raw_norm']} | {r['_href']}" if r[
                        "_href"] else f"{r['_ngay_raw_norm']} | (no_link)",
                    axis=1
                )
                khac_detail = (
                    agg.groupby(TARGET_COL)["pair"]
                    .apply(lambda s: ", ".join(s))
                    .rename("Chi_tiet_khac")
                    .reset_index()
                )
            else:
                # Không có bản ghi 'Khac' → bảng chi tiết rỗng
                khac_detail = pd.DataFrame({TARGET_COL: [], "Chi_tiet_khac": []})

            # === Gộp thành bảng nền kết quả (counts_wide + Chi_tiet_khac) ===
            kq = counts_wide.merge(khac_detail, on=TARGET_COL, how="left")
            kq["Chi_tiet_khac"] = kq["Chi_tiet_khac"].fillna("")
            kq["tong_so_tin"] = kq[bucket_order].sum(axis=1)

            # 6) Dòng TỔNG: tổng số tin theo từng bucket và tổng toàn bảng
            total_row = {
                TARGET_COL: "TỔNG",
                **{b: int(kq[b].sum()) if b in kq.columns else 0 for b in bucket_order},
                "Chi_tiet_khac": "",
                "tong_so_tin": int(kq["tong_so_tin"].sum()) if "tong_so_tin" in kq.columns else 0,
            }

            # 7) Dòng TOP NGÀNH: ngành có số tin lớn nhất ở mỗi bucket
            top_row = {TARGET_COL: "TOP NGÀNH", "Chi_tiet_khac": "", "tong_so_tin": pd.NA}
            if not kq.empty:
                for b in bucket_order:
                    if b in kq.columns and kq[b].notna().any():
                        idx = kq[b].astype(int).idxmax()
                        top_ind = kq.loc[idx, TARGET_COL]
                        top_val = int(kq.loc[idx, b])
                        top_row[b] = f"{top_ind} ({top_val})"
                    else:
                        top_row[b] = pd.NA
            else:
                for b in bucket_order:
                    top_row[b] = pd.NA

            add_rows = pd.DataFrame([total_row, top_row]).dropna(axis=1, how="all")
            ngaylv_out = pd.concat([kq, add_rows], ignore_index=True)

            # 8) Ghi Excel: sheet 'ngay_lam_viec' (replace sheet nếu đã tồn tại)
            try:
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    ngaylv_out.to_excel(writer, sheet_name="ngay_lam_viec", index=False)
                print(f"📄 Đã lưu {len(ngaylv_out)} dòng (sheet: 'ngay_lam_viec').")
            except Exception as e:
                # Log lỗi khi ghi file
                print(f"❌ Lỗi khi ghi Excel (ngay_lam_viec): {e}")
        # ===========================================================================
        # ==== 12 Thống kê giờ làm việc theo ngành ====
        TARGET_COL = "nganh"  # Tên cột ngành để groupby
        COL_START = "gio_bat_dau"  # Giờ bắt đầu (hỗ trợ nhiều định dạng: 8, 08:00, 8h30, 0830, 8.30, ...)
        COL_END = "gio_ket_thuc"  # Giờ kết thúc (các định dạng tương tự COL_START)
        COL_HOURS = "so_gio_lam_ngay"  # Số giờ làm/ngày (giá trị số)

        def _safe_str(x) -> str:
            return "" if pd.isna(x) else str(x).strip()

        def _parse_time_to_minutes(x):
            # Chuẩn hoá chuỗi giờ về phút kể từ 00:00 (None nếu không parse được)
            s = _safe_str(x).lower().replace(".", ":")
            if not s:
                return None

            # Quy về dạng chung: '8h30' -> '8:30', '8h' -> '8:00'; loại bỏ ký tự lạ ngoài [0-9:]
            s = re.sub(r"h", ":", s)
            s = re.sub(r"[^\d:]", "", s)  # chỉ giữ chữ số và ':'

            # Trường hợp toàn số (không có ':'): '8', '0830', '800'
            if s.isdigit():
                try:
                    if len(s) <= 2:
                        hh = int(s)
                        if 0 <= hh <= 24:
                            return hh * 60
                    elif len(s) == 3:
                        hh = int(s[0])
                        mm = int(s[1:])
                        if 0 <= hh <= 24 and 0 <= mm < 60:
                            return hh * 60 + mm
                    elif len(s) == 4:
                        hh = int(s[:2]);
                        mm = int(s[2:])
                        if 0 <= hh <= 24 and 0 <= mm < 60:
                            return hh * 60 + mm
                except:
                    return None
                return None

            # Các dạng có ':' (ví dụ '8:30', '08:00', '8:')
            try:
                parts = s.split(":")
                if len(parts) == 1:
                    hh = int(parts[0]);
                    mm = 0
                elif len(parts) >= 2:
                    hh = int(parts[0]) if parts[0] else 0
                    mm = int(parts[1]) if parts[1] else 0
                else:
                    return None

                if hh == 24 and mm == 0:
                    return 24 * 60  # cho phép '24:00'
                if 0 <= hh <= 23 and 0 <= mm < 60:
                    return hh * 60 + mm
            except:
                return None
            return None

        def _fmt_minutes(m):
            # Định dạng phút -> 'HH:MM' (giới hạn 00:00..24:00; None/NaN -> "")
            if m is None or (isinstance(m, float) and math.isnan(m)):
                return ""
            m = int(m)
            if m < 0: return ""
            if m > 24 * 60: m = 24 * 60
            hh = m // 60;
            mm = m % 60
            return f"{hh:02d}:{mm:02d}"

        def _to_float(x):
            # Ép kiểu số float an toàn (Không hợp lệ -> None)
            try:
                v = pd.to_numeric(x, errors="coerce")
                return float(v) if not pd.isna(v) else None
            except:
                return None

        # Kiểm tra các cột bắt buộc; nếu thiếu -> bỏ qua toàn bộ thống kê giờ làm
        if TARGET_COL not in df.columns or COL_START not in df.columns or COL_END not in df.columns or COL_HOURS not in df.columns:
            print(
                f"⚠️ Bỏ qua thống kê giờ làm: thiếu cột bắt buộc. Cần có: '{TARGET_COL}', '{COL_START}', '{COL_END}', '{COL_HOURS}'.")
        else:
            d = df.copy()

            # Chuẩn hoá ngành (rỗng -> 'no_info' để tránh NaN khi group/pivot)
            s_nganh = (
                d[TARGET_COL].astype(str).str.strip()
                .replace(r"^\s*$", "no_info", regex=True)
            )

            # Parse giờ bắt đầu/kết thúc -> phút; số giờ làm -> float
            d["_start_min"] = d[COL_START].map(_parse_time_to_minutes)
            d["_end_min"] = d[COL_END].map(_parse_time_to_minutes)
            d["_hours"] = d[COL_HOURS].map(_to_float)

            # Hàm agg an toàn (bỏ None/NaN trước khi tính)
            def _min_ignore_null(series):
                s = pd.to_numeric(pd.Series([v for v in series if v is not None]), errors="coerce").dropna()
                return s.min() if len(s) else None

            def _max_ignore_null(series):
                s = pd.to_numeric(pd.Series([v for v in series if v is not None]), errors="coerce").dropna()
                return s.max() if len(s) else None

            def _mean_ignore_null(series):
                s = pd.to_numeric(pd.Series([v for v in series if v is not None]), errors="coerce").dropna()
                return float(s.mean()) if len(s) else None

            # 1) Thống kê theo ngành: giờ bắt đầu/kết thúc sớm-muộn nhất, TB số giờ/ngày, số tin
            glv_stats = (
                pd.DataFrame(
                    {TARGET_COL: s_nganh, "_start_min": d["_start_min"], "_end_min": d["_end_min"],
                     "_hours": d["_hours"]})
                .groupby(TARGET_COL, dropna=False)
                .agg(
                    gio_bat_dau_som_nhat=("_start_min", _min_ignore_null),
                    gio_bat_dau_muon_nhat=("_start_min", _max_ignore_null),
                    gio_ket_thuc_som_nhat=("_end_min", _min_ignore_null),
                    gio_ket_thuc_muon_nhat=("_end_min", _max_ignore_null),
                    tb_so_gio_lam_ngay=("_hours", _mean_ignore_null),
                    so_tin=("._start_min".replace(".", ""), "size"),
                    # Đếm bản ghi theo group (kỹ thuật đặt tên để lấy size)
                )
                .reset_index()
            )

            # 2) Định dạng thời gian (HH:MM) & làm tròn số giờ
            for c in ["gio_bat_dau_som_nhat", "gio_bat_dau_muon_nhat", "gio_ket_thuc_som_nhat",
                      "gio_ket_thuc_muon_nhat"]:
                glv_stats[c] = glv_stats[c].map(_fmt_minutes)
            glv_stats["tb_so_gio_lam_ngay"] = glv_stats["tb_so_gio_lam_ngay"].map(
                lambda x: None if x is None else round(float(x), 2))

            # 3) Dòng TỔNG QUAN (toàn bộ dataset)
            overall_start_min_min = _min_ignore_null(d["_start_min"])
            overall_start_min_max = _max_ignore_null(d["_start_min"])
            overall_end_min_min = _min_ignore_null(d["_end_min"])
            overall_end_min_max = _max_ignore_null(d["_end_min"])
            overall_hours_mean = _mean_ignore_null(d["_hours"])
            overall_count = int(len(d))

            total_row = {
                TARGET_COL: "TỔNG QUAN",
                "gio_bat_dau_som_nhat": _fmt_minutes(overall_start_min_min),
                "gio_bat_dau_muon_nhat": _fmt_minutes(overall_start_min_max),
                "gio_ket_thuc_som_nhat": _fmt_minutes(overall_end_min_min),
                "gio_ket_thuc_muon_nhat": _fmt_minutes(overall_end_min_max),
                "tb_so_gio_lam_ngay": None if overall_hours_mean is None else round(float(overall_hours_mean), 2),
                "so_tin": overall_count,
            }

            glv_out = pd.concat([glv_stats, pd.DataFrame([total_row])], ignore_index=True)

            # 4) Ghi Excel (sheet: 'gio_lam_viec'); nếu sheet tồn tại sẽ replace
            try:
                with pd.ExcelWriter(out_phantich, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                    glv_out.to_excel(writer, sheet_name="gio_lam_viec", index=False)
                print(f"📄 Đã lưu {len(glv_out)} dòng (sheet: 'gio_lam_viec').")
            except Exception as e:
                print(f"❌ Lỗi khi ghi Excel (gio_lam_viec): {e}")
            # =========================================================================
            # ==== 13) Phân tích kỹ năng (sheet: ky_nang & ky_nang_theo_nganh) ====
            from sentence_transformers import SentenceTransformer, util
            SKILL_COL = "ky_nang"
            TARGET_COL = "nganh"

            # Tập ký tự có dấu tiếng Việt (đủ rộng; không cần liệt kê đầy đủ)
            _VN_DIACRITICS = set("ăâđêôơưáàảãạắằẳẵặấầẩẫậéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ")

            def _has_vn_diacritic(s: str) -> bool:
                # Có chứa bất kỳ ký tự có dấu tiếng Việt?
                return any(ch.lower() in _VN_DIACRITICS for ch in s)

            def _has_cjk(s: str) -> bool:
                # Có ký tự CJK Unified Ideographs (Trung/Nhật/Hàn)?
                return any('\u4e00' <= ch <= '\u9fff' for ch in s)

            _ASCII_LETTERS_RE = re.compile(r"[A-Za-z]")

            def _lang_rank(s: str) -> int:
                # Ưu tiên ngôn ngữ đại diện khi chọn "canon" cho cụm đồng nghĩa:
                # 0 = English (ASCII & có ít nhất 1 chữ cái), 1 = Vietnamese (có dấu), 2 = Others
                if not s:
                    return 2
                if all(ord(ch) < 128 for ch in s) and _ASCII_LETTERS_RE.search(s):
                    return 0
                if _has_vn_diacritic(s):
                    return 1
                return 2

            # ---------- Helpers chuẩn hoá ----------
            def _no_accent_lower(s: str) -> str:
                # Bỏ dấu + lower + trim (dùng cho một số so khớp mềm)
                if s is None:
                    return ""
                s = unicodedata.normalize("NFD", str(s))
                s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
                return s.strip().lower()

            # Các kỹ năng ngắn nhưng hợp lệ vẫn giữ (tránh lọc nhầm)
            ALLOWED_SHORT = {
                "c", "r", "go", "qa", "ui", "ux", "ai", "bi", "qa/qc", "ci", "cd", "git",
            }

            # Chuẩn hoá một số biến thể phổ biến để gom về dạng chung
            def _normalize_form(s: str) -> str:
                s = s.replace("cyber security", "cybersecurity")
                s = s.replace("information security", "infosec")
                s = s.replace("ux ui", "ux/ui")
                s = s.replace("ms office", "microsoft office")
                s = s.replace("bao mat thong tin", "an toan thong tin")
                s = s.replace("an ninh thong tin", "an ninh mang")
                return s

            # Loại bỏ token rác / vô nghĩa (toàn dấu, toàn số, 1 ký tự, ...)
            _punct_re = re.compile(r"^[\W_]+$")

            def is_valid_skill(tok: str) -> bool:
                if not tok:
                    return False
                if tok in ALLOWED_SHORT:
                    return True
                if len(tok) == 1:
                    return False
                if tok.isdigit():
                    return False
                if _punct_re.match(tok):
                    return False
                return True

            # ---------- Kiểm tra cột nguồn ----------
            if SKILL_COL not in df.columns or TARGET_COL not in df.columns:
                print(f"⚠️ Bỏ qua phân tích kỹ năng (thiếu cột '{SKILL_COL}' hoặc '{TARGET_COL}').")
            else:
                try:
                    print("🔎 Đang xử lý kỹ năng...")

                    # Cố gắng nạp sentence-transformers để gom cụm đồng nghĩa; nếu không được sẽ fallback
                    model = None
                    util = None
                    try:
                        from sentence_transformers import SentenceTransformer, util as _util
                        model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
                        util = _util
                        print("✅ Dùng sentence-transformers để gom cụm đồng nghĩa.")
                    except Exception as _e:
                        print(
                            f"ℹ️ Không dùng được sentence-transformers (sẽ gom theo trùng khớp chính xác). Lý do: {_e}")

                    # 1) Chuẩn hoá & tách kỹ năng theo các dấu phân tách thông dụng
                    skill_rows = []
                    splitter = re.compile(r"[,;/\|\n]+")
                    for _, row in df.iterrows():
                        nganh_val = str(row.get(TARGET_COL, "")).strip() or "no_info"
                        raw = "" if pd.isna(row.get(SKILL_COL)) else str(row.get(SKILL_COL))

                        # Chuẩn hoá nhẹ (không bỏ dấu để giữ nguyên tên gốc nếu cần)
                        raw_norm = _normalize_form(raw.lower())
                        skills = [s.strip() for s in splitter.split(raw_norm) if s.strip()]

                        for sk in skills:
                            if not is_valid_skill(sk):
                                continue
                            skill_rows.append((nganh_val, sk))

                    if not skill_rows:
                        print("⚠️ Không tìm thấy kỹ năng hợp lệ để phân tích.")
                    else:
                        # 2) Danh sách kỹ năng duy nhất (bảo toàn thứ tự gặp đầu tiên)
                        all_skills = [s for _, s in skill_rows]
                        unique_skills = list(dict.fromkeys(all_skills))

                        # 3) Gom cụm đồng nghĩa (nếu có model); ngược lại: mỗi skill tự là đại diện
                        if model is not None and util is not None and len(unique_skills) > 1:
                            emb = model.encode(unique_skills, convert_to_tensor=True, show_progress_bar=False)

                            clusters = {}
                            used = set()
                            THRESH = 0.80  # ngưỡng cosine similarity để gom cụm
                            for i, sk in enumerate(unique_skills):
                                if i in used:
                                    continue
                                group = [sk]
                                used.add(i)
                                for j in range(i + 1, len(unique_skills)):
                                    if j in used:
                                        continue
                                    # Độ tương đồng cosine giữa embedding i và j
                                    if util.cos_sim(emb[i], emb[j]).item() >= THRESH:
                                        group.append(unique_skills[j])
                                        used.add(j)
                                # Chọn đại diện (canon) theo: ưu tiên ngôn ngữ → độ dài → thứ tự alpha
                                canon = sorted(
                                    group,
                                    key=lambda x: (_lang_rank(x), len(x), x)
                                )[0]

                                clusters[canon] = group

                            skill2canon = {g: canon for canon, group in clusters.items() for g in group}
                        else:
                            # Fallback: không gom; canon = chính kỹ năng đó
                            skill2canon = {s: s for s in unique_skills}

                        # 4) Bảng đếm (ngành, kỹ năng_đại_diện)
                        df_skill = (
                            pd.DataFrame([(ng, skill2canon.get(sk, sk)) for ng, sk in skill_rows],
                                         columns=["nganh", "skill_list"])
                            .groupby(["nganh", "skill_list"], dropna=False)
                            .size()
                            .reset_index(name="so_tin")
                        )

                        # ---------------- A) Sheet: ky_nang ----------------
                        # Tạo cột top10 kỹ năng nhiều nhất & ít nhất theo từng ngành (liệt kê tên kỹ năng)
                        def _top_join(sub_df: pd.DataFrame, ascending=False, k=10) -> str:
                            if sub_df.empty:
                                return ""
                            srt = sub_df.sort_values(["so_tin", "skill_list"],
                                                     ascending=[ascending, True],
                                                     ignore_index=True)
                            pick = srt.head(k) if ascending else srt.tail(k)
                            pick = pick.sort_values(["so_tin", "skill_list"], ascending=[False, True])
                            return ", ".join(pick["skill_list"].tolist())

                        per_nganh = []
                        for ng, sub in df_skill.groupby("nganh", dropna=False):
                            most10 = _top_join(sub, ascending=False, k=10)
                            least10 = _top_join(sub, ascending=True, k=10)
                            per_nganh.append({
                                "nganh": ng,
                                "top10_ky_nang_nhieu_nhat": most10,
                                "top10_ky_nang_it_nhat": least10
                            })
                        ky_nang_df = pd.DataFrame(per_nganh)

                        # ---------------- B) Sheet: ky_nang_theo_nganh ----------------
                        # Lấy 10 kỹ năng phổ biến nhất toàn cục rồi pivot theo ngành
                        top10_global = (
                            df_skill.groupby("skill_list", dropna=False)["so_tin"]
                            .sum()
                            .sort_values(ascending=False)
                            .head(10)
                            .index
                            .tolist()
                        )

                        # Pivot: index = ngành, columns = kỹ năng đại diện, values = số tin
                        pivot = (
                            df_skill.pivot_table(
                                index="nganh",
                                columns="skill_list",
                                values="so_tin",
                                aggfunc="sum",
                                fill_value=0
                            )
                            .reindex(columns=top10_global, fill_value=0)
                            .reset_index()
                        )
                        pivot.columns.name = None
                        ky_nang_theo_nganh_df = pivot

                        # 5) Ghi Excel: 2 sheet 'ky_nang' và 'ky_nang_theo_nganh' (replace nếu đã có)
                        out_path = Path(out_phantich)
                        out_path.parent.mkdir(parents=True, exist_ok=True)

                        mode = "a" if out_path.exists() else "w"
                        try:
                            with pd.ExcelWriter(out_path, engine="openpyxl", mode=mode,
                                                if_sheet_exists="replace") as writer:
                                ky_nang_df.to_excel(writer, sheet_name="ky_nang", index=False)
                                ky_nang_theo_nganh_df.to_excel(writer, sheet_name="ky_nang_theo_nganh", index=False)
                            print(f"📄 Đã lưu {len(ky_nang_df)} dòng (sheet: 'ky_nang').")
                            print(f"📄 Đã lưu {len(ky_nang_theo_nganh_df)} dòng (sheet: 'ky_nang_theo_nganh').")
                        except Exception as e:
                            print(f"❌ Lỗi khi ghi Excel (sheet ky_nang / ky_nang_theo_nganh): {e}")

                except Exception as e:
                    # Bắt mọi lỗi trong pipeline xử lý kỹ năng để không làm gãy toàn bộ luồng
                    print(f"❌ Lỗi khi xử lý kỹ năng: {e}")
    #==============================================================================
    # ==== 14) Thống kê nhóm phúc lợi theo ngành (ghi sheet: phuc_loi_nhom_theo_nganh) ====
    TARGET_COL = "nganh"
    BENEFIT_GROUP_COL = "nhom_phuc_loi"
    SHEET_NAME = "phuc_loi_nhom_theo_nganh"

    # Yêu cầu: phải có cột ngành và cột "nhóm phúc lợi" (dạng text, có thể chứa nhiều nhóm, ngăn cách bằng "|").
    if TARGET_COL not in df.columns or BENEFIT_GROUP_COL not in df.columns:
        print(f"⚠️ Bỏ qua thống kê phúc lợi: thiếu cột '{TARGET_COL}' hoặc '{BENEFIT_GROUP_COL}'.")
    else:
        splitter_groups = re.compile(r"\s*\|\s*")  # Bộ tách các nhóm trong một ô, cho phép khoảng trắng hai bên

        # Lấy phần TRƯỚC dấu ":" làm tên nhóm chuẩn.
        # Ví dụ ô: "Luong-Thuong: Bonus, Thuong hieu suat/KPI | PhucLoiKhac: Bảo hiểm mở rộng"
        # -> groups = ["Luong-Thuong", "PhucLoiKhac"]
        def parse_groups(cell: str):
            if not isinstance(cell, str) or not cell.strip():
                return []
            groups_raw = splitter_groups.split(cell.strip())
            groups = []
            for part in groups_raw:
                if not part:
                    continue
                # Ví dụ: "Luong-Thuong: Bonus, Thuong hieu suat/KPI"
                g = part.split(":", 1)[0].strip()
                if not g:
                    continue
                groups.append(g)
            # Loại trùng lặp trong CÙNG một bản ghi (bảo toàn thứ tự gặp đầu tiên)
            return list(dict.fromkeys(groups))

        # Trải phẳng dữ liệu: mỗi (ngành, nhóm) là một dòng để đếm
        rows = []
        for _, r in df.iterrows():
            nganh_val = str(r.get(TARGET_COL, "")).strip() or "no_info"
            gs = parse_groups(r.get(BENEFIT_GROUP_COL, ""))
            for g in gs:
                rows.append((nganh_val, g))

        if not rows:
            print("⚠️ Không tìm thấy dữ liệu phúc lợi để thống kê.")
        else:
            # Đếm số tin theo (ngành, nhóm phúc lợi)
            df_benefit = (
                pd.DataFrame(rows, columns=["nganh", "nhom_phuc_loi"])
                .groupby(["nganh", "nhom_phuc_loi"], dropna=False)
                .size()
                .reset_index(name="so_tin")
            )

            # Pivot rộng: hàng = ngành, cột = nhóm phúc lợi, ô = số tin (fill 0)
            pivot = (
                df_benefit.pivot_table(
                    index="nganh",
                    columns="nhom_phuc_loi",
                    values="so_tin",
                    aggfunc="sum",
                    fill_value=0
                )
                .reset_index()
            )
            pivot.columns.name = None  # Bỏ tên trục cột do pivot tạo ra

            # Ghi sheet ra Excel (append nếu file đã tồn tại; replace sheet nếu đã có cùng tên)
            out_path = Path(out_phantich)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            mode = "a" if out_path.exists() else "w"
            try:
                with pd.ExcelWriter(out_path, engine="openpyxl", mode=mode, if_sheet_exists="replace") as w:
                    pivot.to_excel(w, sheet_name=SHEET_NAME, index=False)
                print(f"📄 Đã lưu {len(pivot)} dòng (sheet: '{SHEET_NAME}').")
            except Exception as e:
                print(f"❌ Lỗi khi ghi Excel (sheet {SHEET_NAME}): {e}")
    #=====================================================================
    # ==== 15) Điền khuyết toàn bộ các sheet: null/NaN/chuỗi rỗng -> "Không có thông tin" ====
    try:
        out_path = Path(out_phantich)
        if not out_path.exists():
            print(f"⚠️ Không tìm thấy file phân tích để điền khuyết: {out_path}")
        else:
            # Đọc toàn bộ sheets, giữ nguyên thứ tự
            sheets_dict = pd.read_excel(out_path, sheet_name=None, engine="openpyxl")
            if not sheets_dict:
                print("⚠️ File không có sheet nào.")
            else:
                total_cells_filled = 0
                processed = {}

                def _fill_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
                    """Trả về (df_moi, so_o_duoc_dien)."""
                    if df is None or df.empty:
                        # Nếu sheet trống -> tạo 1 ô thông báo
                        return pd.DataFrame({"Thông tin": ["Không có thông tin"]}), 1

                    # Đếm ô thiếu (NaN/None) ban đầu
                    na_before = df.isna().sum().sum()

                    # Chuẩn hoá: chuỗi rỗng/white-space -> NaN
                    def _norm_empty(x):
                        return np.nan if (isinstance(x, str) and x.strip() == "") else x

                    df2 = df.applymap(_norm_empty)

                    # Điền khuyết mọi NaN/None còn lại
                    df2 = df2.fillna("Không có thông tin")

                    # Số ô được điền = NaN ban đầu + số ô là chuỗi rỗng trước đó
                    # (xấp xỉ: đếm lại số ô "Không có thông tin" trừ đi số ô đã có giá trị này từ trước)
                    # Đơn giản hơn: ước lượng bằng na_before + empty_count
                    empty_count = 0
                    if not df.empty:
                        # Đếm chuỗi rỗng ban đầu
                        empty_count = sum(
                            1
                            for col in df.columns
                            for v in df[col].tolist()
                            if isinstance(v, str) and v.strip() == ""
                        )
                    filled = int(na_before + empty_count)
                    return df2, filled

                for sheet_name, df_sheet in sheets_dict.items():
                    df_filled, filled_cnt = _fill_df(df_sheet)
                    processed[sheet_name] = df_filled
                    total_cells_filled += filled_cnt

                # Ghi đè toàn bộ file, giữ tên sheet như cũ
                with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as w:
                    for sheet_name, df_sheet in processed.items():
                        df_sheet.to_excel(w, sheet_name=sheet_name, index=False)

                print(f"🧩 Đã điền khuyết toàn bộ file: {out_path.name}")
                print(f"   • Số sheet: {len(processed)}")
                print(f"   • Số ô được điền (ước lượng): {total_cells_filled}")
    except Exception as e:
        print(f"❌ Lỗi ở bước điền khuyết (mục 15): {e}")

    # Kết thúc phân tích → trả về đường dẫn file Excel tổng hợp
    return out_phantich

def main():
    # Nếu đặt biến môi trường EXCEL_PATH_ANALYZER → chạy phân tích cho đúng file đó
    file_path_env = os.getenv("EXCEL_PATH_ANALYZER")
    if file_path_env:
        src = Path(file_path_env)
        out = analyze_one_file(src)
        print(f"✅ Hoàn tất: {out}")
        return

    # Nếu không có ENV → tự động quét các file chi tiết mới nhất trong thư mục preprocess và xử lý tuần tự
    files = get_latest_detail_files(PREPROCESS_DIR)
    if not files:
        raise SystemExit(f"❌ Không tìm thấy file nào trong {PREPROCESS_DIR}")

    print(f"[INFO] Sẽ xử lý {len(files)} file:")
    for f in files:
        print(" -", f.name)

    ok, fail = 0, 0
    for fp in files:
        try:
            out = analyze_one_file(fp)
            print(f"✅ Hoàn tất: {out}")
            ok += 1
        except Exception as e:
            print(f"❌ Lỗi khi xử lý {fp.name}: {e}")
            fail += 1

    # Tổng kết kết quả chạy batch
    print("\n========== TỔNG KẾT ==========")
    print(f"✔️ Thành công: {ok}")
    print(f"❌ Thất bại : {fail}")


if __name__ == "__main__":
    main()

