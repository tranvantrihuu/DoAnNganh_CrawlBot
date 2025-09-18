# -*- coding: utf-8 -*-
from pathlib import Path
import requests
import time
from typing import Dict, Any, List, Tuple, Optional, Callable, Iterable
import os, re, sys
from datetime import datetime
import pandas as pd

# Mẫu: job_detail_output_giao duc_g1_1001_2025-09-01_091843.xlsx
# Biểu thức chính quy để bắt tên file chi tiết theo cấu trúc cố định.
# Nhóm đặt tên (slug, gid, loc, date, time) giúp tách thông tin phục vụ xử lý tiếp theo:
#   - slug: tên ngành đã slug hoá (có thể chứa khoảng trắng/underscore), dùng non-greedy (.+?)
#   - gid : group id (chỉ số), bắt \d+ để đảm bảo là số
#   - loc : mã địa điểm 4 chữ số (ví dụ 1001)
#   - date: ngày dạng YYYY-MM-DD
#   - time: giờ dạng HHMMSS
# Lưu ý: re.IGNORECASE để không nhạy hoa/thường; nếu thay đổi mẫu tên tệp → cần cập nhật lại regex này.
FNAME_RE = re.compile(
    r"^job_detail_output_(?P<slug>.+?)_g(?P<gid>\d+)_(?P<loc>\d{4})_(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{6})\.xlsx$",
    re.IGNORECASE
)

def _parse_dt(date_str: str, time_str: str) -> datetime:
    # Chuyển cặp chuỗi (YYYY-MM-DD, HHMMSS) → đối tượng datetime
    # Dùng khi so sánh “mới nhất” giữa các file có cùng (slug, gid, loc).
    # Ưu điểm: so sánh datetime an toàn hơn so sánh chuỗi.
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H%M%S")

def get_latest_detail_files(base_dir: Path) -> list[Path]:
    """
    Quét base_dir, gom nhóm theo (slug, gid, loc), chọn file có timestamp mới nhất.
    Trả về list Path các file mới nhất (mỗi nhóm 1 file).

    Cách làm:
    - Duyệt tất cả tệp trong thư mục; khớp tên bằng FNAME_RE để loại những tệp ngoại lệ.
    - Với mỗi key (slug, gid, loc), lưu lại (datetime, path) mới nhất.
    - Kết quả trả về chỉ còn path của phần tử mới nhất cho từng nhóm.
    """
    latest: dict[tuple, tuple[datetime, Path]] = {}
    if not base_dir.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục: {base_dir}")

    for p in base_dir.iterdir():
        if not p.is_file():
            continue
        m = FNAME_RE.match(p.name)
        if not m:
            continue
        slug = m.group("slug")
        gid = m.group("gid")
        loc = m.group("loc")
        dt = _parse_dt(m.group("date"), m.group("time"))
        key = (slug, gid, loc)
        cur = latest.get(key)
        if cur is None or dt > cur[0]:
            latest[key] = (dt, p)

    return [info[1] for info in latest.values()]

def save_combined_with_timestamp(df: pd.DataFrame, out_dir: Path, prefix: str = "job_detail_output__combined"):
    # Gộp nhiều file → xuất 1 file mới kèm timestamp, tránh ghi đè và giúp truy vết lần chạy.
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_path = out_dir / f"{prefix}_{ts}_processed.xlsx"
    df.to_excel(out_path, index=False)
    return out_path
# ==========================
# TIỀN XỬ LÝ: ĐỔI TÊN CỘT KHÔNG DẤU, NGẮN GỌN
# ==========================
def strip_diacritics(s: str) -> str:
    # chuẩn hoá về ascii: bỏ dấu, lower, gọn khoảng trắng
    # Lưu ý QUAN TRỌNG: Hàm này dùng unicodedata nhưng module chưa được import ở đầu file.
    # Khi chạy thực tế cần `import unicodedata` (không thay vào đây theo yêu cầu “không đổi code”).
    s_norm = unicodedata.normalize("NFD", str(s))
    s_ascii = "".join(ch for ch in s_norm if unicodedata.category(ch) != "Mn")
    s_ascii = s_ascii.replace("Đ", "D").replace("đ", "d")
    return " ".join(s_ascii.lower().split())

def build_column_map():
    """
    Map theo yêu cầu -> tên cột ascii, snake_case, ngắn gọn.
    Dùng key đã strip dấu + lower để khớp robust hơn (tránh lệ thuộc hoa/thường/dấu).
    Ghi chú:
    - Bộ map này phản ánh các trường được bóc ở giai đoạn crawl chi tiết (PHẦN 3).
    - Nếu website đổi nhãn (ví dụ “CẤP BẬC” → “Cấp bậc”), hàm rename vẫn khớp nhờ strip_diacritics.
    """
    vn_to_short = {
        "ID": "id",
        "Tên công việc": "ten_cong_viec",
        "Lương": "luong",
        "Hết hạn": "het_han",
        "Lượt xem": "luot_xem",
        "Địa điểm tuyển dụng": "dia_diem_tuyen_dung",
        "Mô tả công việc": "mo_ta_cong_viec",
        "Yêu cầu công việc": "yeu_cau_cong_viec",
        "Phúc lợi": "phuc_loi",
        "NGÀY ĐĂNG": "ngay_dang",
        "CẤP BẬC": "cap_bac",
        "NGÀNH NGHỀ": "nganh_nghe",
        "KỸ NĂNG": "ky_nang",
        "LĨNH VỰC": "linh_vuc",
        "NGÔN NGỮ TRÌNH BÀY HỒ SƠ": "ngon_ngu_cv",
        "SỐ NĂM KINH NGHIỆM TỐI THIỂU": "so_nam_kinh_nghiem",
        "QUỐC TỊCH": "quoc_tich",
        "TRÌNH ĐỘ HỌC VẤN TỐI THIỂU": "trinh_do_hoc_van",
        "GIỚI TÍNH": "gioi_tinh",
        "ĐỘ TUỔI MONG MUỐN": "do_tuoi",
        "TÌNH TRẠNG HÔN NHÂN": "hon_nhan",
        "SỐ LƯỢNG TUYỂN DỤNG": "so_luong_tuyen",
        "NGÀY LÀM VIỆC": "ngay_lam_viec",
        "GIỜ LÀM VIỆC": "gio_lam_viec",
        "LOẠI HÌNH LÀM VIỆC": "loai_hinh_lam_viec",
        "Địa điểm làm việc": "dia_diem_lam_viec",
        "Tên công ty": "ten_cong_ty",
        "Quy mô công ty": "quy_mo_cong_ty",
        "HREF": "href",
    }
    # Chuẩn hoá key để matching bất kể hoa/thường/dấu
    norm_map = {strip_diacritics(k): v for k, v in vn_to_short.items()}
    return norm_map

def rename_columns_no_diacritics(df: pd.DataFrame) -> pd.DataFrame:
    # Đổi tên cột theo map đã chuẩn hoá (strip dấu + lower), giữ nguyên các cột không có trong map.
    # Mục tiêu:
    # - Chuẩn hoá schema đầu vào cho các bước phân tích sau: tên cột dạng snake_case, ASCII.
    # - Log ra phần đã đổi và phần không khớp để dễ rà soát chất lượng dữ liệu.
    norm_map = build_column_map()

    # Tạo dict rename dựa trên header thực tế trong file
    actual_cols = list(df.columns)
    rename_dict = {}
    unmatched = []

    for col in actual_cols:
        key = strip_diacritics(col)
        if key in norm_map:
            rename_dict[col] = norm_map[key]
        else:
            unmatched.append(col)

    df2 = df.rename(columns=rename_dict)

    # Log nhanh
    if rename_dict:
        print("  ✅ Đã đổi tên cột")
    if unmatched:
        print("[WARN] Cột không khớp map (giữ nguyên):")
        for c in unmatched:
            print(f"  - {c}")

    return df2

def save_with_suffix(xlsx_path: Path, df: pd.DataFrame, suffix: str = "_processed") -> Path:
    # Xuất lại file kèm hậu tố, tránh ghi đè file gốc; engine=openpyxl phổ biến, dễ cài.
    out_path = xlsx_path.with_name(xlsx_path.stem + suffix + xlsx_path.suffix)
    df.to_excel(out_path, index=False, engine="openpyxl")
    return out_path

##########################################################################

def call_gpt(prompt: str, model: str = "gpt-4o-mini") -> str:
    # Hàm tiện ích gọi GPT với 2 nhánh SDK:
    # - Ưu tiên SDK mới (openai>=1.x): from openai import OpenAI; client.chat.completions.create(...)
    # - Fallback SDK cũ (openai<1.x): openai.ChatCompletion.create(...)
    # Chính sách:
    # - temperature=0.0 để kết quả ổn định (deterministic) phục vụ tiền xử lý.
    # - Lỗi ở cả 2 nhánh → ném RuntimeError để caller biết nguyên nhân (ghi rõ 2 lỗi new/legacy).
    # Lưu ý vận hành:
    # - Cần biến môi trường OPENAI_API_KEY khi dùng SDK cũ; SDK mới cũng yêu cầu cấu hình khoá phù hợp.
    # - Khi chạy trong môi trường offline/CI không có internet → sẽ fail, cần chặn hoặc mock ở tầng gọi.
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI()
        # dùng chat.completions cho tương thích rộng
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        return completion.choices[0].message.content.strip()
    except Exception as e_new:
        # Fallback sang SDK cũ
        try:
            import openai as openai_legacy  # type: ignore
            openai_legacy.api_key = os.getenv("OPENAI_API_KEY")
            completion = openai_legacy.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
            )
            return completion["choices"][0]["message"]["content"].strip()
        except Exception as e_old:
            raise RuntimeError(
                f"GPT call failed.\n- New SDK error: {type(e_new).__name__}: {e_new}\n- Legacy SDK error: {type(e_old).__name__}: {e_old}"
            )

# Tập hợp từ khoá nhận diện “lương thoả thuận/negotiable” bằng nhiều biến thể gõ tiếng Việt (có/không dấu) + tiếng Anh.
# Ứng dụng: phân loại nhóm “không rõ mức lương cụ thể” để tách khỏi nhóm có số liệu định lượng.
NEGOTIATION_TOKENS = [
    "thương lượng", "thuong luong", "thuong lượng", "deal", "negotiable",
    "thoả thuận", "thoa thuan", "thoả thuan", "thỏa thuận", "thoã thuận",
]

# Bảng map tiền tệ → danh sách ký hiệu/từ khoá thường gặp (cả ký hiệu Unicode và biến thể tiếng Việt)
# Lưu ý:
# - Thứ tự kiểm tra nên bắt các mã đặc thù trước ký hiệu chung chung (ví dụ "$" có thể là USD nhưng cũng dùng trong ngữ cảnh khác).
# - Khi parse lương, nên kết hợp thêm văn cảnh (VD: “triệu”, “k/tháng”) để suy luận VND chính xác hơn.
CURRENCY_MAP = {
    "VND": ["vnd", "vnđ", "vn d", "đ", "₫", "dong", "đồng", "dong viet"],
    "USD": ["usd", "us$", "$", "đô la", "dollar"],
    "EUR": ["eur", "€", "euro"],
    "GBP": ["gbp", "£", "pound"],
    "JPY": ["jpy", "¥", "yen", "yên"],
    "CNY": ["cny", "rmb", "nhân dân tệ", "yuan", "元", "人民币"],
    "KRW": ["krw", "₩", "won"],
    "SGD": ["sgd", "s$", "đô sing"],
    "THB": ["thb", "฿", "baht"],
}

# Heuristic tiếng Việt hay gặp để nhận diện VND khi có ký hiệu rút gọn theo thói quen:
# - "tr"/"triệu" (ví dụ: 15–20tr/tháng)
# - "k" (ví dụ: 20k/giờ)
# Các biến thể có dấu/gạch chéo/gạch nối cũng được liệt kê để tăng độ phủ.
VIET_VND_HINTS = [" tr", "tr/", "tr-", "triệu", "trieu", " triệu", "tr/tháng", "tr/thang", "k/", "k/thang", "k/tháng"]

def normalize_text(s: str) -> str:
    # Chuẩn hoá chuỗi trước khi dò: None -> "", ép str, lower, trim,
    # rồi bỏ dấu bằng strip_accents (HÀM NÀY PHẢI TỒN TẠI ở nơi khác — nếu chưa có, cần định nghĩa).
    # Mục tiêu: tăng độ “khớp” token cho tiếng Việt có/không dấu.
    if s is None:
        return ""
    return strip_accents(str(s).lower().strip())


def is_negotiation(s: str) -> bool:
    # Phát hiện “thương lượng/negotiable” dựa trên danh sách NEGOTIATION_TOKENS đã chuẩn bị.
    # Ý nghĩa: phân tách nhóm lương không có con số cụ thể ra khỏi nhóm có thể định lượng.
    txt = normalize_text(s)
    return any(tok in txt for tok in NEGOTIATION_TOKENS)

def regex_currency_guess(s: str) -> str | None:
    """Đoán nhanh loại tiền tệ bằng regex/ký hiệu/hints thường gặp để giảm số lần gọi GPT.
    Chiến lược theo tầng:
      (1) Ký hiệu/cụm từ đặc thù → chắc chắn (₫, vnđ, vnd, đồng; €, eur; £, gbp; ₩, krw; ฿, thb; s$, sgd).
      (2) Dấu $ hoặc từ khoá usd/us$/dollar/đô la → USD (giải pháp thực dụng).
      (3) Ký hiệu ¥: phân rẽ JPY/CNY bằng ngữ cảnh đi kèm (yen/jpy hay cny/rmb/yuan/人民币/元).
      (4) Heuristic tiếng Việt: “tr/triệu/k …/tháng” → VND.
      (5) Không xác định được → None để upper-layer hỏi GPT.
    Lưu ý: đây là "best-effort", chấp nhận một số biên lỗi và sẽ được vá bằng fix rules phía sau."""
    txt = normalize_text(s)

    # 1) Ký hiệu đặc thù ⇒ chắc chắn
    if any(sym in txt for sym in ["₫", "vnđ", "vnd", "đồng"]):  # VND
        return "VND"
    if "€" in txt or " eur" in txt or "euro" in txt:
        return "EUR"
    if "£" in txt or " gbp" in txt or "pound" in txt:
        return "GBP"
    if "₩" in txt or " krw" in txt or "won" in txt:
        return "KRW"
    if "฿" in txt or " thb" in txt or "baht" in txt:
        return "THB"
    if "s$" in txt or " sgd" in txt or " đô sing" in txt:
        return "SGD"

    # 2) Dấu $: đa phần là USD (trừ khi có ngữ cảnh khác)
    # Ưu tiên chuỗi có "usd" hoặc "us$"
    if "$" in txt or " usd" in txt or "us$" in txt or " dollar" in txt or "đô la" in txt:
        return "USD"

    # 3) Dấu ¥: có thể JPY hoặc CNY — nếu text có "jpy/yen" thì JPY, nếu "cny/rmb" thì CNY
    if "¥" in txt or " yen" in txt or "jpy" in txt:
        if "cny" in txt or "rmb" in txt or "yuan" in txt or "人民币" in txt or "元" in txt:
            return "CNY"
        return "JPY"
    if "cny" in txt or "rmb" in txt or "yuan" in txt or "人民币" in txt or "元" in txt:
        return "CNY"

    # 4) Hints tiếng Việt: "15tr-30tr", "20 triệu", "25tr/tháng" ⇒ VND
    if any(h in txt for h in VIET_VND_HINTS):
        return "VND"

    # 5) Không chắc
    return None

def gpt_currency_guess(s: str, model: str = "gpt-4o-mini") -> str | None:
    """Hỏi GPT để xác định mã tiền tệ khi regex không chắc. Trả về 1 mã trong {VND,USD,EUR,GBP,JPY,CNY,KRW,SGD,THB}.
    Lưu ý vận hành:
    - Chỉ gọi khi regex_currency_guess trả None để tiết kiệm chi phí.
    - Prompt yêu cầu trả đúng một mã, viết in hoa, không giải thích.
    - Có khối try/except để không làm gãy pipeline khi GPT lỗi (mạng/model)."""
    prompt = f"""Bạn là bộ phân loại tiền tệ. Văn bản lương: {s!r}
Hãy trả về đúng một mã tiền tệ trong tập sau: VND, USD, EUR, GBP, JPY, CNY, KRW, SGD, THB.
Chỉ trả về duy nhất mã (ví dụ: VND). Không giải thích thêm."""
    try:
        raw = call_gpt(prompt, model=model)
        code = (raw or "").strip().upper()
        if code in {"VND","USD","EUR","GBP","JPY","CNY","KRW","SGD","THB"}:
            return code
    except Exception as e:
        # Không fail pipeline; chỉ cảnh báo để có thể kiểm tra log khi cần.
        print(f"[WARN] GPT nhận dạng tiền tệ lỗi: {e}")
    return None

def add_salary_columns_check_loai(df: pd.DataFrame, model: str = "gpt-4o-mini") -> pd.DataFrame:
    """
    Bổ sung thông tin cho cột lương:
    - 'check_luong': False nếu là "Thương lượng/negotiable", True nếu có thể định lượng (có số/khoảng).
    - 'loai_tien_te': cố gắng xác định mã tiền tệ. Ưu tiên heuristic (regex_currency_guess), cuối cùng mới hỏi GPT.

    Nguyên tắc:
    - Không thay đổi dữ liệu gốc; chỉ chèn thêm cột ngay sau 'luong' để giữ schema dễ đọc.
    - Chịu lỗi (robust): nếu thiếu cột 'luong', log cảnh báo và trả df nguyên trạng.
    """
    if "luong" not in df.columns:
        print("[WARN] Không thấy cột 'luong' sau khi rename, bỏ qua bước xử lý lương.")
        return df

    # Tạo cột nếu chưa có
    # Nếu chưa có cột 'loai_tien_te' thì thêm ngay sau 'luong'
    if "loai_tien_te" not in df.columns:
        if "luong" in df.columns:
            luong_idx = df.columns.get_loc("luong")
            df.insert(luong_idx + 1, "loai_tien_te", None)
        else:
            df["loai_tien_te"] = None

    # Nếu chưa có cột 'check_luong' thì thêm ngay sau 'loai_tien_te'
    if "check_luong" not in df.columns:
        if "loai_tien_te" in df.columns:
            tien_te_idx = df.columns.get_loc("loai_tien_te")
            df.insert(tien_te_idx + 1, "check_luong", None)
        else:
            df["check_luong"] = None

    for idx, val in df["luong"].items():
        # Xử lý từng ô lương: chuẩn hoá text, check "thương lượng", suy đoán tiền tệ.
        text = str(val) if pd.notna(val) else ""
        if not text:
            df.at[idx, "check_luong"] = False
            continue

        # 1) check_luong
        negotiable = is_negotiation(text)
        df.at[idx, "check_luong"] = not negotiable

        # 2) Điền loai_tien_te
        if negotiable:
            # Thương lượng ⇒ không suy đoán tiền tệ (đánh dấu 'no_info' để downstream biết lý do trống)
            df.at[idx, "loai_tien_te"] = "no_info"
            continue

        # Ưu tiên regex (nhanh, rẻ)
        code = regex_currency_guess(text)
        if code is None:
            # Cuối cùng mới hỏi GPT (tốn phí/thời gian)
            code = gpt_currency_guess(text, model=model)
        df.at[idx, "loai_tien_te"] = code
    return df
#============================================
def fix_currency_conflict(df, col_salary="luong", col_currency="loai_tien_te"):
    # Sửa/chuẩn hoá loại tiền tệ khi phát hiện xung đột tín hiệu trong chuỗi lương.
    # Đây là “hậu kiểm” (post-fix) sau khi đã đoán tiền tệ, nhằm giảm lỗi phổ biến:
    #   (1) Có đồng thời "$" và "tr" → nhiều khả năng là VND (đơn vị hiển thị dùng $, nhưng thực chất là triệu VND).
    #   (2) Có đ/₫ nhưng số nhỏ bất thường (val < 500000) và không có dấu hiệu VND khác → có thể là USD (ví dụ "400 đ/giờ").
    import re

    def _num_mean(text: str):
        # Lấy trung bình các số xuất hiện trong chuỗi (hỗ trợ "10-15", "10,000", "1.2")
        # Dùng làm "độ lớn" xấp xỉ để áp quy tắc phân biệt VND/USD ở một số câu chữ mập mờ.
        s = str(text)
        s = s.replace(",", "")  # bỏ dấu phẩy ngăn cách nghìn
        nums = re.findall(r"\d+(?:\.\d+)?", s)
        if not nums:
            return None
        vals = [float(n) for n in nums]
        return sum(vals) / len(vals)

    def _has_tr_token(text: str) -> bool:
        # Phát hiện token “tr” (triệu) theo cách khoan dung (có thể dính số).
        t = str(text).lower()
        return bool(re.search(r"\d+\s*tr\b", t) or re.search(r"\btr\b", t))

    fixes = []
    for i, row in df.iterrows():
        text = row.get(col_salary, "")
        cur = row.get(col_currency, None)

        t = str(text)
        t_lower = t.lower()
        has_dollar = "$" in t
        has_tr = _has_tr_token(t_lower)
        has_d = ("đ" in t_lower) or ("₫" in t)
        has_ty = ("tỷ" in t_lower) or ("ty" in t_lower)

        val = _num_mean(t)
        new_cur = cur
        reason = ""

        # 1) Có cả $ và tr -> VND
        # Lý do: nhiều JD ghi "20-30tr$" (hoặc "$ 20-30tr") theo thói quen, thực chất là triệu VND.
        if has_dollar and has_tr:
            if new_cur != "VND":
                new_cur = "VND"
                reason = "Có cả ký hiệu $ và 'tr' → sửa loại tiền tệ thành VND"

        # 2) Chỉ có đ/₫ (không có $, không có 'tr', không có 'tỷ/ty') và giá trị < 500000 -> USD
        # Trực giác: số nhỏ kèm đ/₫ đôi khi là đơn giá/giờ theo USD nhưng viết nhầm/ký hiệu gây nhiễu.
        # Quy tắc này "mạnh tay", có thể điều chỉnh ngưỡng/điều kiện khi đánh giá thực tế.
        elif has_d and (not has_dollar) and (not has_tr) and (not has_ty) \
                and (val is not None) and (val < 500000):
            if new_cur != "USD":
                new_cur = "USD"
                reason = "Chỉ có ký hiệu đ/₫, không có $/tr/tỷ/ty và giá trị < 500000 → sửa loại tiền tệ thành USD"

        # Ghi kết quả
        df.at[i, col_currency] = new_cur
        fixes.append(reason)

    # Lưu cột lý do để phục vụ audit/debug về sau (biết vì sao bị đổi).
    df["fix_reason"] = fixes
    return df

#===========================================
# --- Detect kỳ trả lương ---
def detect_period(text: str) -> str | None:
    # Nhận diện “kỳ trả lương” (chu kỳ thời gian) từ mô tả lương tự do.
    # Chiến lược: chuẩn hoá → so khớp regex các biến thể viết thường gặp (vi/eng, có/không dấu, có/không “/”).
    # Trả về một trong: {'thang','nam','tuan','gio'} hoặc None nếu không xác định được.
    if not isinstance(text, str):
        return None
    t = strip_accents(text.lower())

    if re.search(r'(thang|/thang|month|/month|per month|mo|mth)', t):
        return 'thang'
    if re.search(r'(nam|/nam|year|/year|per year|yr)', t):
        return 'nam'
    if re.search(r'(tuan|/tuan|week|/week|per week|wk)', t):
        return 'tuan'
    if re.search(r'(gio|/gio|hour|/hour|per hour|hr|h)', t):
        return 'gio'
    return None

# Bỏ dấu
def strip_accents(s: str) -> str:
    # Chuẩn hoá chuỗi về dạng “không dấu” bằng Unicode NFD.
    # Lưu ý: cần import unicodedata ở phạm vi module trước khi gọi (đã dùng ở các phần khác).
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

# --- Helpers số ---
def _parse_number_million(raw_num: str) -> float | None:
    """Dùng khi có đơn vị 'tr/triệu' => trả về số TRIỆU (float).
    Ví dụ: '12,5' → 12.5 (triệu); có tác dụng khi lương viết '12-15tr' hoặc '12,5 triệu'."""
    if not raw_num:
        return None
    # đổi phẩy thập phân kiểu VN thành dấu chấm để float() hiểu đúng
    num = raw_num.replace(' ', '').replace(',', '.')
    try:
        return float(num)
    except:
        # An toàn: nếu còn ký tự nhiễu (do OCR/nhập liệu), trả None để caller tự quyết
        return None

def _strip_thousand_seps_keep_decimal(s: str) -> str:
    """
    Xóa dấu phân tách nghìn (',' hoặc '.') nhưng giữ lại dấu thập phân nếu có.
    Quy tắc:
      - Bỏ các dấu , . nếu nằm giữa nhóm 3 chữ số (thousand sep).
      - Giữ lại 1 dấu thập phân cuối cùng nếu không phải dạng nghìn.
    Lợi ích: chuẩn hoá '1,500'/'1.500' → '1500' mà không làm mất phần thập phân thực sự.
    """
    # Bỏ các dấu , . đóng vai trò THOUSAND: ...\d[,.]\d{3}(?!\d)
    s2 = re.sub(r'(?<=\d)[,.](?=\d{3}(?:\D|$))', '', s)
    return s2

def _parse_number_plain(raw_num: str) -> float | None:
    """
    Dùng khi KHÔNG có đơn vị 'tr/triệu' => trả về số bình thường (float).
    '1,500' -> 1500 ; '1.500' -> 1500 ; '500.75' giữ 500.75 nếu có dạng thập phân.
    Ghi chú: có nhánh fallback bỏ toàn bộ kí tự không số trong trường hợp dữ liệu rất nhiễu.
    """
    if not raw_num:
        return None
    s = _strip_thousand_seps_keep_decimal(raw_num)
    try:
        return float(s)
    except:
        # Thêm một nhánh dễ tính: nếu vẫn lỗi, bỏ hết , . rồi parse int
        digits = re.sub(r'[^0-9]', '', raw_num)
        return float(digits) if digits else None

# --- Parse 1 ô lương ---
def parse_salary_cell(cell: str):
    # Chuẩn hoá & bóc tách lương từ một ô văn bản tự do.
    # Trả về tuple (min, max, median, period) trong đó:
    #   - min/max/median: số float; đơn vị “triệu” nếu input có 'tr/triệu', ngược lại là đơn vị “thường” (theo số đã parse).
    #   - period: {'thang','nam','tuan','gio'} hoặc None; định danh kỳ trả (detect_period).
    # Nếu không bóc được → (None, None, None, period) hoặc nếu “thương lượng” → ('no_info', ...).
    if not isinstance(cell, str):
        return ("no_info", "no_info", "no_info", "no_info")

    text = cell.strip()
    text_noacc = strip_accents(text.lower())

    # Nếu là thương lượng → không định lượng được
    if "thuong luong" in text_noacc or "thương lượng" in text.lower():
        return ("no_info", "no_info", "no_info", "no_info")

    period = detect_period(text)

    # 1) Khoảng: [num1][tr?] - [num2][tr?]
    m_range = re.search(
        r'(\d[\d.,]*)\s*(tr|tri[eê]u)?\s*[-–—]\s*(\d[\d.,]*)\s*(tr|tri[eê]u)?',
        text_noacc
    )
    if m_range:
        n1, u1, n2, u2 = m_range.groups()
        if u1 or u2:  # có 'tr/triệu' ở ít nhất một bên => hiểu theo TRIỆU
            a = _parse_number_million(n1)
            b = _parse_number_million(n2)
        else:         # không có triệu => số thường (500, 1500, ...)
            a = _parse_number_plain(n1)
            b = _parse_number_plain(n2)
        if a is not None and b is not None:
            mn, mx = (a, b) if a <= b else (b, a)
            return (mn, mx, (mn + mx) / 2, period)

    # 2) Tối đa: "tới/đến/toida ... [num][tr?]"
    m_max = re.search(r'(toi|t[ơo]i|den|đ[eê]n|toida|toi da).*?(\d[\d.,]*)\s*(tr|tri[eê]u)?', text_noacc)
    if m_max:
        num, unit = m_max.group(2), m_max.group(3)
        v = _parse_number_million(num) if unit else _parse_number_plain(num)
        if v is not None:
            return (None, v, v, period)

    # 3) Tối thiểu: "từ ... [num][tr?]"
    m_min = re.search(r'(tu|t[uư]|t[ừu])\s*.*?(\d[\d.,]*)\s*(tr|tri[eê]u)?', text_noacc)
    if m_min:
        num, unit = m_min.group(2), m_min.group(3)
        v = _parse_number_million(num) if unit else _parse_number_plain(num)
        if v is not None:
            return (v, None, v, period)

    # 4) Chỉ một số
    m_single = re.search(r'(\d[\d.,]*)\s*(tr|tri[eê]u)?', text_noacc)
    if m_single:
        num, unit = m_single.group(1), m_single.group(2)
        v = _parse_number_million(num) if unit else _parse_number_plain(num)
        if v is not None:
            return (v, v, v, period)

    # Không match gì đáng tin → trả None cho 3 giá trị, vẫn giữ 'period' nếu có
    return (None, None, None, period)


def add_salary_columns_maxminmed_ky(df: pd.DataFrame, salary_col: str = 'luong') -> pd.DataFrame:
    # Thêm các cột suy diễn từ cột 'luong':
    #   - min_luong, max_luong, med_luong: (min/max/median) theo logic parse_salary_cell.
    #   - ky_tra_luong: kỳ trả lương do detect_period xác định.
    # Đồng thời đảm bảo có cột 'check_luong' (nếu chưa có thì thêm vào cuối để giữ tương thích pipeline).
    df = df.copy()
    results = [parse_salary_cell(val) for val in df[salary_col].fillna('')]
    min_vals, max_vals, med_vals, periods = zip(*results)

    # Nếu chưa có check_luong thì thêm cuối
    if "check_luong" not in df.columns:
        df["check_luong"] = None

    # Chèn các cột ngay sau check_luong để nhóm thông tin lương cạnh nhau (dễ quan sát/so sánh).
    check_idx = df.columns.get_loc("check_luong")
    df.insert(check_idx + 1, "min_luong", min_vals)
    df.insert(check_idx + 2, "max_luong", max_vals)
    df.insert(check_idx + 3, "med_luong", med_vals)
    df.insert(check_idx + 4, "ky_tra_luong", periods)

    return df



#==========================================================
# Map thứ VN -> số (Mon=1 ... Sun=7)
DOW_MAP = {
    "T2": 1, "T3": 2, "T4": 3, "T5": 4, "T6": 5, "T7": 6,
    "CN": 7
}

def _parse_dow_token(tok: str) -> set[int]:
    """
    Chuẩn hoá một token “ngày làm việc” → tập chỉ số ngày {1..7}.
    Hỗ trợ định dạng: 'T2', 'CN', 'T2-T6', 'T3 - T7' (coi thường khoảng trắng và kiểu gạch).
    Lưu ý:
    - Nếu khoảng bị đảo (ví dụ 'T6-T2') → hiểu vòng qua CN (thứ tự tuần khép kín).
    """
    tok = tok.strip().upper()
    tok = tok.replace(" ", "")
    if not tok:
        return set()
    if "-" in tok or "–" in tok or "—" in tok:
        # chuẩn hóa dấu gạch
        tok = tok.replace("–", "-").replace("—", "-")
        a, b = tok.split("-", 1)
        if a in DOW_MAP and b in DOW_MAP:
            ia, ib = DOW_MAP[a], DOW_MAP[b]
            if ia <= ib:
                return set(range(ia, ib + 1))
            else:
                # nếu lỡ nhập ngược T6-T2 → hiểu vòng qua CN
                return set(list(range(ia, 8)) + list(range(1, ib + 1)))
        return set()
    # single day
    return {DOW_MAP[tok]} if tok in DOW_MAP else set()

def count_workdays_week(ngay_lam_viec: str | float | None) -> int | None:
    """
    Đếm số ngày làm việc/tuần từ chuỗi lịch làm việc tự do như:
      'T2-T6', 'T2-T7', 'T2,T4,T6', 'T2-T6, CN', ...
    Cách làm:
    - Tách theo dấu phẩy/chấm phẩy/slash; mỗi phần gọi _parse_dow_token để nhận tập {1..7}.
    - Hợp nhất các tập rồi lấy kích thước → số ngày làm việc/tuần.
    Trả về None nếu input không phải chuỗi hoặc parse không ra ngày nào.
    """
    if not isinstance(ngay_lam_viec, str):
        return None
    text = ngay_lam_viec.strip()
    if not text:
        return None
    # Tách theo dấu phẩy (và một số phân cách phổ biến)
    parts = [p for p in re.split(r"[,\;/]+", text) if p.strip()]
    days: set[int] = set()
    for p in parts:
        days |= _parse_dow_token(p)
    if not days:
        return None
    return len(days)

# ---------- Giờ làm việc ----------
# Regex: bắt các cặp giờ như "09:00 - 18:00", có thể có AM/PM.
# Giải thích chi tiết:
# - (?P<h1>\d{1,2}):(?P<m1>\d{2})  → giờ/phút bắt đầu (1–2 chữ số giờ, đúng 2 chữ số phút)
# - (?P<ap1>AM|PM|am|pm)?         → tuỳ chọn AM/PM ngay sau giờ bắt đầu
# - \s*[-–—]\s*                   → chấp nhận cả ba loại gạch nối: -, en-dash, em-dash; cho phép khoảng trắng linh hoạt
# - (?P<h2>...):(?P<m2>...)       → giờ/phút kết thúc
# - (?P<ap2>AM|PM|am|pm)?         → tuỳ chọn AM/PM cho giờ kết thúc
# - re.VERBOSE để xuống dòng/chú thích regex dễ đọc.
TIME_RANGE_PAT = re.compile(
    r"""
    (?P<h1>\d{1,2})\s*:\s*(?P<m1>\d{2})\s*(?P<ap1>AM|PM|am|pm)?      # start
    \s*[-–—]\s*
    (?P<h2>\d{1,2})\s*:\s*(?P<m2>\d{2})\s*(?P<ap2>AM|PM|am|pm)?      # end
    """,
    re.VERBOSE
)

def _to_24h(h: int, m: int, ampm: str | None) -> tuple[int, int]:
    """Chuyển (h, m, am/pm) → 24h. Nếu ampm=None: giữ nguyên (coi như 24h).
    Quy tắc chuẩn:
    - 12 AM → 00h; 12 PM → 12h; 1–11 PM → +12 giờ.
    - Dùng modulo để đảm bảo h trong [0..23], m trong [0..59] nếu input lệch chuẩn.
    """
    if ampm:
        ap = ampm.upper()
        if ap == "AM":
            # 12 AM -> 00
            if h == 12:
                h = 0
        elif ap == "PM":
            # 12 PM -> 12; 1-11 PM -> +12
            if h != 12:
                h = (h % 12) + 12
    return h % 24, m % 60

def _mins(h: int, m: int) -> int:
    """Quy đổi (giờ, phút) → tổng phút kể từ 00:00 để tính toán chênh lệch dễ dàng."""
    return h * 60 + m

def parse_longest_time_span(cell: str | float | None) -> tuple[str | None, str | None, float | None]:
    """
    Trả về (gio_bat_dau_str, gio_ket_thuc_str, so_gio_lam_ngay)
    - Nếu trong ô có nhiều cặp giờ (nhiều dòng/khoảng), chọn khoảng có độ dài lớn nhất (giả định ca chính).
    - Hỗ trợ AM/PM (chuyển về 24h để tính toán thống nhất).
    - Nếu ca qua đêm (end < start), cộng bù 24h (ví dụ 22:00–06:00 → 8.00 giờ).
    - Định dạng hiển thị kết quả luôn là 'HH:MM' 24h (chuẩn hoá nhìn trực quan).
    - Trả (None, None, None) nếu không parse được cặp giờ nào.
    """
    if not isinstance(cell, str):
        return (None, None, None)
    text = cell.strip()
    if not text:
        return (None, None, None)

    best = None  # (duration_minutes, start_display, end_display)
    for m in TIME_RANGE_PAT.finditer(text):
        h1 = int(m.group("h1")); m1 = int(m.group("m1")); ap1 = m.group("ap1")
        h2 = int(m.group("h2")); m2 = int(m.group("m2")); ap2 = m.group("ap2")

        h1_24, m1_24 = _to_24h(h1, m1, ap1)
        h2_24, m2_24 = _to_24h(h2, m2, ap2)

        start_min = _mins(h1_24, m1_24)
        end_min   = _mins(h2_24, m2_24)
        dur = end_min - start_min
        if dur < 0:
            dur += 24 * 60  # ca qua đêm

        # Chuẩn hóa hiển thị: ưu tiên 24h 'HH:MM'
        start_disp = f"{h1_24:02d}:{m1_24:02d}"
        end_disp   = f"{h2_24:02d}:{m2_24:02d}"

        if (best is None) or (dur > best[0]):
            best = (dur, start_disp, end_disp)

    if best is None:
        return (None, None, None)

    dur_h = round(best[0] / 60.0, 2)  # làm tròn 2 chữ số thập phân
    return (best[1], best[2], dur_h)

# ================== Hàm áp dụng cho DataFrame ==================

def enrich_work_schedule_columns(
    df: pd.DataFrame,
    col_ngay_lam_viec: str = "ngay_lam_viec",
    col_gio_lam_viec: str = "gio_lam_viec",
    anchor_col: str = "ky_tra_luong",
) -> pd.DataFrame:
    """
    Bổ sung các cột liên quan lịch/ngày/giờ làm việc vào DataFrame.
    - Tính 'so_ngay_lam' từ 'ngay_lam_viec' bằng count_workdays_week (hỗ trợ T2–T7, CN, khoảng/chuỗi).
    - Tách 'gio_bat_dau', 'gio_ket_thuc' và 'so_gio_lam_ngay' từ 'gio_lam_viec' (ưu tiên khoảng dài nhất).
    - Sắp xếp lại thứ tự cột để nhóm thông tin gần nhau: chèn ngay sau 'anchor_col' (mặc định 'ky_tra_luong').
      Cụ thể thứ tự chèn: ngay_lam_viec, so_ngay_lam, gio_lam_viec, gio_bat_dau, gio_ket_thuc, so_gio_lam_ngay.
    Chính sách chịu lỗi:
    - Nếu thiếu cột nguồn, tự tạo rỗng để pipeline không gãy.
    - Nếu không có anchor_col, vẫn tạo được kết quả với dãy cột mong muốn đưa lên trước.
    """
    # Bảo đảm 2 cột nguồn tồn tại (nếu chưa có thì tạo rỗng)
    if col_ngay_lam_viec not in df.columns:
        df[col_ngay_lam_viec] = None
    if col_gio_lam_viec not in df.columns:
        df[col_gio_lam_viec] = None

    # so_ngay_lam (số ngày làm/tuần)
    df["so_ngay_lam"] = df[col_ngay_lam_viec].apply(count_workdays_week)

    # gio_bat_dau, gio_ket_thuc, so_gio_lam_ngay (từ cột giờ làm việc tự do)
    res = df[col_gio_lam_viec].apply(parse_longest_time_span)
    df["gio_bat_dau"]     = res.map(lambda x: x[0])
    df["gio_ket_thuc"]    = res.map(lambda x: x[1])
    df["so_gio_lam_ngay"] = res.map(lambda x: x[2])

    # Sắp xếp vị trí cột: chèn ngay sau anchor_col
    desired_seq = [
        col_ngay_lam_viec, "so_ngay_lam",
        col_gio_lam_viec, "gio_bat_dau", "gio_ket_thuc", "so_gio_lam_ngay"
    ]

    # Xây danh sách cột mới theo nguyên tắc:
    # - Nếu có anchor_col: giữ tất cả cột hiện hữu, nhưng di chuyển desired_seq ngay sau anchor.
    # - Nếu không có anchor_col: đưa desired_seq lên đầu, các cột còn lại giữ nguyên thứ tự sau đó.
    cols = list(df.columns)
    if anchor_col in cols:
        # Loại desired_seq khỏi cols (nếu đã có) để tránh trùng
        remaining = [c for c in cols if c not in desired_seq or c == anchor_col]
        # Vị trí anchor
        i = remaining.index(anchor_col)
        # new order = trước anchor + anchor + desired_seq + phần còn lại (không lặp)
        new_cols = remaining[: i + 1] + [c for c in desired_seq if c != anchor_col] + [c for c in remaining[i + 1 :] if c not in desired_seq]
        df = df.reindex(columns=new_cols)
    else:
        # Nếu không có anchor_col, vẫn cố gắng đặt theo thứ tự: desired_seq đứng trước, giữ nguyên các cột còn lại
        new_cols = [c for c in cols if c not in desired_seq]  # phần còn lại
        df = df[[*desired_seq, *new_cols]]

    return df

# ===================== FX helpers =====================

# Cache để tránh gọi API nhiều lần cho cùng 1 base
# Ý tưởng: khi nhiều hàng trong df có cùng loại tiền (USD/EUR...), chỉ fetch 1 lần rồi tái sử dụng.
_FX_CACHE: Dict[str, float] = {}  # base -> rate_to_VND

# Tập mã tiền tệ được hỗ trợ trong hàm get_fx_rate_to_vnd.
SUPPORTED = {"VND","USD","EUR","GBP","JPY","CNY","KRW","SGD","THB","AUD","CAD"}

def _fetch_rate_open_erapi(base: str) -> Optional[float]:
    """Nguồn 1: open.er-api.com (free, ổn định). Trả về rate VND cho base.
    Quy trình:
    - Gọi /v6/latest/{base} → JSON {'result':'success', 'rates': {'VND': value, ...}}
    - Trả về float nếu lấy được VND > 0, ngược lại trả None để caller fallback.
    Lưu ý: có timeout 15s để tránh treo pipeline.
    """
    url = f"https://open.er-api.com/v6/latest/{base}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("result") == "success":
        rates = data.get("rates", {})
        vnd = rates.get("VND")
        if isinstance(vnd, (int, float)) and vnd > 0:
            return float(vnd)
    return None

def _fetch_rate_exchangerate_host(base: str) -> Optional[float]:
    """Nguồn 2: exchangerate.host làm fallback.
    - Gọi /latest?base={base}&symbols=VND → JSON {'rates': {'VND': value}}
    - Trả về float nếu thành công; nếu lỗi/thiếu → None.
    """
    url = f"https://api.exchangerate.host/latest?base={base}&symbols=VND"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    rates = data.get("rates", {})
    vnd = rates.get("VND")
    if isinstance(vnd, (int, float)) and vnd > 0:
        return float(vnd)
    return None

def get_fx_rate_to_vnd(base: str, retries: int = 2, sleep_sec: float = 0.8) -> float:
    """
    Lấy tỷ giá 1 BASE = ? VND. Có cache + retry + 2 nguồn.
    - Trả về 1.0 nếu base == 'VND' (đơn vị chuẩn hoá sẵn).
    - Kiểm tra base thuộc SUPPORTED; nếu không → ValueError.
    - Nếu đã có trong _FX_CACHE → trả ngay.
    - Nếu chưa có: thử nguồn 1 (open.er-api), nếu fail → nguồn 2 (exchangerate.host);
      lặp tối đa 'retries' lần, có sleep giữa các lần.
    - Nếu hết lượt vẫn thất bại → RuntimeError, kèm thông tin lỗi cuối để debug.
    Ghi chú vận hành:
    - Hạn chế gọi mạng trong vòng lặp dữ liệu lớn: nên gom unique currencies rồi gọi get_fx_rate_to_vnd từng loại.
    """
    base = (base or "").upper().strip()
    if base == "VND":
        return 1.0
    if base not in SUPPORTED:
        raise ValueError(f"Unsupported currency: {base}")

    if base in _FX_CACHE:
        return _FX_CACHE[base]

    last_err = None
    for i in range(retries + 1):
        try:
            rate = _fetch_rate_open_erapi(base)
            if rate:
                _FX_CACHE[base] = rate
                return rate
        except Exception as e:
            last_err = e
        # fallback
        try:
            rate = _fetch_rate_exchangerate_host(base)
            if rate:
                _FX_CACHE[base] = rate
                return rate
        except Exception as e:
            last_err = e

        time.sleep(sleep_sec)

    raise RuntimeError(f"FX fetch failed for {base}: {last_err}")

# ===================== Pay period helpers =====================
def exchange_luong(
    df: pd.DataFrame,
    currency_col: str = "loai_tien_te",
    min_col: str = "min_luong",
    med_col: str = "med_luong",
    max_col: str = "max_luong",
    period_col: str = "ky_tra_luong",
    hours_per_day_col: str = "so_gio_lam_ngay",
    days_per_week_col: str = "so_ngay_lam",
    salary_text_col: Optional[str] = "luong",
) -> pd.DataFrame:
    """
    Quy đổi min_luong / med_luong / max_luong về VND/tháng.

    Ý tưởng tổng quát:
    - Đầu vào là các cột min/med/max lương từ bước parse (PHẦN 5), cùng với kỳ trả (giờ/tuần/tháng/năm)
      và gợi ý số giờ/ngày, số ngày/tuần. Hàm này chuẩn hoá toàn bộ về “VND theo THÁNG”.
    - Dựa vào văn bản lương gốc (salary_text_col) để nhận biết đơn vị 'tỷ'/'triệu'/'đ' (đồng) theo ưu tiên:
        1) Có 'tỷ/tỉ'  → nhân 1e9 rồi đổi kỳ.
        2) Có 'triệu'   → nhân 1e6 rồi đổi kỳ.
        3) Chỉ 'đ/vnđ/₫'→ không nhân, chỉ đổi kỳ.
        4) Không rõ     → giữ nguyên, không đổi kỳ (bảo thủ).
    - Với ngoại tệ (USD/EUR/...): lấy tỷ giá → quy đổi sang VND → đổi kỳ như VND 'đ'.

    Lưu ý vận hành/điều kiện tiên quyết:
    - Cần đã import 'unicodedata' và 'numpy as np' ở cấp module vì hàm dùng chúng trong helpers.
    - Các cột 'so_gio_lam_ngay' và 'so_ngay_lam' nếu không có/không hợp lệ, mặc định hpd=8, dpw=6 (quy ước thực dụng).

    Chi tiết logic kỳ:
    - Với trị VND đơn vị 'triệu' (sau khi nhân 1e6):
        năm  -> *1_000_000/12
        tháng-> *1_000_000
        tuần -> *1_000_000*4
        giờ  -> *1_000_000*hpd*dpw*4
    - Với trị VND đơn vị 'đ' (đồng):
        năm  -> /12
        tháng-> giữ nguyên
        tuần -> *4
        giờ  -> *hpd*dpw*4
    - Với ngoại tệ:
        value_in_VND = value * fx_rate(base→VND), rồi đổi kỳ như "đ".
    """

    # ===== Helpers =====
    def _strip_accents_lower(s: str) -> str:
        # Bỏ dấu + lower + trim: chuẩn hoá văn bản để dò đơn vị ('ty', 'tr', 'dong') không phụ thuộc dấu/hoa-thường.
        # Cảnh báo: cần `import unicodedata` ở đầu file.
        s = "" if s is None else str(s)
        s = unicodedata.normalize("NFD", s)
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
        return s.lower().strip()

    def _to_num(x):
        # Chuyển x về float “khoan dung”:
        # - Chuỗi: bỏ ',' và space trước khi float().
        # - Giá trị NaN → None (đồng nhất với các nhánh None khác).
        # Cảnh báo: cần `import numpy as np` ở đầu file.
        if x is None:
            return None
        try:
            if isinstance(x, str):
                x = x.replace(",", "").replace(" ", "")
            v = float(x)
            return None if np.isnan(v) else v
        except Exception:
            return None

    def _norm_period(val: str) -> str:
        # Chuẩn hoá kỳ trả về {year, month, week, hour}; mặc định 'month' nếu không rõ.
        # Chấp nhận nhiều biến thể viết tắt: y/yr, m/month, w/week, h/hour; và tiếng Việt không dấu.
        v = _strip_accents_lower(val)
        if not v: return "month"
        if "nam"   in v or v in {"y","year","yr"}:   return "year"
        if "thang" in v or v in {"m","month"}:       return "month"
        if "tuan"  in v or v in {"w","week"}:        return "week"
        if "gio"   in v or v in {"h","hour"}:        return "hour"
        return "month"

        # >>>>> bổ sung 2 helper bạn thiếu <<<<<
    def _get_hpd_dpw(row):
        # Lấy số giờ/ngày (hpd) và số ngày/tuần (dpw) từ DataFrame; fallback 8h và 6 ngày nếu thiếu/không hợp lệ.
        h = _to_num(row.get(hours_per_day_col))
        d = _to_num(row.get(days_per_week_col))
        if not h or h <= 0: h = 8.0
        if not d or d <= 0: d = 6.0
        return h, d
    def _to_month(val: Optional[float], period: str, hpd: float, dpw: float) -> Optional[float]:
        # Quy đổi giá trị “val” từ kỳ {year, month, week, hour} về "theo tháng".
        if val is None: return None
        if period == "year":  return val / 12.0
        if period == "month": return val
        if period == "week":  return val * 4.0
        if period == "hour":  return val * hpd * dpw * 4.0
        return val

    # --- regex phát hiện 'tỷ/tỉ' (sau khi bỏ dấu: 'ty' hoặc 'ti')
    # Lý do dùng regex thay vì contains đơn thuần: bắt được cả dạng dính số (vd: "20ty").
    RE_TY = re.compile(r"(?:(?<=\d)\s*t[yi]\b|\bt[yi]\b)", re.IGNORECASE)
    RE_TRIEU = re.compile(r"(?:(?<=\d)\s*tr\b|\btr\b|tri[eê]u)", re.IGNORECASE)
    RE_DONG = re.compile(r"(?:\bvn?đ\b|₫|\bđ\b|\bd\b|\bvnd\b|\bdong\b|\bđong\b)", re.IGNORECASE)

    def _is_ty(text: str) -> bool:
        t = _strip_accents_lower(text)
        return bool(RE_TY.search(t))

    def _is_trieu(text: str) -> bool:
        t = _strip_accents_lower(text)
        return bool(RE_TRIEU.search(t))

    def _is_dong(text: str) -> bool:
        t = _strip_accents_lower(text)
        return bool(RE_DONG.search(t))

    def quy_doi_vnd(value: Optional[float], period: str, salary_text: str, hpd: float, dpw: float) -> Optional[float]:
        """
        Ưu tiên phát hiện đơn vị theo thứ tự:
        1) 'tỷ/tỉ'  -> scale 1_000_000_000 rồi đổi kỳ
        2) 'triệu'  -> scale 1_000_000     rồi đổi kỳ
        3) 'đ/d'    -> KHÔNG scale, chỉ đổi kỳ
        4) không rõ -> giữ nguyên (không đổi kỳ)

        Ghi chú: Ưu tiên 'tỷ' cao hơn 'triệu' để tránh double-scale nếu text có cả hai (trường hợp hiếm).
        """
        if value is None:
            return None

        has_ty = _is_ty(salary_text)
        has_trieu = _is_trieu(salary_text)
        has_dong = _is_dong(salary_text)

        # 1) ƯU TIÊN 'tỷ/tỉ'
        if has_ty:
            scale = 1_000_000_000.0
            if period == "year":   return value * scale / 12.0
            if period == "month":  return value * scale
            if period == "week":   return value * scale * 4.0
            if period == "hour":   return value * scale * hpd * dpw * 4.0
            return value * scale  # mặc định coi như tháng

        # 2) 'triệu'
        if has_trieu:
            scale = 1_000_000.0
            if period == "year":   return value * scale / 12.0
            if period == "month":  return value * scale
            if period == "week":   return value * scale * 4.0
            if period == "hour":   return value * scale * hpd * dpw * 4.0
            return value * scale

        # 3) Chỉ 'đ/d' (không có 'tỷ' và không có 'triệu')
        if has_dong:
            if period == "year":   return value / 12.0
            if period == "month":  return value
            if period == "week":   return value * 4.0
            if period == "hour":   return value * hpd * dpw * 4.0
            return value

        # 4) Không rõ -> giữ nguyên
        return value

    # ===== Hàm con 2: Quy đổi ngoại tệ =====
    def quy_doi_ngoai_te(
            value: Optional[float],
            period: str,
            base_ccy: str,
            hpd: float,
            dpw: float
    ) -> Optional[float]:
        """Nhân tỷ giá về VND, sau đó đổi kỳ như với VND 'đ'.
        Chính sách an toàn:
        - Bắt tất cả lỗi khi fetch tỷ giá (mạng, API, rate None) và trả None nếu không có rate hợp lệ.
        - Caller sẽ gán lại vào DataFrame và có thể lọc bỏ dòng thiếu về sau.
        """
        if value is None:
            return None

        base = (base_ccy or "").upper().strip()
        if not base or base == "VND":
            # Phòng hờ: nếu ghi nhầm VND vào nhánh ngoại tệ thì vẫn dùng quy_doi_vnd
            return quy_doi_vnd(value, period, "", hpd, dpw)

        # Lấy tỷ giá an toàn (không làm sập hàm)
        rate = None
        try:
            rate = get_fx_rate_to_vnd(base)
        except Exception:
            rate = None

        if not rate or rate <= 0:
            # Không có tỷ giá hợp lệ -> không thể quy đổi
            return None

        v_vnd = value * rate
        # Đổi kỳ như VND 'đ'
        return _to_month(v_vnd, period, hpd, dpw)

    # ===== Xử lý từng dòng =====
    new_min, new_med, new_max = [], [], []

    for _, row in df.iterrows():
        # Đọc loại tiền và kỳ trả chuẩn hoá
        ccy = (row.get(currency_col) or "").upper().strip()
        period = _norm_period(row.get(period_col, ""))
        hpd, dpw = _get_hpd_dpw(row)
        s_text = "" if salary_text_col is None else (row.get(salary_text_col) or "")

        # Ép kiểu số cho min/med/max (khoan dung)
        vmin = _to_num(row.get(min_col))
        vmed = _to_num(row.get(med_col))
        vmax = _to_num(row.get(max_col))

        if ccy == "VND":
            # Trường hợp VND:
            # - ưu tiên phát hiện 'tỷ'/'triệu' trong salary_text để scale đúng *1e9/*1e6
            # - nếu chỉ có 'đ' → chỉ đổi kỳ; nếu không có gợi ý nào → giữ nguyên (bảo thủ)
            vmin = quy_doi_vnd(vmin, period, s_text, hpd, dpw)
            vmed = quy_doi_vnd(vmed, period, s_text, hpd, dpw)
            vmax = quy_doi_vnd(vmax, period, s_text, hpd, dpw)
        else:
            # Ngoại tệ:
            # - Đổi về VND theo tỷ giá, sau đó đổi kỳ như VND 'đ'
            vmin = quy_doi_ngoai_te(vmin, period, ccy, hpd, dpw)
            vmed = quy_doi_ngoai_te(vmed, period, ccy, hpd, dpw)
            vmax = quy_doi_ngoai_te(vmax, period, ccy, hpd, dpw)

        new_min.append(vmin)
        new_med.append(vmed)
        new_max.append(vmax)

    # Ghi đè lại ba cột đích sau quy đổi
    df[min_col] = new_min
    df[med_col] = new_med
    df[max_col] = new_max
    return df

################################################################
import pandas as pd
import numpy as np

def danh_dau_luong_bat_thuong(df: pd.DataFrame,
                              min_col: str = "min_luong",
                              med_col: str = "med_luong",
                              max_col: str = "max_luong") -> pd.DataFrame:
    """
    Nếu med_luong > 6 × mean(med_luong) hoặc < 10% × mean(med_luong)
    thì ghi đè min_luong, med_luong, max_luong = 'bat_thuong'.
    """
    if med_col not in df.columns:
        raise ValueError(f"❌ Không tìm thấy cột '{med_col}' trong DataFrame")

    # Chuyển sang số
    s_med = pd.to_numeric(df[med_col], errors="coerce")
    mean_val = s_med.mean(skipna=True)

    if not np.isfinite(mean_val) or mean_val == 0:
        print("⚠️ Không tính được trung bình hợp lệ.")
        return df

    # Tạo mask điều kiện bất thường
    mask = (s_med > mean_val * 7) | (s_med < mean_val * 0.1)

    # Ghi đè giá trị
    for c in [min_col, med_col, max_col]:
        if c in df.columns:
            df["min_luong"] = df["min_luong"].astype("object")
            df["max_luong"] = df["max_luong"].astype("object")
            df["med_luong"] = df["med_luong"].astype("object")
            df.loc[mask, c] = "bat_thuong"

    return df
#=============================================================
def extract_number(text):
    # Nếu text là NaN (theo Pandas) → trả None
    if pd.isna(text):
        return None
    # Tìm số đầu tiên xuất hiện trong chuỗi (liên tiếp các ký tự digit)
    m = re.search(r"\d+", str(text))
    # Nếu có match → lấy group() và ép int, ngược lại trả None
    return int(m.group()) if m else None
#============================================================
BENEFIT_TOKENS = {
    "Luong-Thuong": {
        "title": "Luong-Thuong",
        "items": {
            "salary_competitive": {
                "label": "Luong canh tranh/thoan thuan",
                "any": [
                    "negotiable", "deal", "deal salary", "at interview",
                    "competitive", "attractive", "market rate", "commensurate",
                    "thuong luong", "thoa thuan", "luong thoa thuan", "muc luong thoa thuan",
                    "thu nhap thoa thuan", "luong canh tranh", "luong hap dan",
                    "wage agreement", "wage agreements", "agreed in interview",
                    "to be discussed", "tbd salary"
                ],
            },
            "thang_luong_13": {
                "label": "Luong thang 13",
                "any": ["13th", "13th month salary", "thang luong 13", "luong 13", "t13", "13th salary", "month 13"],
            },
            "bonus_generic": {
                "label": "Bonus",
                "any": ["bonus", "bonuses", "incentive", "incentives", "thuong", "thuong them"],
            },
            "bonus_performance": {
                "label": "Thuong hieu suat/KPI",
                "all": [["performance","bonus"], ["kpi","bonus"], ["hieu suat","thuong"]],
                "any": ["kpi", "performance incentive", "thuong hieu suat", "thuong kpi", "monthly performance bonus"],
            },
            "bonus_quarter_year_end": {
                "label": "Thuong quy/cuoi nam",
                "any": [
                    "quarter bonus", "quarterly bonus", "year end bonus", "year-end bonus",
                    "thuong quy", "thuong cuoi nam", "thuong nam", "bonus cuoi nam"
                ],
            },
            "bonus_sales_profit": {
                "label": "Thuong doanh so/loi nhuan",
                "all": [["sales","bonus"], ["profit","bonus"], ["doanh so","thuong"], ["loi nhuan","thuong"]],
                "any": ["commission", "hoa hong", "sales incentive"],
            },
            "bonus_holiday_tet": {
                "label": "Thuong le/Tet",
                "any": [
                    "holiday bonus", "tet bonus", "thuong le", "thuong tet",
                    "lucky money", "red envelope", "li xi", "li xi tet"
                ],
            },
            "salary_review": {
                "label": "Xet tang luong",
                "any": [
                    "salary review", "annual salary review", "salary adjustment", "salary raise", "pay raise",
                    "xet tang luong", "tang luong hang nam", "review luong", "danh gia luong"
                ],
            },
            "profit_sharing": {
                "label": "Chia loi nhuan",
                "any": ["profit sharing", "profit-share", "chia loi nhuan", "share profit"],
            },
            "esop_stock": {
                "label": "ESOP/Co phieu thuong",
                "any": [
                    "esop", "stock option", "share option", "stock grant", "rsu", "equity", "co phieu thuong"
                ],
            },
            "wage_agree_interview": {
                "label": "Luong/thuong luong khi phong van",
                "all": [["wage","interview"], ["discussed","interview"], ["agreed","interview"]],
                "any": ["to be discussed in interview", "deal in interview"],
            },
        },
    },

    "BaoHiem-SK": {
        "title": "BaoHiem-SK",
        "items": {
            "bhxh_bhyt_bhtn": {
                "label": "BHXH/BHYT/BHTN",
                "any": [
                    "bhxh", "bhyt", "bhtn", "bao hiem xa hoi", "bao hiem y te", "bao hiem that nghiep",
                    "compulsory insurance", "statutory insurance", "full insurance", "social insurance coverage"
                ],
                "all": [["social","insurance"], ["health","insurance"]],
            },
            "health_insurance_generic": {
                "label": "Health/Medical insurance",
                "all": [["health","insurance"], ["medical","insurance"]],
                "any": [
                    "premium health insurance", "private health insurance", "pvi", "bao viet", "insurance plan",
                    "bao hiem suc khoe", "medical plan", "healthcare insurance", "health care insurance",
                    "family healthcare", "family health care"
                ],
            },
            "annual_health_check": {
                "label": "Kham suc khoe dinh ky",
                "any": [
                    "annual health check", "yearly health check", "health check", "kham suc khoe",
                    "kham suc khoe dinh ky", "kham suc khoe hang nam"
                ],
            },
            "family_package": {
                "label": "Bao hiem nguoi than",
                "any": [
                    "family package", "family plan", "dependents coverage", "dependents insurance",
                    "bao hiem nguoi than", "family healthcare", "family health care", "cover family members"
                ],
            },
            "accident_24_7": {
                "label": "Tai nan 24/7",
                "any": ["24/7 accident", "personal accident", "accident insurance", "bao hiem tai nan"],
            },
            "dental_vision": {
                "label": "Dental/Vision",
                "any": ["dental", "vision", "nha khoa", "nhan khoa", "dental care", "vision care"],
            },
            "mental_health_eap": {
                "label": "Mental health/EAP",
                "any": ["mental health", "eap", "wellbeing", "well-being", "wellness program", "employee assistance"],
            },
            "sport_clubs": {
                "label": "CLB the thao/Wellness",
                "any": ["sport club", "sports club", "wellness", "gym", "yoga", "clb the thao", "fitness"],
            },
        },
    },

    "NghiPhep-Time": {
        "title": "NghiPhep-Time",
        "items": {
            "annual_leave_12_plus": {
                "label": "Annual leave",
                "any": [
                    "annual leave", "paid time off", "pto", "nghi phep", "nghi phep nam", "phep nam",
                    "nghi le", "nghi tet"
                ],
            },
            "legal_annual_leave_12": {  # thêm mục phép theo luật = 12 ngày
                "label": "Annual leave theo luat",
                "any": [
                    "theo luat", "theo luat dinh", "phep nam theo luat", "annual leave as per law",
                    "statutory leave"
                ],
            },
            "special_days": {
                "label": "Ngay nghi dac biet",
                "any": ["christmas", "company day", "company day off", "ngay nghi dac biet", "company holidays"],
            },
            "five_days_week": {
                "label": "5 ngay/tuan",
                "any": ["5 days a week", "5-day work week", "5 ngay/tuan", "lam 5 ngay/tuần", "lam 5 ngay"],
            },
            "flexible_hours": {
                "label": "Gio linh hoat",
                "any": ["flexible hours", "flexible time", "flexible schedule", "gio linh hoat", "flextime", "flexitime"],
            },
            "remote_hybrid": {
                "label": "Remote/Hybrid/WFH",
                "any": ["remote", "hybrid", "work from home", "wfh", "lam viec tu xa", "lam viec hybrid"],
            },
            "paid_personal_leave": {
                "label": "Nghi rieng huong luong",
                "any": ["paid personal leave", "personal leave paid", "nghi viec rieng", "nghi rieng huong luong"],
            },
            "sick_leave": {
                "label": "Nghi om",
                "any": ["sick leave", "sick days", "nghi om", "om dau nghi"],
            },
            "parental_leave": {
                "label": "Thai san/Parental",
                "any": [
                    "maternity leave", "paternity leave", "parental leave", "thai san",
                    "nghi sinh", "che do thai san"
                ],
            },
            "overtime_pay": {
                "label": "OT/Tang ca",
                "any": ["overtime", "ot", "overtime pay", "tang ca", "phu cap tang ca"],
            },
        },
    },

    "DaoTao-PT": {
        "title": "DaoTao-PT",
        "items": {
            "training_internal_external": {
                "label": "Training/On-the-job",
                "any": [
                    "training", "on the job", "on-the-job", "ojt",
                    "coaching", "workshop", "seminar", "dao tao", "dao tao noi bo", "dao tao ben ngoai",
                    "internal training", "external training", "ky nang mem", "soft skills"
                ],
            },
            "overseas_training_work": {
                "label": "Overseas/Abroad",
                "any": [
                    "overseas", "abroad", "secondment", "assignment abroad", "nuoc ngoai", "quoc te",
                    "onsite overseas", "training abroad"
                ],
            },
            "language_stipend": {
                "label": "Phu cap ngoai ngu",
                "any": [
                    "language allowance", "language stipend", "language class", "language course",
                    "phu cap ngoai ngu", "tro cap ngoai ngu"
                ],
            },
            "career_path_review": {
                "label": "Career path/Review",
                "any": [
                    "career path", "promotion", "review level", "lo trinh nghe nghiep", "thang tien",
                    "performance review", "appraisal", "level review"
                ],
            },
            "cert_sponsorship": {
                "label": "Chung chi/Khoa hoc",
                "any": [
                    "certification", "exam reimbursement", "education budget", "education reimbursement",
                    "conference fee", "tai tro chung chi", "tai tro khoa hoc", "hoc bong noi bo"
                ],
            },
            "mentoring": {
                "label": "Mentoring/Coaching",
                "any": ["mentoring", "mentor", "mentorship", "coaching", "buddy program"],
            },
        },
    },

    "VanHoa-Team": {
        "title": "VanHoa-Team",
        "items": {
            "team_building_trip": {
                "label": "Team building/Trip",
                "any": [
                    "team building", "company trip", "annual trip", "du lich", "du lich he", "outing",
                    "offsite"
                ],
            },
            "year_end_party_events": {
                "label": "Year End Party/Events",
                "any": [
                    "year end party", "year-end party", "company event", "su kien noi bo",
                    "yearly party", "year end celebration"
                ],
            },
            "sports_activities": {
                "label": "Giai the thao",
                "any": [
                    "football tournament", "soccer tournament", "giai bong da",
                    "the thao", "sports day", "sports activities"
                ],
            },
            "snacks_tea_break": {
                "label": "Tea break/Snacks/Pantry",
                "any": [
                    "tea break", "snack", "snacks", "pantry", "coffee", "free coffee",
                    "beer", "free beer", "do an nhe", "snack bar"
                ],
            },
            "birthday_gifts": {
                "label": "Qua sinh nhat/Le",
                "any": ["birthday gift", "birthday gifts", "sinh nhat", "gift", "holiday gift", "sinh nhat cong ty"],
            },
            "library_reading": {
                "label": "Thu vien/Resources",
                "any": ["library", "thu vien", "reading resources", "tai lieu hoc tap"],
            },
            "pet_friendly": {
                "label": "Pet-friendly",
                "any": ["pet-friendly", "pet friendly"],
            },
        },
    },

    "PhuCap-CanTin": {
        "title": "PhuCap-CanTin",
        "items": {
            "meal_allowance": {
                "label": "An trua/Canteen",
                "any": [
                    "meal allowance", "lunch allowance", "meal provided", "lunch provided",
                    "canteen", "cafeteria", "an trua", "can tin", "bua trua"
                ],
            },
            "phone_allowance": {
                "label": "Phu cap dien thoai",
                "any": [
                    "mobile allowance", "phone allowance", "cell phone allowance",
                    "data plan", "phu cap dien thoai", "tro cap dien thoai"
                ],
            },
            "transport_parking": {
                "label": "Xang xe/Di lai/Parking",
                "any": [
                    "transport allowance", "transportation allowance", "commute allowance",
                    "parking fee", "parking support", "parking allowance",
                    "grab", "taxi", "cab allowance", "xang xe", "phi gui xe", "tro cap di lai"
                ],
            },
            "travel_per_diem": {
                "label": "Cong tac phi/Per diem",
                "any": [
                    "per diem", "travel allowance", "travel reimbursement", "site-work allowance",
                    "cong tac phi", "cong tac", "di cong tac", "business travel"
                ],
            },
            "housing_hostel": {
                "label": "Nha o/KTX/Accommodation",
                "any": [
                    "housing", "accommodation", "hostel", "dormitory", "apartment",
                    "ktx", "nha o", "khach san", "o tro", "allowance housing"
                ],
            },
            "childcare_support": {
                "label": "Ho tro nha tre",
                "any": [
                    "childcare", "child care", "child allowance", "nha tre", "giu tre",
                    "daycare", "nursery allowance"
                ],
            },
            "internet_remote": {
                "label": "Phu cap Internet/WFH",
                "any": [
                    "internet allowance", "internet reimbursement", "remote work support",
                    "home internet", "wifi allowance"
                ],
            },
            "relocation_visa": {
                "label": "Relocation/Visa",
                "any": [
                    "relocation", "relocation package", "relocation support",
                    "visa sponsorship", "work permit", "work visa"
                ],
            },
        },
    },

    "ThietBi-CongCu": {
        "title": "ThietBi-CongCu",
        "items": {
            "laptop_pc": {
                "label": "Laptop/PC/MacBook",
                "any": [
                    "laptop", "macbook", "pc", "desktop", "workstation", "may tinh xach tay",
                    "company laptop", "issued laptop"
                ],
            },
            "monitor_accessories": {
                "label": "Man hinh/Phu kien",
                "any": ["monitor", "man hinh", "keyboard", "mouse", "headset", "webcam", "dock", "docking"],
            },
            "work_tools_studio": {
                "label": "Thiet bi/Dung cu/Studio/PPE",
                "any": [
                    "studio", "ppe", "bao ho lao dong", "protective equipment",
                    "thiet bi", "dung cu", "tooling", "gear provided"
                ],
            },
            "uniform": {
                "label": "Dong phuc",
                "any": ["uniform", "dong phuc", "cong ty cap dong phuc", "uniform provided"],
            },
            "phone_device": {
                "label": "Sim/Thiet bi dien thoai",
                "any": ["company phone", "sim", "sim card", "dien thoai provided", "mobile device"],
            },
            "parking_slot": {
                "label": "Cho gui xe/Do xe",
                "any": ["parking", "gui xe", "do xe", "parking slot"],
            },
        },
    },

    "XeDuaDon": {
        "title": "XeDuaDon",
        "items": {
            "shuttle_bus": {
                "label": "Xe dua don/Bus tuyen",
                "any": ["shuttle bus", "company bus", "xe dua don", "shuttle service", "bus service"],
            },
            "fixed_travel_allowance": {
                "label": "Tro cap di lai co dinh",
                "any": ["travel allowance", "transport stipend", "phu cap di lai", "commute stipend"],
            },
        },
    },
}
#============================================================
def strip_accents(s: str) -> str:
    # Bỏ dấu tiếng Việt: chuẩn hoá NFD rồi loại bỏ các ký tự Mn (dấu, dấu thanh)
    if not isinstance(s, str): return ""
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")

def normalize_text(*parts: str) -> str:
    # Ghép nhiều chuỗi đầu vào, bỏ None/rỗng, chuẩn hoá chữ thường, bỏ dấu, gọn khoảng trắng
    txt = " \n ".join([p for p in parts if isinstance(p, str) and p.strip()])
    txt = strip_accents(txt.lower())
    # Chuẩn hoá từ viết tắt hay gặp
    txt = txt.replace("wfh", "work from home")
    txt = re.sub(r"\s+", " ", txt)
    return txt

def _contains_any(text: str, tokens: List[str]) -> bool:
    # Kiểm tra chỉ cần có ít nhất 1 token xuất hiện trong text
    for t in tokens:
        # Nếu token có khoảng trắng -> match trực tiếp (substring)
        if " " in t:
            if t in text: return True
        else:
            # Nếu token ngắn (chữ/số) -> match nguyên từ bằng \b
            if re.search(rf"\b{re.escape(t)}\b", text):
                return True
    return False

def _contains_all(text: str, token_sets: List[List[str]]) -> bool:
    """
    Kiểm tra tập hợp token_sets: mỗi set yêu cầu tất cả token trong set phải xuất hiện.
    - Nếu 1 set thoả (tất cả token đều có) thì trả True.
    - Logic tương tự lookahead nhưng viết dễ hiểu hơn.
    """
    for group in token_sets:
        ok = True
        for t in group:
            if " " in t:
                if t not in text: ok = False; break
            else:
                if not re.search(rf"\b{re.escape(t)}\b", text):
                    ok = False; break
        if ok:
            return True
    return False

def detect_benefits_tokens(text_norm: str, BENEFIT_TOKENS: Dict) -> Dict[str, List[str]]:
    # Tìm các phúc lợi xuất hiện trong văn bản đã chuẩn hoá dựa trên cấu hình BENEFIT_TOKENS
    found: Dict[str, List[str]] = {}
    for g_key, g_val in BENEFIT_TOKENS.items():
        for i_key, i_val in g_val.get("items", {}).items():
            any_tokens = i_val.get("any", [])
            all_sets = i_val.get("all", [])

            hit = False
            # Nếu match bất kỳ token trong any_tokens
            if any_tokens and _contains_any(text_norm, any_tokens):
                hit = True
            # Hoặc match đủ toàn bộ token trong ít nhất 1 set trong all_sets
            if not hit and all_sets and _contains_all(text_norm, all_sets):
                hit = True

            if hit:
                found.setdefault(g_key, []).append(i_key)
    # Loại bỏ trùng lặp, sắp xếp kết quả
    for g in list(found.keys()):
        found[g] = sorted(set(found[g]))
    return found

def _scan_row(row):
    """
    Ghép 3 cột văn bản (mô tả công việc, yêu cầu công việc, phúc lợi),
    chuẩn hoá rồi dò tìm phúc lợi dựa vào BENEFIT_TOKENS.
    Trả về tuple:
      - nhom_phuc_loi (chuỗi mô tả nhóm và item phúc lợi)
      - so_phuc_loi_tim_duoc (số lượng phúc lợi tìm được)
    """
    try:
        # Ghép text từ 3 cột chính
        raw = " \n ".join([
            str(row.get("mo_ta_cong_viec", "")),
            str(row.get("yeu_cau_cong_viec", "")),
            str(row.get("phuc_loi", ""))
        ])
        text_norm = normalize_text(raw)

        # Phát hiện token phúc lợi
        found = detect_benefits_tokens(text_norm, BENEFIT_TOKENS)

        parts, total = [], 0
        for g_key, g_val in BENEFIT_TOKENS.items():
            items = sorted(set(found.get(g_key, [])))
            if not items:
                continue

            # Lấy nhãn hiển thị (label) nếu có, fallback về key nếu thiếu
            labels = []
            item_map = g_val.get("items", {})
            for i_key in items:
                labels.append(item_map.get(i_key, {}).get("label", i_key))

            title = g_val.get("title", g_key)
            parts.append(f"{title}: " + ", ".join(labels))
            total += len(items)

        return " | ".join(parts), int(total)
    except Exception:
        # Nếu lỗi trong quá trình xử lý → trả về mặc định rỗng
        return "", 0
#============================================================
NGANH_NGHE = {
    "Bán Lẻ/Tiêu Dùng": [
        "Quản Lý Cửa Hàng",
        "Quản Lý Khu Vực",
        "Thu Mua",
        "Trợ Lý Bán Lẻ",
    ],
    "Bảo Hiểm": [
        "Bao Tiêu/Bảo Lãnh",
        "Bồi Thường Bảo Hiểm",
        "Tư Vấn Rủi Ro",
        "Định Phí Bảo Hiểm",
    ],
    "Bất Động Sản": [
        "Cho Thuê & Quản Lý Căn Hộ",
        "Kinh Doanh Thương Mại, Cho Thuê & Quản Lý Tài Sản",
        "Phát Triển Bất Động Sản",
        "Phân Tích Dự Án Bất Động Sản",
        "Quản Lý Cơ Sơ Vật Chất",
        "Định Giá",
    ],
    "CEO & General Management": [
        "Ceo",
        "Quản Lý Cấp Cao",
    ],
    "Chính Phủ/Phi Lợi Nhuận": [
        "Chính sách, Quy hoạch & Quy Định",
        "NGO/Phi Lợi Nhuận",
    ],
    "Công Nghệ Thông Tin/Viễn Thông": [
        "Bảo Mật Công Nghệ Thông Tin",
        "Chuyển Đổi Số",
        "Data Engineer/Data Analyst/AI",
        "IT Support/Help Desk",
        "Phân Tích Kinh Doanh/Phân Tích Hệ Thống",
        "Phần Cứng Máy Tính",
        "Phần Mềm Máy Tính",
        "QA/QC/Software Testing",
        "Quản Lý Công Nghệ Thông Tin",
        "Quản Lý Dự Án Công Nghệ",
        "Quản Trị Cơ Sở Dữ Liệu",
        "System/Cloud/DevOps Engineer",
        "UX/UI Design",
        "Viễn Thông",
    ],
    "Dược": ["Phân Phối Dược Phẩm"],
    "Dệt May/Da Giày": [
        "Phát Triển Sản Phẩm May Mặc",
        "Quản Lý Đơn Hàng",
    ],
    "Dịch Vụ Khách Hàng": [
        "Dịch Vụ Khách Hàng",
        "Dịch Vụ Khách Hàng - Call Center",
        "Dịch Vụ Khách Hàng - Hướng Khách Hàng",
    ],
    "Dịch Vụ Ăn Uống": [
        "Quản Lý F&B",
        "Quầy Bar/Đồ Uống/Phục vụ",
        "Đầu Bếp",
    ],
    "Giáo Dục": [
        "Dịch Vụ Sinh Viên/Hỗ Trợ Học Viên",
        "Giảng Dạy/Đào Tạo",
        "Nghiên Cứu Học Thuật",
        "Quản Lý Giáo Dục",
        "Tư Vấn Giáo Dục",
    ],
    "Hành Chính Văn Phòng": [
        "Biên Phiên DỊch",
        "Bảo Vệ",
        "Hành Chính",
        "Lễ Tân/Tiếp Tân",
        "Quản Lý Văn Phòng",
        "Thu Mua",
        "Thư Ký",
        "Trợ Lý Kinh Doanh",
        "Điều Phối",
    ],
    "Hậu Cần/Xuất Nhập Khẩu/Kho Bãi": [
        "Quản Lý Chuỗi Cung Ứng",
        "Quản Lý Kho & Phân Phối",
        "Quản Lý Đội Xe",
        "Thu Mua & Quản Trị Hàng Tồn Kho",
        "Vận Tải/Giao Nhận Hàng Hóa",
        "Xuất Nhập Khẩu & Thủ Tục Hải Quan",
    ],
    "Khoa Học & Kỹ Thuật": [
        "Công Nghệ Sinh Học",
        "Công Nghệ Thực Phẩm",
        "Cơ Khí & Điện Lạnh",
        "Khai Thác Mỏ",
        "Kỹ Thuật Hóa Học",
        "Kỹ Thuật Môi Trường",
        "Kỹ Thuật Ô Tô",
        "Kỹ Thuật Điện/Điện Tử",
        "Điện/Nước/Chất Thải",
    ],
    "Kinh Doanh": [
        "Bán Hàng Kỹ Thuật",
        "Bán Hàng Qua Điện Thoại",
        "Bán Hàng/Phát Triển Kinh Doanh",
    ],
    "Kiến Trúc/Xây Dựng": [
        "An Toàn Lao Động",
        "Phát Triển Dự Án/Đấu Thầu",
        "Quản Lý Dự Án",
        "Thiết Kế & Quy Hoạch Đô Thị",
        "Thiết Kế Kiến Trúc/Họa Viên Kiến Trúc",
        "Thiết Kế Nội Thất",
        "Xây Dựng",
    ],
    "Kế Toán/Kiểm Toán": [
        "Kiểm Soát Viên Tài Chính",
        "Kiểm Toán",
        "Kế Toán Chi Phí",
        "Kế Toán Công Nợ",
        "Kế Toán Doanh Thu",
        "Kế Toán Quản Trị",
        "Kế Toán Thanh Toán",
        "Kế Toán Thuế",
        "Kế Toán Tài Chính",
        "Kế Toán Tổng Hợp",
        "Kế hoạch/Tư Vấn Doanh Nghiệp",
    ],
    "Kỹ Thuật": [
        "Bảo trì/Bảo Dưỡng",
        "Cơ Khí Tự Động Hoá",
        "In Ấn",
        "Kỹ Thuật CNC",
    ],
    "Nghệ thuật, Truyền thông/In ấn/Xuất bản": [
        "In Ấn & Xuất Bản",
        "Sản Xuất Chương Trình",
        "Đạo Diễn Nghệ Thuật/Nhiếp Ảnh",
    ],
    "Ngân Hàng & Dịch Vụ Tài Chính": [
        "Dịch Vụ Hỗ Trợ Khách Hàng",
        "Môi Giới & Giao Dịch Chứng Khoán",
        "Phân Tích & Báo Cáo Tài Chính",
        "Quản Lý Quan Hệ Khách Hàng",
        "Quản Lý Quỹ",
        "Thu Hồi Nợ",
        "Tuân Thủ & Kiểm Soát Rủi Ro",
        "Tín Dụng",
        "Đầu Tư Tài Chính",
    ],
    "Nhà Hàng - Khách Sạn/Du Lịch": [
        "Bộ Phận Tiền Sảnh & Dịch Vụ Khách Hàng",
        "Công Ty Kinh Doanh Lữ Hành",
        "Hướng Dẫn Viên Du Lịch",
        "Vệ Sinh Buồng Phòng",
        "Đại Lý Du Lịch",
        "Đặt Phòng Khách Sạn",
    ],
    "Nhân Sự/Tuyển Dụng": [
        "Nhân Sự/Tuyển Dụng",
        "Lương Thưởng & Phúc Lợi",
        "Nhân Sự Tổng Hợp",
        "Quản Trị Hiệu Suất & Sự Nghiệp",
        "Tuyển Dụng",
        "Đào Tạo Và Phát Triển",
    ],
    "Nông/Lâm/Ngư Nghiệp": ["Nông/Lâm/Ngư Nghiệp"],
    "Pháp Lý": [
        "Luật Lao động/Hưu Trí",
        "Luật Sở Hữu Trí Tuệ",
        "Luật Thuế",
        "Luật Tài Chính Ngân Hàng Thương mại",
        "Luật Xây Dựng",
        "Quản Lý Thi Hành Pháp Luật",
        "Thư Ký Luật & Trợ Lý Luật",
        "Thư Ký Pháp Lý",
        "Tư Vấn Pháp Lý",
    ],
    "Sản Xuất": [
        "Hoạch Định & Quản Lý Sản Xuất",
        "Nghiên Cứu & Phát Triển",
        "Phân Tích Sản Xuất",
        "Quy Trình & Lắp Ráp",
        "Vận Hành Máy Móc",
        "Đảm Bảo Chất Lượng/Kiểm Soát Chất Lượng/Quản Lý Chất Lượng",
    ],
    "Thiết Kế": [
        "Chỉnh Sửa Video",
        "Thiết Kế Công Nghiệp/Kỹ Thuật",
        "Thiết Kế Thời Trang/Trang Sức",
        "Thiết Kế Đồ Họa",
    ],
    "Tiếp Thị, Quảng Cáo/Truyền Thông": [
        "Nghiên Cứu & Phân Tích Thị Trường",
        "Quan Hệ Công Chúng",
        "Quản Lý & Phát Triển Sản Phẩm",
        "Quản Lý Sự Kiện",
        "Quản Lý Thương Hiệu",
        "Quản Lý Tài Khoản Khách Hàng",
        "Tiếp Thị",
        "Tiếp Thị Nội Dung",
        "Tiếp Thị Thương Mại",
        "Tiếp Thị Trực Tuyến",
    ],
    "Vận Tải": [
        "Dịch Vụ Hàng Không",
        "Dịch Vụ Vận Tải Công Cộng",
        "Vận Tải Đường Bộ",
        "Vận Tải Đường Sắt & Hàng Hải",
    ],
    "Y Tế/Chăm Sóc Sức Khoẻ": [
        "Bác Sĩ/Điều Trị Đa Khoa/Điều Trị Nội Trú",
        "Dược Sĩ",
        "Kỹ Thuật Viên Y Tế",
        "Tư Vấn Tâm Lý & Công Tác Xã Hội",
        "Y Tá",
    ],
}
import pandas as pd
def extract_nganh_nghe(df: pd.DataFrame,
                       col: str = "nganh_nghe",
                       child_col: str = "nganh") -> pd.DataFrame:
    # Chuẩn hoá cột ngành: tách Cha/Con từ biểu diễn dạng "Cha > Con"
    # - Đảm bảo tồn tại cột con (child_col) ngay sau cột cha (col) để schema nhất quán.
    if col not in df.columns:
        raise ValueError(f"Không thấy cột '{col}' trong DataFrame")

    # 1) Đảm bảo cột `nganh` nằm ngay sau `nganh_nghe`
    insert_at = df.columns.get_loc(col) + 1
    if child_col not in df.columns:
        df.insert(insert_at, child_col, "no_info")
    else:
        # nếu đã có, đưa về đúng vị trí ngay sau `nganh_nghe`
        cols = df.columns.tolist()
        cols.insert(insert_at, cols.pop(cols.index(child_col)))
        df = df.loc[:, cols]

    # 2) Tách Cha/Con
    s = df[col].fillna("").astype(str).str.strip()

    def _parse(text: str):
        # Quy ước: nếu có dấu '>' thì phần trước là Cha, phần sau là Con.
        # Nếu không có '>' thì coi toàn bộ là Cha, Con = "no_info".
        if ">" in text:
            p, c = text.split(">", 1)
            return p.strip(), c.strip() if c.strip() else "no_info"
        return text if text else "no_info", "no_info"

    parsed = s.apply(_parse)
    df[col]       = parsed.apply(lambda x: x[0])  # Cha -> ghi đè vào nganh_nghe
    df[child_col] = parsed.apply(lambda x: x[1])  # Con -> ghi vào nganh

    return df

############################################################
def extract_age_range(df: pd.DataFrame, col: str = "do_tuoi") -> pd.DataFrame:
    # Rút khoảng tuổi từ cột 'do_tuoi' (ví dụ "22-30" hoặc "25") thành min/max/med.
    # Chèn 3 cột mới ngay sau cột gốc để dễ quan sát và giữ ngữ cảnh.
    # Lưu ý: dữ liệu nhiễu (text không chứa số) sẽ trả None cho cả 3.
    # Xác định vị trí cột do_tuoi
    idx = df.columns.get_loc(col)

    # Tạo sẵn cột rỗng ngay sau do_tuoi
    df.insert(idx + 1, "min_tuoi", None)
    df.insert(idx + 2, "max_tuoi", None)
    df.insert(idx + 3, "med_tuoi", None)
    def parse_age(text):
        if pd.isna(text):
            return (None, None, None)
        text = str(text).strip()
        nums = re.findall(r"\d+", text)
        if len(nums) == 2:
            min_t = int(nums[0])
            max_t = int(nums[1])
        elif len(nums) == 1:
            min_t = max_t = int(nums[0])
        else:
            return (None, None, None)
        med_t = (min_t + max_t) / 2
        return (min_t, max_t, med_t)

    df[["min_tuoi", "max_tuoi", "med_tuoi"]] = df[col].apply(lambda x: pd.Series(parse_age(x)))
    return df

def split_quymo(df: pd.DataFrame, col: str) -> pd.DataFrame:
    # Chuẩn hoá "quy_mo_cong_ty" thành min/max/med (ví dụ "50-100 nhân viên" → 50/100/75).
    # Hỗ trợ các biến thể: có dấu nghìn ".", ","; có hậu tố "nhân viên/nhan vien"; chuỗi chỉ một số ("10+").
    idx = df.columns.get_loc(col)
    # Tạo sẵn cột rỗng
    df.insert(idx + 1, "min_quymo", None)
    df.insert(idx + 2, "max_quymo", None)
    df.insert(idx + 3, "med_quymo", None)
    def _parse_quymo(val):
        if pd.isna(val):
            return ("no_info", "no_info", "no_info")

        s = str(val).lower().replace("nhân viên", "").replace("nhan vien", "").strip()
        if not s:
            return ("no_info", "no_info", "no_info")

        # bỏ dấu . hoặc , trong số (ngăn cách hàng nghìn)
        s = re.sub(r"[.,](?=\d{3})", "", s)

        # tách khoảng min-max
        m = re.match(r"(\d+)\s*-\s*(\d+)", s)
        if m:
            mn, mx = int(m.group(1)), int(m.group(2))
            return (mn, mx, (mn + mx) / 2)

        # chỉ có 1 số (ví dụ "10+")
        m = re.match(r"(\d+)", s)
        if m:
            val = int(m.group(1))
            return (val, val, val)

        return ("no_info", "no_info", "no_info")

    # áp dụng
    df["min_quymo"], df["max_quymo"], df["med_quymo"] = zip(*df[col].map(_parse_quymo))
    return df
import unicodedata
import pandas as pd

def _strip_accents(s: str) -> str:
    """Bỏ dấu tiếng Việt, giữ chữ thường.
    Dùng cho chuẩn hoá giá trị 'thiếu' (không hiển thị/none/nan...) trước khi quyết định thay bằng 'no_info'."""
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn").lower()

def xu_ly_thieu(df: pd.DataFrame) -> pd.DataFrame:
    # Chuẩn hoá giá trị thiếu/rác thành "no_info" trên toàn DataFrame:
    # - NaN/None -> "no_info"
    # - Chuỗi trống, "nan", "không hiển thị", "none" (không dấu) -> "no_info"
    # Lợi ích: đơn giản hoá các bước lọc và thống kê về sau.
    def _clean_cell(x):
        if pd.isna(x):
            return "no_info"
        s = str(x).strip()
        s_norm = _strip_accents(s)

        if s_norm in {"", "nan", "khong hien thi", "none"}:
            return "no_info"
        return x

    # pandas >= 2.1 có DataFrame.map
    if hasattr(df, "map"):
        return df.map(_clean_cell)
    return df.applymap(_clean_cell)
#================
def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Sắp xếp lại cột theo nhóm logic phục vụ phân tích/đọc hiểu:
    # - Đặt các cột khoá & meta lên đầu (id, tiêu đề, ngành...),
    # - nhóm lương/ngày/giờ, phúc lợi, ứng viên, loại hình & hạn, tuyển dụng, công ty,
    # - giữ nguyên các cột còn lại theo thứ tự xuất hiện,
    # - luôn để 'href' ở cuối (tham chiếu nguồn).
    # Nhóm cột theo yêu cầu
    cols_order = [
        # 1. ID và job info
        "id", "ten_cong_viec", "nganh", "nganh_nghe","ky_nang",

        # 2. Nhóm lương
        "luong", "loai_tien_te", "check_luong",
        "min_luong", "max_luong", "med_luong", "ky_tra_luong",

        # 3. Nhóm ngày làm việc
        "ngay_lam_viec", "so_ngay_lam",

        # 4. Nhóm giờ làm việc
        "gio_lam_viec", "gio_bat_dau", "gio_ket_thuc", "so_gio_lam_ngay",

        # 5. Phúc lợi
        "phuc_loi", "nhom_phuc_loi","so_phuc_loi_tim_duoc",

        # 6. Thông tin ứng viên
        "ngon_ngu_cv", "so_nam_kinh_nghiem", "quoc_tich",
        "trinh_do_hoc_van", "gioi_tinh", "do_tuoi",
        "min_tuoi", "max_tuoi", "med_tuoi", "hon_nhan",

        # 7. Loại hình làm việc và hạn nộp
        "loai_hinh_lam_viec", "het_han",

        # 8. Tuyển dụng
        "so_luong_tuyen", "luot_xem",

        # 9. Công ty
        "ten_cong_ty", "quy_mo_cong_ty",
        "min_quymo", "max_quymo", "med_quymo",
    ]

    # Các cột còn lại ngoài danh sách trên
    remaining = [c for c in df.columns if c not in cols_order and c != "href"]

    # Ghép tất cả, đảm bảo href cuối cùng
    final_order = cols_order + remaining + ["href"]

    # Lọc lại theo danh sách (có check tồn tại)
    final_order = [c for c in final_order if c in df.columns]
    return df[final_order]
def drop_rows_with_too_much_noinfo(df: pd.DataFrame, threshold: float = 0.85) -> pd.DataFrame:
    # Loại các dòng có tỷ lệ "no_info" quá cao (mặc định >85% số cột):
    # - Hữu ích để làm sạch những bản ghi không đủ dữ liệu cho phân tích.
    # - Có thể điều chỉnh 'threshold' tuỳ bài toán.
    # Đếm số 'no_info' trên mỗi dòng
    counts_noinfo = (df.astype(str)
                     .apply(lambda row: row.str.lower().eq("no_info"))
                     .sum(axis=1))

    # Ngưỡng số lượng 'no_info' cho 1 dòng
    limit = int(df.shape[1] * threshold)

    # Giữ lại các dòng đạt điều kiện
    df_clean = df.loc[counts_noinfo <= limit].copy()

    return df_clean
#=============================================================
def clean_nganh_nghe(df: pd.DataFrame, col: str = "nganh_nghe") -> pd.DataFrame:
    # Làm sạch nhanh cột "nganh_nghe": nếu có cấu trúc "Cha > Con" thì giữ phần "Cha".
    # Trường hợp cột không tồn tại: cảnh báo và bỏ qua (không làm gãy pipeline).
    if col not in df.columns:
        print(f"[WARN] Không thấy cột {col}, bỏ qua.")
        return df

    df[col] = df[col].astype(str).str.split(">").str[0].str.strip()
    return df
#=================================================================
def _norm_no_accent(s: str) -> str:
    """Lowercase + bỏ dấu tiếng Việt để so khớp không dấu.
    Dùng cho module phát hiện ngôn ngữ từ mô tả công việc."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.lower()

# Từ khóa nhận diện (đã bỏ dấu, viết thường)
# Gợi ý: mở rộng dần theo tập dữ liệu thực tế (ví dụ thêm chứng chỉ/kiểu viết tắt mới).
_LANG_PATTERNS = {
    "Tiếng Anh": [
        r"\benglish\b", r"\bielts\b", r"\btoeic\b", r"\btoefl\b",
        r"\banh van\b", r"\btieng anh\b", r"\bt.a\b", r"\benglish communication\b"
    ],
    "Tiếng Nhật": [
        r"\bjapanese\b", r"\bnihongo\b", r"\bjlpt\b", r"\bn[1-5]\b",
        r"\btieng nhat\b", r"\bnhat ngu\b"
    ],
    "Tiếng Hàn": [
        r"\bkorean\b", r"\btopik\b", r"\bhangul\b", r"\btieng han\b", r"\bhan ngu\b"
    ],
    "Tiếng Trung": [
        r"\bchinese\b", r"\bmandarin\b", r"\bhsk\b", r"\btieng trung\b",
        r"\bphien am pho thong\b", r"\bquan thoai\b"
    ],
    "Tiếng Pháp": [
        r"\bfrench\b", r"\bdelf\b", r"\bdalf\b", r"\btieng phap\b"
    ],
    "Tiếng Đức": [
        r"\bgerman\b", r"\bdeutsch\b", r"\bgoethe\-?zertifikat\b", r"\btieng duc\b"
    ],
    "Tiếng Tây Ban Nha": [
        r"\bspanish\b", r"\bespanol\b", r"\bdele\b", r"\btieng tay ban nha\b"
    ],
    # Dùng cho bước “quét lần 2” -> nếu có tiếng Việt => Bất Kỳ
    "Tiếng Việt": [
        r"\bvietnamese\b", r"\btieng viet\b", r"\btv\b"
    ],
}

# Biên dịch regex (OR) cho mỗi ngôn ngữ, chạy trên phiên bản "không dấu"
_COMPILED = {
    lang: re.compile("|".join(pats), flags=re.IGNORECASE)
    for lang, pats in _LANG_PATTERNS.items()
}

# Thứ tự ưu tiên khi hiển thị nếu có nhiều ngôn ngữ (không bao gồm Tiếng Việt vì sẽ -> Bất Kỳ)
_LANG_ORDER = [
    "Tiếng Anh", "Tiếng Nhật", "Tiếng Hàn", "Tiếng Trung",
    "Tiếng Pháp", "Tiếng Đức", "Tiếng Tây Ban Nha"
]

def _detect_languages(text: str) -> set[str]:
    """Trả về tập các nhãn ngôn ngữ chuẩn tìm thấy trong text.
    Cách làm: bỏ dấu + lower → so khớp regex đã biên dịch theo từng ngôn ngữ."""
    norm = _norm_no_accent(text)
    found = set()
    for lang, rx in _COMPILED.items():
        if rx.search(norm):
            found.add(lang)
    return found

def update_ngon_ngu_cv(
    df: pd.DataFrame,
    desc_cols=("mo_ta_cong_viec", "yeu_cau_cong_viec"),
    out_col="ngon_ngu_cv"
) -> pd.DataFrame:
    """
    Suy luận 'ngon_ngu_cv' từ mô tả & yêu cầu công việc:
    - Quét các cột văn bản (desc_cols), nhận diện token liên quan ngôn ngữ.
    - Nếu có 'Tiếng Việt' ở bất kỳ đâu -> đặt 'Bất Kỳ' (không ràng buộc ngoại ngữ).
    - Nếu nhiều ngôn ngữ: sắp xếp theo _LANG_ORDER rồi join, thêm các ngôn ngữ khác (nếu có) ở cuối.
    - Nếu không phát hiện: giữ nguyên giá trị cũ (tránh ghi đè bừa).
    """
    for c in desc_cols:
        if c not in df.columns:
            # Cột không có thì coi như chuỗi rỗng
            df[c] = ""

    if out_col not in df.columns:
        df[out_col] = None

    def _resolve_lang(row) -> str:
        text_all = " | ".join(str(row[c]) if pd.notna(row[c]) else "" for c in desc_cols)
        found = _detect_languages(text_all)

        # Bước 2: nếu có 'Tiếng Việt' -> Bất Kỳ
        if "Tiếng Việt" in found:
            return "Bất Kỳ"

        # Không có -> giữ nguyên hiện tại
        if not found:
            return row[out_col] if pd.notna(row[out_col]) and str(row[out_col]).strip() else None

        # Có nhiều -> sắp theo thứ tự ưu tiên rồi join
        ordered = [lang for lang in _LANG_ORDER if lang in found]
        # Nếu có lang khác không nằm trong danh sách ưu tiên (hiếm), thêm vào cuối
        others = sorted(l for l in found if l not in _LANG_ORDER)
        result_list = ordered + others
        return ", ".join(result_list) if result_list else None

    df[out_col] = df.apply(_resolve_lang, axis=1)

    # Lần quét thứ hai độc lập (nếu ai muốn chắc chắn):
    # nếu ngay trong out_col vẫn còn 'Tiếng Việt' (do nguồn trước đó), đổi thành Bất Kỳ
    mask_vn = df[out_col].astype(str).str.contains(r"\bTiếng Việt\b", case=False, na=False)
    df.loc[mask_vn, out_col] = "Bất Kỳ"

    return df

#======================================================================
def _project_root() -> Path:
    # Xác định thư mục gốc của project:
    # - File hiện tại (preprocess.py) nằm trong ./processor/, do đó parents[1] chính là root cấp trên.
    # Ưu điểm: không phụ thuộc vào nơi chạy script, đường dẫn IO ổn định theo cấu trúc repo.
    return Path(__file__).resolve().parents[1]

def _apply_pipeline(df: pd.DataFrame, out_file: Path) -> pd.DataFrame:
    """Chạy toàn bộ các bước xử lý lên df và GHI ĐÈ dần vào out_file sau mỗi bước.
    Mục tiêu:
      - Biến đổi dữ liệu thô (crawl) → dữ liệu phân tích được (đã chuẩn hoá).
      - Mỗi bước đều 'checkpoint' ra Excel để tiện debug/so sánh diff giữa các giai đoạn.
    Lưu ý:
      - Mỗi khối try/except tự chịu lỗi: pipeline không dừng toàn cục khi một bước fail.
      - Đảm bảo hiệu ứng từng bước độc lập và có log rõ ràng để truy vết.
    """
    import os

    # 1) Đổi tên cột về không dấu
    try:
        # Chuẩn hoá header: bỏ dấu, snake_case ngắn gọn → thuận tiện cho xử lý & join về sau.
        df = rename_columns_no_diacritics(df)
        df.to_excel(out_file, index=False)
        print("  ✅ Đổi tên cột về không dấu")
    except NameError:
        # Trường hợp module đổi tên chưa được import/định nghĩa → giữ nguyên cột để pipeline đi tiếp.
        print("  ❕ [WARN] rename_columns_no_diacritics chưa có, giữ nguyên tên cột.")
    except Exception as e:
        print(f"  ❌ Lỗi đổi tên cột: {e}")

    # 2) Điền loại tiền tệ/Thương lượng
    try:
        # Suy luận 'loai_tien_te' (VND/USD/...) và 'check_luong' (True/False nếu thương lượng)
        # Ưu tiên heuristic; nếu cần mới hỏi model (qua biến môi trường OPENAI_MODEL).
        df = add_salary_columns_check_loai(df, model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        df.to_excel(out_file, index=False)
        print("  ✅ Điền loại tiền tệ/Thương lượng")
    except NameError as ne:
        print(f"  ❕ [WARN] Thiếu helper check_luong ({ne}). Bỏ qua.")
    except Exception as e:
        print(f"  ❌ Lỗi điền loại tiền tệ/Thương lượng: {e}")

    # 3) Tạo min/max/med/kỳ trả lương từ cột lương thô
    try:
        # Parse text lương tự do (ví dụ '15-20tr/tháng', 'từ 500$') → min/max/med + kỳ trả (giờ/tuần/tháng/năm).
        df = add_salary_columns_maxminmed_ky(df, salary_col="luong")
        df.to_excel(out_file, index=False)
        print("  ✅ Tạo min/max/med/kỳ trả lương")
    except NameError:
        print("  ❕ [WARN] add_salary_columns_maxminmed_ky chưa có, bỏ qua.")
    except TypeError as te:
        # Trường hợp chữ ký hàm khác (tham số không khớp) → log để chỉnh sửa tương thích.
        print(f"  ❕ [WARN] add_salary_columns_maxminmed_ky chữ ký khác. Bỏ qua. {te}")
    except Exception as e:
        print(f"  ❌ Lỗi tạo min/max/med/kỳ trả lương: {e}")

    # 4) Ngày/giờ làm việc
    try:
        # Tự động tính 'so_ngay_lam' (T2–T6, CN, …) và tách khoảng giờ làm việc lớn nhất trong text.
        # Chèn các cột mới (gio_bat_dau, gio_ket_thuc, so_gio_lam_ngay) cạnh anchor 'ky_tra_luong' để dễ quan sát.
        df = enrich_work_schedule_columns(
            df,
            col_ngay_lam_viec="ngay_lam_viec",
            col_gio_lam_viec="gio_lam_viec",
            anchor_col="ky_tra_luong"
        )
        df.to_excel(out_file, index=False)
        print("  ✅ Xử lý ngày/giờ làm việc")
    except Exception as e:
        print(f"  ❌ Lỗi xử lý ngày/giờ làm việc: {e}")
    try:
        # Vá lỗi phổ biến về nhận diện loại tiền tệ từ text (vd có cả '$' và 'tr' → ưu tiên VND).
        fix_currency_conflict(df, col_salary="luong", col_currency="loai_tien_te")
        df.to_excel(out_file, index=False)
        print("  ✅ chỉnh loai tien ")
    except Exception as e:
        print(f"  ❌ chỉnh loai tien {e}")
    # 5) Quy đổi lương về VND/tháng
    try:
        # Chuẩn hoá quy đổi: tất cả min/med/max đưa về VND theo THÁNG.
        # Dùng số giờ/ngày & số ngày/tuần (nếu thiếu: mặc định 8h, 6 ngày) để quy đổi từ đơn giá giờ/tuần.
        df = exchange_luong(
            df,
            currency_col="loai_tien_te",
            min_col="min_luong",
            max_col="max_luong",
            med_col="med_luong",
            period_col="ky_tra_luong",
            hours_per_day_col="so_gio_lam_ngay",
            days_per_week_col="so_ngay_lam",
            salary_text_col="luong",
        )
        df.to_excel(out_file, index=False)
        print("  ✅ Quy đổi lương về VND/tháng")
    except Exception as e:
        print(f"  ❌ Lỗi quy đổi lương: {e}")

    try:
        # Đánh dấu các trường hợp lương bất thường (outlier) theo tiêu chí riêng (hàm do người viết định nghĩa).
        danh_dau_luong_bat_thuong(df,
        min_col = "min_luong",
        med_col = "med_luong",
        max_col = "max_luong")
        df.to_excel(out_file, index=False)
        print("  ✅ Đánh dấu bất thường lương ")
    except Exception as e:
        print(f"  ❌ Đánh dấu bất thường lương {e}")
    # 6) Chuẩn hóa het_han/luot_xem (giữ số)
    try:
        # Trích số nguyên từ text (ví dụ 'Còn 12 ngày' → 12), tiện cho thống kê/sắp xếp.
        for col in ["het_han", "luot_xem"]:
            if col in df.columns:
                df[col] = df[col].apply(extract_number)
        df.to_excel(out_file, index=False)
        print("  ✅ Chuẩn hóa 'het_han' & 'luot_xem'")
    except Exception as e:
        print(f"  ❌ Lỗi chuẩn hóa het_han/luot_xem: {e}")
    try:
        # Suy luận ngôn ngữ hồ sơ (ngon_ngu_cv) từ mô tả & yêu cầu công việc (token hoá không dấu).
        df = update_ngon_ngu_cv(df)
        print(f"  ✅ Đã update ngôn ngữ")
    except Exception as e:
        print(f"  ⚠️ Lỗi khi quét ngôn ngữ trong preprocess: {e}")
    # 7) Phúc lợi
    try:
        # Gán nhãn phúc lợi theo cấu hình BENEFIT_TOKENS (any/all tokens).
        # Kết quả:
        #  - 'nhom_phuc_loi': chuỗi tóm tắt theo nhóm + item
        #  - 'so_phuc_loi_tim_duoc': đếm số item match
        def _scan_row(row):
            raw_text = " \n ".join([
                str(row.get("mo_ta_cong_viec", "")),
                str(row.get("yeu_cau_cong_viec", "")),
                str(row.get("phuc_loi", "")),
            ])
            text_norm = normalize_text(raw_text)
            found = detect_benefits_tokens(text_norm, BENEFIT_TOKENS)

            parts, total = [], 0
            for g_key, g_val in BENEFIT_TOKENS.items():
                items = found.get(g_key, [])
                if not items:
                    continue
                labels = [g_val["items"].get(i, {}).get("label", i) for i in items]
                parts.append(f"{g_val.get('title', g_key)}: " + ", ".join(labels))
                total += len(items)
            return " | ".join(parts), total

        # Áp trên từng dòng: result_type="expand" để nhận 2 cột kết quả
        results = df.apply(_scan_row, axis=1, result_type="expand")
        pos = df.columns.get_loc("phuc_loi") if "phuc_loi" in df.columns else len(df.columns) - 1
        # Tránh trùng cột cũ nếu từng chạy trước đó
        for c in ["nhom_phuc_loi", "so_phuc_loi_tim_duoc"]:
            if c in df.columns:
                df.drop(columns=[c], inplace=True)
        # Chèn cột mới cạnh 'phuc_loi' (nếu có), đảm bảo thứ tự hợp lý
        df.insert(min(pos + 1, len(df.columns)), "nhom_phuc_loi", results[0])
        df.insert(min(pos + 2, len(df.columns) + 1), "so_phuc_loi_tim_duoc", results[1])
        df.to_excel(out_file, index=False)
        print("  ✅ Gắn nhãn phúc lợi")
    except Exception as e:
        print(f"  ❌ Lỗi phúc lợi: {e}")

    # 8) Ngành nghề
    try:
        # Chuẩn hoá 'nganh_nghe' dạng 'Cha > Con' → tách ra 'nganh_nghe' (Cha) và 'nganh' (Con).
        df = extract_nganh_nghe(df, col="nganh_nghe", child_col="nganh")
        print("  ✅ Đã tách nganh_nghe.")
    except Exception as e:
        print(f"⚠️ Bỏ qua tách nganh_nghe->nganh: {e}")
    # 9) Độ tuổi
    try:
        # Parse 'do_tuoi' → min/max/med_tuoi (hỗ trợ '22-30', '25', ...)
        df = extract_age_range(df, "do_tuoi")
        df.to_excel(out_file, index=False)
        print("  ✅ Xử lý độ tuổi")
    except Exception as e:
        print(f"  ❌ Lỗi độ tuổi: {e}")

    # 10) Quy mô công ty
    try:
        # Chuẩn hoá 'quy_mo_cong_ty' → min/max/med_quymo (hỗ trợ dấu nghìn, '10+', '50-100 nhân viên', ...)
        df = split_quymo(df, "quy_mo_cong_ty")
        df.to_excel(out_file, index=False)
        print("  ✅ Xử lý quy_mo_cong_ty")
    except Exception as e:
        print(f"  ❌ Lỗi quy_mo_cong_ty: {e}")

    # 11) Điền no_info
    try:
        # Quy về 'no_info' cho NaN/chuỗi rỗng/các biến thể 'không hiển thị' → thống nhất dữ liệu thiếu.
        df = xu_ly_thieu(df)
        df.to_excel(out_file, index=False)
        print("  ✅ Điền 'no_info'")
    except Exception as e:
        print(f"  ❌ Lỗi điền 'no_info': {e}")
    try:
        # Loại các dòng quá thiếu dữ liệu (tỷ lệ 'no_info' > 85% số cột) để nâng chất lượng tập phân tích.
        df = drop_rows_with_too_much_noinfo(df, threshold=0.85)
        print("  ✅ Đã xoá dòng no_info vượt quá 85%")

    except Exception as e:
        print(f"❌ Lỗi khi xử lý no_info: {e}")
    # 12) Làm sạch & sắp xếp cột
    try:
        try:
            # Dọn 'nganh_nghe' lần cuối: giữ phần 'Cha' nếu lỡ dư 'Cha > Con'.
            df = clean_nganh_nghe(df, col="nganh_nghe")
            print("  ✅ Làm sạch nganh_nghe")
        except Exception as e:
            print(f"  [WARN] clean_nganh_nghe: {e}")
        # Sắp theo thứ tự cột chuẩn hoá (ID, lương, ngày/giờ, phúc lợi, ứng viên, công ty, ...).
        df = reorder_columns(df)
        df.to_excel(out_file, index=False)
        print("  ✅ Sắp xếp lại cột")
    except Exception as e:
        print(f"  ❌ Lỗi sắp xếp cột: {e}")

    return df
###########################################

def main():
    from dotenv import load_dotenv
    import pandas as pd

    load_dotenv()

    # --- Định vị project root & thư mục IO ---
    # - JOBSDETAIL_DIR: đầu vào là các file chi tiết đã crawl (mỗi ngành/mỗi lần chạy 1 file).
    # - PREPROCESS_DIR: đầu ra của pipeline preprocess (mỗi file input → 1 file _preprocessed.xlsx).
    ROOT = _project_root()
    JOBSDETAIL_DIR = Path(os.getenv("JOBSDETAIL_DIR", ROOT / "output" / "jobsdetail"))
    PREPROCESS_DIR = Path(os.getenv("PREPROCESS_DIR", ROOT / "output" / "preprocess"))
    PREPROCESS_DIR.mkdir(parents=True, exist_ok=True)

    # In ra để debug (vị trí đọc/ghi)
    print(f"[DEBUG] ROOT          = {ROOT}")
    print(f"[DEBUG] READ  FROM    = {JOBSDETAIL_DIR}")
    print(f"[DEBUG] WRITE TO      = {PREPROCESS_DIR}")

    try:
        # Lấy danh sách file mới nhất theo từng nhóm (slug, gid, loc)
        # — tránh trộn nhiều phiên bản cũ/mới của cùng một ngành/địa phương.
        files = get_latest_detail_files(JOBSDETAIL_DIR)
        if not files:
            raise RuntimeError(f"Không tìm thấy file nào khớp mẫu trong {JOBSDETAIL_DIR}")

        print("[INFO] Sẽ xử lý lần lượt các file:")
        for f in files:
            print(" -", f.name)

        # Xử lý từng file độc lập để nếu 1 file lỗi vẫn không ảnh hưởng các file khác
        for fp in files:
            print("\n" + "=" * 80)
            print(f"[RUN] Đang xử lý: {fp.name}")
            try:
                # Đọc Excel bằng openpyxl (an toàn với định dạng mới)
                df = pd.read_excel(fp, engine="openpyxl")
                print(f"  ✅ Đọc {fp.name}: {df.shape[0]} dòng × {df.shape[1]} cột")

                out_file = PREPROCESS_DIR / f"{fp.stem}_preprocessed.xlsx"
                # Ghi bản gốc ngay lập tức: tiện theo dõi chênh lệch sau từng bước pipeline
                df.to_excel(out_file, index=False)

                # Chạy pipeline và nhận DataFrame đã xử lý
                df_done = _apply_pipeline(df, out_file)
                print(f"🎉 Hoàn tất file: {out_file} ({df_done.shape[0]} dòng)")

            except Exception as e:
                # Không dừng toàn bộ: log lỗi file hiện tại và chuyển sang file kế tiếp
                print(f"[ERROR] Lỗi xử lý {fp.name}: {e}")

        print("\n✅ Tất cả file đã được xử lý xong.")

    except Exception as e:
        print(f"[ERROR] Lỗi xử lý preprocess: {e}", file=sys.stderr)
        sys.exit(5)

if __name__ == "__main__":
    main()
