import unicodedata
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from datetime import datetime
import pandas as pd
import re
import uvicorn
import unicodedata as _ud
from typing import Optional
import traceback
import regex as re
MAPPING = {
    "ban le tieu dung": "Bán lẻ Tiêu dùng",
    "bao hiem": "Bảo hiểm",
    "bat dong san": "Bất động sản",
    "ceo general management": "CEO & General Management",
    "chinh phu phi loi nhuan": "Chính phủ Phi lợi nhuận",
    "cong nghe thong tin vien thong": "Công nghệ thông tin Viễn thông",
    "det may da giay": "Dệt may Da giày",
    "dich vu an uong": "Dịch vụ Ăn uống",
    "dich vu khach hang": "Dịch vụ Khách hàng",
    "duoc": "Dược",
    "giao duc": "Giáo dục",
    "hanh chinh van phong": "Hành chính Văn phòng",
    "hau can xuat nhap khau kho bai": "Hậu cần Xuất nhập khẩu Kho bãi",
    "ke toan kiem toan": "Kế toán Kiểm toán",
    "khoa hoc ky thuat": "Khoa học Kỹ thuật",
    "kien truc xay dung": "Kiến trúc Xây dựng",
    "kinh doanh": "Kinh doanh",
    "ky thuat": "Kỹ thuật",
    "nghe thuat truyen thong in an xuat ban": "Nghệ thuật Truyền thông In ấn Xuất bản",
    "nha hang khach san du lich": "Nhà hàng Khách sạn Du lịch",
    "nhan su tuyen dung": "Nhân sự Tuyển dụng",
    "nong lam ngu nghiep": "Nông lâm Ngư nghiệp",
    "phap ly": "Pháp lý",
    "san xuat": "Sản xuất",
    "thiet ke": "Thiết kế",
    "tiep thi quang cao truyen thong": "Tiếp thị Quảng cáo Truyền thông",
    "van tai": "Vận tải",
    "y te cham soc suc khoe": "Y tế Chăm sóc sức khỏe",
}
# ===== Alias (từ khoá/viết tắt/tiếng Anh -> slug chuẩn) =====
ALIASES = {
    # CNTT / Viễn thông
    "it": "cong nghe thong tin vien thong",
    "cntt": "cong nghe thong tin vien thong",
    "cong nghe": "cong nghe thong tin vien thong",
    "cong nghe thong tin": "cong nghe thong tin vien thong",
    "may tinh": "cong nghe thong tin vien thong",
    "developer": "cong nghe thong tin vien thong",
    "software": "cong nghe thong tin vien thong",
    "telecom": "cong nghe thong tin vien thong",
    "information": "cong nghe thong tin vien thong",
    "information tech": "cong nghe thong tin vien thong",
    "information technology": "cong nghe thong tin vien thong",
    "infotech": "cong nghe thong tin vien thong",
    "informatics": "cong nghe thong tin vien thong",
    "computer": "cong nghe thong tin vien thong",
    "computer science": "cong nghe thong tin vien thong",
    "cs": "cong nghe thong tin vien thong",
    "software engineer": "cong nghe thong tin vien thong",
    "software engineering": "cong nghe thong tin vien thong",
    "network": "cong nghe thong tin vien thong",
    "networking": "cong nghe thong tin vien thong",

    # Kế toán / Kiểm toán
    "ke toan": "ke toan kiem toan",
    "kiem toan": "ke toan kiem toan",
    "accounting": "ke toan kiem toan",
    "accountant": "ke toan kiem toan",
    "audit": "ke toan kiem toan",
    "ap": "ke toan kiem toan", "ar": "ke toan kiem toan", "gl": "ke toan kiem toan",

    # Logistics / XNK / Kho bãi
    "logistics": "hau can xuat nhap khau kho bai",
    "logistic": "hau can xuat nhap khau kho bai",
    "xuat nhap khau": "hau can xuat nhap khau kho bai",
    "warehouse": "hau can xuat nhap khau kho bai",
    "kho bai": "hau can xuat nhap khau kho bai",
    "supply chain": "hau can xuat nhap khau kho bai",
    "shipping": "hau can xuat nhap khau kho bai",
    "freight": "hau can xuat nhap khau kho bai",

    # Bất động sản
    "bat dong san": "bat dong san",
    "real estate": "bat dong san",
    "property": "bat dong san",

    # Marketing / Truyền thông / Quảng cáo
    "marketing": "tiep thi quang cao truyen thong",
    "quang cao": "tiep thi quang cao truyen thong",
    "truyen thong": "tiep thi quang cao truyen thong",
    "pr": "tiep thi quang cao truyen thong",
    "seo": "tiep thi quang cao truyen thong",
    "social media": "tiep thi quang cao truyen thong",

    # Bán lẻ / Tiêu dùng
    "ban le": "ban le tieu dung",
    "retail": "ban le tieu dung",
    "fmcg": "ban le tieu dung",
    "consumer goods": "ban le tieu dung",
    "sieu thi": "ban le tieu dung",

    # Giáo dục
    "giao duc": "giao duc",
    "education": "giao duc",
    "day hoc": "giao duc",
    "teacher": "giao duc",

    # Y tế / Chăm sóc sức khoẻ
    "y te": "y te cham soc suc khoe",
    "suc khoe": "y te cham soc suc khoe",
    "healthcare": "y te cham soc suc khoe",
    "medical": "y te cham soc suc khoe",
    "benh vien": "y te cham soc suc khoe",
    "phong kham": "y te cham soc suc khoe",

    # Nhân sự / Tuyển dụng
    "nhan su": "nhan su tuyen dung",
    "tuyen dung": "nhan su tuyen dung",
    "hr": "nhan su tuyen dung",
    "recruitment": "nhan su tuyen dung",
    "talent acquisition": "nhan su tuyen dung",
    "c b": "nhan su tuyen dung",
    "c&b": "nhan su tuyen dung",

    # Sản xuất
    "san xuat": "san xuat",
    "manufacturing": "san xuat",
    "factory": "san xuat",
    "production": "san xuat",

    # Pháp lý
    "phap ly": "phap ly",
    "legal": "phap ly",
    "law": "phap ly",
    "luat": "phap ly",

    # Nhà hàng / Khách sạn / Du lịch
    "nha hang": "nha hang khach san du lich",
    "khach san": "nha hang khach san du lich",
    "du lich": "nha hang khach san du lich",
    "hospitality": "nha hang khach san du lich",
    "hotel": "nha hang khach san du lich",

    # Dệt may / Da giày
    "det may": "det may da giay",
    "garment": "det may da giay",
    "apparel": "det may da giay",
    "giay dep": "det may da giay",
    "footwear": "det may da giay",

    # Thiết kế
    "thiet ke": "thiet ke",
    "design": "thiet ke",
    "graphic": "thiet ke",
    "ui": "thiet ke", "ux": "thiet ke", "ux ui": "thiet ke", "ux/ui": "thiet ke",

    # Kiến trúc / Xây dựng
    "xay dung": "kien truc xay dung",
    "kien truc": "kien truc xay dung",
    "construction": "kien truc xay dung",
    "civil": "kien truc xay dung",

    # Khoa học / Kỹ thuật
    "khoa hoc": "khoa hoc ky thuat",
    "rd": "khoa hoc ky thuat",
    "r&d": "khoa hoc ky thuat",
    "lab": "khoa hoc ky thuat",

    # Nông / Lâm / Ngư nghiệp
    "nong nghiep": "nong lam ngu nghiep",
    "lam nghiep": "nong lam ngu nghiep",
    "ngu nghiep": "nong lam ngu nghiep",
    "agriculture": "nong lam ngu nghiep",

    # Vận tải
    "van tai": "van tai",
    "transport": "van tai",
    "driver": "van tai",
    "delivery": "van tai",

    # Chính phủ / Phi lợi nhuận
    "chinh phu": "chinh phu phi loi nhuan",
    "ngo": "chinh phu phi loi nhuan",
    "non profit": "chinh phu phi loi nhuan",
    "non-profit": "chinh phu phi loi nhuan",
    "charity": "chinh phu phi loi nhuan",

    # CEO & General Management
    "ceo": "ceo general management",
    "general manager": "ceo general management",
    "managing director": "ceo general management",
}

def _no_accent_lower(s: str) -> str:
    if not s:
        return ""
    s = _ud.normalize("NFD", str(s))
    s = "".join(ch for ch in s if _ud.category(ch) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _ranked_suggestions(latest: dict, q: str) -> list[dict]:

    s = _no_accent_lower(q)

    # Chuẩn hoá items từ latest
    items = [
        {"slug": k[0], "label": v["label"], "gid": k[1], "loc": k[2], "file": v["file"].name}
        for k, v in latest.items()
    ]

    if not s:
        # Không có q -> trả rỗng cho dropdown, UI giữ danh sách đầy đủ trong <select>
        return []

    # 1) Alias EXACT -> chỉ trả về đúng 1 ngành
    exact_slug = ALIASES.get(s)
    if exact_slug:
        target = next(
            (it for it in items if _no_accent_lower(it["slug"]) == _no_accent_lower(exact_slug)),
            None
        )
        return [target] if target else []

    # 2) Alias PARTIAL -> trả các ngành từ những alias có chứa q (hoặc q chứa alias)
    partial_slugs = {
        v for k, v in ALIASES.items() if s in k or k in s
    }
    if partial_slugs:
        out = []
        seen = set()
        for slug in partial_slugs:
            it = next((x for x in items if _no_accent_lower(x["slug"]) == _no_accent_lower(slug)), None)
            if it and it["slug"] not in seen:
                seen.add(it["slug"]); out.append(it)
        return out  # KHÔNG bồi thêm

    # 3) Direct match: tất cả token trong query phải xuất hiện trong label/slug (không bồi thêm)
    toks = s.split()
    def _hit(it):
        lab = _no_accent_lower(it["label"])
        slg = _no_accent_lower(it["slug"])
        # mọi token đều phải có trong label hoặc slug
        return all((t in lab) or (t in slg) for t in toks)

    direct = [it for it in items if _hit(it)]
    return direct


# app.py đang ở <root>/web/app.py
HERE = Path(__file__).resolve().parent      # .../web
ROOT = HERE.parent                          # .../<root>

ANALYZER_DIR = ROOT / "output" / "analyzer" # đúng vị trí dữ liệu
INDEX_HTML   = HERE / "index.html"          # index.html nằm cạnh app.py

FNAME_RE = re.compile(
    r"^job_detail_output_(?P<slug>.+?)_g(?P<gid>\d+)_(?P<loc>\d{4})_"
    r"(?P<date>\d{4}-\d{2}-\d{2})_(?P<time>\d{6})_analyzed\.xlsx$",
    re.IGNORECASE,
)

app = FastAPI(title="One-Page Analyzer (React+FastAPI)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

app.mount("/web", StaticFiles(directory=HERE), name="web")

# ----------------- Helpers -----------------
def _strip_accents_lower(s):
    if s is None:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.lower().strip()
def _norm_slug_key(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower().replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    return s

def slug_to_label(slug: str) -> str:
    """
    Trả về tên hiển thị có dấu nếu có trong MAPPING,
    nếu không có thì fallback Title Case ASCII.
    """
    key = _norm_slug_key(slug)
    return MAPPING.get(key, key.title())

def _parse_dt(date_str: str, time_str: str) -> datetime:
    # date: YYYY-MM-DD, time: HHMMSS
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H%M%S")

def _scan_latest_by_key() -> dict:
    ANALYZER_DIR.mkdir(parents=True, exist_ok=True)

    latest: dict[tuple[str, str, str], dict] = {}
    for p in ANALYZER_DIR.glob("*.xlsx"):
        m = FNAME_RE.match(p.name)
        if not m:
            continue

        slug_raw = m.group("slug")
        gid      = m.group("gid")
        loc      = m.group("loc")
        dt       = _parse_dt(m.group("date"), m.group("time"))

        # CHUẨN HOÁ slug để so sánh & làm key
        slug_key = _norm_slug_key(slug_raw)
        key      = (slug_key, gid, loc)

        cur = latest.get(key)
        if cur is None or dt > cur["dt"]:
            latest[key] = {
                "file": p,
                "dt": dt,
                "label": slug_to_label(slug_key),  # <-- tên có dấu
                "slug": slug_key,                  # <-- slug đã chuẩn hoá (dùng cho API/search)
                "gid": gid,
                "loc": loc,
            }
    return latest

def _latest_for_slug(slug_pick: str) -> Path:
    slug_pick_norm = _norm_slug_key(slug_pick)
    latest = _scan_latest_by_key()

    # Khớp chính xác trước
    best = None
    for info in latest.values():
        if info["slug"] == slug_pick_norm:
            if best is None or info["dt"] > best["dt"]:
                best = info

    # Nếu chưa có, thử chứa chuỗi (tìm gần đúng)
    if best is None:
        for info in latest.values():
            if slug_pick_norm in info["slug"]:
                if best is None or info["dt"] > best["dt"]:
                    best = info

    if best is None:
        raise FileNotFoundError(slug_pick)

    return best["file"]

# ----------------- Routes -----------------
@app.get("/")
def serve_index():
    if not INDEX_HTML.exists():
        raise HTTPException(404, detail="index.html not found")
    return FileResponse(str(INDEX_HTML))


@app.get("/api/industries")
def list_industries(q: Optional[str] = None):
    latest = _scan_latest_by_key()

    if not q:
        items = []
        for (slug, gid, loc), info in latest.items():
            items.append({
                "slug": slug,
                "label": info["label"],
                "gid": gid,
                "loc": loc,
                "file": info["file"].name,
            })
        items.sort(key=lambda x: x["label"])
        return JSONResponse({"items": items, "count": len(items)})

    # Có q -> trả danh sách gợi ý đã xếp hạng theo alias + direct match
    items = _ranked_suggestions(latest, q)
    return JSONResponse({"items": items, "count": len(items)})


SHEET_MAPPING = {
    "tin_theo_nganh": "Số lượng tin",
    "phan_tich_luong": "Lương (VND/tháng)",
    "nam_kinh_nghiem": "Yêu cầu kinh nghiệm (năm)",
    "ngon_ngu_cv": "Yêu cầu ngôn ngữ",
    "trinh_do_hoc_van": "Yêu cầu trình độ học vấn",
    "loai_hinh_lam_viec": "Yêu cầu loại hình làm việc",
    "do_tuoi": "Yêu cầu độ tuổi",
    "ngay_lam_viec": "Ngày làm việc",
    "gio_lam_viec": "Giờ làm việc",
    "ky_nang": "Top 10 kỹ năng",
    "ky_nang_theo_nganh": "Kỹ năng yêu cầu nhiều",
    "phuc_loi_nhom_theo_nganh": "Phúc lợi nhận được",
}

@app.get("/api/analysis/{slug}")
def get_analysis(slug: str):
    try:
        fp = _latest_for_slug(slug)
    except FileNotFoundError:
        raise HTTPException(404, detail=f"Không tìm thấy file cho slug: {slug}")
    except Exception as e:
        raise HTTPException(400, detail=f"Lỗi tìm file cho slug {slug}: {e}")

    # Lấy slug từ tên file
    m = re.match(r"^job_detail_output_(.+?)_g\d+", fp.name)
    if not m:
        raise HTTPException(400, detail=f"Không parse được ngành từ tên file: {fp.name}")
    slug_raw = m.group(1).strip()
    slug_clean = slug_raw.replace("_", " ")
    nganh_text = MAPPING.get(slug_clean.lower(), slug_clean.title())

    print(f"[DEBUG] File: {fp.name}")
    print(f"[DEBUG] Slug raw: {slug_raw}")
    print(f"[DEBUG] Slug clean: {slug_clean}")
    print(f"[DEBUG] Ngành mapped: {nganh_text}")

    try:
        xls = pd.ExcelFile(fp, engine="openpyxl")
    except Exception as e:
        raise HTTPException(400, detail=f"Không đọc được Excel: {e}")

    sheets = []
    try:
        for sheet_name in xls.sheet_names:
            try:
                df = xls.parse(sheet_name)
            except Exception:
                continue

            # SỬA: bỏ dấu cách thừa & typo
            df = df.rename(columns={
                "nganh": "Tên ngành",
                "so_tin": "Số tin",
                "so_tin_co_luong": "Có thông tin lương",
                "so_tin_co_thuong_luong": "Lương thương lượng",
                "ty_le": "Tỷ lệ (%)",
                "med_luong_tb": "Mức lương trung bình",
                "min_luong": "Lương nhỏ nhất",
                "max_luong": "Lương lớn nhất",
                "so_voi_tb_chung": "Chênh lệch với lương trung bình",
                "so_tin_luong_bat_thuong": "Lương bất thường",
                "chi_tiet_bat_thuong": "Chi tiết",
                "mean_exp": "Số năm trung bình",
                "min_exp": "Số năm nhỏ nhất",
                "max_exp": "Số năm lớn nhất",
                "so_tin_no_exp": "Không yêu cầu (tin)",
                "so_tin_co_exp": "Có yêu cầu (tin)",
                "Duoi_1": "Dưới 1 năm",
                "1_3": "Từ 1-3 năm",
                "4_6": "Từ 4-6 năm",
                "Tren_7": "Từ 7 năm",
                "ngon_ngu": "Ngôn ngữ",
                "ty_le(%)": "Tỷ lệ (%)",
                "top_nganh": "Ngành yêu cầu nhiều nhất",
                "so_tin_top_nganh": "Số lượng",
                "trinh_do": "Trình độ",
                "loai_hinh": "Loại hình làm việc",         # <<< bỏ khoảng trắng đầu
                "min_tuoi": "Độ tuổi nhỏ nhất",
                "max_tuoi": "Độ tuổi lớn nhất",
                "mean_tuoi": "Độ tuổi trung bình",
                "15-24": "Từ 15-24 tuổi",
                "25-34": "Từ 25-34 tuổi",
                "35-54": "Từ 35-54 tuổi",
                "55+": "Trên 55 tuổi",
                "no_info": "Không có thông tin",
                "tong_so_tin": "Tổng số tin",
                "T2-T6": "Thứ 2 - thứ 6",
                "T2-T7": "Thứ 2 - thứ 7",
                "Khac": "Các ngày khác",
                "Chi_tiet_khac": "Chi tiết",
                "gio_bat_dau_som_nhat": "Giờ bắt đầu sớm nhất",
                "gio_bat_dau_muon_nhat": "Giờ bắt đầu muộn nhất",
                "gio_ket_thuc_som_nhat": "Giờ kết thúc sớm nhất",
                "gio_ket_thuc_muon_nhat": "Giờ kết thúc muộn nhất",
                "tb_so_gio_lam_ngay": "Số giờ trung bình trong ngày",
                "top10_ky_nang_nhieu_nhat": "10 kỹ năng yêu cầu nhiều nhất theo ngành",  # <<< sửa "năm" -> "năng"
                "BaoHiem-SK": "Bảo hiểm - sức khoẻ",
                "DaoTao-PT": "Được đào tạo",
                "Luong-Thuong": "Lương - Thưởng",
                "NghiPhep-Time": "Nghỉ phép",
                "PhuCap-CanTin": "Phụ cấp - căn tin",
                "ThietBi-CongCu": "Hỗ trợ thiết bị",        # <<< bỏ khoảng trắng đầu
                "VanHoa-Team": "Hoạt động - du lịch",
                "XeDuaDon": "Xe đưa đón",
            })

            display_name = SHEET_MAPPING.get(sheet_name, sheet_name)

            def _to_json_safe(df_):
                out = []
                for r in df_.to_dict(orient="records"):
                    clean = {}
                    for k, v in r.items():
                        try:
                            if pd.isna(v):
                                clean[k] = None
                            elif isinstance(v, (int, float)):
                                fv = float(v)
                                clean[k] = int(fv) if fv.is_integer() else fv
                            else:
                                clean[k] = str(v)
                        except Exception:
                            clean[k] = str(v)
                    out.append(clean)
                return out

            df_show = df.head(1000)
            df_for_table = df_show

            if display_name == "Số lượng tin":
                df_for_table = df_for_table.drop(columns=["Tỷ lệ", "Tỷ lệ (%)"], errors="ignore")

            if display_name == "Lương (VND/tháng)":
                df_for_table = df_for_table.drop(
                    columns=["Lương bất thường", "Chi tiết", "Chênh lệch với lương trung bình"],
                    errors="ignore"
                )

            if display_name == "Yêu cầu kinh nghiệm (năm)":
                df_for_table = df_for_table.drop(
                    columns=["Không yêu cầu (tin)", "Dưới 1 năm", "Từ 1-3 năm", "Từ 4-6 năm", "Từ 7 năm"],
                    errors="ignore"
                )

            if display_name == "Yêu cầu ngôn ngữ":
                df_for_table = df_for_table.drop(
                    columns=["Ngôn ngữ", "Số tin", "Tỷ lệ (%)", "Ngành yêu cầu nhiều nhất", "Số lượng"],
                    errors="ignore"
                )

            if display_name == "Yêu cầu độ tuổi":
                df_for_table = df_for_table.drop(
                    columns=["Từ 15-24 tuổi", "Từ 25-34 tuổi", "Từ 35-54 tuổi", "Trên 55 tuổi","Không có thông tin", "Tổng số tin","15–24","25–34","35–54"],
                    errors="ignore"
                )
                col_name = next((c for c in ["Tên ngành", "ten_nganh", "Tên Ngành", "nganh"]
                                 if c in df_for_table.columns), None)
                if col_name:
                    import unicodedata
                    def _norm(s):
                        if s is None: return ""
                        s = unicodedata.normalize("NFD", str(s))
                        s = "".join(ch for ch in s if unicodedata.category(s) != "Mn")
                        return s.strip().lower()
                    mask = ~df_for_table[col_name].map(_strip_accents_lower).isin(["top nganh"])
                    df_for_table = df_for_table[mask]

            if display_name == "Ngày làm việc":
                df_for_table = df_for_table.drop(
                    columns=["Các ngày khác", "Chi tiết", "Tổng số tin"], errors="ignore"
                )

                col_name = next((c for c in ["Tên ngành", "ten_nganh", "Tên Ngành", "nganh"]
                                 if c in df_for_table.columns), None)
                if col_name:
                    import unicodedata
                    def _norm(s):
                        if s is None: return ""
                        s = unicodedata.normalize("NFD", str(s))
                        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
                        return s.strip().lower()
                    mask = ~df_for_table[col_name].map(_norm).isin(["top nganh"])
                    df_for_table = df_for_table[mask]

            if display_name == "Giờ làm việc":
                df_for_table = df_for_table.drop(
                    columns=["Giờ bắt đầu sớm nhất", "Giờ bắt đầu muộn nhất",
                             "Giờ kết thúc sớm nhất", "Giờ kết thúc muộn nhất"],
                    errors="ignore"
                )

            html = df_for_table.to_html(index=False, classes="table table-sm", border=0)
            sheets.append({
                "name": display_name,
                "rows": int(len(df)),
                "preview_rows": int(len(df_show)),
                "html": html,
                "json": _to_json_safe(df_show),
            })
    except Exception as e:
        print("[ERROR] Lỗi xử lý sheet:")
        print(traceback.format_exc())
        raise HTTPException(500, detail=f"Lỗi xử lý sheet: {e}")

    meta = {
        "file": fp.name,
        "path": str(fp),
        "sheets": len(sheets),
        "nganh": nganh_text,
        "title": nganh_text,
    }
    return JSONResponse({"meta": meta, "sheets": sheets})



# ========== MAIN ==========
if __name__ == "__main__":
   import uvicorn
   uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
