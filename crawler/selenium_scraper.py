from bs4 import BeautifulSoup
import os
import re
import time
import unicodedata
from datetime import datetime
from typing import List, Dict
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ===========================================================
# MỤC ĐÍCH TỆP: CÁC HÀM & HẰNG SỐ PHỤ TRỢ CHO BỘ THU THẬP
# VIỆC LÀM TỪ VIETNAMWORKS (CRAWL + PHÂN TÍCH).
#
# Lưu ý đọc hiểu cho giảng viên:
# - File này KHÔNG tự chạy độc lập mà được import vào luồng crawler chính.
# - Các comment giải thích rõ "tại sao làm vậy" để phục vụ thẩm định học thuật.
# - Chúng tôi cố gắng viết selector/logic ở mức ổn định, nhưng giao diện VNW
#   có thể thay đổi (class động dạng sc-xxxx). Khi đó cần cập nhật selector.
# - Các thao tác đều tuân thủ quy tắc: chờ trang tải, cuộn lazy-load, tách link.
# ===========================================================

# ===================== CẤU HÌNH NGÀNH =====================
# Map tên ngành (hiển thị) -> group_id trên VietnamWorks
# Dùng cho việc build URL /viec-lam?g=<group_id>
# Ghi chú:
# - Đây là ánh xạ thủ công dựa trên trạng thái website tại thời điểm triển khai.
# - Nếu VietnamWorks thay đổi taxonomy/ID, cần cập nhật lại bảng này.
VNWORKS_GROUPS: Dict[str, int] = {
    "Bán Lẻ/Tiêu Dùng": 24,
    "Bảo Hiểm": 14,
    "Bất Động Sản": 23,
    "CEO & General Management": 29,
    "Chính Phủ/Phi Lợi Nhuận": 25,
    "Công Nghệ Thông Tin/Viễn Thông": 5,
    "Dược": 28,
    "Dệt May/Da Giày": 26,
    "Dịch Vụ Khách Hàng": 6,
    "Dịch Vụ Ăn Uống": 11,
    "Giáo Dục": 1,
    "Hành Chính Văn Phòng": 20,
    "Hậu Cần/Xuất Nhập Khẩu/Kho Bãi": 13,
    "Khoa Học & Kỹ Thuật": 9,
    "Kinh Doanh": 21,
    "Kiến Trúc/Xây Dựng": 4,
    "Kế Toán/Kiểm Toán": 2,
    "Kỹ Thuật": 22,
    "Nghệ thuật, Truyền thông/In ấn/Xuất bản": 18,
    "Ngân Hàng & Dịch Vụ Tài Chính": 10,
    "Nhà Hàng - Khách Sạn/Du Lịch": 15,
    "Nhân Sự/Tuyển Dụng": 12,
    "Nông/Lâm/Ngư Nghiệp": 3,
    "Pháp Lý": 16,
    "Sản Xuất": 27,
    "Thiết Kế": 7,
    "Tiếp Thị, Quảng Cáo/Truyền Thông": 17,
    "Vận Tải": 8,
    "Y Tế/Chăm Sóc Sức Khoẻ": 19
}

def slugify_vn(s: str) -> str:
        # Chuẩn hoá chuỗi tiếng Việt về 'slug' không dấu, dùng cho tên file/thư mục.
        # Lý do:
        #   (1) Tránh ký tự đặc biệt gây lỗi khi tạo path trên nhiều hệ điều hành.
        #   (2) Dễ tìm kiếm, so khớp, và đảm bảo tính nhất quán trong pipeline.
        # Các bước xử lý:
        #   - Thay '/' bằng space để không vô tình tạo cấp thư mục.
        #   - Chuyển riêng 'Đ/đ' sang 'D/d' (normalize NFD không đụng đến ký tự này).
        #   - Dùng NFD để tách dấu tiếng Việt; loại mọi ký tự Mn (dấu) để "bỏ dấu".
        #   - Chỉ giữ [a-zA-Z0-9] và khoảng trắng; ký tự khác thay bằng space.
        #   - Ép nhiều khoảng trắng liên tiếp về 1 space, trim 2 đầu, rồi lower-case.
    s = s.replace("/", " ")
    s = s.replace("Đ", "D").replace("đ", "d")  # xử lý Đ/đ riêng
    s = unicodedata.normalize("NFD", s)        # tách dấu
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # bỏ dấu
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)      # chỉ giữ chữ/số/space
    s = re.sub(r"\s+", " ", s).strip()         # gộp space thừa
    return s.lower()

# DOMAIN cơ sở để ghép với các đường dẫn tương đối khi trích xuất link.
BASE = "https://www.vietnamworks.com"

def _scroll_lazy(driver, times=8, dy=1500, pause=0.25):
    """
    Cuộn trang theo 'đợt' để kích hoạt lazy-loading.

    Vì danh sách job ở VietnamWorks tải dần (infinite/lazy load), nếu không cuộn
    thì DOM chỉ có một phần. Hàm này mô phỏng hành vi người dùng cuộn xuống.

    Tham số:
    - times: số lần cuộn liên tiếp (tăng nếu thấy chưa tải đủ job).
    - dy: số pixel mỗi lần cuộn (lớn hơn → cuộn nhanh hơn nhưng dễ bỏ sót render).
    - pause: thời gian chờ giữa hai lần cuộn để trang kịp render/thêm node mới.

    Kỹ thuật:
    - Dùng ActionChains.scroll_by_amount thay vì send_keys(PAGE_DOWN) để chủ động.
    - Cần đảm bảo viewport đang focus đúng phần thân trang; nếu trang có modal/
      banner, cuộn có thể không tác dụng → xử lý ở nơi gọi (đóng pop-up trước).
    """
    for _ in range(times):
        ActionChains(driver).scroll_by_amount(0, dy).perform()
        time.sleep(pause)

def _extract_links_stepwise_from_card(card) -> List[str]:
    """
    Trích xuất các liên kết (href) đến trang chi tiết job từ 1 "card" trong danh sách.

    Ý tưởng & bối cảnh:
    - Giao diện VietnamWorks render card bằng styled-components (class dạng sc-xxxx),
      khá "mỏng manh" vì class có thể đổi tên theo mỗi lần build/deploy.
    - Do đó, ta đi xuyên từng lớp bao để đến thẻ <a> hiển thị ảnh/tiêu đề có
      class 'img_job_card' — đây là điểm bám ổn định hơn ở thời điểm triển khai.
    - Pattern '-jv' trong href là dấu hiệu URL chi tiết tuyển dụng (quan sát thực tế).

    An toàn:
    - Bọc try/except: nếu cấu trúc DOM đổi, đừng ném lỗi toàn cục; trả về [] để
      caller có thể tiếp tục với các card khác.
    - Nếu href là đường dẫn tương đối (bắt đầu bằng '/'), ghép với BASE để tạo URL đầy đủ.

    Trả về:
    - Danh sách các URL (thường 0 hoặc 1 phần tử cho mỗi card).
    """
    links = []
    try:
        # Lần lượt tìm các lớp chứa ảnh/anchor của card:
        sc1 = card.find_element(By.CSS_SELECTOR, "div.sc-iVDsrp")
        sc2 = sc1.find_element(By.CSS_SELECTOR, "div.sc-frWhYi")
        sc3 = sc2.find_element(By.CSS_SELECTOR, "div.sc-hxAGuE")
        # Anchor chính có class 'img_job_card' và href chứa '-jv' (pattern link chi tiết)
        a = sc3.find_element(By.CSS_SELECTOR, "a.img_job_card[href*='-jv']")
        href = (a.get_attribute("href") or "").strip()
        if href:
            # Nếu là href tương đối, prepend BASE để tạo absolute URL.
            if href.startswith("/"):
                href = BASE + href
            links.append(href)
    except Exception:
        # Card không theo cấu trúc kỳ vọng -> bỏ qua yên lặng (tránh gãy luồng xử lý).
        pass
    return links

def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)   # tối đa 60s load trang
    driver.set_script_timeout(60)      # timeout khi chạy JS
    return driver

def get_vietnamworks_jobs_by_group(
    group_id: int,
    group_name: str,
    max_pages: int = 0,           # 0 = không giới hạn theo tham số này (chỉ còn giới hạn bởi safety_max_pages / no_gain_patience)
    delay: float = 1.0,           # nghỉ giữa 2 page (lịch sự với server, giúp tránh bị rate-limit / CAPTCHA)
    safety_max_pages: int = 200,  # chốt an toàn chống loop vô hạn/redirect lặp (kể cả khi max_pages=0)
    no_gain_patience: int = 2,    # số trang liên tiếp không thu thêm link mới -> dừng để tránh cuộn vô ích
) -> List[Dict]:
    """
    Trình thu thập link job theo 'group_id' (ngành) trên VietnamWorks.
    Trả về list dict: {title: "", href, group_id, group_name}
    (title để trống vì chỉ lấy link; có thể mở trang chi tiết để fill sau).

    Mục tiêu & Lý do thiết kế:
    - Thu gom "đường dẫn chi tiết việc làm" theo từng ngành (group_id) thông qua trang listing.
    - Hạn chế phụ thuộc vào giao diện dễ đổi (class động sc-xxxx) bằng cách:
        (i) chờ phần tử "xương sống" của trang xuất hiện (block-job-list),
        (ii) cuộn kích hoạt lazy-load,
        (iii) đi xuyên card để lấy <a> chi tiết (đã đóng gói trong _extract_links_stepwise_from_card).
    - Tích hợp các cơ chế dừng an toàn:
        * max_pages: giới hạn do người dùng truyền vào (0 = không giới hạn theo tham số này).
        * safety_max_pages: "cầu chì" chống lỗi vòng lặp/redirect.
        * no_gain_patience: dừng khi nhiều trang liền không thêm được liên kết mới (tiết kiệm tài nguyên).

    Thứ tự xử lý (high-level):
    1) Lặp qua các trang /viec-lam?g=<id>&page=<n>.
    2) Chờ khối block-job-list → cuộn lazy-load → trích "card" → rút href chi tiết job.
    3) Dùng seen_hrefs khử trùng lặp trong phiên; dùng "chữ ký trang" (hash của tập href) để phát hiện trang lặp.
    4) Dừng theo một trong các điều kiện: đạt giới hạn, trang rỗng, trang lặp, nhiều trang không tăng dữ liệu.
    5) Trả về danh sách bản ghi tối thiểu (title="", href, group_id, group_name) đã khử trùng lặp lần cuối.

    Ghi chú kỹ thuật:
    - WebDriverWait(wait=12s) nhằm cân bằng giữa độ ổn định (mạng chậm) và tổng thời gian crawl.
    - "window-size=1920x1080" giúp bố cục desktop render đầy đủ, giảm rủi ro layout khác biệt.
    - "user-agent" đặt rõ ràng để tránh bị phân loại là trình tự động quá "lộ liễu".
    - "page_hrefs" lưu tất cả href trên trang (kể cả trùng) để tạo signature ổn định; "page_links" chỉ là phần mới.
    - Hash Python có ngẫu nhiên giữa các tiến trình (PYTHONHASHSEED), nhưng đủ ổn trong phạm vi 1 phiên chạy.
    """

    base_url = f"{BASE}/viec-lam?g={group_id}"

    # ---- Khởi tạo Chrome WebDriver ----
    driver = create_driver()
    wait = WebDriverWait(driver, 25)   # tăng timeout từ 12 → 25s

    # ---- Biến trạng thái thu thập ----
    results: List[Dict] = []      # chứa record tối thiểu cho từng job
    seen_hrefs: set = set()       # set để khử trùng lặp trong phiên (O(1) tra cứu)
    seen_signatures: set = set()  # chữ ký trang: hash(sorted(set(page_hrefs))) để phát hiện vòng lặp/redirect
    page = 1
    no_gain_streak = 0            # đếm số lần liên tiếp không thêm được link mới

    try:
        while True:
            # --- Giới hạn trang bởi tham số/khoá an toàn ---
            if max_pages > 0 and page > max_pages:
                print(f"[{group_name}] Đạt giới hạn max_pages. Dừng.")
                break
            if page > safety_max_pages:
                print(f"[{group_name}] Vượt safety_max_pages. Dừng.")
                break

            # Build URL: trang 1 dùng base_url, từ trang 2 thêm &page=
            url = base_url if page == 1 else f"{base_url}&page={page}"
            print(f"[{group_name}] [FETCH] {url}")
            driver.get(url)

            # Chờ khối 'block-job-list' xuất hiện (cột sống của page listing)
            # Nếu không thấy: không vội kết luận lỗi → có thể là hết dữ liệu/redirect/băng thông chậm.
            try:
                wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.block-job-list"))
                )
            except Exception:
                # Có thể do mạng/chuyển trang/hết trang -> vẫn tiếp tục xử lý bên dưới để xác nhận
                print(f"[{group_name}] [WARN] Chưa thấy block-job-list sau timeout.")

            # Cuộn để kích hoạt lazy-load các card (danh sách thường tải dần khi người dùng cuộn)
            _scroll_lazy(driver, times=8, dy=1500, pause=0.25)

            # Tìm container danh sách job (điểm neo để lấy các card)
            try:
                block = driver.find_element(By.CSS_SELECTOR, "div.block-job-list")
            except Exception:
                # Nếu không có container => có thể là trang cuối/DOM thay đổi mạnh -> dừng vòng lặp chính
                print(f"[{group_name}] Không tìm thấy block-job-list. Dừng.")
                break

            # Lấy tất cả 'card' job (mẫu class chung). Không phụ thuộc index (item-0..49)
            # Ưu tiên selector đủ cụ thể để tránh lẫn với các khối khác, nhưng vẫn tránh "quá chặt" vào class động.
            cards = block.find_elements(
                By.CSS_SELECTOR, "div.search_list.view_job_item.new-job-card"
            )

            # --- Gom link trên trang hiện tại ---
            # page_hrefs: mọi href rút được (kể cả trùng) -> dùng tạo "chữ ký trang".
            # page_links: chỉ các href chưa từng thấy trong phiên -> dùng để push vào results.
            page_links, page_hrefs = [], []
            for card in cards:
                for href in _extract_links_stepwise_from_card(card):
                    if href:
                        page_hrefs.append(href)  # chữ ký trang dùng toàn bộ (kể cả trùng)
                        if href not in seen_hrefs:
                            seen_hrefs.add(href)
                            page_links.append(href)  # chỉ những link mới được thêm

            # Nếu không thấy bất kỳ href nào -> coi như trang rỗng / kết thúc dữ liệu
            if not page_hrefs:
                print(f"[{group_name}] Trang không có job. Dừng.")
                break

            # --- Chống vòng lặp/redirect bằng chữ ký trang ---
            # Sort + set để tạo signature ổn định, rồi hash; nếu trùng lặp -> khả năng redirect/vòng lặp.
            page_signature = "|".join(sorted(set(page_hrefs)))
            sig_hash = hash(page_signature)
            if sig_hash in seen_signatures:
                print(f"[{group_name}] Trang có chữ ký lặp lại (redirect/lặp). Dừng.")
                break
            seen_signatures.add(sig_hash)

            # --- Kiểm soát 'không tăng dữ liệu' ---
            if not page_links:
                no_gain_streak += 1
                print(f"[{group_name}] Không có job mới ở trang {page}. no_gain_streak={no_gain_streak}.")
                if no_gain_streak >= no_gain_patience:
                    print(f"[{group_name}] Nhiều trang liên tiếp không tăng dữ liệu. Dừng.")
                    break
            else:
                # Có link mới -> reset streak & push kết quả
                no_gain_streak = 0
                for href in page_links:
                    results.append({
                        "title": "",          # placeholder: chưa parse tiêu đề (sẽ điền khi crawl chi tiết)
                        "href": href,
                        "group_id": group_id,
                        "group_name": group_name,
                    })
                print(f"[{group_name}] Trang {page}: +{len(page_links)} job (tổng {len(results)}).")

            # Sang trang kế, nghỉ 'delay' để đỡ bị nghi ngờ spam (giả lập hành vi người dùng thật)
            page += 1
            time.sleep(delay)

    finally:
        # Đảm bảo đóng trình duyệt dù lỗi hay hoàn tất (giải phóng tài nguyên hệ thống)
        driver.quit()

    # --- Khử trùng lặp lần cuối (phòng trường hợp hi hữu do race/DOM trùng) ---
    # Dựa trên key 'href' (định danh đường dẫn job) để giữ bản ghi cuối cùng cho mỗi href.
    dedup = {it["href"]: it for it in results}
    return list(dedup.values())


def save_group_to_excel(rows: List[Dict], group_name: str, location_code: str = "1001", out_dir: str = "outputs") -> str:
    # Tạo thư mục đầu ra nếu chưa có
    # Lý do: Khi chạy batch cho nhiều ngành/địa điểm, đảm bảo path luôn tồn tại, tránh lỗi I/O.
    os.makedirs(out_dir, exist_ok=True)
    # Chuẩn hoá tên ngành thành slug (không dấu) để đặt tên file an toàn
    # Mục tiêu: tên file thống nhất, không có ký tự đặc biệt gây lỗi trên Windows/Linux.
    name_slug = slugify_vn(group_name)
    # File đầu ra có kèm mã location_code trong tên (chỉ ảnh hưởng tên file, không lọc dữ liệu)
    # Ghi chú: hiện tại file KHÔNG gắn timestamp → lần sau ghi cùng ngành sẽ ghi đè.
    # Tôi chủ động để như vậy nhằm có “bản mới nhất” cho mỗi ngành; nếu cần lịch sử, sẽ bổ sung datetime vào tên.
    filename = f"vietnamworks_jobs_{name_slug}_{location_code}.xlsx"
    path = os.path.join(out_dir, filename)

    # Chuyển list[dict] -> DataFrame để ghi Excel
    df = pd.DataFrame(rows)
    # Chèn cột thời điểm crawl vào đầu bảng (format yyyy-mm-dd HH:MM:SS, timezone-naive)
    # Lý do: giúp truy vết thời điểm thu thập; một số phân tích downstream cần mốc thời gian này.
    df.insert(0, "crawled_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Ưu tiên engine 'xlsxwriter'; nếu thiếu package thì fallback sang 'openpyxl'
    # Nhận xét: xlsxwriter cho tốc độ/định dạng tốt khi ghi mới; openpyxl tiện lợi khi cần đọc/ghi chỉnh sửa.
    engine = "xlsxwriter"
    try:
        with pd.ExcelWriter(path, engine=engine) as writer:
            df.to_excel(writer, index=False, sheet_name="jobs")
    except ModuleNotFoundError:
        engine = "openpyxl"
        with pd.ExcelWriter(path, engine=engine) as writer:
            df.to_excel(writer, index=False, sheet_name="jobs")

    # Log đường dẫn file, số dòng và engine đã dùng
    print(f"[SAVE] {path} ({len(rows)} dòng) bằng engine={engine}")
    return path


def crawl_all_groups(
    groups: Dict[str, int] = VNWORKS_GROUPS,
    max_pages: int = 0,
    delay: float = 1.0,
    safety_max_pages: int = 200,
    no_gain_patience: int = 2,
    location_code: str = "1001",
    out_dir: str = "outputs",
):
    # Lặp qua từng ngành (group_name, gid) trong map cấu hình
    # Triết lý triển khai:
    # - Mỗi ngành chạy độc lập: nếu 1 ngành lỗi, các ngành khác vẫn tiếp tục (batch bền bỉ).
    # - Tham số (max_pages, delay, no_gain_patience) truyền xuyên suốt để giữ hành vi nhất quán.
    for group_name, gid in groups.items():
        try:
            # Gọi crawler cho từng group_id; các tham số kiểm soát vòng lặp trang
            # Ghi chú: safety_max_pages được áp dụng ngay trong get_vietnamworks_jobs_by_group (cầu chì vòng lặp).
            rows = get_vietnamworks_jobs_by_group(
                group_id=gid,
                group_name=group_name,
                max_pages=max_pages,
                delay=delay,
                no_gain_patience=no_gain_patience,
            )
            # Ghi kết quả riêng từng ngành ra file Excel (đặt tên theo group + location_code)
            # Lợi ích: pipeline xử lý phía sau (tiền xử lý/biểu đồ) dễ chọn đúng file theo ngành/địa điểm.
            save_group_to_excel(rows, group_name, location_code=location_code, out_dir=out_dir)
        except Exception as e:
            # Bắt mọi lỗi để không làm dừng toàn bộ batch; log tên ngành + id
            # Thực tế: DOM có thể thay đổi theo đợt deploy → nếu lỗi 1 ngành, vẫn muốn thu thập ngành còn lại.
            print(f"[ERROR] {group_name} (g={gid}): {e}")


# ===================== PHẦN 2: Hàm hỗ trợ bóc chi tiết =====================

def _extract_benefits(driver) -> str:
    # Dùng BeautifulSoup trên source HTML hiện tại của driver (đã load trang chi tiết)
    # Lý do: Có nội dung render sẵn trong DOM; Soup thao tác nhanh, phù hợp để bóc text/phúc lợi.
    soup = BeautifulSoup(driver.page_source, "html.parser")
    # Khu vực phúc lợi: selector theo class 'sc-b8164b97-0 kxYTHC' (class styled-component -> dễ thay đổi)
    # Tôi chấp nhận độ “mỏng” của selector này vì chưa thấy hook ổn định hơn; khi vỡ selector sẽ cập nhật.
    benefit_zone = soup.find("div", class_="sc-b8164b97-0 kxYTHC")
    benefits = []
    if benefit_zone:
        # Mỗi block phúc lợi: 'sc-8868b866-0 hoIaMz' (cũng là class "mỏng", có thể đổi theo lần build)
        # Nếu sau này đổi class, chiến lược dự phòng là tìm theo cấu trúc lân cận (ví dụ tiêu đề + mô tả).
        blocks = benefit_zone.find_all("div", class_="sc-8868b866-0 hoIaMz")
        for block in blocks:
            # Tiêu đề phúc lợi
            title_tag = block.find("p", class_="sc-ab270149-0 jlpjAq")
            # Nội dung mô tả chi tiết
            desc_tag = block.find("div", class_="sc-c683181c-2 fGxLZh")
            if title_tag and desc_tag:
                title = title_tag.get_text(strip=True)
                # Ghép các dòng mô tả bằng xuống dòng để dễ đọc
                # Lợi ích: phục vụ xuất Excel/CSV hoặc hiển thị UI text wrap gọn gàng.
                desc = desc_tag.get_text(separator="\n", strip=True)
                benefits.append(f"{title}: {desc}")
    # Trả về chuỗi nhiều dòng, mỗi dòng một phúc lợi
    return "\n".join(benefits)


def _get_text_by_class(soup, tag, class_part, index=0) -> str:
    # Tìm tất cả element theo tên thẻ 'tag' có class chứa chuỗi con 'class_part'
    # (match "chứa" chứ không phải bằng tuyệt đối; hữu ích khi class có nhiều token)
    # Ưu điểm: bớt phụ thuộc vào toàn bộ chuỗi class; chỉ cần một phần ổn định là đủ.
    els = soup.find_all(tag, class_=lambda x: x and class_part in x)
    # Lấy text của phần tử thứ 'index' nếu có, ngược lại trả chuỗi rỗng
    # Dùng get_text(strip=True) để loại bỏ khoảng trắng dư thừa.
    return els[index].get_text(strip=True) if len(els) > index else ""


def _click_expand_buttons(driver, wait: WebDriverWait, max_clicks: int = 20):
    # Click các nút "Xem thêm"/"Xem đầy đủ mô tả công việc" để mở rộng nội dung ẩn
    # Bối cảnh: nhiều trang chi tiết rút gọn mô tả bằng accordion; cần mở ra để Soup thu đủ nội dung.
    # Lưu ý: tham số 'wait' hiện chưa được sử dụng (giữ để đồng bộ interface, có thể hữu ích nếu chờ element động)
    clicked_count = 0
    for _ in range(max_clicks):
        try:
            # Tìm các button bên trong vùng có class chứa 'sc-8868b866-0' và 'kAAFiO'
            # Chỉ giữ các button hiển thị & có text hợp lệ
            # Lý do dùng XPATH: biểu thức contains() giúp “nới lỏng” điều kiện khi class có nhiều token động.
            buttons = [
                btn for btn in driver.find_elements(
                    By.XPATH,
                    "//div[contains(@class, 'sc-8868b866-0') and contains(@class, 'kAAFiO')]//button"
                )
                if btn.is_displayed() and btn.text.strip() and (
                    "Xem thêm" in btn.text or "Xem đầy đủ mô tả công việc" in btn.text
                )
            ]
            # Nếu không còn button mở rộng -> dừng vòng lặp
            if not buttons:
                break
            # Dùng JS click để tránh lỗi intercept (che khuất/overlay), ổn định hơn click() thông thường
            # Sau click, DOM thường reflow → sleep ngắn để nội dung mới render xong trước khi bóc.
            driver.execute_script("arguments[0].click();", buttons[0])
            clicked_count += 1
            # Nghỉ ngắn để nội dung mở rộng kịp render
            time.sleep(0.4)
        except Exception:
            # Nếu có lỗi (hết element, DOM đổi…) -> dừng an toàn
            # Đây là quyết định bảo thủ để tránh vỡ luồng chính do lỗi giao diện cục bộ.
            break
    # Log số lần đã click mở rộng (nếu có)
    if clicked_count:
        print(f"  [INFO] Đã click mở rộng {clicked_count} lần.")

# ===================== PHẦN 3: Crawl chi tiết job =====================
def scrape_job_details_from_links(job_links: List[str], start_id: int = 1000001) -> pd.DataFrame:
    # Cấu hình Chrome. Có thể bật headless khi chạy server/CI để tiết kiệm tài nguyên.
    # Ghi chú triển khai:
    # - "--window-size" cố định giúp layout ổn định, tránh case giao diện mobile.
    # - Headless đôi khi render khác headful; nếu selector lỗi ở headless thì chuyển sang headful để kiểm chứng.
    driver = create_driver()
    wait = WebDriverWait(driver, 30)   # chi tiết cần chờ lâu hơn

    all_jobs = []  # danh sách dict từng job để ghép thành DataFrame
    # Quy ước dữ liệu:
    # - Mỗi bản ghi là 1 dict gồm trường cố định (ID, HREF) + các trường bóc được (tiêu đề, lương, mô tả, phúc lợi...).
    # - Các trường phụ thuộc DOM (class styled-components) có thể trống nếu website đổi class → xử lý downstream cần tolerant.

    try:
        for index, job_url in enumerate(job_links):
            print(f"\n[{index + 1}/{len(job_links)}] Đang xử lý: {job_url}")
            try:
                # Điều hướng đến trang job chi tiết
                driver.get(job_url)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                # Cho trang có thời gian load, sau đó scroll xuống đáy để kích hoạt lazy-load
                # Lý do: nhiều phần (mô tả/phúc lợi) chỉ render khi người dùng cuộn xuống.
                time.sleep(1.2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.8)

                # Mở rộng các vùng mô tả bị ẩn ("Xem thêm", "Xem đầy đủ mô tả công việc")
                # Mục tiêu: đảm bảo BeautifulSoup nhìn thấy full nội dung để bóc text sạch, tránh thiếu đoạn.
                _click_expand_buttons(driver, wait, max_clicks=20)
                time.sleep(1.0)  # đợi DOM render sau khi expand

                # Trích phúc lợi (dạng text nhiều dòng)
                # Tách riêng bước phúc lợi để dễ thay đổi chiến lược selector tại một nơi.
                benefits_text = _extract_benefits(driver)

                # Dùng BeautifulSoup để parse HTML sau cùng (đã expand)
                # Lợi thế của Soup: xử lý văn bản thuận tiện (get_text, separator), dễ map ra dict.
                soup = BeautifulSoup(driver.page_source, "html.parser")

                # Khởi tạo dict trường dữ liệu chính. Lưu ý: class như 'hAejeW', 'cVbwLK', 'ePOHWr'
                # là class styled-component -> dễ thay đổi theo build; khi đổi sẽ trả rỗng.
                # Chúng tôi chấp nhận rủi ro này và sẽ cập nhật selector khi phát hiện thay đổi DOM.
                job_fields = {
                    "ID": start_id + index,  # ID nội bộ tăng dần để quản lý
                    "Tên công việc": _get_text_by_class(soup, "h1", "hAejeW"),
                    "Lương": _get_text_by_class(soup, "span", "cVbwLK"),
                    "Hết hạn": _get_text_by_class(soup, "span", "ePOHWr", 0),
                    "Lượt xem": _get_text_by_class(soup, "span", "ePOHWr", 1),
                    "Địa điểm tuyển dụng": _get_text_by_class(soup, "span", "ePOHWr", 2)
                }

                # Mỗi section mô tả có pattern:
                #   div.gDSEwb  -> chứa
                #       h2.cjuZti   (tiêu đề mục, ví dụ "Mô tả công việc")
                #       div.dVvinc  (nội dung chi tiết)
                # Chiến lược: map tiêu đề section → nội dung, giúp giữ cấu trúc tự mô tả, thuận tiện khi phân tích text.
                description_sections = soup.find_all("div", class_=lambda x: x and "gDSEwb" in x)
                for section in description_sections:
                    title_tag = section.find("h2", class_=lambda x: x and "cjuZti" in x)
                    content_tag = section.find("div", class_=lambda x: x and "dVvinc" in x)
                    if title_tag and content_tag:
                        title = title_tag.get_text(strip=True)
                        # separator="\n" để giữ xuống dòng -> đọc dễ hơn, phục vụ xử lý text sau này (tokenize/bigram…)
                        content = content_tag.get_text(separator="\n", strip=True)
                        job_fields[title] = content  # map theo tiêu đề section

                # Gán phúc lợi đã trích ở trên
                # Lưu ý: định dạng "tiêu đề: nhiều dòng" giúp đọc hiểu nhanh khi xuất Excel.
                job_fields["Phúc lợi"] = benefits_text

                # Khu vực thông tin theo cặp Label/Value (ví dụ: "Cấp bậc", "Kinh nghiệm", "Hình thức")
                #   div.dHvFzj -> nhiều item JtIju
                #       label.dfyRSX (nhãn) + p.cLLblL (giá trị)
                # Cách làm: duyệt từng cặp nhãn-giá trị và ghi thẳng vào dict job_fields dưới key là nhãn.
                job_info_section = soup.find("div", class_=lambda x: x and "dHvFzj" in x)
                if job_info_section:
                    info_items = job_info_section.find_all("div", class_=lambda x: x and "JtIju" in x)
                    for item in info_items:
                        label_tag = item.find("label", class_=lambda x: x and "dfyRSX" in x)
                        value_tag = item.find("p", class_=lambda x: x and "cLLblL" in x)
                        if label_tag and value_tag:
                            job_fields[label_tag.get_text(strip=True)] = value_tag.get_text(strip=True)

                # Địa điểm làm việc (cụ thể hơn so với 'Địa điểm tuyển dụng' ở trên)
                #   div.bAqPjv -> p.cLLblL
                # Mục tiêu: tách rõ "nơi làm việc thực tế" (đôi khi khác cụm hiển thị chung).
                loc = soup.find("div", class_=lambda x: x and "bAqPjv" in x)
                if loc:
                    val = loc.find("p", class_=lambda x: x and "cLLblL" in x)
                    if val:
                        job_fields["Địa điểm làm việc"] = val.get_text(strip=True)

                # Thông tin công ty: tên & quy mô
                #   div.drWnZq -> name trong a.egZKeY, size trong span.ePOHWr
                # Lưu thêm "Quy mô công ty" để phục vụ phân tích tương quan (quy mô vs lương/yêu cầu).
                comp = soup.find("div", class_=lambda x: x and "drWnZq" in x)
                if comp:
                    name = comp.find("a", class_=lambda x: x and "egZKeY" in x)
                    size = comp.find("span", class_=lambda x: x and "ePOHWr" in x)
                    if name:
                        job_fields["Tên công ty"] = name.get_text(strip=True)
                    if size:
                        job_fields["Quy mô công ty"] = size.get_text(strip=True)

                # Lưu lại URL nguồn để trace/debug
                # Nguyên tắc reproducibility: luôn giữ tham chiếu về nguồn gốc dữ liệu.
                job_fields["HREF"] = job_url

                # Thêm vào danh sách kết quả
                all_jobs.append(job_fields)

            except Exception as e:
                # Không để vỡ cả batch khi 1 link lỗi; log và tiếp tục
                # Ví dụ lỗi phổ biến: timeout, DOM thay đổi nhẹ, job đã bị gỡ (404).
                print(f"  ❌ Lỗi khi xử lý link: {e}")

    finally:
        # Đảm bảo đóng driver dù thành công hay lỗi
        # Tránh rò rỉ tiến trình Chrome/ChromeDriver, nhất là khi chạy nhiều batch.
        driver.quit()

    # Trả về DataFrame tổng hợp tất cả job đã bóc
    # Downstream có thể to_excel(...) hoặc merge với bảng listing bằng khóa HREF.
    return pd.DataFrame(all_jobs)


# ===================== PHẦN 4: Pipeline end-to-end =====================
# Mục tiêu khối này:
# - Cung cấp 2 lối vận hành:
#   (A) run_vnw_scraper(...): quy trình end-to-end cho TRƯỜNG HỢP TÌM THEO KEYWORD (độc lập với lặp qua ngành).
#   (B) Khối __main__: quy trình end-to-end THEO NGÀNH (duyệt VNWORKS_GROUPS), xuất 2 loại tệp cho mỗi ngành:
#       + File danh sách (list) các link việc làm theo ngành.
#       + File chi tiết (detail) bóc từ các link ở trên, đặt tên kèm dấu thời gian để tránh ghi đè.
# - Triết lý thiết kế: pipeline "chịu lỗi" (fault-tolerant) theo từng đơn vị công việc (mỗi ngành/mỗi link),
#   có log rõ ràng để kiểm thử/đối soát, và giữ tính tái lập (reproducibility) qua việc đóng dấu tên tệp.

def run_vnw_scraper(keyword: str, location_code: str = "1001",
                    output_dir: str = "output/jobs", max_pages: int = 0,
                    start_id: int = 1000001) -> Dict[str, str]:
    # Đảm bảo thư mục đầu ra tồn tại
    # Lý do: tránh lỗi I/O khi lần đầu chạy trên máy/CI chưa có sẵn cấu trúc thư mục.
    os.makedirs(output_dir, exist_ok=True)

    # Chuẩn hóa tên file từ keyword: thay ký tự không phải chữ/số thành "_", bỏ "_" thừa, lowercase
    # Mục đích: tên tệp bền vững, an toàn trên nhiều hệ điều hành, thuận tiện truy hồi.
    safe_keyword = re.sub(r"\W+", "_", keyword.strip()).strip("_").lower()
    # Dùng ngày hiện tại để đóng dấu file danh sách/chi tiết theo định dạng dd-mm-YYYY
    # (Lưu ý: phía dưới phần __main__ dùng run_ts = YYYY-mm-dd_HHMMSS nên khác format)
    # Việc dùng định dạng ngày trong tên tệp giúp phân biệt các lần chạy trong ngày (ở mức ngày).
    date_str = datetime.now().strftime("%d-%m-%Y")

    print(f"[INFO] Đang tìm kiếm với từ khóa: '{keyword}', địa điểm: '{location_code}'")
    # GỌI HÀM TÌM DANH SÁCH THEO KEYWORD:
    # ***LƯU Ý***: Hàm get_vietnamworks_jobs_by_group ở file trước đó dùng group_id/group_name,
    # trong khi ở đây gọi theo keyword/location_code. Bạn cần đúng phiên bản hàm hỗ trợ keyword.
    # Mục đích cmt này: nhắc người đọc/giảng viên rằng có 2 biến thể crawler (theo ngành vs theo keyword).
    job_list = get_vietnamworks_jobs_by_group(
        keyword=keyword,
        location_code=location_code,
        max_pages=max_pages
    )
    # Không có kết quả -> ném lỗi để caller biết và dừng
    # Lý do: pipeline end-to-end cần fail-fast ở điểm này để không tạo tệp rỗng/tệp sai lệch.
    if not job_list:
        raise RuntimeError("Không thu được job nào ở bước danh sách.")

    # Lưu danh sách cơ bản (href, group_id/name nếu có…) trước khi bóc chi tiết
    # Quy ước: xuất sheet "jobs", không ghi index, tên tệp gồm keyword + location + ngày.
    df_list = pd.DataFrame(job_list)
    list_path = os.path.join(
        output_dir,
        f"vietnamworks_jobs_{safe_keyword}_{location_code}_{date_str}.xlsx"
    )
    df_list.to_excel(list_path, index=False)
    print(f"[✔] Đã lưu danh sách: {list_path}")

    # Bóc chi tiết từng link (mở trang detail, mở rộng nội dung, trích trường…)
    # Chúng tôi tách rõ 2 pha (list → detail) để:
    # - Dễ tái chạy chỉ pha chi tiết khi selector thay đổi.
    # - Cho phép kiểm thử chất lượng list trước khi tốn thời gian crawl chi tiết.
    links = df_list["href"].dropna().tolist()
    df_detail = scrape_job_details_from_links(links, start_id=start_id)
    detail_path = os.path.join(
        output_dir,
        f"job_detail_output_{safe_keyword}_{location_code}_{date_str}.xlsx"
    )
    df_detail.to_excel(detail_path, index=False)
    print(f"[✔] Đã lưu chi tiết: {detail_path}")

    # Trả về đường dẫn 2 file để caller có thể dùng tiếp
    # Điều này hỗ trợ việc ghép vào các bước tiền xử lý/phân tích/biểu đồ phía sau.
    return {"list_path": list_path, "detail_path": detail_path}


if __name__ == "__main__":
    # ==== THAM SỐ CHUNG ====
    # Ghi chú:
    # - LOCATION_CODE ở đây CHỈ tham gia vào tên tệp, không lọc dữ liệu trong hàm listing theo ngành.
    # - Nếu cần lọc theo location khi crawl THEO NGÀNH, phải mở rộng hàm get_vietnamworks_jobs_by_group tương ứng.
    LOCATION_CODE = r"1001"          # Chỉ dùng trong tên file ở khối dưới; không ảnh hưởng filter nếu hàm list không dùng
    LIST_OUT_DIR = r"output/jobslist"         # Thư mục lưu file danh sách (mỗi ngành 1 file)
    DETAIL_OUT_DIR = r"output/jobsdetail"     # Thư mục lưu file chi tiết
    MAX_PAGES = 0                    # 0 = đi hết theo điều kiện dừng (no_gain/safety)
    DELAY = 1.0                      # delay giữa các trang khi crawl list
    NO_GAIN_PATIENCE = 2             # số trang liên tiếp không có link mới -> dừng
    START_ID_BASE = 1000001          # ID khởi tạo cho job chi tiết (nội bộ)
    ID_STEP_PER_GROUP = 1000000      # Bước nhảy mỗi ngành để tránh trùng ID giữa ngành khác nhau

    # Dấu thời gian chạy để tên file chi tiết mỗi lần chạy là duy nhất (tránh ghi đè)
    # Chọn định dạng YYYY-mm-dd_HHMMSS để đảm bảo chuỗi sort được theo thời gian.
    run_ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # Đảm bảo thư mục tồn tại (Windows path ở đây dùng backslash; đa nền tảng có thể dùng os.path.join/Path)
    # Giữ nguyên chuỗi literal như hiện có để minh hoạ môi trường phát triển của nhóm (Windows).
    os.makedirs(LIST_OUT_DIR, exist_ok=True)
    os.makedirs(DETAIL_OUT_DIR, exist_ok=True)

    summary = []  # lưu tổng kết cuối cùng: (tên ngành, path list, path detail, số dòng list, số dòng detail)
    # Ý nghĩa: tạo báo cáo nhanh ở cuối chương trình, tiện kiểm tra không cần mở từng file.

    # Lặp qua tất cả ngành trong VNWORKS_GROUPS, crawl tuần tự từng ngành
    # Lý do tuần tự: hạn chế tải lên website nguồn; nếu muốn song song phải thêm cơ chế giới hạn concurrency/rate.
    for idx, (group_name, gid) in enumerate(VNWORKS_GROUPS.items(), start=0):
        try:
            print("\n" + "="*80)
            print(f"[{idx+1}/{len(VNWORKS_GROUPS)}] NGÀNH: {group_name} (g={gid})")

            # 1) Crawl danh sách link cho ngành hiện tại
            # Truyền tham số kiểm soát hành vi (MAX_PAGES, DELAY, NO_GAIN_PATIENCE) để đảm bảo nhất quán giữa các ngành.
            rows = get_vietnamworks_jobs_by_group(
                group_id=gid,
                group_name=group_name,
                max_pages=MAX_PAGES,
                delay=DELAY,
                no_gain_patience=NO_GAIN_PATIENCE,
            )

            # 2) Lưu danh sách thành 1 file Excel/NGÀNH (đặt tên theo slug + location_code)
            # Quy ước: mỗi ngành 1 tệp để dễ theo dõi biến động riêng ngành; downstream có thể hợp nhất khi cần.
            list_path = save_group_to_excel(
                rows=rows,
                group_name=group_name,
                location_code=LOCATION_CODE,
                out_dir=LIST_OUT_DIR
            )

            # 3) Chuẩn bị bóc chi tiết từ danh sách link (nếu có)
            # Nếu danh sách trống, vẫn xuất 1 tệp chi tiết rỗng nhằm đảm bảo tính đầy đủ của lứa chạy.
            links = [r["href"] for r in rows if r.get("href")]
            name_slug = slugify_vn(group_name)  # dùng cho tên file chi tiết
            # Tạo khoảng ID riêng cho mỗi ngành để tránh va chạm ID giữa các ngành
            # Ví dụ: ngành 0 sẽ dùng [START_ID_BASE, START_ID_BASE + ID_STEP_PER_GROUP),
            #        ngành 1 sẽ dịch sang khoảng kế tiếp, v.v.
            start_id = START_ID_BASE + idx * ID_STEP_PER_GROUP

            if links:
                print(f"[DETAIL] Bắt đầu bóc chi tiết {len(links)} link cho ngành '{group_name}'...")
                df_detail = scrape_job_details_from_links(links, start_id=start_id)
            else:
                # Không có link -> tạo DataFrame rỗng để vẫn xuất file chi tiết cho đủ bộ
                # Lợi ích: quy trình xử lý phía sau (đọc thư mục, hợp nhất) không phải kiểm tra ngoại lệ.
                print(f"[DETAIL][WARN] Ngành '{group_name}' không có link nào. Tạo file chi tiết rỗng.")
                df_detail = pd.DataFrame([])

            # MỖI NGÀNH 1 FILE CHI TIẾT RIÊNG (mỗi lần chạy là 1 file mới nhờ run_ts)
            # Đặt tên: job_detail_output_{slug}_g{gid}_{LOCATION_CODE}_{run_ts}.xlsx
            # Cấu trúc tên cho phép parser phía sau extract được slug ngành, gid, location, timestamp.
            detail_filename = f"job_detail_output_{name_slug}_g{gid}_{LOCATION_CODE}_{run_ts}.xlsx"
            detail_path = os.path.join(DETAIL_OUT_DIR, detail_filename)
            df_detail.to_excel(detail_path, index=False)
            print(f"[SAVE] {detail_path} ({len(df_detail)} dòng)")

            # Lưu vào tổng kết để in cuối chương trình
            summary.append((group_name, list_path, detail_path, len(rows), len(df_detail)))

        except Exception as e:
            # Bắt lỗi theo ngành để không dừng toàn bộ loop; in tên ngành + gID để dễ trace
            # Các lỗi hay gặp: thay đổi DOM theo đợt deploy, nghẽn mạng tạm thời, CAPTCHA cục bộ.
            print(f"[ERROR] Lỗi ở ngành '{group_name}' (g={gid}): {e}")

    # ==== TỔNG KẾT ====
    print("\n" + "="*80)
    print("[SUMMARY]")
    for (group_name, list_path, detail_path, n_list, n_detail) in summary:
        # In số lượng bản ghi và đường dẫn file để tiện kiểm tra nhanh
        # Đây là “báo cáo miệng” của chương trình, giúp giám khảo/giảng viên nắm được kết quả từng ngành ngay trên console.
        print(f"- {group_name}: list={n_list} ({list_path}) | detail={n_detail} ({detail_path})")
