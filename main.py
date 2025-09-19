#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trình điều phối cho Ubuntu (AWS t3.micro):
- Chạy pipeline tuần tự (selenium_scraper.py -> preprocess.py -> analyzer.py)
- Khi start main.py sẽ chạy ngay một lần
- Đồng thời hẹn giờ chạy lại vào mỗi Thứ Hai lúc 23:59 (giờ Việt Nam)
- Song song quản lý dịch vụ web (nginx / fastapi)
- Ghi log chi tiết, báo các file mới sinh ra
- Tối ưu để tránh quá tải cho t3.micro

Yêu cầu:
  pip install apscheduler

Khuyến nghị: chạy file này bằng systemd service để luôn hoạt động nền.
"""

import os
import sys
import time
import signal
import logging
import subprocess
from pathlib import Path
from datetime import datetime
import threading
from typing import Set

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ====== ĐƯỜNG DẪN ======
ROOT = Path(__file__).resolve().parent
PY = sys.executable  # python hiện tại (trong venv)
SCRAPER = ROOT / "crawler" / "selenium_scraper.py"
PREPROCESS = ROOT / "processor" / "preprocess.py"
ANALYZER = ROOT / "processor" / "analyzer.py"
OUTPUT_DIR = ROOT / "output"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ====== LOGGING ======
LOG_FILE = LOG_DIR / "main.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("main")

# ====== Biến khóa tránh chạy trùng ======
_run_lock = threading.Lock()
_running_flag = threading.Event()

# ====== HÀM PHỤ ======

def run_cmd(cmd: str):
    """Chạy lệnh shell, ghi log đầy đủ (tối ưu cho Ubuntu)."""
    log.info(f"$ {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
            cwd=str(ROOT),
            env=os.environ.copy(),
        )
        if result.stdout:
            for line in result.stdout.splitlines():
                log.info(line)
        if result.stderr:
            for line in result.stderr.splitlines():
                log.warning(line)
    except subprocess.CalledProcessError as e:
        log.error(f"❌ Lỗi khi chạy lệnh: {cmd}")
        if e.stdout:
            for line in e.stdout.splitlines():
                log.error(line)
        if e.stderr:
            for line in e.stderr.splitlines():
                log.error(line)


def list_files_under(root: Path) -> Set[str]:
    return {p.name for p in root.rglob("*") if p.is_file()}


def run_script(path: Path, name: str):
    assert path.exists(), f"{name} not found: {path}"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"{name}_{ts}.log"
    cmd = [PY, str(path)]

    log.info(f"▶️ Chạy {name}: {' '.join(cmd)}")

    before = list_files_under(OUTPUT_DIR)
    with open(log_path, "ab", buffering=0) as f:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
            bufsize=1,                # line-buffered
        )

        # Đọc từng dòng và ghi ra file + console
        for raw_line in iter(proc.stdout.readline, b""):
            f.write(raw_line)
            try:
                line = raw_line.decode("utf-8", "ignore").rstrip()
                if line:
                    log.info(f"[{name}] {line}")
            except Exception:
                pass

        ret = proc.wait()

    if ret != 0:
        raise RuntimeError(f"{name} exited with code {ret}. See log: {log_path}")

    after = list_files_under(OUTPUT_DIR)
    new_files = sorted(after - before)
    if new_files:
        for nf in new_files:
            log.info(f"📄 {name} đã tạo file: {nf}")
    else:
        log.info(f"ℹ️ {name} hoàn tất, không phát hiện file mới.")

    log.info(f"✅ Kết thúc {name}. Log: {log_path}")


def pipeline():
    """Chạy pipeline tuần tự, có khóa tránh chạy trùng."""
    if _running_flag.is_set():
        log.warning("Pipeline đang chạy, bỏ qua lần kích hoạt này.")
        return

    with _run_lock:
        if _running_flag.is_set():
            log.warning("Pipeline đang chạy, bỏ qua.")
            return
        _running_flag.set()

    try:
        log.info("🚀 BẮT ĐẦU PIPELINE")
        run_script(SCRAPER, "selenium_scraper")
        run_script(PREPROCESS, "preprocess")
        run_script(ANALYZER, "analyzer")
        log.info("🎉 PIPELINE HOÀN TẤT")
    except Exception:
        log.exception("❌ PIPELINE THẤT BẠI")
    finally:
        _running_flag.clear()


def manage_services():
    """Khởi động / enable nginx và fastapi service."""
    os.chdir(str(ROOT))
    run_cmd("sudo nginx -t")
    run_cmd("sudo systemctl start nginx")
    run_cmd("sudo systemctl enable nginx")
    # Đổi tên 'fastapi' nếu service bạn đặt khác
    run_cmd("sudo systemctl restart fastapi")
    run_cmd("sudo systemctl status fastapi --no-pager")


# ====== LẬP LỊCH ======

def start_scheduler():
    sched = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh")
    # Hẹn giờ: Thứ Hai 23:59
    trigger = CronTrigger(day_of_week="mon", hour=23, minute=59)
    sched.add_job(
        pipeline,
        trigger,
        id="weekly_pipeline",
        coalesce=True,          # gộp nếu bị trễ
        max_instances=1,        # tránh chạy song song
        misfire_grace_time=3600 # cho phép muộn 1h
    )
    sched.start()
    log.info("⏰ Scheduler đã bật: Thứ Hai hàng tuần lúc 23:59 (giờ VN)")
    return sched


# ====== MAIN ======

def main():
    log.info("===== Orchestrator khởi động (t3.micro) =====")

    # Bật scheduler
    scheduler = start_scheduler()

    # Chạy pipeline ngay khi khởi động
    t = threading.Thread(target=pipeline, name="initial_pipeline", daemon=True)
    t.start()

    # Quản lý dịch vụ web song song
    svc = threading.Thread(target=manage_services, name="service_manager", daemon=True)
    svc.start()

    # Giữ tiến trình chạy, xử lý tín hiệu dừng
    stop_event = threading.Event()

    def handle_signal(signum, frame):
        log.info(f"Nhận tín hiệu {signum}. Đang dừng...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        log.info("Orchestrator đã dừng.")


if __name__ == "__main__":
    main()
