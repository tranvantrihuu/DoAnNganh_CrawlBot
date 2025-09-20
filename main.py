#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
def _kill_process_tree_pgid(pgid: int, gentle_seconds: float = 2.0):
    """Kill cả group theo PGID: SIGTERM -> chờ -> SIGKILL."""
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(gentle_seconds)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass

def _reap_children_by_name(names=("chrome", "chromedriver", "Xvfb")):
    """Diệt các tiến trình rơi rớt theo tên (phòng hờ driver còn sống)."""
    for p in psutil.process_iter(["name", "cmdline"]):
        try:
            nm = (p.info.get("name") or "").lower()
            cmd = " ".join(p.info.get("cmdline") or []).lower()
            if any(n in nm or n in cmd for n in names):
                p.kill()
        except Exception:
            pass

def run_script(path: Path, name: str, timeout: float | None = None) -> None:
    """
    Chạy file Python con:
    - Ghi log vào file + stream realtime ra terminal
    - Tạo process group để kill cả cây
    - Thu dọn RAM/child processes sau khi xong
    """
    log_path = LOG_DIR / f"{name}_{datetime.now():%Y%m%d_%H%M%S}.log"

    # môi trường unbuffered cho log tức thời
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    with open(log_path, "w", buffering=1) as lf:
        # -u để stdout/stderr không buffer
        proc = subprocess.Popen(
            [PY, "-u", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
            preexec_fn=os.setsid,  # Linux: tạo process group mới (PGID = PID)
        )

        # PGID để kill cả cây sau này
        pgid = os.getpgid(proc.pid)

        try:
            # stream từng dòng: terminal + file
            assert proc.stdout is not None
            for line in iter(proc.stdout.readline, ""):
                # hiện trên terminal (orchestrator)
                sys.stdout.write(line)
                # ghi file log
                lf.write(line)
                lf.flush()
            ret = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _kill_process_tree_pgid(pgid)
            raise RuntimeError(f"{name} timed out. See log: {log_path}")
        finally:
            # đóng stream sớm để giải phóng FD
            try:
                if proc.stdout:
                    proc.stdout.close()
            except Exception:
                pass

    # Thu dọn tiến trình con còn sót
    try:
        p = psutil.Process(proc.pid)
        for c in p.children(recursive=True):
            try:
                c.kill()
            except Exception:
                pass
    except psutil.NoSuchProcess:
        pass

    # Dọn “mồ côi” phổ biến (chrome/driver)
    _reap_children_by_name()

    # Thu gom rác Python
    gc.collect()

    if ret != 0:
        raise RuntimeError(f"{name} exited with code {ret}. See log: {log_path}")
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
    run_cmd("sudo systemctl restart fastapi")
    run_cmd("sudo systemctl status nginx --no-pager")
    run_cmd("sudo systemctl status fastapi --no-pager")


# ====== LẬP LỊCH ======

def start_scheduler():
    sched = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh")
    # Hẹn giờ: Thứ Ba 00:00
    trigger = CronTrigger(day_of_week="tue", hour=00, minute=00)
    sched.add_job(
        pipeline,
        trigger,
        id="weekly_pipeline",
        coalesce=True,          # gộp nếu bị trễ
        max_instances=1,        # tránh chạy song song
        misfire_grace_time=3600 # cho phép muộn 1h
    )
    sched.start()
    log.info("⏰ Scheduler đã bật: Thứ Ba hàng tuần lúc 00:00 (giờ VN)")
    return sched


# ====== MAIN ======

def main():
    log.info("===== Orchestrator khởi động (t3.small) =====")

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
