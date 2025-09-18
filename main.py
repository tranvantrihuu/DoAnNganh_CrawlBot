import os
import sys
import subprocess
import logging
from pathlib import Path
from datetime import datetime
import threading

# ====== PATHS ======
ROOT = Path(__file__).resolve().parent
PY = sys.executable  # python hiện tại trong venv
SCRAPER = ROOT / "crawler" / "selenium_scraper.py"
PREPROCESS = ROOT / "processor" / "preprocess.py"
ANALYZER = ROOT / "processor" / "analyzer.py"
OUTPUT_DIR = ROOT / "output"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ====== LOGGING ======
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "main.log", encoding="utf-8")
    ],
)
log = logging.getLogger("main")

# ====== helper chạy script tuần tự ======
def run_script(path: Path, name: str):
    assert path.exists(), f"{name} not found: {path}"
    log_path = LOG_DIR / f"{name}_{datetime.now():%Y%m%d_%H%M%S}.log"
    cmd = f"{PY} {path}"
    log.info(f"▶️ CMD: {cmd}")

    # lấy danh sách file trước khi chạy
    before_files = set(p.name for p in OUTPUT_DIR.rglob("*") if p.is_file())

    with open(log_path, "ab", buffering=0) as f:
        proc = subprocess.Popen(
            [PY, str(path)],
            cwd=str(ROOT),
            stdout=f,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        ret = proc.wait()

    if ret != 0:
        raise RuntimeError(f"{name} exited with code {ret}. See log: {log_path}")

    # lấy file mới xuất ra (so sánh trước/sau)
    after_files = set(p.name for p in OUTPUT_DIR.rglob("*") if p.is_file())
    new_files = sorted(after_files - before_files)

    if new_files:
        for nf in new_files:
            log.info(f"📄 {name} created file: {nf}")
    else:
        log.info(f"ℹ️ {name} finished, no new files detected.")

    log.info(f"✅ Finished {name}. Log: {log_path}")

def pipeline():
    try:
        run_script(SCRAPER, "selenium_scraper")
        run_script(PREPROCESS, "preprocess")
        run_script(ANALYZER, "analyzer")
        log.info("🎉 Pipeline DONE")
    except Exception as e:
        log.exception(f"Pipeline FAILED: {e}")

# ====== helper chạy lệnh shell (nginx, systemctl) ======
def run_cmd(cmd: str):
    log.info(f"$ {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        if result.stdout:
            log.info(result.stdout.strip())
        if result.stderr:
            log.warning(result.stderr.strip())
    except subprocess.CalledProcessError as e:
        log.error(f"Lỗi khi chạy lệnh: {cmd}")
        log.error(e.stderr.strip())

def manage_services():
    os.chdir(str(ROOT))
    run_cmd("sudo nginx -t")
    run_cmd("sudo systemctl start nginx")
    run_cmd("sudo systemctl enable nginx")
    run_cmd("sudo systemctl restart fastapi")
    run_cmd("sudo systemctl status fastapi --no-pager")

# ====== MAIN ======
def main():
    # chạy pipeline tuần tự trong background
    t = threading.Thread(target=pipeline, daemon=True)
    t.start()

    # song song quản lý nginx + fastapi
    manage_services()

    # đợi pipeline xong
    t.join()

if __name__ == "__main__":
    main()


