import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import tempfile

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from core.globals import shared_stats
from core.config_loader import TASK_MAPPING, load_crontab_file, load_json_tasks

# ==========================================
# [1. 메인 오케스트레이터 전용 파일 로거 설정]
# ==========================================
def setup_main_logger():
    main_logger = logging.getLogger("SchedulerMain")
    main_logger.setLevel(logging.INFO)
    main_logger.propagate = False

    # 1. 파일 핸들러 설정
    log_dir = os.path.join("logs", "SchedulerMain")
    os.makedirs(log_dir, exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    initial_filename = os.path.join(log_dir, f"{today_str}.log")

    file_handler = TimedRotatingFileHandler(
        filename=initial_filename, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    )
    file_handler.suffix = "%Y-%m-%d.log"

    # 2. 콘솔(Stream) 핸들러 설정
    console_handler = logging.StreamHandler()

    # 3. 포맷터 설정 (공통 사용)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 4. 로거에 핸들러 등록
    main_logger.addHandler(file_handler)
    main_logger.addHandler(console_handler)

    return main_logger


logger = setup_main_logger()

# Global variables loaded from environment (assumes load_dotenv was already called)
USE_DB = os.environ.get("USE_DB", "False").lower() in ("true", "1", "yes")
USE_JSON_CONFIG = os.environ.get("USE_JSON_CONFIG", "True").lower() in ("true", "1", "yes")
USE_TXT_CONFIG = os.environ.get("USE_TXT_CONFIG", "False").lower() in ("true", "1", "yes")
SUBPROCESS_TIMEOUT = int(os.environ.get("SUBPROCESS_TIMEOUT", "60"))

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR_CRON = os.path.join(ROOT_DIR, "logs", "cron")
os.makedirs(LOG_DIR_CRON, exist_ok=True)

stats_lock = threading.Lock()


# ==========================================
# [2. 크론 작업 코어 오케스트레이터]
# ==========================================
def run_job(command_id, is_callable, task_args=None, task_kwargs=None):
    if task_args is None:
        task_args = []
    if task_kwargs is None:
        task_kwargs = {}

    safe_name = re.sub(r"[^\w\-_]", "_", command_id)[:50]

    try:
        if is_callable:
            task_func = TASK_MAPPING.get(command_id)
            if not task_func:
                raise ImportError(f"TASK_MAPPING에서 함수 '{command_id}'를 찾을 수 없습니다.")
            task_func(*task_args, **task_kwargs)
        else:
            with stats_lock:
                current_cron = shared_stats.get(command_id, {}).get("cron", "-")
                shared_stats[command_id] = {
                    "cron": current_cron,
                    "last_result": "실행중",
                    "last_time": datetime.now().strftime("%H:%M:%S"),
                }

            log_file = f"{LOG_DIR_CRON}/{safe_name}_{datetime.now().strftime('%Y%m%d')}.log"

            def _decode_output(data):
                """서브프로세스 출력 바이트를 UTF-8 우선, CP949 폴백으로 디코딩"""
                try:
                    return data.decode("utf-8")
                except UnicodeDecodeError:
                    return data.decode("cp949", errors="replace")

            def _write_log(file_path, output_text, status="완료"):
                """타임스탬프를 포함하여 로그 파일에 기록"""
                with open(file_path, "a", encoding="utf-8") as f:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    f.write(f"[{ts}] --- 실행 시작: {command_id} ---\n")
                    for line in output_text.splitlines():
                        f.write(f"[{ts}] {line}\n")
                    f.write(f"[{ts}] --- 실행 {status} ---\n\n")

            # cmd.exe가 배치 파일을 읽기 전에 UTF-8 코드 페이지를 먼저 설정
            utf8_command = f"chcp 65001 > nul && {command_id}"
            proc = subprocess.Popen(
                utf8_command, shell=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                env=os.environ.copy(), cwd=ROOT_DIR
            )
            try:
                stdout_data, _ = proc.communicate(timeout=SUBPROCESS_TIMEOUT)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout_data, _ = proc.communicate()
                _write_log(log_file, _decode_output(stdout_data), status=f"시간 초과({SUBPROCESS_TIMEOUT}초)")
                raise subprocess.SubprocessError(
                    f"작업 시간 초과({SUBPROCESS_TIMEOUT}초)로 인해 강제 종료되었습니다."
                )

            _write_log(log_file, _decode_output(stdout_data), status="완료" if proc.returncode == 0 else f"실패(코드:{proc.returncode})")

            if proc.returncode != 0:
                raise subprocess.SubprocessError(
                    f"프로세스가 에러 코드({proc.returncode})를 반환했습니다."
                )

            with stats_lock:
                shared_stats[command_id] = {
                    "cron": current_cron,
                    "last_result": "성공",
                    "last_time": datetime.now().strftime("%m-%d %H:%M:%S"),
                }

    except Exception as ex:
        logger.exception(f"작업 내부 런타임 실패 리포트 [{command_id}]: {ex}")
        if not is_callable:
            with stats_lock:
                shared_stats[command_id] = {
                    "cron": current_cron,
                    "last_result": "실패",
                    "last_time": datetime.now().strftime("%m-%d %H:%M:%S"),
                }


# ==========================================
# [3. 파일 안전 쓰기 기반 모니터링 인프라]
# ==========================================
def get_status_text(scheduler):
    jobs = scheduler.get_jobs()
    lines = [
        f"{'=' * 125}",
        f"--- [크론 모니터링 대시보드] 업데이트: {time.strftime('%Y-%m-%d %H:%M:%S')} ---",
        f"{'-' * 125}",
        f"{'ID (명령어/함수)':<35} | {'Cron 주기':<15} | {'최종 결과':<15} | {'마지막 실행':<15} | {'다음 실행':<20}",
        f"{'-' * 125}",
    ]

    with stats_lock:
        for job in jobs:
            stat = shared_stats.get(job.id, {"cron": "-", "last_result": "대기", "last_time": "-"})
            next_run = (
                job.next_run_time.strftime("%Y-%m-%d %H:%M") if job.next_run_time else "종료/중지"
            )
            lines.append(
                f"{job.id[:35]:<35} | {stat.get('cron', '-'):<15} | {stat.get('last_result', '대기'):<15} | {stat.get('last_time', '-'):<15} | {next_run:<20}"
            )

    return "\n".join(lines + [f"{'=' * 125}"])


def update_ui(scheduler):
    status_file = "logs/scheduler_status.txt"
    os.makedirs("logs", exist_ok=True)

    while scheduler.running:
        try:
            text = get_status_text(scheduler)
            with tempfile.NamedTemporaryFile("w", dir="logs", delete=False, encoding="utf-8") as tf:
                tf.write(text + "\n")
                temp_path = tf.name
            os.replace(temp_path, status_file)
        except Exception as e:
            logger.error(f"대시보드 UI 스냅샷 덤프 중 예외 발생: {str(e)}")
        time.sleep(10)


# ==========================================
# [4. 메인 엔트리포인트]
# ==========================================
def main():
    jobstores = {}
    if USE_DB:
        os.makedirs("data", exist_ok=True)
        jobstores["default"] = SQLAlchemyJobStore(url="sqlite:///data/jobs.sqlite")
        logger.info("인프라 데이터베이스 영속 모드 기동 (SQLite)")
    else:
        logger.info("인메모리 전용 가용 스위치 기동")

    executors = {"default": ThreadPoolExecutor(20)}
    scheduler = BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults={"misfire_grace_time": 300, "coalesce": True},
    )

    scheduler.start()
    logger.info("운영 표준 오케스트레이션 코어 엔진 가동 마감.")

    active_txt_ids = load_crontab_file(scheduler, run_job_func=run_job, file_path="config/crontab_list.txt", enabled=USE_TXT_CONFIG)
    active_json_ids = load_json_tasks(scheduler, run_job_func=run_job, file_path="config/tasks_config.json", enabled=USE_JSON_CONFIG)

    all_active_ids = active_json_ids.union(active_txt_ids)

    if USE_DB:
        try:
            db_jobs = scheduler.get_jobs()
            for db_job in db_jobs:
                if db_job.id not in all_active_ids:
                    scheduler.remove_job(db_job.id)
                    logger.info(
                        f"설정 파일 누락 혹은 비활성화로 인한 좀비 스케줄 클리닝 마감: {db_job.id}"
                    )
        except Exception as e:
            logger.error(f"백엔드 영속 스토어 정합성 동기화 점검 오류: {str(e)}")

    logger.info("모든 배치 파이프라인 현행화 로드가 정상 마감되었습니다.")

    threading.Thread(target=update_ui, args=(scheduler,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("커널 프로세스 인터럽트 감지. 자원 자가 청소 시퀀스를 개시합니다.")
    finally:
        scheduler.shutdown(wait=True)
        logger.info("스케줄러 커널 엔진 완전 안전 정지 마감.")
