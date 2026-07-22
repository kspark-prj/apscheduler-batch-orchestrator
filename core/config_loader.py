import json
import logging
import os
import threading
import importlib
import pkgutil
import inspect
import sys
from apscheduler.triggers.cron import CronTrigger
from core.globals import shared_stats

logger = logging.getLogger("SchedulerMain")
stats_lock = threading.Lock()

# Dynamic task discovery from tasks/ package
def discover_tasks():
    task_mapping = {}
    
    # Locate the root tasks directory
    core_parent = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if core_parent not in sys.path:
        sys.path.insert(0, core_parent)

    try:
        import tasks
    except ImportError:
        logger.error("Could not import 'tasks' package. Ensure a 'tasks/' directory exists at the root.")
        return task_mapping

    # Walk through the tasks package and find all submodules/modules
    try:
        package = tasks
        for _, module_name, is_pkg in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
            if is_pkg:
                continue
            try:
                module = importlib.import_module(module_name)
                for name, obj in inspect.getmembers(module, inspect.isfunction):
                    # Only register functions defined inside the module itself (not imported)
                    if obj.__module__ == module_name:
                        task_mapping[name] = obj
            except Exception as e:
                logger.error(f"Failed to load module {module_name}: {e}")
    except Exception as e:
        logger.error(f"Error during task discovery: {e}")

    logger.info(f"Dynamically discovered {len(task_mapping)} task(s): {list(task_mapping.keys())}")
    return task_mapping

# Populated dynamically
TASK_MAPPING = discover_tasks()


def load_crontab_file(scheduler, run_job_func, file_path="config/crontab_list.txt", enabled=False):
    """
    환경변수 enabled가 True이고 파일이 존재할 때만 리눅스 명령어 스케줄을 로드합니다.
    """
    valid_ids = set()

    if not enabled:
        return valid_ids

    # Fallback checking
    resolved_path = file_path
    if not os.path.exists(resolved_path):
        # Fallback to root directory if not found in config/
        fallback_path = os.path.basename(file_path)
        if os.path.exists(fallback_path):
            resolved_path = fallback_path
        else:
            logger.warning(f"TXT 로드가 활성화되었으나 설정 파일({file_path})이 존재하지 않습니다.")
            return valid_ids

    try:
        with open(resolved_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip() or line.startswith("#"):
                    continue

                parts = line.strip().split(maxsplit=5)
                if len(parts) < 6:
                    logger.warning(f"TXT 문법 규격 오류 (인자 부족): {line.strip()}")
                    continue

                cron_expr = " ".join(parts[:5])
                cmd = parts[5]
                valid_ids.add(cmd)

                with stats_lock:
                    shared_stats[cmd] = {"cron": cron_expr, "last_result": "대기", "last_time": "-"}

                scheduler.add_job(
                    run_job_func,
                    CronTrigger.from_crontab(cron_expr),
                    args=[cmd, False],
                    kwargs={"task_args": [], "task_kwargs": {}},
                    id=cmd,
                    executor="default",
                    max_instances=1,
                    replace_existing=True,
                )
                logger.info(f"동적 OS 명령어 바인딩 완료: CMD={cmd[:30]}... [{cron_expr}]")
    except Exception as e:
        logger.error(f"crontab_list.txt 환경 분석 중 오류 발생: {str(e)}")

    return valid_ids


def load_json_tasks(scheduler, run_job_func, file_path="config/tasks_config.json", enabled=False):
    """
    환경변수 enabled가 True이고 파일이 존재할 때만 파이썬 함수형 태스크를 로드합니다.
    """
    valid_ids = set()

    if not enabled:
        return valid_ids

    # Fallback checking
    resolved_path = file_path
    if not os.path.exists(resolved_path):
        # Fallback to root directory
        fallback_path = os.path.basename(file_path)
        if os.path.exists(fallback_path):
            resolved_path = fallback_path
        else:
            logger.warning(f"JSON 로드가 활성화되었으나 설정 파일({file_path})이 존재하지 않습니다.")
            return valid_ids

    try:
        with open(resolved_path, "r", encoding="utf-8") as f:
            tasks_list = json.load(f)

        for task_info in tasks_list:
            task_name = task_info.get("name")
            cron_exp = task_info.get("cron")
            task_args = task_info.get("args", [])
            task_kwargs = task_info.get("kwargs", {})

            if not task_name or not cron_exp:
                logger.error("JSON 파싱 에러: 'name' 또는 'cron' 필수 지시문 누락")
                continue

            if task_name in TASK_MAPPING:
                valid_ids.add(task_name)

                with stats_lock:
                    shared_stats[task_name] = {
                        "cron": cron_exp,
                        "last_result": "대기",
                        "last_time": "-",
                    }

                scheduler.add_job(
                    run_job_func,
                    CronTrigger.from_crontab(cron_exp),
                    args=[task_name, True],
                    kwargs={"task_args": task_args, "task_kwargs": task_kwargs},
                    id=task_name,
                    executor="default",
                    max_instances=1,
                    replace_existing=True,
                )
                logger.info(f"동적 작업 현행화 바인딩 완료: ID={task_name} [{cron_exp}]")
            else:
                logger.error(f"등록 실패 거부: TASK_MAPPING 내 명세 부재 -> '{task_name}' (tasks/ 디렉토리에 정의되지 않았거나 임포트할 수 없음)")
    except Exception as e:
        logger.error(f"JSON 환경 분석 동기화 도중 크래시: {str(e)}")

    return valid_ids
