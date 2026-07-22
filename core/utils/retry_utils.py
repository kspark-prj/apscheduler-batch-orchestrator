import functools
import logging
from contextvars import ContextVar
from datetime import datetime

from tenacity import retry_any, retry_if_exception_type, retry_if_result, stop_after_attempt, wait_fixed

# 1. 의존성 가져오기
from core.globals import shared_stats
from core.utils.logger_setup import get_task_logger

# 격리된 시도 횟수 저장소 (스레드 세이프)
current_attempt_var: ContextVar[int] = ContextVar("current_attempt", default=1)


# 🚨 [신규 통합] 매 회차(최초 실행 포함) 실행 직전에 자동으로 공통 파일 로그를 기록하는 훅
def _before_execution_callback(retry_state):
    task_name = retry_state.fn.__name__
    logger = get_task_logger(task_name)
    attempt = retry_state.attempt_number

    # 함수 호출 인자 정보와 함께 [N회차 시도] 시작 로그를 공통 처리합니다.
    logger.info(f"[{attempt}회차 시도] 작업 시작 (인자: args={retry_state.args}, kwargs={retry_state.kwargs})")


# 재시도 직전 호출될 콜백 (🚨 정합성 교정: 딕셔너리 덮어쓰기 방지 및 특정 키만 변경)
def _before_sleep_callback(retry_state):
    next_attempt = retry_state.attempt_number + 1
    current_attempt_var.set(next_attempt)

    task_name = retry_state.fn.__name__
    logger = get_task_logger(task_name)
    logger.warning(f"작업 실패로 인해 재시도 준비 중... (다음 시도: {next_attempt}회차)")

    # 💥 [교정]: 통째로 덮어쓰지 않고, 기존에 main.py가 등록해 둔 'cron'을 보존하며 키 단위 업데이트
    if task_name in shared_stats:
        shared_stats[task_name]["last_result"] = f"실행중 ({next_attempt}회차)"
        shared_stats[task_name]["last_time"] = datetime.now().strftime("%H:%M:%S")
    else:
        shared_stats[task_name] = {
            "cron": "-",
            "last_result": f"실행중 ({next_attempt}회차)",
            "last_time": datetime.now().strftime("%H:%M:%S"),
        }


# 3. 최종 실패 시 예외를 전파 (🚨 정합성 교정: 딕셔너리 덮어쓰기 방지 및 특정 키만 변경)
def _after_ultimate_failure(retry_state):
    task_name = retry_state.fn.__name__
    logger = get_task_logger(task_name)

    # 💥 [교정]: 기존 'cron' 필드를 파괴하지 않고 마감 상태만 주입
    if task_name in shared_stats:
        shared_stats[task_name]["last_result"] = "실패"
        shared_stats[task_name]["last_time"] = datetime.now().strftime("%m-%d %H:%M")

    exception = retry_state.outcome.exception()
    if exception:
        logger.error(f"❌ [최종 실패] 모든 재시도가 실패했습니다. 최종 예외: {exception}")
        raise exception
    else:
        result = retry_state.outcome.result()
        logger.error(f"❌ [최종 실패] 모든 재시도가 실패했습니다. 최종 결과: {result}")
        raise ValueError(f"작업이 실패 결과를 반환하여 최종 실패 처리되었습니다: {result}")


# 4. 최초 진입 상태 관리 데코레이터 (🚨 정합성 교정: 딕셔너리 덮어쓰기 방지 및 특정 키만 변경)
def track_task_status(func):
    """
    [통합형 데코레이터 - 로거 자동 주입 버전]
    1. 함수의 이름을 기반으로 격리된 로거를 자동 생성하여 하위 함수에 주입합니다.
    2. 최초 진입 시 재시도 카운터를 1로 강제 초기화합니다.
    3. shared_stats에 기존 크론 설정을 파괴하지 않고 최초 시작 및 최종 성공 상태를 기록합니다.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        task_name = func.__name__

        # 하위 함수 이름으로 로거를 자동 생성 및 획득하여 강제 주입
        task_logger = get_task_logger(task_name)
        kwargs["logger"] = task_logger

        # 진입 시 무조건 카운터 1로 초기화
        current_attempt_var.set(1)

        # 💥 [교정]: 최초 실행 시 main.py가 tasks_config.json을 읽어 등록해 둔 'cron' 주기를 유지합니다.
        if task_name in shared_stats:
            shared_stats[task_name]["last_result"] = "실행중"
            shared_stats[task_name]["last_time"] = datetime.now().strftime("%H:%M:%S")
        else:
            shared_stats[task_name] = {
                "cron": "-",
                "last_result": "실행중",
                "last_time": datetime.now().strftime("%H:%M:%S"),
            }

        try:
            # 비즈니스 로직 실행 (tenacity를 거쳐 실행됨)
            result = func(*args, **kwargs)

            # 최종 성공 시 처리
            if task_name in shared_stats:
                shared_stats[task_name]["last_result"] = "성공"
                shared_stats[task_name]["last_time"] = datetime.now().strftime("%m-%d %H:%M")
            return result

        except Exception as e:
            # 🛠️ 예기치 못한 에러나 재시도 대상이 아닌 에러로 튕겨 나갈 때도 '실패' 상태를 확실히 기록
            if task_name in shared_stats:
                shared_stats[task_name]["last_result"] = "실패"
                shared_stats[task_name]["last_time"] = datetime.now().strftime("%m-%d %H:%M")
            raise e

    return wrapper


# 재시도 여부를 판단할 결과값 기준
def is_result_failure(result):
    return result in [None, "FAILURE", "FAIL"]


# 공통 재시도 정책 정의
COMMON_RETRY_POLICY = {
    "stop": stop_after_attempt(3),
    "wait": wait_fixed(10),
    "retry": retry_any(
        # 💡 [핵심] 모든 예외(Exception) 발생시 재처리.
        retry_if_exception_type(Exception),
        retry_if_result(is_result_failure),
    ),
    "before": _before_execution_callback,
    "before_sleep": _before_sleep_callback,
    "retry_error_callback": _after_ultimate_failure,
}
