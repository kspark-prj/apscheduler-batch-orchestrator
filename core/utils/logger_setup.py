import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


def get_task_logger(task_name):
    logger = logging.getLogger(task_name)

    if not logger.handlers:  # 이미 설정된 핸들러가 없을 때만 생성
        logger.setLevel(logging.INFO)

        # 🚨 [핵심 교정] 상위(루트) 로거로 로그를 전파하지 않도록 차단합니다.
        # 이 설정을 해야 main.py의 basicConfig(콘솔 출력) 설정을 무시하고 파일로만 격리됩니다.
        logger.propagate = False

        # 1. 태스크 이름별 하위 폴더 경로 설정 (logs/test_task/ 형태)
        log_dir = os.path.join("logs", task_name)
        os.makedirs(log_dir, exist_ok=True)

        # 2. 오늘 날짜로 최초 파일명 정의 (예: logs/test_task/2026-06-27.log)
        today_str = datetime.now().strftime("%Y-%m-%d")
        initial_filename = os.path.join(log_dir, f"{today_str}.log")

        # 3. 핸들러 설정
        # 기본적으로 TimedRotatingFileHandler는 로테이션 시 원래 파일명 뒤에 지정을 붙입니다.
        # 당일 파일명을 항상 깔끔하게 유지하기 위해 아래와 같이 정교하게 세팅합니다.
        handler = TimedRotatingFileHandler(
            filename=initial_filename, when="midnight", interval=1, backupCount=30, encoding="utf-8"
        )

        # 자정이 지나 백업(로테이션)될 때 붙을 확장자 포맷 정의 (.log 뒤에 날짜가 붙는 걸 방지)
        # 기본값 구조를 깨고 깔끔하게 파일명이 떨어지도록 suffix를 세팅합니다.
        handler.suffix = "%Y-%m-%d.log"

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
