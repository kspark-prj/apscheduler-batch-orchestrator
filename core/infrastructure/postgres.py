import os
import traceback
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.utils.logger_setup import get_task_logger

# 환경 변수에서 직접 URL 읽기 (값이 없으면 에러 발생 유도)
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise ValueError("환경 변수 'DATABASE_URL'이 설정되지 않았습니다.")

# 엔진 및 세션 생성
engine = create_engine(db_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

# 3. DB 작업 전용 로거 생성 (태스크 이름으로 구분)
logger = get_task_logger("database")


@contextmanager
def get_db_session():
    """DB 세션을 생성하고 트랜잭션을 관리하는 컨텍스트 매니저"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        logger.error(traceback.format_exc())
        session.rollback()
        raise e
    finally:
        session.close()
