import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# 1. 다른 모듈을 임포트하기 전에 '가장 먼저' 환경 변수를 로드합니다.
scripts_dir = Path(__file__).resolve().parent
root_dir = scripts_dir.parent

if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# config/env 또는 root env 로드
config_env = root_dir / "config" / "env"
if config_env.exists():
    load_dotenv(dotenv_path=config_env)
else:
    root_env = root_dir / "env"
    if root_env.exists():
        load_dotenv(dotenv_path=root_env)
    else:
        load_dotenv(dotenv_path=root_dir / ".env")

# 2. 환경 변수가 로드된 후 DB 엔진과 모델을 임포트합니다.
from sqlalchemy import text  # 💡 SQL 문 실행을 위해 text 임포트

from core.infrastructure.postgres import engine
from models.models import Base


def run_table_registration():
    print("🚀 기존 소스 변경 없이 테이블 DDL 등록을 시작합니다...")
    try:
        # 1. 스키마 및 테이블 생성
        Base.metadata.create_all(bind=engine)
        print("✅ 데이터베이스에 모든 테이블이 성공적으로 생성되었습니다!")

        # 2. 초기 데이터 INSERT 등록
        print("🌱 초기 데이터 인서트를 시작합니다...")
        with (
            engine.begin() as connection
        ):  # engine.begin()은 자동으로 commit/rollback을 처리합니다.
            # pg_stat_statements 익스텐션 생성 시도
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"))

            # 최초 실행을 위한 0값 초기 데이터 인서트 (중복 insert 방지를 위해 ON CONFLICT 추가)
            insert_query = text("""
                INSERT INTO public.batch_checkpoints (job_name, last_processed_id)
                VALUES (:job_name, :last_processed_id)
                ON CONFLICT (job_name) DO NOTHING;
            """)

            # 두 개의 작업 등록
            connection.execute(
                insert_query, {"job_name": "save_all_fields_to_jsonl", "last_processed_id": 0}
            )
            connection.execute(
                insert_query, {"job_name": "export_and_upload", "last_processed_id": 0}
            )
            connection.execute(
                insert_query,
                {"job_name": "save_polars_all_fields_to_jsonl", "last_processed_id": 0},
            )

        print("✅ 초기 데이터가 성공적으로 등록되거나 유지되었습니다!")

    except Exception as e:
        print(f"❌ 테이블 생성 및 데이터 등록 중 오류 발생: {e}")


if __name__ == "__main__":
    run_table_registration()
