import io
import json
import time
import uuid
from contextlib import ExitStack
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Any, cast

import orjson
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.json as pa_json
from psycopg2.extensions import connection as Connection
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from tenacity import retry

# 인프라 및 유틸리티 import (Scheduler Core SDK)
from core import COMMON_RETRY_POLICY, get_db_session, get_sftp_client, track_task_status


@track_task_status
def sample_task(logger: Logger = None) -> str:
    """단순 작업: 예외가 발생하지 않는 표준 구조"""
    try:
        logger.info("태스크가 정상적으로 시작되었습니다.")
        # 작업 로직 수행
        return "Done"
    except Exception as e:
        logger.error(f"태스크 실행 중 오류: {e}")
        raise


@track_task_status
@retry(**COMMON_RETRY_POLICY)
def retry_task(user_id: str, action: str, logger: Logger = None) -> str:
    """재시도 로직이 필요한 작업"""
    try:
        logger.info(f"사용자: {user_id}, 동작: {action} -> 작업 시작")
        # 실제 외부 API 호출이나 네트워크 작업 등
        return "Done"
    except Exception as e:
        logger.warning(f"작업 실패, 재시도 정책에 따라 재시도: {e}")
        raise  # tenacity가 감지하여 재시도하도록 raise 필수


@track_task_status
def hybrid_task(task_id: str, logger: Logger = None) -> str:
    """조건부 결과 처리: 명시적 상태 관리가 필요한 경우"""
    logger.info(f"task_id: {task_id} 작업 수행 중")

    # 비즈니스 로직에 따른 실패 처리
    is_success = True  # 예시 조건

    if not is_success:
        logger.error(f"비즈니스 로직 위반: {task_id}")
        raise ValueError("작업 상태가 FAILURE 입니다.")

    return "Done"


@track_task_status
@retry(**COMMON_RETRY_POLICY)
def save_all_fields_to_jsonl(
    output_filename: str = "bulk_user_data.jsonl", chunk_size: int = 50_000, logger: Logger = None
) -> str:
    """
    bulk_test_users 테이블 데이터를 체크포인트(Checkpoint) 기반으로 중복/누락 없이 안전하게 읽어
    data/ 폴더 하위에 JSONL 파일로 저장합니다.

    - 배치 시작 시점의 MAX(id)까지만 조회하여 실시간 인서트에 의한 라이브 락(무한 루프)을 전면 차단합니다.
    - 모든 프로세스가 성공적으로 완료되었을 때만 체크포인트를 업데이트(Fault-Tolerance)합니다.
    - 예외 발생 시 생성 중이던 불완전한 파일을 삭제하여 데이터 오염을 방지합니다.
    """
    start_total = time.time()  # 전체 프로세스 시작 시간
    JOB_NAME = "save_all_fields_to_jsonl"

    # 프로젝트 구조에 맞게 데이터 저장 경로 설정
    DATA_DIR = Path(__file__).resolve().parent.parent / "export_data"
    DATA_DIR.mkdir(exist_ok=True)
    full_path = DATA_DIR / output_filename

    count = 0

    try:
        # 1. 💡 배치 시작 시점의 체크포인트(이전 최대 ID) 및 현재 테이블의 최고 ID 조회
        with get_db_session() as session:
            checkpoint_result = session.execute(
                text(
                    "SELECT last_processed_id FROM public.batch_checkpoints WHERE job_name = :job_name;"
                ),
                {"job_name": JOB_NAME},
            )
            last_seen_id = checkpoint_result.scalar() or 0

            max_id_result = session.execute(text("SELECT MAX(id) FROM public.bulk_test_users;"))
            max_id = max_id_result.scalar() or 0

        # 만약 새로 추가된 데이터가 없다면 바로 종료
        if last_seen_id >= max_id:
            if logger:
                logger.info(
                    f"[{JOB_NAME}] 새로 추가된 데이터가 없어 배치를 종료합니다. (현재 마일스톤: {last_seen_id})"
                )
            return "No New Data"

        if logger:
            logger.info(
                f"대용량 유저 데이터 ID 기반 분할 조회 시작...\n"
                f"▶️ 시작 체크포인트 ID: {last_seen_id} | ⏹️ 이번 배치 상한선 Max ID: {max_id}"
            )

        start_fetch_write = time.time()

        # 2. 파일 쓰기 스트림 오픈 및 Chunking 루프 가동
        with open(full_path, "wb") as f:
            while True:
                # 💡 [방어벽] 실시간 인서트를 무시하고, 시작 시점의 max_id를 넘겼다면 루프 즉시 탈출
                if last_seen_id >= max_id:
                    break

                with get_db_session() as session:
                    # 💡 WHERE 절에 id <= :max_id 조건을 추가하여 범위를 명확히 가둡니다.
                    query = text("""
                        SELECT id, user_id, username, email, score, created_at, updated_at
                        FROM public.bulk_test_users
                        WHERE id > :last_seen_id
                        AND id <= :max_id
                        ORDER BY id ASC
                        LIMIT :chunk_size
                    """)

                    result = session.execute(
                        query,
                        {"last_seen_id": last_seen_id, "max_id": max_id, "chunk_size": chunk_size},
                    )
                    rows = result.fetchall()

                    if not rows:
                        break  # 더 이상 범위 내에 가져올 데이터가 없으면 탈출

                    for row in rows:
                        data = dict(row._mapping)
                        f.write(orjson.dumps(data) + b"\n")

                    # 현재 청크의 마지막 로우 ID를 다음 루프의 기준으로 설정
                    last_seen_id = rows[-1]._mapping["id"]
                    count += len(rows)

                    if logger and count % (chunk_size * 5) == 0:
                        logger.info(
                            f"진행 중... 현재까지 {count:,}건 파일 기록 완료 (현재 ID: {last_seen_id})"
                        )

        time_fetch_write = time.time() - start_fetch_write

        # 3. 💡 파일 작성이 '최종 성공'했을 때만 다음 배치를 위해 체크포인트를 갱신합니다.
        with get_db_session() as session:
            session.execute(
                text("""
                    UPDATE public.batch_checkpoints
                    SET last_processed_id = :max_id, updated_at = NOW()
                    WHERE job_name = :job_name;
                """),
                {"max_id": max_id, "job_name": JOB_NAME},
            )
            session.commit()

        time_total = time.time() - start_total

        summary_log = (
            f"🎉 성공: 전체 유저 필드 저장 및 체크포인트 갱신 완료 ({max_id})\n"
            f"총 {count:,}건 처리 [DB추출및파일작성: {time_fetch_write:.2f}초]\n"
            f"총 소요시간: {time_total:.2f}초"
        )

        if logger:
            logger.info(summary_log)

        return "Done"

    except Exception as e:
        # 정합성 보호: 작업 실패 시 불완전한 파일 삭제 (체크포인트는 UPDATE 되지 않아 안전함)
        if "full_path" in locals() and full_path.exists():
            full_path.unlink()
            if logger:
                logger.warning(f"🚨 오류 발생으로 인해 불완전한 유저 데이터 파일 삭제: {full_path}")

        if logger:
            logger.error(f"🚨 유저 데이터 파일 저장 중 오류 발생: {e!s}")
        raise e


@track_task_status
@retry(**COMMON_RETRY_POLICY)
def save_polars_all_fields_to_jsonl(
    output_filename: str = "save_pyarrow_all_fields_to_jsonl.jsonl",
    chunk_size: int = 50_000,
    logger: Logger = None,
) -> str:
    """
    bulk_test_users 테이블 데이터를 체크포인트 기반으로 읽어
    PyArrow -> Polars 기반 C++/Rust 고속 엔지니어링으로 JSONL(NDJSON) 파일로 저장합니다.
    """
    start_total = time.time()
    JOB_NAME = "save_polars_all_fields_to_jsonl"

    DATA_DIR = Path(__file__).resolve().parent.parent / "export_data"
    DATA_DIR.mkdir(exist_ok=True)
    full_path = DATA_DIR / output_filename

    count = 0

    try:
        # 1. 체크포인트 및 현재 최고 ID 조회
        with get_db_session() as session:
            checkpoint_result = session.execute(
                text(
                    "SELECT last_processed_id FROM public.batch_checkpoints WHERE job_name = :job_name;"
                ),
                {"job_name": JOB_NAME},
            )
            last_seen_id = checkpoint_result.scalar() or 0

            max_id_result = session.execute(text("SELECT MAX(id) FROM public.bulk_test_users;"))
            max_id = max_id_result.scalar() or 0

        # 새로 추가된 데이터가 없으면 즉시 종료
        if last_seen_id >= max_id:
            if logger:
                logger.info(
                    f"[{JOB_NAME}] 새로 추가된 데이터가 없어 배치를 종료합니다. (현재 마일스톤: {last_seen_id})"
                )
            return "No New Data"

        if logger:
            logger.info(
                f"PyArrow + Polars 기반 대용량 유저 데이터 ID 분할 추출 시작...\n"
                f"▶️ 시작 체크포인트 ID: {last_seen_id} | ⏹️ 이번 배치 상한선 Max ID: {max_id}"
            )

        start_fetch_write = time.time()

        # 2. PyArrow Explicit Schema 정의 (타입 안전성 보장)
        arrow_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("user_id", pa.string()),
                ("username", pa.string()),
                ("email", pa.string()),
                ("score", pa.float64()),
                ("created_at", pa.timestamp("us")),
                ("updated_at", pa.timestamp("us")),
            ]
        )

        # 3. DB 추출 및 PyArrow -> Polars write_ndjson 파일 쓰기
        with open(full_path, "wb") as f:
            while True:
                if last_seen_id >= max_id:
                    break

                with get_db_session() as session:
                    query = text("""
                        SELECT id, user_id, username, email, score, created_at, updated_at
                        FROM public.bulk_test_users
                        WHERE id > :last_seen_id
                        AND id <= :max_id
                        ORDER BY id ASC
                        LIMIT :chunk_size
                    """)

                    result = session.execute(
                        query,
                        {
                            "last_seen_id": last_seen_id,
                            "max_id": max_id,
                            "chunk_size": chunk_size,
                        },
                    )
                    rows = result.fetchall()

                    if not rows:
                        break

                    # PyArrow RecordBatch 생성
                    columns = list(zip(*rows))
                    record_batch = pa.RecordBatch.from_arrays(
                        [pa.array(col) for col in columns],
                        schema=arrow_schema,
                    )

                    # 💡 [핵심] PyArrow Table -> Polars DataFrame 변환 (Zero-Copy)
                    pa_table = pa.Table.from_batches([record_batch])
                    df = pl.from_arrow(pa_table)

                    # 💡 Polars의 C++/Rust 엔진 기반으로 JSONL(NDJSON) 고속 Append 쓰기
                    df.write_ndjson(f)  # type:ignore

                    # 청크 마일스톤 및 카운트 update
                    last_seen_id = rows[-1]._mapping["id"]
                    count += len(rows)

                    if logger and count % (chunk_size * 5) == 0:
                        logger.info(
                            f"진행 중... Polars 처리 {count:,}건 파일 기록 완료 (현재 ID: {last_seen_id})"
                        )

        time_fetch_write = time.time() - start_fetch_write

        # 4. 파일 작성이 '최종 성공' 했을 때만 체크포인트 커밋 (Fault-Tolerance)
        with get_db_session() as session:
            session.execute(
                text("""
                    UPDATE public.batch_checkpoints
                    SET last_processed_id = :max_id, updated_at = NOW()
                    WHERE job_name = :job_name;
                """),
                {"max_id": max_id, "job_name": JOB_NAME},
            )
            session.commit()

        time_total = time.time() - start_total

        summary_log = (
            f"🎉 성공: PyArrow + Polars 기반 전체 유저 필드 저장 및 체크포인트 갱신 완료 ({max_id})\n"
            f"총 {count:,}건 처리 [DB추출 및 Polars NDJSON 쓰기: {time_fetch_write:.2f}초]\n"
            f"총 소요시간: {time_total:.2f}초"
        )

        if logger:
            logger.info(summary_log)

        return "Done"

    except Exception as e:
        # 정합성 보호: 실패 시 생성되다 만 불완전한 파일 삭제
        if "full_path" in locals() and full_path.exists():
            full_path.unlink()
            if logger:
                logger.warning(f"🚨 오류 발생으로 인해 불완전한 유저 데이터 파일 삭제: {full_path}")

        if logger:
            logger.error(f"🚨 유저 데이터 파일 저장 중 오류 발생: {e!s}")
        raise e


from logging import Logger

from tenacity import retry


@track_task_status
@retry(**COMMON_RETRY_POLICY)
def save_postgres_native_to_csv(
    output_filename: str = "bulk_user_data.csv",
    chunk_size: int = 50_000,
    include_header: bool = False,
    logger: Logger = None,
) -> str:
    """
    bulk_test_users 테이블 데이터를 PostgreSQL Native COPY (CSV 포맷)를 활용하여
    DB 연산 병목 없이 극강의 속도로 CSV 파일에 저장합니다.

    - json_build_object() 연산을 제거하여 DB CPU 부하를 없앴습니다.
    - 첫 번째 청크 작성 시에만 CSV 헤더(Header)를 선택적으로 출력합니다.
    - ID Range 기반 Chunking으로 파일 버퍼(f.flush)를 비워 OOM을 방지하고 진행 로그를 출력합니다.
    """
    start_total = time.time()
    JOB_NAME = "save_postgres_native_to_csv"

    DATA_DIR = Path(__file__).resolve().parent.parent / "export_data"
    DATA_DIR.mkdir(exist_ok=True)
    full_path = DATA_DIR / output_filename

    count = 0

    try:
        # 1. 체크포인트 및 Max ID 조회
        with get_db_session() as session:
            last_seen_id = (
                session.execute(
                    text(
                        "SELECT last_processed_id FROM public.batch_checkpoints WHERE job_name = :job_name;"
                    ),
                    {"job_name": JOB_NAME},
                ).scalar()
                or 0
            )

            max_id = (
                session.execute(text("SELECT MAX(id) FROM public.bulk_test_users;")).scalar() or 0
            )

        if last_seen_id >= max_id:
            if logger:
                logger.info(
                    f"[{JOB_NAME}] 새로 추가된 데이터가 없어 배치를 종료합니다. (현재 마일스톤: {last_seen_id})"
                )
            return "No New Data"

        if logger:
            logger.info(
                f"PostgreSQL Native CSV COPY 추출 시작...\n"
                f"▶️ 시작 체크포인트 ID: {last_seen_id} | ⏹️ 이번 배치 상한선 Max ID: {max_id}"
            )

        start_fetch_write = time.time()
        is_first_chunk = True

        # 2. Append Binary 모드로 open
        with open(full_path, "ab") as f:
            while last_seen_id < max_id:
                current_chunk_max = min(last_seen_id + chunk_size, max_id)

                with get_db_session() as session:
                    raw_conn = session.connection().connection.driver_connection

                    # 💡 [핵심] json_build_object() 대신 Native CSV 포맷으로 직접 스트리밍
                    # 첫 번째 청크일 때만 HEADER 옵션을 켤 수 있도록 제어
                    header_option = (
                        "HEADER true" if (include_header and is_first_chunk) else "HEADER false"
                    )

                    copy_sql = f"""
                        COPY (
                            SELECT id, user_id, username, email, score, created_at, updated_at
                            FROM public.bulk_test_users
                            WHERE id > {last_seen_id} AND id <= {current_chunk_max}
                            ORDER BY id ASC
                        ) TO STDOUT WITH (FORMAT csv, {header_option}, DELIMITER ',');
                    """

                    with raw_conn.cursor() as cur:
                        cur.copy_expert(copy_sql, f)

                        # 처리된 행 개수 카운트
                        rows_written = (
                            cur.rowcount
                            if cur.rowcount != -1
                            else (current_chunk_max - last_seen_id)
                        )
                        count += rows_written

                # 파일 버퍼 강제 비우기 (OOM 방지 및 디스크 동기화)
                f.flush()

                last_seen_id = current_chunk_max
                is_first_chunk = False

                if logger and count % (chunk_size * 5) == 0:
                    logger.info(
                        f"진행 중... Native CSV COPY 누적 완료: {count:,}건 (현재 ID 마일스톤: {last_seen_id})"
                    )

        time_fetch_write = time.time() - start_fetch_write

        # 3. 체크포인트 갱신 (Fault-Tolerance)
        with get_db_session() as session:
            session.execute(
                text("""
                    UPDATE public.batch_checkpoints
                    SET last_processed_id = :max_id, updated_at = NOW()
                    WHERE job_name = :job_name;
                """),
                {"max_id": max_id, "job_name": JOB_NAME},
            )
            session.commit()

        time_total = time.time() - start_total

        if logger:
            logger.info(
                f"🎉 성공: PostgreSQL Native CSV COPY 완료 ({count:,}건 처리)\n"
                f"[DB COPY 및 파일작성: {time_fetch_write:.2f}초 | 총 소요시간: {time_total:.2f}초]"
            )

        return "Done"

    except Exception as e:
        if "full_path" in locals() and full_path.exists():
            full_path.unlink()
            if logger:
                logger.warning(f"🚨 오류 발생으로 인해 불완전한 CSV 파일 삭제: {full_path}")

        if logger:
            logger.error(f"🚨 CSV COPY 처리 중 오류 발생: {e!s}")
        raise e


@track_task_status
@retry(**COMMON_RETRY_POLICY)
def export_and_upload(logger: Logger = None, chunk_size: int = 50_000) -> str:
    """
    ID 기반 조각내기(Chunking) 및 체크포인트 방식으로 DB 부하를 최소화하며 데이터를 추출하고,
    로컬 JSONL 저장 후 SFTP로 안전하게 전송하는 최적화 파이프라인 함수입니다.

    - 배치 시작 시점의 MAX(id)까지만 조회하여 실시간 인서트에 의한 라이브 락(무한 루프)을 방지합니다.
    - SFTP 업로드 및 사이즈 정합성 검증까지 완전히 '성공'했을 때만 체크포인트를 업데이트합니다.
    - 예외 발생 시 원격지와 로컬의 불완전한 파일들을 깔끔하게 정리합니다.
    """
    start_total = time.time()  # 전체 프로세스 시작 시간
    JOB_NAME = "export_and_upload"

    # 프로젝트 구조에 맞게 로컬 데이터 저장 경로 설정
    DATA_DIR = Path(__file__).resolve().parent.parent / "export_data"
    DATA_DIR.mkdir(exist_ok=True)

    unique_filename = "bulk_user_data.jsonl"
    local_path = DATA_DIR / unique_filename
    remote_dir = "/uploads"
    remote_path = f"{remote_dir}/{unique_filename}"

    # 구간별 시간 측정을 위한 변수 초기화
    time_init = 0.0
    time_fetch_write = 0.0
    time_sftp_upload = 0.0
    time_validation = 0.0

    count = 0

    try:
        with ExitStack() as stack:
            # 1. 초기 세션 연결 및 준비 구간 (체크포인트 및 Max ID 획득, SFTP 연결)
            start_init = time.time()

            # 💡 [마일스톤 조회] 지난 배치 성공 시점의 ID와 현재 최고 ID를 먼저 판별합니다.
            with get_db_session() as session:
                checkpoint_result = session.execute(
                    text(
                        "SELECT last_processed_id FROM public.batch_checkpoints WHERE job_name = :job_name;"
                    ),
                    {"job_name": JOB_NAME},
                )
                last_seen_id = checkpoint_result.scalar() or 0

                max_id_result = session.execute(text("SELECT MAX(id) FROM public.bulk_test_users;"))
                max_id = max_id_result.scalar() or 0

            # 처리할 새 데이터가 없다면 커넥션을 맺지 않고 바로 종료 (자원 절약)
            if last_seen_id >= max_id:
                if logger:
                    logger.info(
                        f"[{JOB_NAME}] 새로 추가된 데이터가 없어 배치를 종료합니다. (현재 마일스톤: {last_seen_id})"
                    )
                return "No New Data"

            sftp = stack.enter_context(get_sftp_client())
            assert sftp is not None, "SFTP 클라이언트 객체가 생성되지 않았습니다 (None)."

            # SFTP 경로 사전 체크
            try:
                sftp.chdir(remote_dir)
            except OSError:
                if logger:
                    logger.error(
                        f"SFTP 경로 접근 실패: {remote_dir}. 디렉토리 존재 여부를 확인하세요."
                    )
                raise
            time_init = time.time() - start_init

            # 2. 데이터베이스 쿼리 및 파일 쓰기 구간 (ID 기반 Chunking + 상한선 제한)
            if logger:
                logger.info(
                    f"대용량 유저 데이터베이스 분할 조회 시작...\n"
                    f"▶️ 시작 체크포인트 ID: {last_seen_id} | ⏹️ 이번 배치 상한선 Max ID: {max_id}"
                )
            start_fetch_write = time.time()

            with open(local_path, "wb") as f:
                while True:
                    # 💡 [차단벽] 데이터가 진행되는 동안 상한선인 max_id를 넘겼다면 루프 즉시 탈출
                    if last_seen_id >= max_id:
                        break

                    with get_db_session() as session:
                        # 💡 id <= :max_id 조건을 추가하여 실시간 유입 데이터를 격리합니다.
                        query = text("""
                            SELECT id, user_id, username, email, score, created_at, updated_at
                            FROM public.bulk_test_users
                            WHERE id > :last_seen_id
                            AND id <= :max_id
                            ORDER BY id ASC
                            LIMIT :chunk_size
                        """)

                        result = session.execute(
                            query,
                            {
                                "last_seen_id": last_seen_id,
                                "max_id": max_id,
                                "chunk_size": chunk_size,
                            },
                        )
                        rows = result.fetchall()

                        if not rows:
                            break  # 범위 내 데이터 소진 시 탈출

                        for row in rows:
                            data = dict(row._mapping)
                            f.write(orjson.dumps(data) + b"\n")

                        # 마지막 ID 갱신 및 카운트 증가
                        last_seen_id = rows[-1]._mapping["id"]
                        count += len(rows)

                        if logger and count % (chunk_size * 5) == 0:
                            logger.info(
                                f"진행 중: 유저 데이터 {count:,}건 파일 작성 완료 (현재 ID: {last_seen_id})"
                            )

            time_fetch_write = time.time() - start_fetch_write

            # 3. SFTP 전송 구간
            if logger:
                logger.info(f"SFTP 전송 시작: {unique_filename} ({count:,}건 데이터)")
            start_sftp_upload = time.time()
            sftp.put(str(local_path), remote_path)
            time_sftp_upload = time.time() - start_sftp_upload

            # 4. 정합성 검증 구간
            start_validation = time.time()
            if sftp.stat(remote_path).st_size != local_path.stat().st_size:
                raise OSError("정합성 오류: 로컬 파일과 원격 파일의 크기가 일치하지 않음.")
            time_validation = time.time() - start_validation

            # 5. 💡 [체크포인트 최종 커밋] 추출, 전송, 정합성 검증까지 교차 확인이 끝난 후 마일스톤을 밀어줍니다.
            with get_db_session() as session:
                session.execute(
                    text("""
                        UPDATE public.batch_checkpoints
                        SET last_processed_id = :max_id, updated_at = NOW()
                        WHERE job_name = :job_name;
                    """),
                    {"max_id": max_id, "job_name": JOB_NAME},
                )
                session.commit()

            # 6. 전체 소요 시간 계산 및 로그 출력
            time_total = time.time() - start_total

            summary_log = (
                f"🎉 Done: 유저 데이터 {count:,}건 추출 및 SFTP 업로드 완료 (체크포인트 {max_id}번 마킹) | "
                f"총 소요시간: {time_total:.2f}초 "
                f"[1.연결및준비: {time_init:.2f}초 | "
                f"2.DB추출및파일작성: {time_fetch_write:.2f}초 | "
                f"3.SFTP전송: {time_sftp_upload:.2f}초 | "
                f"4.정합성검증: {time_validation:.2f}초]"
            )

            if logger:
                logger.info(summary_log)

            return summary_log

    except Exception as e:
        if logger:
            logger.error(f"🚨 파이프라인 작업 실패: {e!s}")

        # 실패 시 원격 서버에 생성되다 만 찌꺼기 파일 제거
        try:
            if "sftp" in locals() and sftp:
                sftp.remove(remote_path)
                if logger:
                    logger.warning(f"오류 발생으로 원격지 불완전 파일 제거 완료: {remote_path}")
        except Exception:
            pass
        raise e

    finally:
        # 7. 로컬 임시 파일 완전 삭제 (성공/실패 공통)
        if local_path.exists():
            local_path.unlink()
            if logger:
                logger.debug(f"로컬 임시 백업 파일 정리 완료: {unique_filename}")


@track_task_status
@retry(**COMMON_RETRY_POLICY)
def download_and_import(logger: Logger = None, chunk_size: int = 50_000) -> str:
    """[로컬 로그 보관 + 메모리 OOM 방지 완료 버전]
    1. SFTP에서 로컬 data/ 폴더로 원본 파일을 안전하게 다운로드하여 보관합니다.
    2. CopyStreamWrapper가 로컬 파일을 읽을 때 size 버퍼를 준수하여 메모리 OOM을 방지합니다.
    3. LIKE 구문 및 차분 UPSERT로 PG 카탈로그와 MVCC 부하를 최소화합니다.
    """
    func_start = time.perf_counter()

    # 💡 로그 보관을 위한 로컬 디스크 경로 설정
    DATA_DIR = Path(__file__).resolve().parent.parent / "export_data"
    DATA_DIR.mkdir(exist_ok=True)

    remote_dir = "/uploads"
    remote_filename = "bulk_user_data.jsonl"
    remote_path = f"{remote_dir}/{remote_filename}"

    # 이력 관리를 위해 고유한 파일명으로 로컬에 저장 (예: download_20260707_abcd.jsonl)
    # 날짜나 UUID를 조합하여 기존 파일이 덮어써지는 것을 방지합니다.
    unique_filename = f"backup_{uuid.uuid4().hex}_{remote_filename}"
    local_path = DATA_DIR / unique_filename

    total_count = 0
    target_keys = ["user_id", "username", "email", "score", "created_at", "updated_at"]

    try:
        with ExitStack() as stack:
            session = stack.enter_context(get_db_session())
            sftp = stack.enter_context(get_sftp_client())
            assert sftp is not None, "SFTP 클라이언트 객체가 유효하지 않습니다."

            try:
                remote_stat = sftp.stat(remote_path)
            except OSError:
                if logger:
                    logger.error(f"SFTP 파일 접근 실패: {remote_path}")
                raise

            # 1. SFTP 다운로드 구간 (로컬에 로그 보관용 파일 생성)
            if logger:
                logger.info(f"SFTP 파일 다운로드 시작 -> 로컬 보관 경로: {local_path}")
            sftp_start = time.perf_counter()
            sftp.get(remote_path, str(local_path))

            if logger:
                logger.info(
                    f"⏱️ [구간 통계] 다운로드 완료: {time.perf_counter() - sftp_start:.4f}초"
                )

            # 파일 정합성 검증
            if local_path.stat().st_size != remote_stat.st_size:
                raise OSError("정합성 오류: 다운로드된 파일의 크기가 원격지와 일치하지 않습니다.")

            # 2. DB 임시 테이블 세팅
            session.execute(text("SET LOCAL temp_buffers = '256MB';"))
            session.execute(
                text("""
                CREATE TEMP TABLE temp_bulk_test_users (
                    LIKE bulk_test_users INCLUDING DEFAULTS
                ) ON COMMIT DROP;
                """)
            )

            raw_conn = cast(Connection, session.connection().connection.driver_connection)
            assert raw_conn is not None

            # 3. 💡 고속 스트림 변환 래퍼 (로컬 파일을 청크 단위로 쪼개 읽어 OOM 방지)
            class CopyStreamWrapper(io.TextIOBase):
                def __init__(self, file_path):
                    self.f = open(file_path, "r", encoding="utf-8")
                    self.count = 0
                    self.buffer = ""

                def read(self, size: int = -1) -> str:
                    if len(self.buffer) >= size > 0:
                        chunk = self.buffer[:size]
                        self.buffer = self.buffer[size:]
                        return chunk

                    lines = []
                    current_length = 0

                    # 💡 파일 전체를 리스트에 담지 않고, PG 드라이버가 요청한 size만큼만 한 줄씩 읽음
                    for line in self.f:
                        if not line.strip():
                            continue
                        try:
                            row_data = orjson.loads(line)
                            vals = []
                            for k in target_keys:
                                v = row_data.get(k)
                                if v is None:
                                    vals.append("NULL")
                                else:
                                    vals.append(
                                        str(v)
                                        .replace("\\", "\\\\")
                                        .replace("\n", " ")
                                        .replace("\t", " ")
                                    )

                            converted_line = "\t".join(vals) + "\n"
                            lines.append(converted_line)
                            current_length += len(converted_line)
                            self.count += 1

                            if size > 0 and current_length >= size:
                                break
                        except orjson.JSONDecodeError:
                            continue

                    if not lines and not self.buffer:
                        return ""  # EOF

                    total_str = self.buffer + "".join(lines)
                    if size > 0 and len(total_str) > size:
                        self.buffer = total_str[size:]
                        return total_str[:size]
                    else:
                        self.buffer = ""
                        return total_str

                def close(self):
                    if not self.f.closed:
                        self.f.close()

            # 4. COPY 실행
            copy_query = """
                COPY temp_bulk_test_users (
                    user_id, username, email, score, created_at, updated_at
                ) FROM STDIN WITH (FORMAT text, NULL 'NULL');
            """

            copy_start = time.perf_counter()
            with raw_conn.cursor() as cursor:
                stream_wrapper = CopyStreamWrapper(local_path)
                try:
                    cursor.copy_expert(sql=copy_query, file=stream_wrapper)
                finally:
                    stream_wrapper.close()
                total_count = stream_wrapper.count

            if logger:
                logger.info(
                    f"⏱️ [구간 통계] 로컬 파일 -> DB COPY 완료: {total_count:,}건 ({time.perf_counter() - copy_start:.4f}초)"
                )

            # 5. 최종 내부 병합 (Bulk UPSERT)
            session.execute(text("SET LOCAL synchronous_commit = off;"))
            session.execute(text("SET LOCAL work_mem = '256MB';"))

            session.execute(
                text("""
                INSERT INTO bulk_test_users (
                    user_id, username, email, score, created_at, updated_at
                )
                SELECT
                    user_id, username, email, score, created_at, timezone('Asia/Seoul', now())
                FROM temp_bulk_test_users
                ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    email = EXCLUDED.email,
                    score = EXCLUDED.score,
                    updated_at = timezone('Asia/Seoul', now())
                WHERE
                    bulk_test_users.username IS DISTINCT FROM EXCLUDED.username OR
                    bulk_test_users.email IS DISTINCT FROM EXCLUDED.email OR
                    bulk_test_users.score IS DISTINCT FROM EXCLUDED.score;
                """)
            )

            session.commit()

            total_elapsed = time.perf_counter() - func_start
            if logger:
                logger.info(
                    f"🎉 [성공] 총 {total_count:,}건 적재 완료 및 원본 파일 보관 성공 (총 소요시간: {total_elapsed:.4f}초)"
                )

            return f"Done: {total_count}건 적재 완료 (파일 보관됨)"

    except Exception as e:
        if logger:
            logger.error(f"🚨 파이프라인 작업 실패: {e!s}")
        if "session" in locals() and session:
            session.rollback()

        # 💡 실패한 경우는 불완전한 데이터이므로 보관하지 않고 지워줍니다.
        if "local_path" in locals() and local_path.exists():
            try:
                local_path.unlink()
            except Exception:
                pass
        raise e
