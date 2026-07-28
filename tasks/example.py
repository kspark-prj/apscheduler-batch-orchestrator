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

                    with raw_conn.cursor() as cur:  # type:ignore
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
    """[PyArrow + Polars C++/Rust 고속 파싱 & SFTP 최적화 파이프라인]
    1. DB CPU 부하를 일으키는 json_build_object()를 제거하고 튜플 데이터만 빠르게 추출
    2. PyArrow -> Polars(Zero-Copy) 변환 후 C++/Rust 엔진 기반 NDJSON(JSONL) 초고속 파일 쓰기
    3. DB 세션과 SFTP 세션 스코프 분리로 커넥션 점유 최소화
    4. SFTP 업로드 및 정합성 검증 성공 시에만 체크포인트 갱신 (Fault-Tolerance)
    """
    start_total = time.perf_counter()
    JOB_NAME = "export_and_upload"

    DATA_DIR = Path(__file__).resolve().parent.parent / "export_data"
    DATA_DIR.mkdir(exist_ok=True)

    # 동시성 및 안전성을 위한 고유 임시 파일명 설정
    temp_filename = "export_upload_bulk_user_data.jsonl"
    remote_filename = "bulk_user_data.jsonl"

    local_path = DATA_DIR / temp_filename
    remote_dir = "/uploads"
    remote_path = f"{remote_dir}/{remote_filename}"

    count = 0
    time_fetch_write = 0.0
    time_sftp_upload = 0.0
    time_validation = 0.0

    try:
        # ==========================================================
        # 1. 체크포인트 및 상한선 ID 조회
        # ==========================================================
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

        if last_seen_id >= max_id:
            if logger:
                logger.info(
                    f"[{JOB_NAME}] 새로 추가된 데이터가 없어 배치를 종료합니다. (현재 마일스톤: {last_seen_id})"
                )
            return "No New Data"

        if logger:
            logger.info(
                f"PyArrow + Polars 기반 대용량 유저 데이터 ID 분할 추출 및 SFTP 파이프라인 시작...\n"
                f"▶️ 시작 체크포인트 ID: {last_seen_id} | ⏹️ 이번 배치 상한선 Max ID: {max_id}"
            )

        # ==========================================================
        # 2. Explicit PyArrow Schema 정의 (타입 안전성 및 속도 확보)
        # ==========================================================
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

        # ==========================================================
        # 3. DB 추출 및 Polars write_ndjson 고속 Append 쓰기
        # ==========================================================
        start_fetch_write = time.perf_counter()

        with open(local_path, "wb") as f:
            while last_seen_id < max_id:
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

                    # 💡 DB 튜플 -> PyArrow RecordBatch 생성
                    columns = list(zip(*rows))
                    record_batch = pa.RecordBatch.from_arrays(
                        [pa.array(col) for col in columns],
                        schema=arrow_schema,
                    )

                    # 💡 PyArrow Table -> Polars DataFrame (Zero-Copy)
                    pa_table = pa.Table.from_batches([record_batch])
                    df = pl.from_arrow(pa_table)

                    # 💡 Polars의 C++/Rust 엔진 기반으로 JSONL(NDJSON) 고속 Append 쓰기
                    df.write_ndjson(f)  # type: ignore

                    last_seen_id = rows[-1]._mapping["id"]
                    count += len(rows)

                    if logger and count % (chunk_size * 5) == 0:
                        logger.info(
                            f"진행 중... Polars 처리 {count:,}건 파일 기록 완료 (현재 ID: {last_seen_id})"
                        )

        time_fetch_write = time.perf_counter() - start_fetch_write

        # ==========================================================
        # 4. SFTP 업로드 및 정합성 검증 (DB 세션 완전 분리)
        # ==========================================================
        if logger:
            logger.info(f"SFTP 전송 시작 -> {remote_path} ({count:,}건 데이터)")

        with get_sftp_client() as sftp:
            assert sftp is not None, "SFTP 클라이언트 객체가 유효하지 않습니다."

            try:
                sftp.chdir(remote_dir)
            except OSError:
                if logger:
                    logger.error(f"SFTP 경로 접근 실패: {remote_dir}")
                raise

            # SFTP 파일 전송
            start_upload = time.perf_counter()
            sftp.put(str(local_path), remote_path)
            time_sftp_upload = time.perf_counter() - start_upload

            # 정합성 검증 (파일 크기 비교)
            start_val = time.perf_counter()
            if sftp.stat(remote_path).st_size != local_path.stat().st_size:
                raise OSError("정합성 오류: 로컬 파일과 원격 파일의 크기가 일치하지 않습니다.")
            time_validation = time.perf_counter() - start_val

        # ==========================================================
        # 5. 체크포인트 최종 커밋
        # ==========================================================
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

        time_total = time.perf_counter() - start_total

        summary_log = (
            f"🎉 Done: 유저 데이터 {count:,}건 Polars 추출 및 SFTP 업로드 완료 (체크포인트 {max_id}번 마킹) | "
            f"총 소요시간: {time_total:.2f}초 "
            f"[DB추출및Polars작성: {time_fetch_write:.2f}초 | "
            f"SFTP전송: {time_sftp_upload:.2f}초 | "
            f"정합성검증: {time_validation:.2f}초]"
        )

        if logger:
            logger.info(summary_log)

        return summary_log

    except Exception as e:
        if logger:
            logger.error(f"🚨 파이프라인 작업 실패: {e!s}")

        # 실패 시 원격지의 불완전 파일 안전하게 제거
        try:
            with get_sftp_client() as sftp:
                if sftp:
                    sftp.remove(remote_path)
                    if logger:
                        logger.warning(f"오류 발생으로 원격지 불완전 파일 제거 완료: {remote_path}")
        except Exception as cleanup_err:
            if logger:
                logger.debug(f"원격지 cleanup 중 경고 (무시됨): {cleanup_err!s}")

        raise e

    finally:
        # 로컬 임시 파일 삭제 (성공/실패 공통)
        if local_path.exists():
            try:
                local_path.unlink()
                if logger:
                    logger.debug(f"로컬 임시 파일 정리 완료: {temp_filename}")
            except Exception:
                pass


@track_task_status
@retry(**COMMON_RETRY_POLICY)
def download_and_import(logger: Logger = None, chunk_size: int = 100_000) -> str:
    """[Polars Explicit Schema JSONL Parsing + Type Safe COPY 최적화 파이프라인]
    1. SFTP에서 로컬로 파일 다운로드 (DB 커넥션 점유 최소화)
    2. Polars 기반 타입 형변환(Float->Int) 처리 및 Chunk 단위 TSV 바이트 변환 (COPY Syntax 에러 방지)
    3. TEMP TABLE 인덱싱 및 차분 UPSERT로 PG CPU/MVCC 부하 최소화
    """
    func_start = time.perf_counter()

    DATA_DIR = Path(__file__).resolve().parent.parent / "export_data"
    DATA_DIR.mkdir(exist_ok=True)

    remote_dir = "/uploads"
    remote_filename = "bulk_user_data.jsonl"
    remote_path = f"{remote_dir}/{remote_filename}"

    unique_filename = f"backup_{uuid.uuid4().hex}_{remote_filename}"
    local_path = DATA_DIR / unique_filename

    total_count = 0

    try:
        # ==========================================================
        # 1. SFTP 파일 다운로드
        # ==========================================================
        sftp_start = time.perf_counter()
        with get_sftp_client() as sftp:
            assert sftp is not None, "SFTP 클라이언트 객체가 유효하지 않습니다."
            try:
                remote_stat = sftp.stat(remote_path)
            except OSError:
                if logger:
                    logger.error(f"SFTP 파일 접근 실패: {remote_path}")
                raise

            if logger:
                logger.info(f"SFTP 파일 다운로드 시작 -> 로컬 보관 경로: {local_path}")
            sftp.get(remote_path, str(local_path))

        if logger:
            logger.info(f"⏱️ [구간 통계] 다운로드 완료: {time.perf_counter() - sftp_start:.4f}초")

        if local_path.stat().st_size != remote_stat.st_size:
            raise OSError("정합성 오류: 다운로드된 파일의 크기가 원격지와 일치하지 않습니다.")

        # ==========================================================
        # 2. JSONL 매핑 및 타입 지정을 위한 Explicit Schema 정의
        # ==========================================================
        jsonl_schema = pl.Schema(
            {
                "id": pl.Int64,
                "user_id": pl.Utf8,
                "username": pl.Utf8,
                "email": pl.Utf8,
                "score": pl.Float64,  # JSONL 상에서 25.0 과 같은 소수점으로 읽힐 수 있으므로 Float64 선언
                "created_at": pl.Utf8,
                "updated_at": pl.Utf8,
            }
        )

        # ==========================================================
        # 3. DB 임시 테이블 세팅 및 Chunk 단위 COPY
        # ==========================================================
        db_start = time.perf_counter()

        with get_db_session() as session:
            session.execute(text("SET LOCAL temp_buffers = '256MB';"))
            session.execute(text("SET LOCAL work_mem = '256MB';"))
            session.execute(text("SET LOCAL synchronous_commit = off;"))

            session.execute(
                text("""
                CREATE TEMP TABLE temp_bulk_test_users (
                    LIKE bulk_test_users INCLUDING DEFAULTS
                ) ON COMMIT DROP;
                """)
            )

            raw_conn = cast(Connection, session.connection().connection.driver_connection)
            assert raw_conn is not None

            copy_query = """
                COPY temp_bulk_test_users (
                    user_id, username, email, score, created_at, updated_at
                ) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\\N');
            """

            lazy_df = pl.scan_ndjson(str(local_path), schema=jsonl_schema)

            with raw_conn.cursor() as cursor:
                for chunk_df in lazy_df.collect_batches(chunk_size=chunk_size):
                    chunk_len = len(chunk_df)
                    if chunk_len == 0:
                        continue

                    # 💡 [핵심] DB Integer 컬럼에 "25.0" 문자열이 들어가 발생하는 오류를 방지하기 위해 Int64로 캐스팅
                    mapped_df = chunk_df.select(
                        [
                            "user_id",
                            "username",
                            "email",
                            pl.col("score").fill_null(0).round(0).cast(pl.Int64).alias("score"),
                            "created_at",
                            "updated_at",
                        ]
                    )

                    tsv_bytes = mapped_df.write_csv(
                        file=None,
                        separator="\t",
                        include_header=False,
                        null_value="\\N",
                    )

                    buf = io.BytesIO(
                        tsv_bytes.encode("utf-8") if isinstance(tsv_bytes, str) else tsv_bytes
                    )
                    cursor.copy_expert(sql=copy_query, file=buf)

                    total_count += chunk_len

                    if logger and total_count % (chunk_size * 5) == 0:
                        logger.info(f"진행 중... Staging Table 누적 적재: {total_count:,}건")

            if logger:
                logger.info(
                    f"⏱️ [구간 통계] Polars Streaming 파싱 & Staging COPY 완료: {total_count:,}건 ({time.perf_counter() - db_start:.4f}초)"
                )

            # ==========================================================
            # 4. 차분 UPSERT
            # ==========================================================
            upsert_start = time.perf_counter()

            session.execute(text("ALTER TABLE temp_bulk_test_users ADD PRIMARY KEY (user_id);"))

            session.execute(
                text("""
                INSERT INTO bulk_test_users (
                    user_id, username, email, score, created_at, updated_at
                )
                SELECT
                    user_id, username, email, score,
                    created_at::timestamp, CURRENT_TIMESTAMP
                FROM temp_bulk_test_users
                ON CONFLICT (user_id) DO UPDATE SET
                    username   = EXCLUDED.username,
                    email      = EXCLUDED.email,
                    score      = EXCLUDED.score,
                    updated_at = CURRENT_TIMESTAMP
                WHERE
                    bulk_test_users.username IS DISTINCT FROM EXCLUDED.username OR
                    bulk_test_users.email    IS DISTINCT FROM EXCLUDED.email OR
                    bulk_test_users.score    IS DISTINCT FROM EXCLUDED.score;
                """)
            )

            session.commit()

            if logger:
                logger.info(
                    f"⏱️ [구간 통계] 차분 UPSERT 완료: ({time.perf_counter() - upsert_start:.4f}초)"
                )

        total_elapsed = time.perf_counter() - func_start
        if logger:
            logger.info(
                f"🎉 [성공] 총 {total_count:,}건 적재 완료 및 원본 파일 보관 성공 (총 소요시간: {total_elapsed:.4f}초)"
            )

        return f"Done: {total_count}건 적재 완료 (파일 보관됨)"

    except Exception as e:
        if logger:
            logger.error(f"🚨 파이프라인 작업 실패: {e!s}")

        if local_path.exists():
            try:
                local_path.unlink()
            except Exception:
                pass
        raise e
