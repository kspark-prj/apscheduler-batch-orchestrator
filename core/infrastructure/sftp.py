import os
from contextlib import contextmanager

import paramiko

from core.utils.logger_setup import get_task_logger

logger = get_task_logger("sftp")


@contextmanager
def get_sftp_client():
    # 환경 변수 가져오기 (문자열 기본값 사용)
    host = os.getenv("SFTP_HOST")
    user = os.getenv("SFTP_USER")
    pwd = os.getenv("SFTP_PASS")
    port = int(os.getenv("SFTP_PORT", "22"))
    timeout = int(os.getenv("SFTP_TIMEOUT", "10"))

    # 필수값 검증 강화
    if not all([host, user, pwd]):
        raise ValueError("SFTP 필수 환경 변수(HOST, USER, PASS)가 누락되었습니다.")

    transport = None
    sftp = None

    try:
        # 내부 소켓의 기본 타임아웃 및 SSH 핸드셰이크 타임아웃을 아래 방식으로 설정합니다.
        transport = paramiko.Transport((host, port))
        transport.set_keepalive(30)
        transport.banner_timeout = timeout  # SSH 배너 획득 타임아웃 설정

        transport.connect(username=user, password=pwd)
        sftp = paramiko.SFTPClient.from_transport(transport)

        yield sftp

    except paramiko.AuthenticationException:
        logger.error(f"SFTP 인증 실패: [Host: {host}, User: {user}]")
        raise
    except paramiko.SSHException as e:
        logger.error(f"SSH 프로토콜 오류 [Host: {host}]: {e}")
        raise
    except Exception as e:
        logger.error(f"SFTP 예기치 못한 오류: {e}")
        raise

    finally:
        # 리소스 해제 순서 보장 및 안전성 확보
        if sftp:
            try:
                sftp.close()
            except Exception as e:
                logger.warning(f"SFTP Close 오류: {e}")
        if transport:
            try:
                transport.close()
            except Exception as e:
                logger.warning(f"Transport Close 오류: {e}")

        logger.debug("SFTP 세션 종료 완료")
