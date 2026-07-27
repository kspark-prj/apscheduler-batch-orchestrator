from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, String, Text, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# 1. 모든 ORM 모델의 기본 클래스가 되는 DeclarativeBase 선언
class Base(DeclarativeBase):
    """SQLAlchemy 2.0 스타일 선언적 매핑을 위한 베이스 클래스"""


class BulkTestUser(Base):
    __tablename__ = "bulk_test_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(50), nullable=False)
    username = Column(String(100), nullable=False)
    email = Column(String(150), nullable=True)
    score = Column(Integer, default=0)

    # 💡 [INSERT 시] DB가 직접 CURRENT_TIMESTAMP를 넣도록 설정
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    # 💡 [UPDATE 시] SQLAlchemy가 UPDATE 쿼리에 DB 현재 시간 함수를 포함시킴
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class BatchCheckpoint(Base):
    __tablename__ = "batch_checkpoints"
    __table_args__ = {"schema": "public"}  # SQL의 public 스키마 명시

    # job_name VARCHAR(100) PRIMARY KEY
    job_name: Mapped[str] = mapped_column(String(100), primary_key=True)

    # last_processed_id BIGINT NOT NULL
    last_processed_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    # 💡 timezone=True를 설정하여 TIMESTAMP WITH TIME ZONE을 대응하며,
    #    server_default를 통해 DB가 직접 NOW()를 넣도록 하고 onupdate를 통해 수정 시에도 시간이 갱신되도록 합니다.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
