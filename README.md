# 🚀 APScheduler 기반 고성능 배치 오케스트레이터 및 대용량 ETL 파이프라인

본 프로젝트는 **APScheduler**를 핵심 엔진으로 삼아 모듈화된 다양한 태스크들을 효율적으로 관리하고 안정적으로 주기 실행(Cron)하는 배치 오케스트레이터 시스템입니다.

현재 아키텍처는 공통 엔진/인프라 영역인 **Core**와 개발자 비즈니스 영역인 **User Space**가 명확하게 분리되어 있으며, **동적 태스크 탐색(Dynamic Task Discovery)** 기능을 탑재하여 신규 배치 태스크 등록 시 핵심 코드를 전혀 수정하지 않고 편리하게 설정하여 배포할 수 있는 강력한 개발자 사용성을 가집니다.

### ⚠️ [필독] 아키텍처 선택 가이드: Python 함수형 태스크 vs OS 명령어 태스크

본 스케줄러는 성능 최적화와 파이썬의 **GIL(Global Interpreter Lock) 제약**을 해결하기 위해 **멀티쓰레딩**과 **멀티프로세싱(서브프로세스)**을 모두 지원합니다. 작업의 성격에 맞게 올바른 방식을 선택해야 성능 저하나 스케줄러 지연을 방지할 수 있습니다.

| 구분                | 방법 A: Python 함수형 태스크 (동적 로드)                              | 방법 B: OS 명령어/셸 스크립트 태스크                                    |
| :------------------ | :-------------------------------------------------------------------- | :---------------------------------------------------------------------- |
| **작동 방식**       | 스케줄러와 동일한 프로세스 내 **멀티쓰레딩** ([ThreadPoolExecutor]][] | `subprocess.Popen`을 통한 **독립된 OS 자식 프로세스** 실행              |
| **GIL 영향**        | **영향 있음** (파이썬 코드 연산 시 스레드 간 대기 발생)               | **영향 없음** (새 프로세스는 독자적인 GIL을 가짐)                       |
| **적합한 작업**     | **I/O 바운드 작업** (DB CRUD, 웹/API 요청, SFTP 파일 전송 등)         | **CPU 바운드 작업** (대규모 데이터 연산, 파싱, 외부 프로그램 실행 등)   |
| **상태 관리**       | `shared_stats` 전역 메모리를 직접 공유하여 모니터링 실시간 반영       | 독립 프로세스 상태이므로 콘솔 로그(`logs/cron/`)와 타임아웃만 제어 가능 |
| **메모리 오버헤드** | 매우 낮음 (스레드 간 메모리 공유)                                     | 높음 (Windows 환경은 자식 프로세스 생성 시 파이썬 초기화 비용 발생)     |

### 💡 태스크 등록 및 관리 요약:\*\*

### 방법 A: Python 함수형 태스크 (스레드 실행)\*\*

    1.  **함수 구현**: [tasks/]하위에 새로운 파이썬 파일 생성 후 `@track_task_status` 데코레이터를 적용한 함수를 작성합니다.
    2.  **스케줄 설정**: [config/tasks_config.json]에 함수명, 실행 주기(Cron), 인자값을 등록합니다.

### 방법 B: OS 명령어/셸 스크립트 태스크 (서브프로세스 실행)\*\*

    1.  **스크립트/명령어 준비**: 독립 실행 가능한 파일(`.py`, `.bat`, `.sh` 등) 또는 셸 명령어를 준비합니다.
    2.  **스케줄 설정**: [config/crontab_list.txt]에 한 줄 단위로 크론 주기와 실행할 명령어를 작성합니다.
    **공통 가동**: [main.py]프로세스를 실행하여 오케스트레이션을 시작합니다.

=====================================================================================================================================================================================

## ⚙️ 퀵 스타트 및 실행 방법 (Quick Start)

배치 스케줄러 시스템을 가동하기 전, 환경 변수 설정 및 데이터베이스 테이블 초기화가 필요합니다. 아래 가이드라인을 순서대로 진행해 주세요.

### 1. 환경 변수 세팅 (`config/env`)

프로젝트 내 [config/env]파일에 데이터베이스, SFTP 연결 세팅 및 스케줄러 제어 플래그를 입력합니다. (시스템 시작 시 자동으로 기동 환경을 탐색하여 로드합니다.)

```text
# ====== 크론 스케줄러 운영 인프라 설정 ======
USE_DB=False               # SQLite 데이터베이스를 이용한 작업 영속화(Persistent Store) 사용 여부
USE_JSON_CONFIG=True      # config/tasks_config.json에 등록된 파이썬 함수 태스크 로드 여부
USE_TXT_CONFIG=False      # config/crontab_list.txt에 등록된 OS 명령어 태스크 로드 여부

# 데이터베이스 PostgreSQL (ETL용)
DATABASE_URL=postgresql+psycopg2://postgres:root@localhost:5432/my-app-db

# SFTP 연결 설정 (ETL용)
SFTP_HOST=127.0.0.1
SFTP_USER=tester
SFTP_PASS=password
SFTP_PORT=22
SFTP_TIMEOUT=10
```

### 2. 가상환경 및 의존성 패키지 설치

```bash
python -m venv venv
source venv/Scripts/activate  # Windows 환경
# Linux/macOS 환경: source venv/bin/activate
pip install -r requirements.txt
```

### 3. 데이터베이스 테이블 스키마 생성 및 초기화 (ETL 파이프라인 필수)

[scripts/init_db.py] 스크립트를 실행하여 ORM 모델에 대응하는 PostgreSQL 테이블을 자동 생성하고, 시스템 운영에 필요한 배치 체크포인트 초기 레코드를 안전하게 주입합니다.

```bash
python scripts/init_db.py
generate.sql 더미데이터 생성 5,000만건
```

> **💡 초기화 스크립트 실행 시 자동 처리 사항:**
>
> - `pg_stat_statements` 성능 분석 익스텐션 활성화
> - 운영 테이블(`bulk_test_users`) 및 체크포인트 제어 테이블(`batch_checkpoints`) 생성
> - 배치 중복 실행 및 라이브 락 방지를 위한 작업(Job) 마일스톤 레코드 2건 자동 반영 (`ON CONFLICT` 예외 방어벽 적용)

### 4. 파이프라인 엔진 가동

```bash
python main.py
```

---

## 🏗️ 시스템 아키텍처 및 데이터 흐름

### 1. 스케줄러 오케스트레이션 아키텍처

스케줄러가 구동되면 설정 플래그에 따라 파이썬 함수와 OS 명령어가 각각 병렬 스레드 풀 형태로 동작하도록 제어됩니다.

```mermaid
flowchart TD
    A[main.py 실행] --> B[환경 설정 파일 config/env 로드]
    B --> C[core/engine.py 메인 기동]
    C --> D[BackgroundScheduler 기동 및 스레드풀 설정]

    C --> E{설정 확인}
    E -->|USE_JSON_CONFIG=True| F[load_json_tasks 호출]
    E -->|USE_TXT_CONFIG=True| G[load_crontab_file 호출]

    F --> H[core/config_loader.py 기동]
    H --> I[tasks/ 디렉토리 내 모듈 자동 스캔 및 동적 태스크 탐색]
    I --> J[TASK_MAPPING 구성완료]
    J --> K[config/tasks_config.json 로드하여 매칭 후 스케줄러 등록]

    G --> L[config/crontab_list.txt 로드]
    L --> M[OS 명령어를 스케줄러에 등록]

    K --> N[스케줄 주기 트리거 실행]
    M --> N

    N -->|파이썬 태스크| O[track_task_status 데코레이터 적용 / tenacity 재시도 제어]
    N -->|OS 명령어| P[subprocess.Popen 실행 및 logs/cron/에 stdout/stderr 저장]

    O --> Q[logs/task_name/ 폴더에 격리된 일별 rotating 로그 기록]
    Q --> R[10초 주기 logs/scheduler_status.txt 대시보드 스냅샷 업데이트]
    P --> R
```

### 2. ETL Bulk UPSERT 파이프라인 데이터 흐름

본 시스템에 내장된 대용량 영화/유저 데이터 적재 파이프라인은 파이썬 인터프리터의 CPU 병목과 데이터베이스의 인덱스 정렬(I/O) 부하를 원천 차단하기 위해 **스테이징(Staging) 전략**을 채택했습니다.

```text
[SFTP 원격 서버]
       │ (1. SFTP 다운로드 - 약 1.2초)
       ▼
[로컬 JSONL 백업 보관] ──> 데이터 이력 관리 및 유실 방지를 위한 디스크 보관 고유 파일 명명
       │ (2. OOM 방지용 분할 스트림 변환: CopyStreamWrapper)
       ▼
[인메모리 청크 버퍼 (C-Engine 기반 고속 파싱: orjson)]
       │ (3. 제약조건 없는 고속 주입: COPY FROM STDIN - 약 0.04초)
       ▼
[임시 대기실 (TEMP TABLE)] ──> 본 테이블(bulk_test_users) 구조를 본뜬 RAM 격리 영역
       │ (4. DB 내부 차분 결합 연산: Bulk UPSERT - 약 0.6초)
       ▼
[운영 테이블 (BulkTestUser)] ──> KST(한국 표준시) 기준으로 변경 내용이 있을 때만 최종 물리 마감
```

---

## ➕ 신규 태스크 등록 및 관리 방법 (Task Management Guide)

코어 엔진(Core)과 사용자 영역(User Tasks)이 완벽히 분리되어 있어 사용자가 신규 배치 작업을 추가할 때 프레임워크 핵심 코드를 전혀 건드릴 필요가 없습니다.

#### 1. Python 함수형 태스크 (스레드) 가이드

- **사용 기준:** 데이터베이스 읽기/쓰기, API 통신, 파일 업로드/다운로드 등 대부분의 시간을 외부 자원 대기에 사용하는 **I/O 중심 작업**에 사용합니다.
- **장점:** 메모리를 공유하므로 리소스 오버헤드가 극히 적고, 실시간 모니터링 대시보드(`logs/scheduler_status.txt`)에 상태가 즉시 연동됩니다.
- **주의사항:** 순수 파이썬 코드로 60초 이상 지속되는 무거운 CPU 연산을 처리할 경우, GIL 제약으로 인해 다른 스케줄러 스레드가 멈추거나 다음 스케줄이 밀릴 수 있습니다.

#### 2. OS 명령어/셸 스크립트 태스크 (서브프로세스) 가이드

- **사용 기준:** 대규모 데이터 파싱, 머신러닝 연산, 독립적으로 분리된 대형 크롤러 스크립트 등 CPU 부하가 크거나 장시간 실행되는 **CPU 중심 작업**에 사용합니다.
- **장점:** 독립된 OS 자식 프로세스로 구동되어 **부모 프로세스의 GIL 제약을 완전히 우회**하므로 멀티코어 성능을 100% 활용합니다.
- **주의사항 및 특징:**
    - **타임아웃 방지:** 외부 스크립트가 무한 루프나 오류로 멈추는 것을 방지하기 위해 [config/env]파일의 `SUBPROCESS_TIMEOUT` 변수를 통해 **최대 제한 시간**을 설정할 수 있습니다.
      설정된 한계를 넘어서면 자동으로 강제 종료(kill)됩니다. (제한시간 내에 일찍 종료되는 작업은 완료 즉시 반환되므로 지연이 없습니다.)
    - **중복 실행 방지:** 이전 회차의 스크립트가 끝나지 않은 채 다음 스케줄링 주기가 도래할 경우, `max_instances=1` 규칙에 의해 **새로운 실행이 시작되지 않고 안전하게 스킵**됩니다.

---

### 방법 A: Python 함수형 태스크 등록

#### 1단계: 태스크 함수 작성

[tasks/] 디렉토리 내에 파이썬 모듈 파일(예: `tasks/example.py` 또는 새로운 `.py` 파일)을 생성하여 작업 함수를 정의합니다.

- `@track_task_status` 데코레이터를 적용합니다. (스케줄러 대시보드 상태 전파 및 전용 로거 자동 주입 필수)
- 네트워크 등 재시도가 필요한 외부 작업인 경우, `@retry(**COMMON_RETRY_POLICY)` 데코레이터를 붙입니다.
- 함수 인자 끝에 `logger: Logger = None`를 정의하면, 별도의 로깅 설정 없이 격리된 로거가 매개변수로 자동 전달됩니다.
- 공통 인프라(DB 세션, SFTP 클라이언트) 등은 `core` SDK 인터페이스 패키지에서 간결히 임포트할 수 있습니다.

```python
# 예시: tasks/my_new_task.py 생성
from logging import Logger
from tenacity import retry
from core import track_task_status, COMMON_RETRY_POLICY, get_db_session

@track_task_status
@retry(**COMMON_RETRY_POLICY)
def hello_world_task(name: str, logger: Logger = None) -> str:
    try:
        logger.info(f"Hello, {name}! 작업을 실행합니다.")

        # 예시: DB 세션 사용 시
        with get_db_session() as session:
            # DB 작업 로직 작성
            pass

        return "Done"
    except Exception as e:
        logger.error(f"오류가 발생했습니다: {e}")
        raise # tenacity가 에러를 감지하고 재시도할 수 있도록 반드시 raise
```

#### 2단계: `config/tasks_config.json`에 스케줄 및 인자 추가

스케줄러가 구동되면 `tasks/` 폴더 내 정의된 모든 파이썬 함수를 **자동으로 탐색(Dynamic Discovery)**하여 바인딩하므로 코드 매핑 작업이 불필요합니다. 사용자는 [config/tasks_config.json]파일에 스케줄 정보만 명시하면 됩니다.

```json
[
    {
        "name": "hello_world_task",
        "cron": "*/5 * * * *",
        "args": ["Gildong"],
        "kwargs": {}
    }
]
```

#### 3단계: 가동

`config/env` 파일의 `USE_JSON_CONFIG=True`인지 확인 후 [main.py]를 실행하면 신규 등록한 함수가 자동으로 감지되고 기동됩니다.

---

### 방법 B: OS 명령어/셸 스크립트 태스크 등록

독립적으로 실행 가능한 외부 파이썬 스크립트, 배시/배치 스크립트, 또는 시스템 명령어를 주기적으로 호출하려면 이 방법을 사용합니다.

#### 1단계: `config/crontab_list.txt` 작성

[config/crontab_list.txt]파일을 열고, 일반 리눅스 크론탭과 동일한 규칙(5필드 주기 + 실행할 명령어)을 추가합니다.

```text
# 분 시 일 월 요일 [명령어]
*/5 * * * * python scripts/independent_job.py
*/10 * * * * echo "OS Command Task is Executed"
```

#### 2단계: 가동

`config/env` 파일의 `USE_TXT_CONFIG=True`로 설정을 변경한 다음 [main.py]를 실행합니다.

---

## 📊 실시간 모니터링 및 로깅 시스템

### 1. 실시간 모니터링 대시보드 (`logs/scheduler_status.txt`)

스케줄러 구동 시 백그라운드 스레드가 10초 주기로 모든 등록된 작업의 최신 상태 스냅샷을 텍스트 파일로 내보냅니다.
해당 텍스트 파일을 열어두면 작업의 성공/실패 여부, 재시도 카운트, 다음 스케줄 일시를 직관적으로 확인할 수 있습니다.

```text
=============================================================================================================================
--- [크론 모니터링 대시보드] 업데이트: 2026-07-20 12:33:13 ---
--------------------------------------------------------------------------------=============================================
ID (명령어/함수)                    | Cron 주기       | 최종 결과       | 마지막 실행     | 다음 실행
--------------------------------------------------------------------------------=============================================
sample_task                         | */1 * * * *     | 성공            | 07-20 12:33     | 2026-07-20 12:34
retry_task                          | */1 * * * *     | 실행중 (2회차)  | 12:33:10        | 2026-07-20 12:34
hello_world_task                    | */5 * * * *     | 성공            | 07-20 12:30     | 2026-07-20 12:35
python scripts/independent_job.py   | */5 * * * *     | 성공            | 07-20 12:30     | 2026-07-20 12:35
--------------------------------------------------------------------------------=============================================
=============================================================================================================================
```

### 2. 격리된 로깅 체계 (Isolated Logging System)

작업 간 로그가 한곳에 섞이지 않도록 로거 전파(`propagate`)를 차단하고 각 실행 주체별로 개별적인 일별 회전 로깅(Rotating)을 수행합니다.

- **스케줄러 핵심 로거**: `logs/SchedulerMain/{yyyy-mm-dd}.log`에 스케줄러 자체 초기화 및 작업 시작/정리 로그 기록.
- **Python 함수형 태스크 로거**: 각 태스크별로 완전히 독립된 폴더(`logs/{task_name}/{yyyy-mm-dd}.log`)에 로그 보관. (`core.utils.logger_setup` 모듈에 의해 자동 생성 및 분할 처리)
- **OS 명령어 로거**: `logs/cron/{safe_command_name}_{yyyy-mm-dd}.log`에 서브프로세스의 모든 터미널 출력(stdout/stderr) 기록.

---

## 📁 디렉토리 구조 (Directory Structure)

```text
📁 apscheduler_scheduler/
│
├── 📄 main.py                  # 스케줄러 메인 실행 파일 및 가볍게 환경 래핑한 기동 스키마
├── 📄 requirements.txt         # 프로젝트 의존성 패키지 정의 파일
├── 📄 README.md                # [현재 파일] 아키텍처 및 시스템 가이드라인 문서
├── 📄 .gitignore               # git 형상 관리 제외 대상 목록
│
├── 📁 core/                    # ⚙️ 공통 프로세싱 (Scheduler Core Framework)
│   ├── 📄 __init__.py          # SDK 포트폴리오 노출 스크립트 (외부 API 바인딩)
│   ├── 📄 engine.py            # 스케줄러 오케스트레이터 백그라운드 구동기
│   ├── 📄 config_loader.py     # JSON/txt 로드 및 Dynamic Task Discovery 지원 스크립트
│   ├── 📄 globals.py           # 실시간 상태 공유 리포지토리 (shared_stats)
│   │
│   ├── 📁 infrastructure/      # 🔌 공통 인프라 모듈
│   │   ├── 📄 __init__.py
│   │   ├── 📄 postgres.py      # PostgreSQL DB 세션 및 연결 유틸리티
│   │   └── 📄 sftp.py          # SFTP 클라이언트 연결 및 세션 유틸리티
│   │
│   └── 📁 utils/               # 🛠️ 유틸리티 모듈
│       ├── 📄 __init__.py
│       ├── 📄 retry_utils.py   # Tenacity 기반 공통 재시도 정책 및 상태 추적 데코레이터
│       ├── 📄 logger_setup.py  # 태스크별 격리된 로그 디렉토리 생성 및 로거 셋업
│       └── 📄 performance.py   # 수행 시간 측정을 위한 StopWatch 클래스
│
├── 📁 config/                  # 📝 사용자 영역: 설정 파일 (User Configuration)
│   ├── 📄 env                  # 운영 인프라 환경 변수 정의
│   ├── 📄 tasks_config.json    # 파이썬 태스크 주기(Cron) 및 인자 매핑 정의
│   └── 📄 crontab_list.txt     # 외부 OS/명령어 기반 크론 스크립트 리스트 정의
│
├── 📁 tasks/                   # 🏃 사용자 영역: 개별 배치 태스크 (User Custom Tasks)
│   ├── 📄 __init__.py          # 파이썬 모듈 초기화 파일
│   └── 📄 example.py           # 예제 태스크 및 대용량 ETL 파이프라인 예제 소스 파일
│
├── 📁 models/                  # 🗄️ 사용자 영역: 데이터베이스 모델 (User DB Models)
│   ├── 📄 __init__.py
│   └── 📄 models.py            # SQLAlchemy 2.0 기반 DB 테이블 스키마 정의
│
├── 📁 scripts/                 # 🐚 초기화 및 운영 유틸리티 스크립트 (Scripts)
│   ├── 📄 init.bat / init.sh   # 테스트 실행 목적의 OS 셸/배치 파일
│   ├── 📄 init_db.py           # 데이터베이스 테이블 및 체크포인트 레코드 초기화 파이썬 스크립트
│   └── 📄 generate.sql         # ETL 테스트를 위한 대용량 테이블 생성 및 Mock Data DDL SQL
│
├── 📁 logs/                    # 전체 로그 저장 디렉토리 (실행 시 자동 생성)
│   ├── 📄 scheduler_status.txt # 10초마다 실시간 자동 갱신되는 텍스트 모니터링 대시보드
│   ├── 📁 SchedulerMain/      # 스케줄러 자체 메인 시스템 로그
│   ├── 📁 cron/               # crontab_list.txt 기반 OS 명령어 실행 로그
│   └── 📁 {task_name}/        # 각 파이썬 태스크별 독립 로테이팅 실행 로그
│
└── 📁 export_data/                    # JSONL 백업 보관 및 SQLite 작업(Jobs) 저장 영역
```

---

## 📊 대용량 데이터 적재 성능 최적화 히스토리 (10만 건 기준)

기존의 순수 파이썬 루프와 ORM 매핑 방식의 병목을 완전히 해결하고, 데이터베이스 커널 엔진의 성능을 100% 이끌어내어 **약 20~30배 이상의 성능 향상**을 달성했습니다.

| 개발 단계        | 적용 핵심 기술                                  | 10만 건 처리 소요 시간         | 비고                            |
| :--------------- | :---------------------------------------------- | :----------------------------- | :------------------------------ |
| **초기 상태**    | 순수 파이썬 루프 + SQLAlchemy ORM               | 약 42초                        | ORM 오버헤드 발생               |
| **1차 튜닝**     | SQLAlchemy 우회 (Raw SQL `execute` 전송)        | 약 65초                        | 네트워크 무작위 I/O로 되려 증가 |
| **🔥 최종 단계** | **psycopg2 + `COPY` + 인메모리 Staging 테이블** | **1~2초 내외 (0.X초 대 완료)** | **최종 아키텍처 채택**          |

---

## 💡 주요 기술적 특징 및 데이터 정합성 메커니즘 (ETL 태스크 기준)

### 1. 무장애 장애 복구 (Fault-Tolerance) 및 체크포인트 제어

- `save_all_fields_to_jsonl` 및 `export_and_upload` 배치 기능은 `batch_checkpoints` 테이블을 기준으로 가동됩니다.
- 배치가 시작되는 시점의 `MAX(id)`를 상한선으로 명시 격리하여, 실시간 데이터가 유입되더라도 배치가 끝나지 않는 라이브 락(Live Lock) 현상을 차단합니다.
- 추출, SFTP 전송, 로컬-원격 파일 크기 교차 검증(`stat().st_size`)까지 완벽하게 성공한 경우에만 최종적으로 체크포인트를 전진(Update Commit)시킵니다. 작업 실패 시에는 불완전한 찌꺼기 파일을 원격/로컬에서 즉시 제거(Unlink)하여 데이터 오염을 예방합니다.

### 2. 메모리(RAM) 효율성 증대 및 OOM 방지

- 대용량 JSONL 파일을 다룰 때 파이썬 프로세스 내에 모든 데이터를 리스트로 탑재하면 Out-Of-Memory(OOM)가 발생합니다.
- 본 시스템은 `CopyStreamWrapper`라는 커스텀 I/O 스트림 래퍼 클래스를 구현하여, PostgreSQL 드라이버가 요청하는 청크 단위(`size`)대로 데이터를 한 줄씩 실시간으로 파싱 및 탭(Tab) 문자로 치환 가공하여 스트리밍 전달하므로 수천만 건의 데이터도 최소한의 메모리로 방어합니다.

### 3. MVCC 부하 완화 (차분 UPSERT)

- 무조건적인 `UPDATE` 처리는 데이터베이스 내부 튜플을 쓸데없이 복사하여 PG 카탈로그 및 디스크를 비대하게 만듭니다(Bloat 현상).
- `IS DISTINCT FROM` 비교 구문을 연산식에 결합하여, 실제 필드 값(`username`, `email`, `score`)에 실질적 변경 사항이 존재하는 행만 물리 데이터 영역을 갱신하도록 최적화되었습니다.

---

## 📄 라이선스 (License)

본 프로젝트는 [MIT License](https://www.google.com/search?q=LICENSE) 라이선스 하에 배포 및 사용이 가능합니다.
