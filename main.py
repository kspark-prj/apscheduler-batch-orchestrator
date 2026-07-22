import os
import sys
from dotenv import load_dotenv

# 1. 실행 경로 설정 및 sys.path 동기화
root_dir = os.path.dirname(os.path.abspath(__file__))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

os.chdir(root_dir)

# 2. .env 환경 설정 파일 자동 로드 (config/env 우선, 루트 env 및 .env 차례로 폴백 지원)
config_env = os.path.join(root_dir, "config", "env")
if os.path.exists(config_env):
    load_dotenv(dotenv_path=config_env)
else:
    root_env = os.path.join(root_dir, "env")
    if os.path.exists(root_env):
        load_dotenv(dotenv_path=root_env)
    else:
        load_dotenv(dotenv_path=os.path.join(root_dir, ".env"))

# 3. 코어 오케스트레이션 엔진 실행
from core.engine import main

if __name__ == "__main__":
    main()
