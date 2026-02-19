# -*- coding: utf-8 -*-
"""
Eugene OpenAPI 초기 환경 설정 스크립트

처음 컴퓨터에서 실행할 때 1회만 실행하면 됩니다.
Conda 32-bit 가상환경을 생성합니다.

사용법:
  python init.py

주의: 외부 패키지를 사용하지 않습니다 (stdlib only).
"""

import os
import shutil
import subprocess
import sys


# ============================================================
#  Constants
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_YML = os.path.join(SCRIPT_DIR, "environment.yml")

CONDA_ENV_NAME = "eugene32"


# ============================================================
#  Helpers
# ============================================================

def print_header(title):
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_ok(msg):
    print(f"  [OK] {msg}")


def print_fail(msg):
    print(f"  [FAIL] {msg}")


def print_skip(msg):
    print(f"  [SKIP] {msg}")


def print_info(msg):
    print(f"  [INFO] {msg}")


def find_conda():
    """conda 실행 파일 경로 탐색. 없으면 None."""
    conda_path = shutil.which("conda")
    if conda_path:
        return conda_path

    user_home = os.path.expanduser("~")
    candidates = [
        os.path.join(user_home, "miniconda3", "Scripts", "conda.exe"),
        os.path.join(user_home, "anaconda3", "Scripts", "conda.exe"),
        os.path.join(user_home, "Miniconda3", "Scripts", "conda.exe"),
        os.path.join(user_home, "Anaconda3", "Scripts", "conda.exe"),
        r"C:\ProgramData\miniconda3\Scripts\conda.exe",
        r"C:\ProgramData\Miniconda3\Scripts\conda.exe",
        r"C:\ProgramData\anaconda3\Scripts\conda.exe",
        r"C:\ProgramData\Anaconda3\Scripts\conda.exe",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


def conda_env_exists(conda_path, env_name):
    """conda 환경이 이미 존재하는지 확인."""
    try:
        result = subprocess.run(
            [conda_path, "env", "list"],
            capture_output=True, text=True, timeout=30,
        )
        for line in result.stdout.splitlines():
            parts = line.split()
            if parts and parts[0] == env_name:
                return True
    except Exception:
        pass
    return False


# ============================================================
#  Conda Environment Setup
# ============================================================

def step_conda_env():
    """Conda 32-bit 환경 생성."""
    print_header("Conda 32-bit 환경 설정")

    conda_path = find_conda()
    if not conda_path:
        print_fail("conda 를 찾을 수 없습니다.")
        print_info("Miniconda 또는 Anaconda 를 설치해주세요.")
        print_info("  다운로드: https://docs.conda.io/en/latest/miniconda.html")
        return False

    print_info(f"conda 경로: {conda_path}")

    if not os.path.isfile(ENV_YML):
        print_fail(f"environment.yml 을 찾을 수 없습니다: {ENV_YML}")
        return False

    if conda_env_exists(conda_path, CONDA_ENV_NAME):
        print_skip(f"'{CONDA_ENV_NAME}' 환경이 이미 존재합니다.")
        print_info("재생성하려면 먼저 삭제하세요:")
        print_info(f"  conda env remove -n {CONDA_ENV_NAME}")
        return True

    print_info(f"'{CONDA_ENV_NAME}' 32-bit 환경을 생성합니다...")
    print_info("(시간이 좀 걸릴 수 있습니다)")
    print()

    env = os.environ.copy()
    env["CONDA_SUBDIR"] = "win-32"

    try:
        result = subprocess.run(
            [conda_path, "env", "create", "-f", ENV_YML],
            env=env,
            timeout=600,
        )
        if result.returncode == 0:
            print()
            print_ok(f"'{CONDA_ENV_NAME}' 환경 생성 완료")

            print_info("32-bit 서브디렉토리 고정 설정 중...")
            subprocess.run(
                [conda_path, "config", "--env", "--set", "subdir", "win-32"],
                env={**env, "CONDA_DEFAULT_ENV": CONDA_ENV_NAME},
                timeout=30,
            )
            print_ok("subdir=win-32 고정 완료")
            return True
        else:
            print()
            print_fail(f"환경 생성 실패 (exit code: {result.returncode})")
            return False
    except subprocess.TimeoutExpired:
        print_fail("환경 생성 시간 초과 (10분)")
        return False
    except Exception as e:
        print_fail(f"환경 생성 오류: {e}")
        return False


# ============================================================
#  Main
# ============================================================

def main():
    print("=" * 60)
    print("  Eugene OpenAPI — 초기 환경 설정")
    print("=" * 60)
    print()
    print("이 스크립트는 처음 1회만 실행하면 됩니다.")

    success = step_conda_env()

    print()
    if success:
        print("  설정이 완료되었습니다!")
        print()
        print("  다음 단계:")
        print("    1. setting.ini 에 로그인 정보를 입력하세요")
        print("    2. eugene_proxy.py 를 실행하세요:")
        print(f"       conda activate {CONDA_ENV_NAME}")
        print(f"       python eugene_proxy.py")
    else:
        print("  설정이 실패했습니다. 위 로그를 확인해주세요.")

    print()
    input("Enter 를 눌러 종료...")


if __name__ == "__main__":
    main()
