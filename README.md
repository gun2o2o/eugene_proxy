# eugene_proxy

유진투자증권 Champion OpenAPI를 64-bit 환경에서 사용할 수 있도록 하는 TCP 프록시 서버입니다.

Champion OpenAPI COM 컨트롤(32-bit 전용)을 32-bit Python 프로세스에서 구동하고, 64-bit 클라이언트와 TCP/JSON 프로토콜로 통신합니다. 유진 OpenAPI가 지원하는 모든 TR 조회, 실시간 데이터, 주문 기능을 그대로 사용할 수 있습니다.

> **이 프로젝트는 유진투자증권 주식회사와 어떠한 관련도 없는 비공식 프로젝트입니다.**

## 구조

```
[64-bit 환경]                           [32-bit conda 환경]
자동매매 프로그램                         eugene_proxy.py (TCP server)
    |                                       |
client.py  <--- TCP/JSON --->  COM dynamicCall 실행
 (thin client)  localhost:5959     (PyQt5 event loop)
                                            |
                                   ChampionCommAgent.ocx
```

## 파일 구성

| 파일 | 설명 | 실행 환경 |
|------|------|-----------|
| `eugene_proxy.py` | TCP proxy server (메인) | 32-bit conda |
| `client.py` | TCP client (자동매매측에서 import) | 아무 환경 |
| `mock_server.py` | 가상 테스트 서버 (GUI, proxy 없이 테스트) | 아무 환경 |
| `init.py` | 최초 환경 설정 (conda 32-bit) | 아무 환경 |
| `setting.ini` | 설정 (인증정보, 서버, 옵션) | - |
| `environment.yml` | conda 환경 스펙 | - |
| `edgecase.md` | 엣지 케이스 및 주의사항 | - |

## 사전 준비

1. **Champion OpenAPI 설치**: 유진투자증권 홈페이지에서 Champion OpenAPI 설치
2. **환경 설정** (최초 1회):
   ```bash
   python init.py
   ```
   또는 수동 설정:
   ```bash
   set CONDA_SUBDIR=win-32
   conda env create -f environment.yml
   conda activate eugene32
   ```
3. **setting.ini 수정**: 본인의 ID/PW/인증서 비밀번호 입력

## 실행 방법

### 1단계: 프록시 서버 실행 (32-bit, 관리자 권한)

```bash
conda activate eugene32
python eugene_proxy.py
```

> 관리자 권한이 필요합니다. CMD를 "관리자 권한으로 실행" 후 실행하세요.

로그에 `Server ready. Entering event loop...` 가 뜨면 준비 완료.

### 2단계: 클라이언트 사용 (64-bit)

```python
from client import EugeneClient

client = EugeneClient("127.0.0.1", 5959)
client.connect()

# 서버 상태 확인
print(client.heartbeat())
```

## 사용 예시

### 국내주식 잔고 조회

```python
result = client.request_tr(
    tr_code="OTD3108Q",
    inputs={"ACNO": "계좌번호", "AC_PWD": "비밀번호", "CMSN_ICLN_YN": "N"},
    outputs={
        "OutRec1": ["RECNM", "AC_TDA", "ORD_ABLE_CSH", "AM_BAL_A"],
        "OutRec2": ["ITEM_COD", "ITEM_NM", "BNS_BAL_Q", "STK_CRPR", "EV_PL_A"],
    },
)
```

### 해외주식 잔고 조회

```python
result = client.request_tr(
    tr_code="OTD6209Q",
    inputs={"ACNO": "계좌번호", "AC_PWD": "비밀번호"},
    outputs={
        "OutRec1": ["RECNM"],
        "OutRec2": ["ITEM_NM", "HLDG_Q", "BUY_UPR", "FRGN_STK_CLPR", "EV_PL_SUM_A"],
    },
)
```

### 국내주식 매수

```python
result = client.request_tr(
    tr_code="OTD1101U",
    inputs={
        "ACNO": "계좌번호",
        "AC_PWD": "비밀번호",
        "ITEM_COD": "005930",
        "ORD_Q": "10",
        "STK_BD_ORD_UPR": "0",
        "BUY_SEL_TR_TCD": "20",
        "ORD_BNS_TCD": "020",
        "CRDTR_TCD": "000",
        "LN_DT": "",
        "ORD_COND_TCD": "0",
    },
    outputs={"OutRec1": ["ORD_NO"]},
)
```

### 실시간 틱 수신

```python
client.subscribe_real("21", "005930", ["LCPRICE", "LVOLUME", "LDIFF"])

while True:
    event = client.recv_event(timeout=1.0)
    if event and event["event"] == "real_data":
        fields = event["data"]["fields"]
        print(f"현재가: {fields['LCPRICE']}, 거래량: {fields['LVOLUME']}")

client.unsubscribe_real("21", "005930")
```

## 지원 메서드

| 메서드 | 설명 |
|--------|------|
| `request_tr` | 범용 TR 조회 (모든 TR 코드 사용 가능) |
| `subscribe_real` | 실시간 데이터 등록 |
| `unsubscribe_real` | 실시간 데이터 해제 |
| `unsubscribe_all` | 모든 실시간 해제 |
| `heartbeat` | 서버 상태 확인 |
| `get_accounts` | 계좌 목록 |
| `get_login_state` | 로그인 상태 |
| `get_last_err_msg` | 마지막 에러 |
| `get_exp_code` | 단축코드 → 표준코드 |
| `get_sh_code` | 표준코드 → 단축코드 |
| `get_name_by_code` | 코드 → 종목명 |
| `get_sh_code_by_name` | 종목명 → 코드 |
| `get_market_kubun` | 시장 구분 |
| `logout` | 로그아웃 |
| `shutdown` | 서버 종료 |

## 주의사항

- **관리자 권한**으로 실행해야 합니다.
- **TR 코드와 필드명은 유진 OpenAPI 공식 문서를 참조**하세요. 이 프록시는 모든 TR을 그대로 전달합니다.
- **초당 TR 전송 제한**이 있으므로 빈번한 조회 시 적절한 간격을 두세요.
- **모의투자 환경에서 먼저 테스트**하는 것을 권장합니다.
- **동시 클라이언트 미지원**: 현재 1개 클라이언트만 접속 가능합니다.

## 면책 조항

이 프로젝트는 유진투자증권 주식회사의 공식 제품이 아니며, 유진투자증권과 어떠한 제휴, 보증, 후원 관계도 없습니다. Champion OpenAPI, ChampionCommAgent.ocx 및 관련 소프트웨어의 모든 저작권과 지적재산권은 유진투자증권 주식회사에 있습니다. 이 프로젝트의 사용으로 인해 발생하는 모든 책임은 사용자 본인에게 있으며, 금전적 손실을 포함한 어떠한 손해에 대해서도 개발자는 책임을 지지 않습니다.
