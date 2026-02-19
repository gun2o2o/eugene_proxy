# -*- coding: utf-8 -*-
"""
Eugene OpenAPI TCP Client

64-bit 자동매매 프로그램에서 사용하는 thin client.
eugene_proxy.py (32-bit TCP proxy server) 에 접속하여
유진투자증권 Champion OpenAPI 기능을 호출합니다.

Requirements:
  - Python 3.8+ (64-bit or 32-bit 무관)
  - 별도 의존성 없음 (표준 라이브러리만 사용)

Usage:
    from client import EugeneClient

    client = EugeneClient("127.0.0.1", 5959)
    client.connect()

    # Heartbeat
    status = client.heartbeat()
    print(status)

    # TR 조회 (예: 국내주식 잔고)
    result = client.request_tr(
        tr_code="OTD3108Q",
        inputs={"ACNO": "계좌번호", "AC_PWD": "비밀번호", "CMSN_ICLN_YN": "N"},
        outputs={
            "OutRec1": ["RECNM", "AC_TDA", "ORD_ABLE_CSH"],
            "OutRec2": ["ITEM_COD", "ITEM_NM", "BNS_BAL_Q", "STK_CRPR", "EV_PL_A"],
        },
    )
    print(result)

    # 실시간 등록
    client.subscribe_real("21", "005930", ["LCPRICE", "LVOLUME", "LDIFF"])

    # 실시간 이벤트 수신 루프
    while True:
        event = client.recv_event(timeout=1.0)
        if event:
            print(event)

    client.disconnect()
"""

import json
import queue
import socket
import struct
import threading
import time


# ============================================================
#  TCP Framing Helpers (eugene_proxy.py 와 동일)
# ============================================================

def _send_msg(sock, obj):
    """JSON obj -> length-prefixed bytes -> socket send."""
    payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    header = struct.pack(">I", len(payload))
    sock.sendall(header + payload)


def _recv_msg(sock):
    """socket -> length-prefixed bytes -> JSON obj. Returns None on disconnect."""
    raw_header = _recv_exact(sock, 4)
    if raw_header is None:
        return None
    length = struct.unpack(">I", raw_header)[0]
    if length == 0:
        return None
    raw_payload = _recv_exact(sock, length)
    if raw_payload is None:
        return None
    return json.loads(raw_payload.decode("utf-8"))


def _recv_exact(sock, n):
    """Receive exactly n bytes. Returns None on disconnect."""
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


# ============================================================
#  Client
# ============================================================

class EugeneClient:
    """
    Eugene OpenAPI TCP Proxy Client.

    eugene_proxy.py 에 TCP 로 접속하여 JSON-RPC 스타일 요청을 보내고
    응답/이벤트를 수신합니다.

    Thread-safe: 내부 reader thread 가 응답/이벤트를 분류합니다.
    """

    def __init__(self, host="127.0.0.1", port=5959):
        self._host = host
        self._port = port
        self._sock = None
        self._connected = False

        # Request ID counter
        self._next_id = 1
        self._id_lock = threading.Lock()

        # Pending requests: id -> threading.Event + result container
        self._pending = {}  # id -> {"event": Event, "result": None}
        self._pending_lock = threading.Lock()

        # Event queue (real_data, agent_event 등)
        self._event_queue = queue.Queue()

        # Reader thread
        self._reader_thread = None
        self._running = False

    # ============================================================
    #  Connection
    # ============================================================

    def connect(self, timeout=5.0):
        """서버에 TCP 접속."""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(timeout)
        self._sock.connect((self._host, self._port))
        self._sock.settimeout(None)
        self._connected = True
        self._running = True

        # Start reader thread
        self._reader_thread = threading.Thread(
            target=self._reader_loop, daemon=True,
        )
        self._reader_thread.start()

    def disconnect(self):
        """접속 종료."""
        self._running = False
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        self._connected = False

    @property
    def is_connected(self):
        return self._connected

    # ============================================================
    #  Internal Reader
    # ============================================================

    def _reader_loop(self):
        """Background thread: read messages, route to pending or event queue."""
        while self._running:
            try:
                msg = _recv_msg(self._sock)
            except Exception:
                break
            if msg is None:
                break

            # Event push (no id)
            if "event" in msg:
                self._event_queue.put(msg)
                continue

            # Response (has id)
            msg_id = msg.get("id")
            if msg_id is not None:
                with self._pending_lock:
                    pending = self._pending.get(msg_id)
                if pending is not None:
                    pending["result"] = msg
                    pending["event"].set()
                continue

        self._connected = False

    # ============================================================
    #  Request / Response
    # ============================================================

    def _next_request_id(self):
        with self._id_lock:
            rid = self._next_id
            self._next_id += 1
        return rid

    def _call(self, method, params=None, timeout=10.0):
        """
        Send request and wait for response.
        Returns result dict on success, raises on error.
        """
        if not self._connected:
            raise ConnectionError("Not connected to server")

        req_id = self._next_request_id()
        event = threading.Event()

        with self._pending_lock:
            self._pending[req_id] = {"event": event, "result": None}

        # Send request
        msg = {"id": req_id, "method": method}
        if params:
            msg["params"] = params

        try:
            _send_msg(self._sock, msg)
        except OSError as e:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise ConnectionError(f"Send failed: {e}")

        # Wait for response
        if not event.wait(timeout=timeout):
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise TimeoutError(f"Request timeout ({timeout}s): method={method}")

        with self._pending_lock:
            result_msg = self._pending.pop(req_id)["result"]

        # Check for error
        if "error" in result_msg:
            err = result_msg["error"]
            raise RuntimeError(f"Server error [{err.get('code')}]: {err.get('message')}")

        return result_msg.get("result")

    # ============================================================
    #  Event Receiving
    # ============================================================

    def recv_event(self, timeout=None):
        """
        실시간 이벤트 수신.
        Returns event dict or None (timeout).
        """
        try:
            return self._event_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def recv_event_nowait(self):
        """실시간 이벤트 비블로킹 수신. 없으면 None."""
        try:
            return self._event_queue.get_nowait()
        except queue.Empty:
            return None

    # ============================================================
    #  Public API Methods
    # ============================================================

    def heartbeat(self, timeout=5.0):
        """
        서버 상태 확인.
        Returns: {"server_running": bool, "api_connected": bool,
                  "logged_in": bool, "uptime": float}
        """
        return self._call("heartbeat", timeout=timeout)

    def request_tr(self, tr_code, inputs, outputs, next_key="", request_cnt=20, timeout=10.0):
        """
        TR 조회 요청 (Bundled).
        
        Args:
            tr_code: TR 코드 (예: "OTD3108Q")
            inputs: 입력 필드 dict (예: {"ACNO": "...", "AC_PWD": "..."})
            outputs: 출력 필드 dict (예: {"OutRec1": [...], "OutRec2": [...]})
            next_key: 연속조회 키 (기본 "")
            request_cnt: 요청 건수 (기본 20)
            timeout: 응답 대기 시간 (초)

        Returns: {"OutRec1": {field: value}, "OutRec2": [{field: value}, ...]}
        """
        return self._call("request_tr", {
            "tr_code": tr_code,
            "inputs": inputs,
            "outputs": outputs,
            "next_key": next_key,
            "request_cnt": request_cnt,
        }, timeout=timeout)

    def subscribe_real(self, real_id, real_key, fields, timeout=5.0):
        """
        실시간 데이터 등록.

        Args:
            real_id: Real ID (예: "21" = 국내주식)
            real_key: 종목코드
            fields: 수신할 필드 목록

        이후 recv_event() 로 실시간 데이터를 수신합니다.
        """
        return self._call("subscribe_real", {
            "real_id": real_id,
            "real_key": real_key,
            "fields": fields,
        }, timeout=timeout)

    def unsubscribe_real(self, real_id, real_key, timeout=5.0):
        """실시간 데이터 해제."""
        return self._call("unsubscribe_real", {
            "real_id": real_id,
            "real_key": real_key,
        }, timeout=timeout)

    def unsubscribe_all(self, timeout=5.0):
        """모든 실시간 데이터 해제."""
        return self._call("unsubscribe_all", timeout=timeout)

    def get_accounts(self, timeout=5.0):
        """
        계좌 목록 조회.
        Returns: {"accounts": ["acc1", "acc2", ...], "count": int}
        """
        return self._call("get_accounts", timeout=timeout)

    def get_login_state(self, timeout=5.0):
        """
        로그인 상태 확인.
        Returns: {"state": 0|1}
        """
        return self._call("get_login_state", timeout=timeout)

    def get_last_err_msg(self, timeout=5.0):
        """
        마지막 에러 메시지.
        Returns: {"message": str}
        """
        return self._call("get_last_err_msg", timeout=timeout)

    def get_exp_code(self, code, timeout=5.0):
        """단축코드 -> 표준코드."""
        return self._call("get_exp_code", {"code": code}, timeout=timeout)

    def get_sh_code(self, code, timeout=5.0):
        """표준코드 -> 단축코드."""
        return self._call("get_sh_code", {"code": code}, timeout=timeout)

    def get_name_by_code(self, code, timeout=5.0):
        """종목코드 -> 종목명."""
        return self._call("get_name_by_code", {"code": code}, timeout=timeout)

    def get_sh_code_by_name(self, name, timeout=5.0):
        """종목명 -> 단축코드."""
        return self._call("get_sh_code_by_name", {"name": name}, timeout=timeout)

    def get_market_kubun(self, code, timeout=5.0):
        """종목코드 시장 구분."""
        return self._call("get_market_kubun", {"code": code}, timeout=timeout)

    def logout(self, timeout=5.0):
        """로그아웃."""
        return self._call("logout", timeout=timeout)

    def shutdown(self, timeout=5.0):
        """서버 종료 요청."""
        result = self._call("shutdown", timeout=timeout)
        self.disconnect()
        return result
