# -*- coding: utf-8 -*-
"""
Eugene OpenAPI Mock TCP Server — GUI Version  (mock_server.py)

eugene_proxy.py 없이 클라이언트를 테스트할 수 있는 가상 서버.
tkinter 기반 인터페이스로 실시간 로그 확인, 보유종목 관리,
가상 종목 생성, 가격 조작이 가능합니다.

Architecture:
  - Main thread: tkinter mainloop
  - TCP listener thread: socket.accept() -> single client
  - TCP reader thread: reads client messages -> request_queue
  - Dispatcher thread: dequeues requests -> returns mock data
  - Per-symbol tick threads: simulated real-time price push
  - Periodic UI refresh: root.after() based updates

Requirements:
  - Python 3.8+ (any platform, any bitness)
  - Standard library only — no 3rd-party packages
  - tkinter (included in standard Python distribution)
"""

import configparser
import io
import json
import logging
import os
import queue
import random
import signal
import socket
import struct
import sys
import threading
import time
import tkinter as tk
import tkinter.ttk as ttk
import tkinter.messagebox as messagebox
import urllib.error
import urllib.request

# Windows 콘솔 UTF-8 출력 보장
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


# ============================================================
#  Config
# ============================================================

def load_config(ini_path):
    """setting.ini 로드. 없으면 RuntimeError."""
    if not os.path.isfile(ini_path):
        raise RuntimeError(f"Config not found: {ini_path}")
    cfg = configparser.ConfigParser()
    cfg.read(ini_path, encoding="utf-8")
    return cfg


# ============================================================
#  TCP Framing Helpers
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
#  Backend Log Handler
# ============================================================

class BackendLogHandler(logging.Handler):
    """
    로그를 백엔드 서버로 비동기 배치 HTTP POST 전송.

    - emit()은 큐에 넣고 즉시 반환 (메인 스레드 블로킹 없음)
    - 별도 데몬 스레드가 flush_interval 간격으로 큐를 소비하여 배치 전송
    - 전송 실패 시 조용히 무시 (콘솔 로그는 기존대로 동작)
    """

    def __init__(self, backend_url, flush_interval=1.0, max_batch=50):
        super().__init__()
        self.backend_url = backend_url
        self.flush_interval = flush_interval
        self.max_batch = max_batch
        self._queue = queue.Queue(maxsize=5000)
        self._closing = False
        self._thread = threading.Thread(target=self._sender_loop, daemon=True)
        self._thread.start()

    def emit(self, record):
        if self._closing:
            return
        try:
            log_entry = {
                "timestamp": time.strftime(
                    "%Y-%m-%dT%H:%M:%S", time.localtime(record.created),
                ),
                "level": record.levelname,
                "message": record.getMessage(),
                "source": "proxy",
            }
            self._queue.put_nowait(log_entry)
        except queue.Full:
            pass  # 큐 가득 차면 최신 로그 드랍

    def _sender_loop(self):
        """큐에서 배치로 꺼내 HTTP POST 전송."""
        while not self._closing:
            batch = []
            # 첫 아이템 대기 (timeout = flush_interval)
            try:
                item = self._queue.get(timeout=self.flush_interval)
                batch.append(item)
            except queue.Empty:
                continue
            # 나머지 drain (non-blocking)
            while len(batch) < self.max_batch:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            self._post_batch(batch)

    def _post_batch(self, batch):
        """HTTP POST 전송. 실패 시 무시."""
        try:
            payload = json.dumps(
                {"logs": batch}, ensure_ascii=False,
            ).encode("utf-8")
            req = urllib.request.Request(
                self.backend_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass

    def close(self):
        self._closing = True
        super().close()


# ============================================================
#  Mock Data — Symbol Prices & Holdings
# ============================================================

# Base prices for known symbols (realistic as of early 2026)
SYMBOL_PRICES = {
    "AAPL": 195.20,
    "NVDA": 892.15,
    "TSLA": 238.40,
    "MSFT": 420.50,
    "GOOGL": 175.30,
    "AMZN": 198.60,
    "META": 510.25,
    "AMD": 168.40,
    "NFLX": 625.80,
    "SPY": 530.15,
}

# Symbol name mapping
SYMBOL_NAMES = {
    "AAPL": "APPLE INC",
    "NVDA": "NVIDIA CORP",
    "TSLA": "TESLA INC",
    "MSFT": "MICROSOFT CORP",
    "GOOGL": "ALPHABET INC-CL A",
    "AMZN": "AMAZON.COM INC",
    "META": "META PLATFORMS INC",
    "AMD": "ADVANCED MICRO DEVICES",
    "NFLX": "NETFLIX INC",
    "SPY": "SPDR S&P 500 ETF TR",
}

# Dummy accounts
MOCK_ACCOUNTS = ["12345678901", "12345678902"]

# Dummy holdings for OTD6209Q
MOCK_HOLDINGS = [
    {
        "EXG_COD": "020", "ITEM_COD": "AAPL", "ITEM_NM": "APPLE INC",
        "HLDG_Q": "25", "SEL_ABLE_Q": "25", "BUY_UPR": "189.50",
        "FRGN_STK_CLPR": "195.20", "FRGN_STK_MKT_TCD": "01",
        "ERN_R": "3.01", "EV_PL_SUM_A": "142.50", "BNS_BAL_EA": "4880.00",
    },
    {
        "EXG_COD": "020", "ITEM_COD": "NVDA", "ITEM_NM": "NVIDIA CORP",
        "HLDG_Q": "10", "SEL_ABLE_Q": "10", "BUY_UPR": "875.30",
        "FRGN_STK_CLPR": "892.15", "FRGN_STK_MKT_TCD": "01",
        "ERN_R": "1.92", "EV_PL_SUM_A": "168.50", "BNS_BAL_EA": "8921.50",
    },
    {
        "EXG_COD": "020", "ITEM_COD": "TSLA", "ITEM_NM": "TESLA INC",
        "HLDG_Q": "15", "SEL_ABLE_Q": "15", "BUY_UPR": "245.80",
        "FRGN_STK_CLPR": "238.40", "FRGN_STK_MKT_TCD": "01",
        "ERN_R": "-3.01", "EV_PL_SUM_A": "-111.00", "BNS_BAL_EA": "3576.00",
    },
]


class PriceHistory:
    """Track recent price history for chart rendering (thread-safe)."""

    def __init__(self, max_points=120):
        self.max_points = max_points
        self._data = {}   # symbol -> list of (timestamp, price)
        self._lock = threading.Lock()

    def record(self, symbol, price):
        """Append a price point for the given symbol."""
        with self._lock:
            if symbol not in self._data:
                self._data[symbol] = []
            self._data[symbol].append((time.time(), price))
            if len(self._data[symbol]) > self.max_points:
                self._data[symbol] = self._data[symbol][-self.max_points:]

    def get(self, symbol):
        """Return a copy of the price history for the given symbol."""
        with self._lock:
            return list(self._data.get(symbol, []))


def _parse_symbol_from_scode(scode):
    """
    SCODE에서 심볼 추출.
    SCODE = 거래소코드(4자리) + 심볼(16자리, 공백패딩)
    예: "0537AAPL            " -> ("0537", "AAPL")
    """
    if not scode or len(scode) < 5:
        return "", ""
    exg = scode[:4]
    symbol = scode[4:].strip()
    return exg, symbol


def _get_base_price(symbol):
    """심볼의 기본 가격. 미등록 심볼은 50~500 사이 랜덤."""
    return SYMBOL_PRICES.get(symbol, round(random.uniform(50.0, 500.0), 2))


def _get_symbol_name(symbol):
    """심볼의 이름. 미등록이면 심볼명 자체 반환."""
    return SYMBOL_NAMES.get(symbol, symbol)


# ============================================================
#  Tkinter Log Handler
# ============================================================

class TkTextHandler(logging.Handler):
    """
    Thread-safe logging handler that writes to a tkinter Text widget.

    Uses root.after() to schedule GUI updates from any thread.
    Auto-scrolls to bottom and trims to max_lines when exceeded.
    """

    MAX_LINES = 5000

    def __init__(self, text_widget, root):
        super().__init__()
        self._text = text_widget
        self._root = root
        self._pending = queue.Queue(maxsize=10000)
        self._scheduled = False

    def emit(self, record):
        try:
            msg = self.format(record) + "\n"
            self._pending.put_nowait(msg)
            # Schedule a flush on the main thread if not already scheduled
            if not self._scheduled:
                self._scheduled = True
                try:
                    self._root.after(50, self._flush)
                except RuntimeError:
                    # root destroyed
                    pass
        except Exception:
            self.handleError(record)

    def _flush(self):
        """Drain pending messages into the Text widget (runs on main thread)."""
        self._scheduled = False
        text = self._text
        try:
            text.config(state=tk.NORMAL)
        except tk.TclError:
            return

        batch = []
        while True:
            try:
                batch.append(self._pending.get_nowait())
            except queue.Empty:
                break
            if len(batch) >= 200:
                break

        if not batch:
            try:
                text.config(state=tk.DISABLED)
            except tk.TclError:
                pass
            return

        for msg in batch:
            try:
                # Color-code based on log level
                tag = None
                if " [ERROR] " in msg or " [CRITICAL] " in msg:
                    tag = "error"
                elif " [WARNING] " in msg:
                    tag = "warning"
                elif " [DEBUG] " in msg:
                    tag = "debug"
                else:
                    tag = "info"

                text.insert(tk.END, msg, tag)
            except tk.TclError:
                return

        # Auto-trim old lines
        try:
            line_count = int(text.index("end-1c").split(".")[0])
            if line_count > self.MAX_LINES:
                trim = line_count - self.MAX_LINES
                text.delete("1.0", f"{trim}.0")
        except (tk.TclError, ValueError):
            pass

        try:
            text.config(state=tk.DISABLED)
            text.see(tk.END)
        except tk.TclError:
            pass

        # If there are still pending messages, schedule another flush
        if not self._pending.empty():
            self._scheduled = True
            try:
                self._root.after(50, self._flush)
            except RuntimeError:
                pass


# ============================================================
#  Mock Proxy Server
# ============================================================

class EugeneMockServer:
    """
    TCP mock server mimicking proxy.py protocol.
    Returns dummy data for all methods. No COM, no PyQt5, no admin.
    """

    def __init__(self, cfg):
        self._cfg = cfg
        self._start_time = time.time()

        # Server
        self._host = cfg.get("server", "host", fallback="127.0.0.1")
        self._port = cfg.getint("server", "port", fallback=5959)
        self._api_key = cfg.get("server", "api_key", fallback="")

        # State
        self._logged_in = True
        self._api_connected = True
        self._shutting_down = False

        # Real-time subscriptions: {real_key: {stop_event, thread, ...}}
        self._real_subs = {}
        self._real_subs_lock = threading.Lock()

        # Per-symbol live prices (shared across ticks)
        self._live_prices = {}
        self._live_prices_lock = threading.Lock()

        # Thread-safe structures
        self._request_queue = queue.Queue()
        self._send_lock = threading.Lock()

        # TCP
        self._server_sock = None
        self._client_sock = None
        self._listener_thread = None
        self._reader_thread = None
        self._dispatcher_thread = None

        # Backend heartbeat
        self._heartbeat_url = None
        self._heartbeat_thread = None
        self._heartbeat_stop = threading.Event()

        # Shutdown event
        self._shutdown_event = threading.Event()

        # Method dispatch table
        self._dispatch = {
            "request_tr": self._handle_request_tr,
            "subscribe_real": self._handle_subscribe_real,
            "unsubscribe_real": self._handle_unsubscribe_real,
            "unsubscribe_all": self._handle_unsubscribe_all,
            "unsubscribe_all_real": self._handle_unsubscribe_all,
            "get_accounts": self._handle_get_accounts,
            "heartbeat": self._handle_heartbeat,
            "get_login_state": self._handle_get_login_state,
            "get_last_err_msg": self._handle_get_last_err_msg,
            "get_exp_code": self._handle_get_exp_code,
            "get_sh_code": self._handle_get_sh_code,
            "get_name_by_code": self._handle_get_name_by_code,
            "get_sh_code_by_name": self._handle_get_sh_code_by_name,
            "get_market_kubun": self._handle_get_market_kubun,
            "logout": self._handle_logout,
            "shutdown": self._handle_shutdown,
            "restart": self._handle_restart,
        }

        # TR dispatch table
        self._tr_dispatch = {
            "OCA1725Q": self._mock_tr_oca1725q,
            "OTD6209Q": self._mock_tr_otd6209q,
            "OTD6224Q": self._mock_tr_otd6224q,
            "OTD6238Q": self._mock_tr_otd6238q,
            "OTD6101U": self._mock_tr_otd6101u,
            "OTD6103U": self._mock_tr_otd6103u,
            "gbpbid": self._mock_tr_gbpbid,
            "gbmst": self._mock_tr_gbmst,
            "OTD3108Q": self._mock_tr_otd3108q,
        }

        # Price history tracker for charts
        self._price_history = PriceHistory()

    # ============================================================
    #  TCP Server
    # ============================================================

    def start_tcp(self):
        """Bind TCP server and start listener thread."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self._host, self._port))
        self._server_sock.listen(1)
        logging.info("TCP server listening on %s:%d", self._host, self._port)

        self._listener_thread = threading.Thread(
            target=self._tcp_listener, daemon=True,
        )
        self._listener_thread.start()

    def _tcp_listener(self):
        """Accept ONE client connection with API key auth, then start reader thread."""
        while not self._shutting_down:
            srv = self._server_sock
            if srv is None:
                break
            try:
                srv.settimeout(1.0)
                client, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            logging.info("Client connected: %s", addr)

            # API key authentication
            if self._api_key:
                try:
                    client.settimeout(5.0)
                    auth_msg = _recv_msg(client)
                    client.settimeout(None)

                    if (auth_msg is None
                            or auth_msg.get("method") != "auth"
                            or auth_msg.get("params", {}).get("api_key") != self._api_key):
                        logging.warning("Client auth failed from %s", addr)
                        try:
                            _send_msg(client, {
                                "id": auth_msg.get("id") if auth_msg else None,
                                "error": {"code": -401, "message": "Invalid API key"},
                            })
                        except OSError:
                            pass
                        client.close()
                        continue

                    _send_msg(client, {
                        "id": auth_msg.get("id"),
                        "result": {"status": "authenticated"},
                    })
                    logging.info("Client authenticated: %s", addr)
                except Exception as e:
                    logging.warning("Client auth error from %s: %s", addr, e)
                    try:
                        client.close()
                    except OSError:
                        pass
                    continue

            # Close previous client if any
            self._close_client()
            self._client_sock = client

            self._reader_thread = threading.Thread(
                target=self._tcp_reader, daemon=True,
            )
            self._reader_thread.start()

    def _tcp_reader(self):
        """Read messages from client and put on request queue."""
        sock = self._client_sock
        while not self._shutting_down and sock is not None:
            try:
                msg = _recv_msg(sock)
            except Exception as e:
                logging.error("TCP read error: %s", e)
                break
            if msg is None:
                logging.info("Client disconnected")
                break
            logging.debug("Received: %s", msg)
            self._request_queue.put(msg)

        self._close_client()

    def _close_client(self):
        """Close client socket safely."""
        sock = self._client_sock
        if sock is not None:
            self._client_sock = None
            # Unsubscribe all real-time on disconnect
            self._unsubscribe_all_real()
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def _send_to_client(self, obj):
        """Thread-safe send to client."""
        sock = self._client_sock
        if sock is None:
            return
        with self._send_lock:
            try:
                _send_msg(sock, obj)
            except OSError as e:
                logging.error("TCP send error: %s", e)
                self._close_client()

    # ============================================================
    #  Dispatcher Thread
    # ============================================================

    def start_dispatcher(self):
        """Start dispatcher thread to process request queue."""
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop, daemon=True,
        )
        self._dispatcher_thread.start()

    def _dispatch_loop(self):
        """Dequeue and dispatch requests."""
        while not self._shutting_down:
            try:
                msg = self._request_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            self._dispatch_request(msg)

    def _dispatch_request(self, msg):
        """Route request to handler."""
        req_id = msg.get("id")
        method = msg.get("method")
        params = msg.get("params", {})

        if method is None:
            self._send_error(req_id, -1, "Missing 'method' field")
            return

        handler = self._dispatch.get(method)
        if handler is None:
            self._send_error(req_id, -2, f"Unknown method: {method}")
            return

        logging.debug("Dispatching: method=%s id=%s", method, req_id)
        try:
            handler(req_id, params)
        except Exception as e:
            logging.exception("Handler error: method=%s", method)
            self._send_error(req_id, -99, str(e))

    # ============================================================
    #  Response Helpers
    # ============================================================

    def _send_result(self, req_id, result):
        """Send success response."""
        self._send_to_client({"id": req_id, "result": result})

    def _send_error(self, req_id, code, message):
        """Send error response."""
        self._send_to_client({
            "id": req_id,
            "error": {"code": code, "message": message},
        })

    def _send_event(self, event_type, data):
        """Send event push (no id)."""
        self._send_to_client({"event": event_type, "data": data})

    # ============================================================
    #  Method Handlers
    # ============================================================

    def _handle_heartbeat(self, req_id, _params):
        """Server status check."""
        uptime = time.time() - self._start_time
        self._send_result(req_id, {
            "server_running": True,
            "api_connected": self._api_connected,
            "logged_in": self._logged_in,
            "uptime": round(uptime, 2),
        })

    def _handle_get_accounts(self, req_id, _params):
        """Get account list."""
        self._send_result(req_id, {
            "accounts": MOCK_ACCOUNTS,
            "count": len(MOCK_ACCOUNTS),
        })

    def _handle_get_login_state(self, req_id, _params):
        """Check login state."""
        self._send_result(req_id, {"state": 1 if self._logged_in else 0})

    def _handle_get_last_err_msg(self, req_id, _params):
        """Get last error message."""
        self._send_result(req_id, {"message": ""})

    def _handle_get_exp_code(self, req_id, params):
        """Short code -> standard code."""
        code = params.get("code", "")
        self._send_result(req_id, {"code": code})

    def _handle_get_sh_code(self, req_id, params):
        """Standard code -> short code."""
        code = params.get("code", "")
        self._send_result(req_id, {"code": code})

    def _handle_get_name_by_code(self, req_id, params):
        """Code -> name."""
        code = params.get("code", "")
        # Try to extract symbol from SCODE-style code
        _, symbol = _parse_symbol_from_scode(code)
        name = _get_symbol_name(symbol) if symbol else code
        self._send_result(req_id, {"name": name})

    def _handle_get_sh_code_by_name(self, req_id, params):
        """Name -> short code."""
        name = params.get("name", "")
        # Reverse lookup
        for sym, nm in SYMBOL_NAMES.items():
            if nm.upper() == name.upper():
                self._send_result(req_id, {"code": sym})
                return
        self._send_result(req_id, {"code": ""})

    def _handle_get_market_kubun(self, req_id, params):
        """Market type for code."""
        self._send_result(req_id, {"market_kubun": "1"})

    def _handle_logout(self, req_id, _params):
        """Logout."""
        logging.info("Logout requested")
        self._logged_in = False
        self._api_connected = False
        self._send_result(req_id, {"status": "ok"})

    def _handle_shutdown(self, req_id, _params):
        """Graceful shutdown."""
        logging.info("Shutdown requested")
        self._send_result(req_id, {"status": "ok"})
        self._shutdown()

    def _handle_restart(self, req_id, _params):
        """Simulate re-login."""
        logging.info("Restart requested - simulating re-login")
        self._unsubscribe_all_real()
        self._logged_in = True
        self._api_connected = True
        logging.info("Re-login successful (mock)")
        self._send_result(req_id, {"status": "ok"})

    # ============================================================
    #  TR Handler — Dispatch by tr_code
    # ============================================================

    def _handle_request_tr(self, req_id, params):
        """Bundled TR cycle: dispatch by tr_code, return mock data."""
        tr_code = params.get("tr_code", "")
        inputs = params.get("inputs", {})
        outputs = params.get("outputs", {})

        logging.info("TR Request: tr_code=%s, inputs=%s", tr_code, inputs)

        handler = self._tr_dispatch.get(tr_code)
        if handler is not None:
            result = handler(inputs, outputs)
        else:
            # Unknown TR code — return empty OutRec1 + empty OutRec2
            logging.warning("Unknown TR code: %s - returning empty", tr_code)
            result = {}
            if "OutRec1" in outputs:
                result["OutRec1"] = {f: "" for f in outputs["OutRec1"]}
            if "OutRec2" in outputs:
                result["OutRec2"] = []

        logging.info("TR Response: tr_code=%s, result_keys=%s", tr_code, list(result.keys()))
        self._send_result(req_id, result)

    # ============================================================
    #  TR Mock Implementations
    # ============================================================

    def _mock_tr_oca1725q(self, inputs, outputs):
        """외화예수금 (OCA1725Q)."""
        result = {}
        if "OutRec1" in outputs:
            rec1 = {}
            field_map = {
                "AC_TDA": "16863887",
                "MNYO_ABLE_A": "15000000",
                "RECNM": "2",
            }
            for f in outputs["OutRec1"]:
                rec1[f] = field_map.get(f, "0")
            result["OutRec1"] = rec1

        if "OutRec2" in outputs:
            rows = [
                {
                    "CURR_COD": "USD", "FRC_DA": "12345.67",
                    "FRC_MNYO_ABLE_A": "10000.00", "FRC_EA": "16863887",
                    "CTRY_NM": "\ubbf8\uad6d",
                },
                {
                    "CURR_COD": "HKD", "FRC_DA": "50000.00",
                    "FRC_MNYO_ABLE_A": "48000.00", "FRC_EA": "8500000",
                    "CTRY_NM": "\ud64d\ucf69",
                },
            ]
            filtered_rows = []
            for row in rows:
                filtered = {}
                for f in outputs["OutRec2"]:
                    filtered[f] = row.get(f, "")
                filtered_rows.append(filtered)
            result["OutRec2"] = filtered_rows

        return result

    def _mock_tr_otd6209q(self, inputs, outputs):
        """보유종목 손익평가 (OTD6209Q)."""
        result = {}
        if "OutRec1" in outputs:
            rec1 = {}
            for f in outputs["OutRec1"]:
                if f == "RECNM":
                    rec1[f] = str(len(MOCK_HOLDINGS))
                else:
                    rec1[f] = ""
            result["OutRec1"] = rec1

        if "OutRec2" in outputs:
            rows = []
            for holding in MOCK_HOLDINGS:
                symbol = holding.get("ITEM_COD", "")

                # Use live price (reflects slider + tick changes in real-time)
                with self._live_prices_lock:
                    live = self._live_prices.get(symbol)

                if live is not None:
                    try:
                        buy = float(holding.get("BUY_UPR", "0"))
                        qty = int(holding.get("HLDG_Q", "0"))
                    except (ValueError, TypeError):
                        buy, qty = 0.0, 0

                    patched = dict(holding)
                    patched["FRGN_STK_CLPR"] = f"{live:.2f}"
                    if buy > 0:
                        pl_pct = (live - buy) / buy * 100
                        patched["ERN_R"] = f"{pl_pct:.2f}"
                        patched["EV_PL_SUM_A"] = f"{(live - buy) * qty:.2f}"
                        patched["BNS_BAL_EA"] = f"{live * qty:.2f}"

                    row = {}
                    for f in outputs["OutRec2"]:
                        row[f] = patched.get(f, "")
                else:
                    row = {}
                    for f in outputs["OutRec2"]:
                        row[f] = holding.get(f, "")

                rows.append(row)
            result["OutRec2"] = rows

        return result

    def _mock_tr_otd6224q(self, inputs, outputs):
        """매수주문가능수량 (OTD6224Q)."""
        result = {}
        if "OutRec1" in outputs:
            rec1 = {}
            field_map = {
                "CSH_ORD_ABLE_Q": "52",
                "FRC_ORD_ABLE_A": "10000.00",
                "BAL_Q": "25",
                "AC_NM": "\ud14c\uc2a4\ud2b8\uacc4\uc88c",
                "IVST_LMT_Q": "0",
            }
            for f in outputs["OutRec1"]:
                rec1[f] = field_map.get(f, "0")
            result["OutRec1"] = rec1

        return result

    def _mock_tr_otd6238q(self, inputs, outputs):
        """매도가능잔고 (OTD6238Q)."""
        item_cod = inputs.get("ITEM_COD", "AAPL")
        result = {}
        if "OutRec1" in outputs:
            rec1 = {}
            for f in outputs["OutRec1"]:
                if f == "RECNM":
                    rec1[f] = "1"
                else:
                    rec1[f] = ""
            result["OutRec1"] = rec1

        if "OutRec2" in outputs:
            # Find matching holding or return dummy
            name = _get_symbol_name(item_cod)
            bal_q = "0"
            sel_q = "0"
            for h in MOCK_HOLDINGS:
                if h["ITEM_COD"] == item_cod:
                    bal_q = h["HLDG_Q"]
                    sel_q = h["SEL_ABLE_Q"]
                    name = h["ITEM_NM"]
                    break

            row = {}
            field_map = {
                "ITEM_COD": item_cod,
                "ITEM_NM": name,
                "BAL_Q": bal_q,
                "SEL_ABLE_Q": sel_q,
            }
            for f in outputs["OutRec2"]:
                row[f] = field_map.get(f, "")
            result["OutRec2"] = [row]

        return result

    def _mock_tr_otd6101u(self, inputs, outputs):
        """주문실행 — 매수/매도 (OTD6101U)."""
        ord_no = str(random.randint(1000000, 9999999))
        logging.info(
            "Order executed: %s %s qty=%s price=%s -> ord_no=%s",
            inputs.get("BUY_SEL_TR_TCD", "?"),
            inputs.get("ITEM_COD", "?"),
            inputs.get("ORD_Q", "?"),
            inputs.get("FGST_ORD_UPR", "?"),
            ord_no,
        )
        result = {}
        if "OutRec1" in outputs:
            rec1 = {}
            for f in outputs["OutRec1"]:
                if f == "ORD_NO":
                    rec1[f] = ord_no
                else:
                    rec1[f] = ""
            result["OutRec1"] = rec1

        return result

    def _mock_tr_otd6103u(self, inputs, outputs):
        """주문취소 (OTD6103U)."""
        ord_no = str(random.randint(1000000, 9999999))
        logging.info(
            "Order cancelled: orig_ord=%s -> new_ord=%s",
            inputs.get("OORD_NO", "?"), ord_no,
        )
        result = {}
        if "OutRec1" in outputs:
            rec1 = {}
            for f in outputs["OutRec1"]:
                if f == "ORD_NO":
                    rec1[f] = ord_no
                else:
                    rec1[f] = ""
            result["OutRec1"] = rec1

        return result

    def _mock_tr_gbpbid(self, inputs, outputs):
        """호가 (gbpbid)."""
        scode = inputs.get("SCODE", "")
        _, symbol = _parse_symbol_from_scode(scode)
        base = _get_base_price(symbol)

        # Slight spread
        bid = round(base, 2)
        offer = round(base + random.uniform(0.01, 0.10), 2)
        prev_close = round(base - random.uniform(0.5, 2.0), 2)
        now = time.strftime("%H%M%S")

        result = {}
        if "OutRec1" in outputs:
            field_map = {
                "LTIME": now,
                "LOFFER1": f"{offer:.2f}",
                "LBID1": f"{bid:.2f}",
                "LOFFERREST1": str(random.randint(500, 5000)),
                "LBIDREST1": str(random.randint(500, 5000)),
                "LCPRICE": f"{base:.2f}",
                "LLSTCPRICE": f"{prev_close:.2f}",
            }
            rec1 = {}
            for f in outputs["OutRec1"]:
                rec1[f] = field_map.get(f, "0")
            result["OutRec1"] = rec1

        return result

    def _mock_tr_gbmst(self, inputs, outputs):
        """마스터 정보 (gbmst)."""
        scode = inputs.get("SCODE", "")
        _, symbol = _parse_symbol_from_scode(scode)
        base = _get_base_price(symbol)
        name = _get_symbol_name(symbol)
        diff = round(random.uniform(-3.0, 3.0), 2)
        diff_rate = round(diff / base * 100, 2)

        result = {}
        if "OutRec1" in outputs:
            field_map = {
                "SKORNAME": name,
                "SCURRENCY": "USD",
                "LCPRICE": f"{base:.2f}",
                "LDIFF": f"{diff:.2f}",
                "LDIFFRATIO": f"{diff_rate:.2f}",
                "LVOLUME": str(random.randint(1000000, 80000000)),
                "LOFFER": f"{base + 0.05:.2f}",
                "LBID": f"{base - 0.05:.2f}",
                "LORDERSIZE": "1",
                "LORDERUNIT": "1",
            }
            rec1 = {}
            for f in outputs["OutRec1"]:
                rec1[f] = field_map.get(f, "")
            result["OutRec1"] = rec1

        return result

    def _mock_tr_otd3108q(self, inputs, outputs):
        """국내주식 잔고 (OTD3108Q)."""
        result = {}
        if "OutRec1" in outputs:
            field_map = {
                "RECNM": "0",
                "AC_TDA": "5000000",
                "D2_ESTI_DA": "5000000",
                "ORD_ABLE_CSH": "4500000",
                "AM_BAL_A": "5000000",
            }
            rec1 = {}
            for f in outputs["OutRec1"]:
                rec1[f] = field_map.get(f, "0")
            result["OutRec1"] = rec1

        if "OutRec2" in outputs:
            result["OutRec2"] = []

        return result

    # ============================================================
    #  Real-time Subscription
    # ============================================================

    def _handle_subscribe_real(self, req_id, params):
        """Register real-time data subscription and start tick generator."""
        real_id = str(params.get("real_id", ""))
        real_key = params.get("real_key", "")
        fields = params.get("fields", [])

        _, symbol = _parse_symbol_from_scode(real_key)
        logging.info(
            "Subscribe real: real_id=%s, real_key=%s, symbol=%s, fields=%d",
            real_id, real_key, symbol, len(fields),
        )

        # Initialize live price for this symbol
        with self._live_prices_lock:
            if symbol and symbol not in self._live_prices:
                self._live_prices[symbol] = _get_base_price(symbol)

        # Start tick generator thread
        sub_key = f"{real_id}:{real_key}"
        stop_event = threading.Event()

        with self._real_subs_lock:
            # Stop existing subscription for same key
            if sub_key in self._real_subs:
                self._real_subs[sub_key]["stop"].set()
            self._real_subs[sub_key] = {
                "stop": stop_event,
                "real_id": real_id,
                "real_key": real_key,
                "symbol": symbol,
                "fields": fields,
            }

        if symbol:
            thread = threading.Thread(
                target=self._tick_generator,
                args=(sub_key, real_id, real_key, symbol, fields, stop_event),
                daemon=True,
            )
            thread.start()
            with self._real_subs_lock:
                self._real_subs[sub_key]["thread"] = thread

        self._send_result(req_id, {"status": "ok"})

    def _handle_unsubscribe_real(self, req_id, params):
        """Unregister real-time data."""
        real_id = str(params.get("real_id", ""))
        real_key = params.get("real_key", "")
        sub_key = f"{real_id}:{real_key}"

        logging.info("Unsubscribe real: %s", sub_key)
        with self._real_subs_lock:
            sub = self._real_subs.pop(sub_key, None)
        if sub:
            sub["stop"].set()

        self._send_result(req_id, {"status": "ok"})

    def _handle_unsubscribe_all(self, req_id, _params):
        """Unregister all real-time data."""
        logging.info("Unsubscribe all real-time")
        self._unsubscribe_all_real()
        self._send_result(req_id, {"status": "ok"})

    def _unsubscribe_all_real(self):
        """Stop all tick generators."""
        with self._real_subs_lock:
            for sub_key, sub in self._real_subs.items():
                sub["stop"].set()
            self._real_subs.clear()

    # ============================================================
    #  Real-time Tick Generator
    # ============================================================

    def _tick_generator(self, sub_key, real_id, real_key, symbol, fields,
                        stop_event):
        """
        Per-symbol daemon thread.
        Generates simulated price ticks every 2-5 seconds.
        """
        cumulative_volume = random.randint(10000000, 50000000)
        prev_close = _get_base_price(symbol)

        logging.info("Tick generator started: %s (%s)", symbol, sub_key)

        while not stop_event.is_set():
            # Random interval 2-5 seconds
            wait = random.uniform(2.0, 5.0)
            if stop_event.wait(timeout=wait):
                break

            # Check if client is still connected
            if self._client_sock is None:
                break

            # Fluctuate price ±0.01~0.50%
            with self._live_prices_lock:
                current = self._live_prices.get(symbol, _get_base_price(symbol))
                pct_change = random.uniform(-0.005, 0.005)
                new_price = round(current * (1.0 + pct_change), 2)
                # Ensure price stays positive
                if new_price < 0.01:
                    new_price = 0.01
                self._live_prices[symbol] = new_price

            # Sync back to MOCK_HOLDINGS so OTD6209Q returns live prices
            SYMBOL_PRICES[symbol] = new_price
            for holding in MOCK_HOLDINGS:
                if holding["ITEM_COD"] == symbol:
                    holding["FRGN_STK_CLPR"] = f"{new_price:.2f}"
                    try:
                        buy = float(holding["BUY_UPR"])
                        if buy > 0:
                            pl_pct = (new_price - buy) / buy * 100
                            holding["ERN_R"] = f"{pl_pct:.2f}"
                            qty = int(holding["HLDG_Q"])
                            holding["EV_PL_SUM_A"] = (
                                f"{(new_price - buy) * qty:.2f}"
                            )
                            holding["BNS_BAL_EA"] = f"{new_price * qty:.2f}"
                    except (ValueError, TypeError):
                        pass
                    break

            # Record price history for charts
            self._price_history.record(symbol, new_price)

            # Calculate fields
            diff = round(new_price - prev_close, 2)
            diff_rate = round(diff / prev_close * 100, 2) if prev_close else 0.0
            cur_volume = random.randint(100, 5000)
            cumulative_volume += cur_volume

            # cPCheck: "2" if up, "5" if down, "3" if flat
            if diff > 0:
                p_check = "2"
            elif diff < 0:
                p_check = "5"
            else:
                p_check = "3"

            now_loc = time.strftime("%H%M%S")
            # Korean time = local time (mock approximation)
            now_kor = now_loc

            offer = round(new_price + random.uniform(0.01, 0.10), 2)
            bid = round(new_price - random.uniform(0.01, 0.05), 2)

            # Build fields dict — only include fields the client requested
            all_values = {
                "sCode": real_key,
                "sName": _get_symbol_name(symbol),
                "lCPrice": f"{new_price:.2f}",
                "lDiff": f"{diff:.2f}",
                "cPCheck": p_check,
                "lDiffRate": f"{diff_rate:.2f}",
                "lCurVolume": str(cur_volume),
                "lVolume": str(cumulative_volume),
                "lLocTime": now_loc,
                "lKorTime": now_kor,
                "lOffer1": f"{offer:.2f}",
                "lBid1": f"{bid:.2f}",
                "cMarketGubun": "1",
                # Domestic fields
                "LCPRICE": f"{new_price:.2f}",
                "LVOLUME": str(cumulative_volume),
                "LDIFF": f"{diff:.2f}",
                "CPCHECK": p_check,
                "LDIFFRATE": f"{diff_rate:.2f}",
            }

            tick_fields = {}
            for f in fields:
                tick_fields[f] = all_values.get(f, "")

            self._send_event("real_data", {
                "real_id": real_id,
                "real_key": real_key,
                "fields": tick_fields,
            })

        logging.info("Tick generator stopped: %s (%s)", symbol, sub_key)

    # ============================================================
    #  Backend Heartbeat
    # ============================================================

    def start_heartbeat(self, cfg):
        """
        백엔드로 주기적 heartbeat 전송 시작.
        웹 대시보드가 proxy 상태를 실시간으로 표시할 수 있도록 한다.
        """
        if not cfg.getboolean("backend", "enabled", fallback=False):
            return

        self._heartbeat_url = cfg.get(
            "backend", "heartbeat_url",
            fallback="http://127.0.0.1:8080/api/heartbeat",
        )
        interval_sec = cfg.getint("backend", "heartbeat_interval", fallback=10)

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(interval_sec,),
            daemon=True,
        )
        self._heartbeat_thread.start()
        logging.info("Backend heartbeat started: %s (every %ds)",
                     self._heartbeat_url, interval_sec)

    def _heartbeat_loop(self, interval_sec):
        """Periodic heartbeat sender thread."""
        while not self._heartbeat_stop.wait(timeout=interval_sec):
            if self._shutting_down:
                break
            self._emit_heartbeat()

    def _emit_heartbeat(self):
        """Collect heartbeat data and post to backend."""
        with self._real_subs_lock:
            sub_count = len(self._real_subs)

        data = {
            "source": "proxy",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "server_running": True,
            "api_connected": self._api_connected,
            "logged_in": self._logged_in,
            "uptime": round(time.time() - self._start_time, 2),
            "client_connected": self._client_sock is not None,
            "pending_tr": 0,
            "real_subscriptions": sub_count,
        }
        threading.Thread(
            target=self._post_heartbeat, args=(data,), daemon=True,
        ).start()

    def _post_heartbeat(self, data):
        """백엔드에 heartbeat POST (백그라운드 스레드). 실패 시 무시."""
        url = self._heartbeat_url
        if url is None:
            return
        try:
            payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass

    # ============================================================
    #  Shutdown
    # ============================================================

    def _shutdown(self):
        """Graceful shutdown sequence."""
        if self._shutting_down:
            return
        self._shutting_down = True
        logging.info("Shutting down...")

        # Stop heartbeat
        self._heartbeat_stop.set()

        # Unsubscribe all real-time
        self._unsubscribe_all_real()

        # Close TCP
        self._close_client()
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None

        # Signal main loop to exit
        self._shutdown_event.set()

        logging.info("Shutdown complete")


# ============================================================
#  GUI Application
# ============================================================

class MockServerGUI:
    """
    Tkinter GUI for the EugeneMockServer.

    Provides tabbed panels:
      Always visible:
        - Terminal/Log (real-time log output)
        - Status bar
      Tab 1 — Holdings & Management:
        - Holdings Table (MOCK_HOLDINGS display)
        - Create Virtual Stock
        - Change Stock Price
      Tab 2 — Charts & Price Control:
        - Real-time price charts per stock
        - Volume-style sliders for price adjustment
    """

    REFRESH_INTERVAL_MS = 2000  # Holdings table refresh interval

    def __init__(self, root, server):
        self._root = root
        self._server = server

        root.title("[TEST MODE] Eugene OpenAPI Mock Server - GUI")
        root.geometry("1200x800")
        root.minsize(800, 600)

        # Configure grid weights for resizing
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=4)   # Log panel
        root.rowconfigure(1, weight=5)   # Notebook (holdings / charts)
        root.rowconfigure(2, weight=0)   # Status bar

        self._build_log_panel(root)

        # Notebook for organized panels
        self._notebook = ttk.Notebook(root)
        self._notebook.grid(row=1, column=0, sticky="nsew", padx=6, pady=2)

        # Tab 1: Holdings & Management
        holdings_tab = ttk.Frame(self._notebook)
        holdings_tab.columnconfigure(0, weight=1)
        holdings_tab.rowconfigure(0, weight=1)
        holdings_tab.rowconfigure(1, weight=0)
        holdings_tab.rowconfigure(2, weight=0)
        self._notebook.add(holdings_tab, text="  보유종목 & 관리  ")
        self._build_holdings_panel(holdings_tab)
        self._build_create_stock_panel(holdings_tab)
        self._build_change_price_panel(holdings_tab)

        # Tab 2: Charts & Price Control
        charts_tab = ttk.Frame(self._notebook)
        charts_tab.columnconfigure(0, weight=1)
        charts_tab.rowconfigure(0, weight=1)
        self._notebook.add(charts_tab, text="  차트 & 가격조정  ")
        self._build_chart_panel(charts_tab)

        # Status bar at bottom
        self._build_status_bar(root)

        # Handle window close
        root.protocol("WM_DELETE_WINDOW", self._on_close)

        # Start periodic refresh
        self._refresh_holdings()
        self._refresh_status()
        self._refresh_charts()

    # --------------------------------------------------------
    #  Panel 1: Terminal / Log
    # --------------------------------------------------------

    def _build_log_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="  Terminal / Log  ", padding=4)
        frame.grid(row=0, column=0, sticky="nsew", padx=6, pady=(6, 2))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        # Text widget with scrollbar
        txt_frame = tk.Frame(frame)
        txt_frame.grid(row=0, column=0, sticky="nsew")
        txt_frame.columnconfigure(0, weight=1)
        txt_frame.rowconfigure(0, weight=1)

        self._log_text = tk.Text(
            txt_frame,
            wrap=tk.WORD,
            font=("Consolas", 9),
            bg="#1e1e1e",
            fg="#d4d4d4",
            insertbackground="#d4d4d4",
            selectbackground="#264f78",
            selectforeground="#ffffff",
            state=tk.DISABLED,
            borderwidth=0,
            highlightthickness=0,
            padx=6,
            pady=4,
        )
        scrollbar = ttk.Scrollbar(txt_frame, orient=tk.VERTICAL,
                                  command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=scrollbar.set)

        self._log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        # Tag colors for log levels
        self._log_text.tag_configure("error", foreground="#f44747")
        self._log_text.tag_configure("warning", foreground="#cca700")
        self._log_text.tag_configure("info", foreground="#d4d4d4")
        self._log_text.tag_configure("debug", foreground="#808080")

        # Buttons row
        btn_frame = tk.Frame(frame)
        btn_frame.grid(row=1, column=0, sticky="ew", pady=(4, 0))

        ttk.Button(btn_frame, text="Clear Log",
                   command=self._clear_log).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_frame, text="Scroll to Bottom",
                   command=self._scroll_log_bottom).pack(side=tk.LEFT)

        # Connection status label
        self._conn_label = ttk.Label(btn_frame, text="Client: disconnected",
                                     foreground="gray")
        self._conn_label.pack(side=tk.RIGHT, padx=(6, 0))

    def _clear_log(self):
        self._log_text.config(state=tk.NORMAL)
        self._log_text.delete("1.0", tk.END)
        self._log_text.config(state=tk.DISABLED)

    def _scroll_log_bottom(self):
        self._log_text.see(tk.END)

    # --------------------------------------------------------
    #  Panel 2: Holdings Table
    # --------------------------------------------------------

    def _build_holdings_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="  Holdings  ", padding=4)
        frame.grid(row=0, column=0, sticky="nsew", padx=6, pady=2)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        # Treeview columns
        columns = ("symbol", "name", "qty", "buy_price", "cur_price",
                    "pl_pct", "mkt_value")
        display_names = {
            "symbol": "Symbol",
            "name": "Name",
            "qty": "Qty",
            "buy_price": "Buy Price",
            "cur_price": "Cur Price",
            "pl_pct": "P/L %",
            "mkt_value": "Mkt Value",
        }

        tree_frame = tk.Frame(frame)
        tree_frame.grid(row=0, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self._holdings_tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings", height=6,
        )
        tree_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL,
                                    command=self._holdings_tree.yview)
        self._holdings_tree.configure(yscrollcommand=tree_scroll.set)

        # Configure columns
        col_widths = {
            "symbol": 80, "name": 200, "qty": 60, "buy_price": 100,
            "cur_price": 100, "pl_pct": 80, "mkt_value": 120,
        }
        for col in columns:
            self._holdings_tree.heading(col, text=display_names[col])
            if col in ("symbol", "qty"):
                anc = "center"
            elif col in ("buy_price", "cur_price", "pl_pct", "mkt_value"):
                anc = "e"
            else:
                anc = "w"
            self._holdings_tree.column(
                col, width=col_widths.get(col, 100),
                anchor=anc,
                minwidth=50,
            )

        self._holdings_tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll.grid(row=0, column=1, sticky="ns")

        # Configure row tags for P/L coloring
        self._holdings_tree.tag_configure("profit", foreground="#4ec9b0")
        self._holdings_tree.tag_configure("loss", foreground="#f44747")
        self._holdings_tree.tag_configure("even", foreground="#d4d4d4")

        # Buttons
        btn_frame = tk.Frame(frame)
        btn_frame.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        ttk.Button(btn_frame, text="Refresh Now",
                   command=self._refresh_holdings_now).pack(side=tk.LEFT)

        self._holdings_count_label = ttk.Label(btn_frame, text="0 holdings")
        self._holdings_count_label.pack(side=tk.RIGHT)

    def _refresh_holdings_now(self):
        """Immediate refresh triggered by button click."""
        self._update_holdings_tree()

    def _refresh_holdings(self):
        """Periodic refresh via root.after()."""
        self._update_holdings_tree()
        self._root.after(self.REFRESH_INTERVAL_MS, self._refresh_holdings)

    def _update_holdings_tree(self):
        """Rebuild holdings treeview from MOCK_HOLDINGS + live prices."""
        tree = self._holdings_tree
        # Clear existing items
        for item in tree.get_children():
            tree.delete(item)

        for holding in MOCK_HOLDINGS:
            symbol = holding.get("ITEM_COD", "")
            name = holding.get("ITEM_NM", "")
            qty_str = holding.get("HLDG_Q", "0")
            buy_str = holding.get("BUY_UPR", "0")

            try:
                qty = int(qty_str)
            except (ValueError, TypeError):
                qty = 0
            try:
                buy_price = float(buy_str)
            except (ValueError, TypeError):
                buy_price = 0.0

            # Get current price from live prices, fall back to base price
            with self._server._live_prices_lock:
                cur_price = self._server._live_prices.get(
                    symbol, SYMBOL_PRICES.get(symbol, 0.0),
                )

            # Calculate P/L %
            if buy_price > 0:
                pl_pct = (cur_price - buy_price) / buy_price * 100
            else:
                pl_pct = 0.0

            mkt_value = cur_price * qty

            # Determine row tag
            if pl_pct > 0.01:
                tag = "profit"
            elif pl_pct < -0.01:
                tag = "loss"
            else:
                tag = "even"

            tree.insert("", tk.END, values=(
                symbol,
                name,
                str(qty),
                f"{buy_price:.2f}",
                f"{cur_price:.2f}",
                f"{pl_pct:+.2f}%",
                f"{mkt_value:,.2f}",
            ), tags=(tag,))

        self._holdings_count_label.config(
            text=f"{len(MOCK_HOLDINGS)} holdings",
        )

    # --------------------------------------------------------
    #  Panel 3: Create Virtual Stock
    # --------------------------------------------------------

    def _build_create_stock_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="  Create Virtual Stock  ",
                               padding=4)
        frame.grid(row=1, column=0, sticky="ew", padx=6, pady=2)

        # Row of inputs
        row = tk.Frame(frame)
        row.pack(fill=tk.X, expand=True)

        ttk.Label(row, text="Symbol:").pack(side=tk.LEFT, padx=(0, 2))
        self._new_symbol_var = tk.StringVar()
        ttk.Entry(row, textvariable=self._new_symbol_var,
                  width=10).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(row, text="Name:").pack(side=tk.LEFT, padx=(0, 2))
        self._new_name_var = tk.StringVar()
        ttk.Entry(row, textvariable=self._new_name_var,
                  width=24).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(row, text="Price:").pack(side=tk.LEFT, padx=(0, 2))
        self._new_price_var = tk.StringVar()
        ttk.Entry(row, textvariable=self._new_price_var,
                  width=10).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(row, text="Qty:").pack(side=tk.LEFT, padx=(0, 2))
        self._new_qty_var = tk.StringVar()
        ttk.Entry(row, textvariable=self._new_qty_var,
                  width=8).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(row, text="Add Stock",
                   command=self._add_stock).pack(side=tk.LEFT, padx=(10, 0))

    def _add_stock(self):
        """Validate inputs and add new stock to mock data."""
        symbol = self._new_symbol_var.get().strip().upper()
        name = self._new_name_var.get().strip()
        price_str = self._new_price_var.get().strip()
        qty_str = self._new_qty_var.get().strip()

        # Validation
        if not symbol:
            messagebox.showwarning("Validation Error",
                                   "Symbol cannot be empty.")
            return
        if not name:
            name = symbol  # Default name to symbol

        try:
            price = float(price_str)
        except (ValueError, TypeError):
            messagebox.showwarning("Validation Error",
                                   "Price must be a valid number.")
            return
        if price <= 0:
            messagebox.showwarning("Validation Error",
                                   "Price must be > 0.")
            return

        try:
            qty = int(qty_str)
        except (ValueError, TypeError):
            messagebox.showwarning("Validation Error",
                                   "Quantity must be a valid integer.")
            return
        if qty <= 0:
            messagebox.showwarning("Validation Error",
                                   "Quantity must be > 0.")
            return

        # Update global mock data
        SYMBOL_PRICES[symbol] = price
        SYMBOL_NAMES[symbol] = name

        # Update server live prices
        with self._server._live_prices_lock:
            self._server._live_prices[symbol] = price

        # Add to MOCK_HOLDINGS
        buy_price = round(price * random.uniform(0.90, 1.05), 2)
        pl_pct = round((price - buy_price) / buy_price * 100, 2)
        ev_pl = round((price - buy_price) * qty, 2)
        mkt_val = round(price * qty, 2)

        holding = {
            "EXG_COD": "020",
            "ITEM_COD": symbol,
            "ITEM_NM": name,
            "HLDG_Q": str(qty),
            "SEL_ABLE_Q": str(qty),
            "BUY_UPR": f"{buy_price:.2f}",
            "FRGN_STK_CLPR": f"{price:.2f}",
            "FRGN_STK_MKT_TCD": "01",
            "ERN_R": f"{pl_pct:.2f}",
            "EV_PL_SUM_A": f"{ev_pl:.2f}",
            "BNS_BAL_EA": f"{mkt_val:.2f}",
        }
        MOCK_HOLDINGS.append(holding)

        logging.info("Stock created: %s (%s) price=%.2f qty=%d",
                     symbol, name, price, qty)

        # Refresh UI
        self._update_holdings_tree()
        self._update_symbol_dropdown()

        # Add chart row for new stock
        if hasattr(self, '_chart_rows'):
            for _ in range(10):
                p = price * (1 + random.uniform(-0.003, 0.003))
                self._server._price_history.record(symbol, round(p, 2))
            self._add_chart_row(symbol)

        # Clear inputs
        self._new_symbol_var.set("")
        self._new_name_var.set("")
        self._new_price_var.set("")
        self._new_qty_var.set("")

    # --------------------------------------------------------
    #  Panel 4: Change Stock Price
    # --------------------------------------------------------

    def _build_change_price_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="  Change Stock Price  ",
                               padding=4)
        frame.grid(row=2, column=0, sticky="ew", padx=6, pady=(2, 6))

        row = tk.Frame(frame)
        row.pack(fill=tk.X, expand=True)

        ttk.Label(row, text="Symbol:").pack(side=tk.LEFT, padx=(0, 2))
        self._chg_symbol_var = tk.StringVar()
        self._chg_symbol_combo = ttk.Combobox(
            row, textvariable=self._chg_symbol_var,
            values=sorted(SYMBOL_PRICES.keys()),
            state="readonly", width=12,
        )
        self._chg_symbol_combo.pack(side=tk.LEFT, padx=(0, 10))
        # Select first item if available
        symbols = sorted(SYMBOL_PRICES.keys())
        if symbols:
            self._chg_symbol_combo.current(0)
        # Show current price on selection change
        self._chg_symbol_combo.bind("<<ComboboxSelected>>",
                                    self._on_symbol_selected)

        self._chg_cur_price_label = ttk.Label(row, text="Current: --",
                                              width=18)
        self._chg_cur_price_label.pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(row, text="New Price:").pack(side=tk.LEFT, padx=(0, 2))
        self._chg_price_var = tk.StringVar()
        ttk.Entry(row, textvariable=self._chg_price_var,
                  width=12).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(row, text="Change Price",
                   command=self._change_price).pack(side=tk.LEFT,
                                                    padx=(10, 0))

        # Trigger initial display
        self._on_symbol_selected(None)

    def _on_symbol_selected(self, _event):
        """Update current price label when symbol selection changes."""
        symbol = self._chg_symbol_var.get()
        if not symbol:
            self._chg_cur_price_label.config(text="Current: --")
            return

        with self._server._live_prices_lock:
            price = self._server._live_prices.get(
                symbol, SYMBOL_PRICES.get(symbol, 0.0),
            )
        self._chg_cur_price_label.config(text=f"Current: {price:.2f}")

    def _change_price(self):
        """Update the price for the selected symbol."""
        symbol = self._chg_symbol_var.get()
        price_str = self._chg_price_var.get().strip()

        if not symbol:
            messagebox.showwarning("Validation Error",
                                   "Please select a symbol.")
            return

        try:
            new_price = float(price_str)
        except (ValueError, TypeError):
            messagebox.showwarning("Validation Error",
                                   "Price must be a valid number.")
            return
        if new_price <= 0:
            messagebox.showwarning("Validation Error",
                                   "Price must be > 0.")
            return

        # Update globals
        SYMBOL_PRICES[symbol] = new_price

        # Update server live prices
        with self._server._live_prices_lock:
            self._server._live_prices[symbol] = new_price

        # Update MOCK_HOLDINGS current price for matching symbol
        for holding in MOCK_HOLDINGS:
            if holding["ITEM_COD"] == symbol:
                holding["FRGN_STK_CLPR"] = f"{new_price:.2f}"
                # Recalculate P/L
                try:
                    buy = float(holding["BUY_UPR"])
                    if buy > 0:
                        pl_pct = (new_price - buy) / buy * 100
                        holding["ERN_R"] = f"{pl_pct:.2f}"
                        qty = int(holding["HLDG_Q"])
                        holding["EV_PL_SUM_A"] = f"{(new_price - buy) * qty:.2f}"
                        holding["BNS_BAL_EA"] = f"{new_price * qty:.2f}"
                except (ValueError, TypeError):
                    pass

        logging.info("Price changed: %s -> %.2f", symbol, new_price)

        # Record in price history for charts
        self._server._price_history.record(symbol, new_price)

        # Refresh UI
        self._on_symbol_selected(None)
        self._update_holdings_tree()
        self._chg_price_var.set("")

    def _update_symbol_dropdown(self):
        """Refresh the symbol combobox values."""
        symbols = sorted(SYMBOL_PRICES.keys())
        self._chg_symbol_combo["values"] = symbols

    # --------------------------------------------------------
    #  Panel 5: Charts & Price Control
    # --------------------------------------------------------

    def _build_chart_panel(self, parent):
        """Build scrollable chart + slider panel for all held stocks."""
        self._chart_canvases = {}
        self._chart_sliders = {}
        self._chart_price_labels = {}
        self._chart_rows = {}
        self._slider_programmatic = False

        # Scrollable container
        outer = tk.Frame(parent, bg="#0a0f1a")
        outer.grid(row=0, column=0, sticky="nsew")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        self._chart_outer_canvas = tk.Canvas(
            outer, bg="#0a0f1a", highlightthickness=0,
        )
        v_scroll = ttk.Scrollbar(
            outer, orient=tk.VERTICAL,
            command=self._chart_outer_canvas.yview,
        )
        self._chart_scroll_frame = tk.Frame(
            self._chart_outer_canvas, bg="#0a0f1a",
        )
        self._chart_scroll_frame.columnconfigure(0, weight=1)
        self._chart_scroll_frame.bind(
            "<Configure>",
            lambda e: self._chart_outer_canvas.configure(
                scrollregion=self._chart_outer_canvas.bbox("all"),
            ),
        )
        self._chart_window_id = self._chart_outer_canvas.create_window(
            (0, 0), window=self._chart_scroll_frame, anchor="nw",
        )
        self._chart_outer_canvas.configure(yscrollcommand=v_scroll.set)

        # Resize inner frame width with canvas
        def _on_canvas_resize(event):
            self._chart_outer_canvas.itemconfig(
                self._chart_window_id, width=event.width,
            )

        self._chart_outer_canvas.bind("<Configure>", _on_canvas_resize)

        self._chart_outer_canvas.grid(row=0, column=0, sticky="nsew")
        v_scroll.grid(row=0, column=1, sticky="ns")

        # Mousewheel scrolling (active only when mouse is over chart area)
        def _on_chart_mousewheel(event):
            self._chart_outer_canvas.yview_scroll(
                int(-1 * (event.delta / 120)), "units",
            )

        self._chart_outer_canvas.bind(
            "<Enter>",
            lambda e: self._chart_outer_canvas.bind_all(
                "<MouseWheel>", _on_chart_mousewheel,
            ),
        )
        self._chart_outer_canvas.bind(
            "<Leave>",
            lambda e: self._chart_outer_canvas.unbind_all("<MouseWheel>"),
        )

        # Title bar
        title = tk.Frame(self._chart_scroll_frame, bg="#0f172a")
        title.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 8))
        tk.Label(
            title, text="  Real-time Price Charts & Sliders",
            font=("Consolas", 10, "bold"), bg="#0f172a", fg="#94a3b8",
            anchor="w", padx=8, pady=6,
        ).pack(fill=tk.X)

        # Seed initial price history and build chart rows
        for holding in MOCK_HOLDINGS:
            symbol = holding.get("ITEM_COD", "")
            if symbol:
                base = SYMBOL_PRICES.get(symbol, 100.0)
                for _ in range(10):
                    p = base * (1 + random.uniform(-0.003, 0.003))
                    self._server._price_history.record(symbol, round(p, 2))
                self._add_chart_row(symbol)

    def _add_chart_row(self, symbol):
        """Add a chart + slider row for a stock symbol."""
        if symbol in self._chart_rows:
            return

        parent = self._chart_scroll_frame
        # +1 to skip title bar at row 0
        row_idx = len(self._chart_rows) + 1

        row_frame = tk.Frame(parent, bg="#0a0f1a")
        row_frame.grid(row=row_idx, column=0, sticky="ew", padx=6, pady=4)
        row_frame.columnconfigure(1, weight=1)

        # Left: Symbol info
        info = tk.Frame(row_frame, bg="#0a0f1a", width=130)
        info.grid(row=0, column=0, sticky="ns", padx=(0, 8))
        info.grid_propagate(False)

        name = _get_symbol_name(symbol) or symbol
        tk.Label(
            info, text=symbol, font=("Consolas", 12, "bold"),
            bg="#0a0f1a", fg="#e2e8f0",
        ).pack(anchor="w", padx=4)
        tk.Label(
            info, text=name, font=("Consolas", 8),
            bg="#0a0f1a", fg="#64748b", wraplength=120, justify="left",
        ).pack(anchor="w", padx=4)

        cur = SYMBOL_PRICES.get(symbol, 0.0)
        price_lbl = tk.Label(
            info, text=f"${cur:.2f}",
            font=("Consolas", 11, "bold"), bg="#0a0f1a", fg="#22d3ee",
        )
        price_lbl.pack(anchor="w", padx=4, pady=(6, 0))
        self._chart_price_labels[symbol] = price_lbl

        # Center: Chart canvas
        chart = tk.Canvas(
            row_frame, width=400, height=130,
            bg="#0a0f1a", highlightthickness=1,
            highlightbackground="#1e293b",
        )
        chart.grid(row=0, column=1, sticky="ew", padx=4)
        self._chart_canvases[symbol] = chart

        # Right: Slider
        sf = tk.Frame(row_frame, bg="#0a0f1a")
        sf.grid(row=0, column=2, sticky="ns", padx=(8, 4))

        base = SYMBOL_PRICES.get(symbol, 100.0)
        hi = round(base * 1.5, 2)
        lo = round(max(base * 0.5, 0.01), 2)

        tk.Label(
            sf, text=f"${hi:.0f}", font=("Consolas", 7),
            bg="#0a0f1a", fg="#64748b",
        ).pack()

        slider = tk.Scale(
            sf, from_=hi, to=lo, resolution=0.01,
            orient=tk.VERTICAL, length=100, width=14,
            bg="#1e293b", fg="#e2e8f0",
            troughcolor="#0f172a", activebackground="#334155",
            highlightthickness=0, showvalue=False,
            command=lambda v, s=symbol: self._on_slider_change(s, float(v)),
        )
        slider.set(cur)
        slider.pack()
        self._chart_sliders[symbol] = slider

        tk.Label(
            sf, text=f"${lo:.0f}", font=("Consolas", 7),
            bg="#0a0f1a", fg="#64748b",
        ).pack()

        # Separator
        sep = tk.Frame(row_frame, bg="#1e293b", height=1)
        sep.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(6, 0))

        self._chart_rows[symbol] = row_frame

    def _on_slider_change(self, symbol, new_price):
        """Handle slider drag — propagate price change through the system."""
        if self._slider_programmatic:
            return

        # 1. Update module-level prices
        SYMBOL_PRICES[symbol] = new_price

        # 2. Update server live prices
        with self._server._live_prices_lock:
            self._server._live_prices[symbol] = new_price

        # 3. Update MOCK_HOLDINGS
        for holding in MOCK_HOLDINGS:
            if holding["ITEM_COD"] == symbol:
                holding["FRGN_STK_CLPR"] = f"{new_price:.2f}"
                try:
                    buy = float(holding["BUY_UPR"])
                    if buy > 0:
                        pl_pct = (new_price - buy) / buy * 100
                        holding["ERN_R"] = f"{pl_pct:.2f}"
                        qty = int(holding["HLDG_Q"])
                        holding["EV_PL_SUM_A"] = (
                            f"{(new_price - buy) * qty:.2f}"
                        )
                        holding["BNS_BAL_EA"] = f"{new_price * qty:.2f}"
                except (ValueError, TypeError):
                    pass

        # 4. Record in price history
        self._server._price_history.record(symbol, new_price)

        # 5. Update price label
        if symbol in self._chart_price_labels:
            self._chart_price_labels[symbol].config(
                text=f"${new_price:.2f}",
            )

    def _draw_chart(self, canvas, symbol):
        """Draw a line chart of recent price history on the canvas."""
        canvas.delete("all")

        w = canvas.winfo_width()
        h = canvas.winfo_height()
        if w < 20 or h < 20:
            w, h = 400, 130

        # Margins: left, right, top, bottom
        ml, mr, mt, mb = 52, 12, 14, 18
        cw = w - ml - mr
        ch = h - mt - mb
        if cw < 10 or ch < 10:
            return

        # Fetch history
        history = self._server._price_history.get(symbol)
        if len(history) < 2:
            canvas.create_text(
                w // 2, h // 2, text="\ub370\uc774\ud130 \uc218\uc9d1\uc911...",
                fill="#64748b", font=("Consolas", 10),
            )
            return

        # Use last 60 data points
        display = history[-60:]
        prices = [p for _, p in display]
        n = len(prices)

        min_p = min(prices)
        max_p = max(prices)
        rng = max_p - min_p
        if rng < 0.01:
            rng = max(abs(min_p) * 0.01, 1.0)
            min_p -= rng / 2
            max_p += rng / 2
            rng = max_p - min_p

        # 5% vertical padding
        pad = rng * 0.05
        min_p -= pad
        max_p += pad
        rng = max_p - min_p

        # Horizontal grid lines
        for i in range(5):
            y = mt + ch * i / 4
            pv = max_p - rng * i / 4
            canvas.create_line(ml, y, w - mr, y, fill="#1e293b", dash=(2, 4))
            canvas.create_text(
                ml - 4, y, text=f"{pv:.1f}",
                fill="#475569", font=("Consolas", 7), anchor="e",
            )

        # Vertical grid lines
        for i in range(1, 4):
            x = ml + cw * i / 4
            canvas.create_line(x, mt, x, mt + ch, fill="#1e293b", dash=(2, 4))

        # Compute plot points
        pts = []
        for i, price in enumerate(prices):
            x = ml + cw * i / max(n - 1, 1)
            y = mt + ch * (1 - (price - min_p) / rng)
            pts.append((x, y))

        # Line color based on direction
        if prices[-1] > prices[0] + 0.005:
            color = "#22d3ee"
        elif prices[-1] < prices[0] - 0.005:
            color = "#ef4444"
        else:
            color = "#94a3b8"

        # Draw the price line
        if len(pts) >= 2:
            coords = []
            for x, y in pts:
                coords.extend([x, y])
            canvas.create_line(*coords, fill=color, width=2, smooth=True)

        # Current price label (top-right)
        canvas.create_text(
            w - mr - 2, mt + 2, text=f"${prices[-1]:.2f}",
            fill=color, font=("Consolas", 9, "bold"), anchor="ne",
        )

        # Last-point dot
        if pts:
            lx, ly = pts[-1]
            r = 3
            canvas.create_oval(
                lx - r, ly - r, lx + r, ly + r,
                fill=color, outline="",
            )

        # Low / High annotations at bottom
        canvas.create_text(
            ml + 2, mt + ch + mb // 2 + 2,
            text=f"L:{min(prices):.2f}", fill="#475569",
            font=("Consolas", 7), anchor="w",
        )
        canvas.create_text(
            w - mr - 2, mt + ch + mb // 2 + 2,
            text=f"H:{max(prices):.2f}", fill="#475569",
            font=("Consolas", 7), anchor="e",
        )

    def _refresh_charts(self):
        """Periodic chart redraw — picks up tick and slider changes."""
        self._slider_programmatic = True
        try:
            for symbol in list(self._chart_canvases.keys()):
                canvas = self._chart_canvases.get(symbol)
                if canvas:
                    self._draw_chart(canvas, symbol)

                with self._server._live_prices_lock:
                    cur = self._server._live_prices.get(
                        symbol, SYMBOL_PRICES.get(symbol, 0.0),
                    )

                # Update price label with P/L color
                if symbol in self._chart_price_labels:
                    buy = 0.0
                    for h in MOCK_HOLDINGS:
                        if h["ITEM_COD"] == symbol:
                            try:
                                buy = float(h["BUY_UPR"])
                            except (ValueError, TypeError):
                                pass
                            break
                    if buy > 0 and cur >= buy:
                        fg = "#22d3ee"
                    elif buy > 0:
                        fg = "#ef4444"
                    else:
                        fg = "#e2e8f0"
                    self._chart_price_labels[symbol].config(
                        text=f"${cur:.2f}", fg=fg,
                    )

                # Sync slider position
                if symbol in self._chart_sliders:
                    self._chart_sliders[symbol].set(cur)
        except tk.TclError:
            pass
        finally:
            self._slider_programmatic = False

        self._root.after(1000, self._refresh_charts)

    # --------------------------------------------------------
    #  Status Bar
    # --------------------------------------------------------

    def _build_status_bar(self, parent):
        self._status_frame = tk.Frame(parent, relief=tk.SUNKEN, bd=1)
        self._status_frame.grid(row=2, column=0, sticky="ew")
        parent.rowconfigure(2, weight=0)

        self._status_label = ttk.Label(
            self._status_frame,
            text="Starting...",
            padding=(6, 2),
        )
        self._status_label.pack(side=tk.LEFT)

        self._uptime_label = ttk.Label(
            self._status_frame,
            text="Uptime: 0s",
            padding=(6, 2),
        )
        self._uptime_label.pack(side=tk.RIGHT)

        self._subs_label = ttk.Label(
            self._status_frame,
            text="Subs: 0",
            padding=(6, 2),
        )
        self._subs_label.pack(side=tk.RIGHT)

    def _refresh_status(self):
        """Periodic status bar refresh."""
        server = self._server

        # Connection status
        connected = server._client_sock is not None
        if connected:
            conn_text = "Client: connected"
            conn_fg = "#4ec9b0"
        else:
            conn_text = "Client: disconnected"
            conn_fg = "gray"
        self._conn_label.config(text=conn_text, foreground=conn_fg)

        # Uptime
        uptime = int(time.time() - server._start_time)
        hours = uptime // 3600
        minutes = (uptime % 3600) // 60
        seconds = uptime % 60
        if hours > 0:
            uptime_text = f"Uptime: {hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            uptime_text = f"Uptime: {minutes}m {seconds}s"
        else:
            uptime_text = f"Uptime: {seconds}s"
        self._uptime_label.config(text=uptime_text)

        # Subscriptions
        with server._real_subs_lock:
            sub_count = len(server._real_subs)
        self._subs_label.config(text=f"Subs: {sub_count}")

        # Status text
        host = server._host
        port = server._port
        status = f"TCP {host}:{port}"
        if server._logged_in:
            status += " | Logged in"
        else:
            status += " | Logged out"
        if server._api_connected:
            status += " | API OK"
        else:
            status += " | API disconnected"
        self._status_label.config(text=status)

        # Update current price display for selected symbol
        self._on_symbol_selected(None)

        self._root.after(1000, self._refresh_status)

    # --------------------------------------------------------
    #  Window Close
    # --------------------------------------------------------

    def _on_close(self):
        """Handle WM_DELETE_WINDOW — shutdown server, destroy root."""
        logging.info("GUI window closing...")
        try:
            self._server._shutdown()
        except Exception:
            pass
        try:
            self._root.destroy()
        except Exception:
            pass

    # --------------------------------------------------------
    #  Public: get TkTextHandler target widget
    # --------------------------------------------------------

    @property
    def log_text_widget(self):
        return self._log_text


# ============================================================
#  Main
# ============================================================

def main():
    # Determine paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ini_path = os.path.join(script_dir, "setting.ini")

    # Check setting.ini exists
    if not os.path.isfile(ini_path):
        # Try to show error in a basic tkinter dialog
        try:
            err_root = tk.Tk()
            err_root.withdraw()
            messagebox.showerror(
                "Config Not Found",
                f"setting.ini not found:\n{ini_path}\n\n"
                "Run proxy.py first to generate it,\n"
                "or create it manually.",
            )
            err_root.destroy()
        except Exception:
            pass
        print("=" * 50)
        print("  setting.ini not found: {}".format(ini_path))
        print("  Run proxy.py first to generate it,")
        print("  or create it manually.")
        print("=" * 50)
        sys.exit(1)

    # Load config
    cfg = load_config(ini_path)

    # Setup basic console logging first
    log_level = cfg.get("options", "log_level", fallback="INFO").upper()
    log_fmt = "%(asctime)s [%(levelname)s] %(message)s"
    log_datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format=log_fmt,
        datefmt=log_datefmt,
    )

    # Backend log handler
    if cfg.getboolean("backend", "enabled", fallback=False):
        backend_url = cfg.get("backend", "url",
                              fallback="http://127.0.0.1:8080/api/logs")
        backend_handler = BackendLogHandler(backend_url)
        logging.getLogger().addHandler(backend_handler)
        logging.info("Backend log handler enabled: %s", backend_url)

    # Startup banner (console)
    print()
    print("=" * 58)
    print("  [TEST MODE] Eugene OpenAPI Mock Server - GUI")
    print("=" * 58)
    print()

    host = cfg.get("server", "host", fallback="127.0.0.1")
    port = cfg.getint("server", "port", fallback=5959)
    api_key = cfg.get("server", "api_key", fallback="")

    logging.info("[TEST MODE] Eugene OpenAPI Mock Server (GUI) starting...")
    logging.info("Python %s (%d-bit)", sys.version.split()[0],
                 struct.calcsize("P") * 8)
    logging.info("No COM, no PyQt5, no admin - pure stdlib mock + tkinter GUI")

    if api_key:
        logging.info("API key authentication: enabled")
    else:
        logging.info("API key authentication: disabled (api_key empty)")

    # Create server
    server = EugeneMockServer(cfg)

    # Create tkinter root
    root = tk.Tk()

    # Build GUI
    gui = MockServerGUI(root, server)

    # Add TkTextHandler to logging
    tk_handler = TkTextHandler(gui.log_text_widget, root)
    tk_handler.setFormatter(logging.Formatter(log_fmt, datefmt=log_datefmt))
    logging.getLogger().addHandler(tk_handler)

    # Signal handler for graceful shutdown
    def signal_handler(_sig, _frame):
        logging.info("Signal received, shutting down...")
        root.after(0, gui._on_close)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start TCP server
    server.start_tcp()

    # Start dispatcher
    server.start_dispatcher()

    # Start backend heartbeat
    server.start_heartbeat(cfg)

    logging.info("Server ready on %s:%d. Waiting for client...", host, port)

    # Run tkinter mainloop (blocks until root.destroy())
    try:
        root.mainloop()
    except KeyboardInterrupt:
        logging.info("Ctrl+C received")
        server._shutdown()

    sys.exit(0)


if __name__ == "__main__":
    main()
