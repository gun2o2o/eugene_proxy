# -*- coding: utf-8 -*-
"""
Eugene OpenAPI TCP Proxy Server (eugene_proxy.py)

32-bit conda env 에서 실행되며, 64-bit 자동매매 프로그램과
유진투자증권 Champion OpenAPI COM 컨트롤을 TCP 소켓으로 연결합니다.

Architecture:
  - Main thread: PyQt5 QApplication event loop + QAxWidget COM + QTimer polling
  - TCP listener thread: socket.accept() -> single client
  - TCP reader thread: reads client messages -> request_queue
  - QTimer: dequeues requests -> executes COM calls on main thread
  - COM events fire on main thread -> send response/event to client via TCP

Protocol:
  [4-byte big-endian uint32 length][JSON payload (UTF-8)]

Requirements:
  - Windows, 32-bit Python 3.8+
  - PyQt5, pywin32
  - Champion OpenAPI installed
"""

import configparser
import json
import logging
import os
import queue
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QTimer

import win32gui


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
#  Version Process
# ============================================================

VERSION_WINDOW_TITLE = "eugeneVersion"
VERSION_MSG_ID = 7422


class _VersionWindow(QMainWindow):
    """버전처리 메시지 수신용 숨겨진 윈도우. 화면에 표시되지 않음."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle(VERSION_WINDOW_TITLE)


def run_version_process(openapi_path, timeout_sec=30):
    """
    Champion OpenAPI 버전 패치 실행.
    숨겨진 윈도우의 hwnd 로 메시지를 수신하며, 화면에 아무것도 표시하지 않음.
    init.py 로 OCX 등록이 사전에 완료되어야 오류 없이 동작합니다.
    Returns (wparam, lparam). lparam == 1 이면 성공.
    """
    # 숨겨진 윈도우 생성 — winId() 로 네이티브 핸들만 확보, show() 안 함
    win = _VersionWindow()
    hwnd = int(win.winId())
    if hwnd == 0:
        raise RuntimeError("Cannot create version window handle")

    ver_exe = os.path.join(openapi_path, "ChampionOpenAPIVersionProcess.exe")
    cmd = f'"{ver_exe}" /{hwnd}'
    logging.info("Running version process: %s", cmd)
    subprocess.Popen(cmd)

    # GetMessage 블로킹 대기 (공식 샘플 방식)
    # timeout은 별도 타이머 스레드로 구현
    result_box = {"wparam": 0, "lparam": 0, "done": False, "error": None}

    def _timeout_watchdog():
        time.sleep(timeout_sec)
        if not result_box["done"]:
            result_box["error"] = f"Version process timeout ({timeout_sec}s)"
            try:
                import ctypes as _ct
                _ct.windll.user32.PostMessageW(hwnd, VERSION_MSG_ID, 0, 0)
            except Exception:
                pass

    watchdog = threading.Thread(target=_timeout_watchdog, daemon=True)
    watchdog.start()

    while True:
        msg = win32gui.GetMessage(hwnd, 0, 0)
        msg_data = msg[1]
        if msg_data[1] == VERSION_MSG_ID:
            result_box["wparam"] = msg_data[2]
            result_box["lparam"] = msg_data[3]
            result_box["done"] = True
            break

    # 정리
    win.close()

    if result_box["error"] and result_box["lparam"] == 0:
        raise RuntimeError(result_box["error"])

    logging.info("Version process complete: wparam=%d, lparam=%d", result_box["wparam"], result_box["lparam"])
    return result_box["wparam"], result_box["lparam"]


# ============================================================
#  Proxy Server
# ============================================================

class EugeneProxyServer:
    """
    TCP proxy server bridging COM control and 64-bit client.
    All COM calls happen on main thread via QTimer dequeue.
    """

    def __init__(self, cfg):
        self._cfg = cfg
        self._start_time = time.time()

        # Credentials
        self._user_id = cfg.get("credentials", "user_id")
        self._user_pw = cfg.get("credentials", "user_pw")
        self._cert_pw = cfg.get("credentials", "cert_pw")

        # Server
        self._host = cfg.get("server", "host", fallback="127.0.0.1")
        self._port = cfg.getint("server", "port", fallback=5959)

        # OpenAPI
        self._openapi_path = cfg.get("openapi", "openapi_path")
        self._com_prog_id = cfg.get(
            "openapi", "com_prog_id",
            fallback="CHAMPIONCOMMAGENT.ChampionCommAgentCtrl.1",
        )

        # Options
        self._login_timeout = cfg.getint("options", "login_timeout", fallback=30)
        self._version_timeout = cfg.getint("options", "version_timeout", fallback=30)
        self._tr_timeout = cfg.getint("options", "tr_timeout", fallback=10)
        self._poll_interval_ms = cfg.getint("options", "poll_interval_ms", fallback=10)

        # State
        self._logged_in = False
        self._api_connected = False
        self._shutting_down = False

        # COM control (created later in init_com)
        self._ctrl = None

        # TR/Real output mappings
        self._pending_tr = {}       # str(rq_id) -> {request_id, tr_code, outputs}
        self._real_output = {}      # str(real_id) -> {real_key: [fields]}

        # Thread-safe structures
        self._request_queue = queue.Queue()
        self._send_lock = threading.Lock()

        # TCP
        self._server_sock = None
        self._client_sock = None
        self._listener_thread = None
        self._reader_thread = None

        # QTimer (created in start_timer)
        self._timer = None

        # Method dispatch table
        self._dispatch = {
            "request_tr": self._handle_request_tr,
            "subscribe_real": self._handle_subscribe_real,
            "unsubscribe_real": self._handle_unsubscribe_real,
            "unsubscribe_all": self._handle_unsubscribe_all,
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
        }

    # ============================================================
    #  COM Initialization
    # ============================================================

    def init_com(self):
        """Create QAxWidget COM control and connect event signals."""
        logging.info("Loading COM control: %s", self._com_prog_id)
        self._ctrl = QAxWidget(self._com_prog_id)
        if not self._ctrl.control():
            raise RuntimeError("COM control load failed")
        logging.info("COM control loaded successfully")

        # Connect event signals
        self._ctrl.OnGetTranData.connect(self._on_tran_data)
        self._ctrl.OnGetRealData.connect(self._on_real_data)
        self._ctrl.OnAgentEventHandler.connect(self._on_agent_event)
        self._api_connected = True

    # ============================================================
    #  Version + Login
    # ============================================================

    def do_login(self, wparam):
        """
        버전처리 완료 후 로그인. wparam = 버전 키값.
        공식 샘플 방식: 항상 버전처리 먼저 → CommLogin(wparam, ...).
        """
        logging.info("Logging in with version key (wparam=%d) for user: %s", wparam, self._user_id)
        ret = self._ctrl.dynamicCall(
            "CommLogin(int, QString, QString, QString)",
            wparam, self._user_id, self._user_pw, self._cert_pw,
        )
        if ret != 0:
            err = self._ctrl.dynamicCall("GetLastErrMsg()")
            raise RuntimeError(f"Login failed: ret={ret}, err={err}")

        # CommLogin은 비동기일 수 있으므로 잠시 대기 후 상태 확인
        deadline = time.time() + self._login_timeout
        while time.time() < deadline:
            state = self._ctrl.dynamicCall("GetLoginState()")
            if state == 1:
                self._logged_in = True
                logging.info("Login successful")
                return
            QApplication.processEvents()
            time.sleep(0.5)

        err = self._ctrl.dynamicCall("GetLastErrMsg()")
        raise RuntimeError(f"Login timeout ({self._login_timeout}s): state={state}, err={err}")

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
        """Accept ONE client connection, then start reader thread."""
        while not self._shutting_down:
            try:
                self._server_sock.settimeout(1.0)
                client, addr = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            logging.info("Client connected: %s", addr)
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
    #  QTimer — Poll request queue on main thread
    # ============================================================

    def start_timer(self):
        """Start QTimer to poll request queue from main thread."""
        self._timer = QTimer()
        self._timer.timeout.connect(self._poll_requests)
        self._timer.start(self._poll_interval_ms)

    def _poll_requests(self):
        """Dequeue and dispatch requests. Called on main thread."""
        while not self._request_queue.empty():
            try:
                msg = self._request_queue.get_nowait()
            except queue.Empty:
                break
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

    def _handle_request_tr(self, req_id, params):
        """Bundled TR cycle: create rq_id -> set inputs -> request -> wait for event."""
        tr_code = params["tr_code"]
        inputs = params["inputs"]
        outputs = params["outputs"]
        next_key = params.get("next_key", "")
        request_cnt = params.get("request_cnt", 20)

        # Create request ID
        rq_id = self._ctrl.dynamicCall("CreateRequestID()")

        # Set input fields
        for field_id, value in inputs.items():
            self._ctrl.dynamicCall(
                "SetTranInputData(int, QString, QString, QString, QString)",
                rq_id, tr_code, "InRec1", field_id, str(value),
            )

        # Register pending TR (event handler will match by rq_id)
        self._pending_tr[str(rq_id)] = {
            "request_id": req_id,
            "tr_code": tr_code,
            "outputs": outputs,
        }

        # Fire request
        self._ctrl.dynamicCall(
            "RequestTran(int, QString, QString, int)",
            rq_id, tr_code, next_key, request_cnt,
        )

    def _handle_subscribe_real(self, req_id, params):
        """Register real-time data subscription."""
        real_id = params["real_id"]
        real_key = params["real_key"]
        fields = params["fields"]

        ret = self._ctrl.dynamicCall(
            "RegisterReal(int, QString)", int(real_id), real_key,
        )

        # Store output field mapping
        rid = str(real_id)
        if rid not in self._real_output:
            self._real_output[rid] = {}
        self._real_output[rid][real_key] = fields

        if ret == 1:
            self._send_result(req_id, {"status": "ok"})
        else:
            self._send_result(req_id, {"status": "ok"})
            logging.warning(
                "RegisterReal returned %d for real_id=%s real_key=%s",
                ret, real_id, real_key,
            )

    def _handle_unsubscribe_real(self, req_id, params):
        """Unregister real-time data."""
        real_id = params["real_id"]
        real_key = params["real_key"]

        self._ctrl.dynamicCall(
            "UnRegisterReal(int, QString)", int(real_id), real_key,
        )

        rid = str(real_id)
        if rid in self._real_output:
            self._real_output[rid].pop(real_key, None)

        self._send_result(req_id, {"status": "ok"})

    def _handle_unsubscribe_all(self, req_id, _params):
        """Unregister all real-time data."""
        self._ctrl.dynamicCall("AllUnRegisterReal()")
        self._real_output.clear()
        self._send_result(req_id, {"status": "ok"})

    def _handle_get_accounts(self, req_id, _params):
        """Get account list."""
        acc_info = self._ctrl.dynamicCall("GetAccInfo()")
        acc_cnt = self._ctrl.dynamicCall("GetAccCnt()")
        accounts = [a for a in acc_info.split(";") if a.strip()] if acc_info else []
        self._send_result(req_id, {"accounts": accounts, "count": acc_cnt})

    def _handle_heartbeat(self, req_id, _params):
        """Server status check."""
        uptime = time.time() - self._start_time
        self._send_result(req_id, {
            "server_running": True,
            "api_connected": self._api_connected,
            "logged_in": self._logged_in,
            "uptime": round(uptime, 2),
        })

    def _handle_get_login_state(self, req_id, _params):
        """Check login state."""
        state = self._ctrl.dynamicCall("GetLoginState()")
        self._send_result(req_id, {"state": state})

    def _handle_get_last_err_msg(self, req_id, _params):
        """Get last error message."""
        msg = self._ctrl.dynamicCall("GetLastErrMsg()")
        self._send_result(req_id, {"message": msg})

    def _handle_get_exp_code(self, req_id, params):
        """Short code -> standard code."""
        code = params["code"]
        result = self._ctrl.dynamicCall("GetExpCode(QString)", code)
        self._send_result(req_id, {"code": result})

    def _handle_get_sh_code(self, req_id, params):
        """Standard code -> short code."""
        code = params["code"]
        result = self._ctrl.dynamicCall("GetShCode(QString)", code)
        self._send_result(req_id, {"code": result})

    def _handle_get_name_by_code(self, req_id, params):
        """Code -> name."""
        code = params["code"]
        result = self._ctrl.dynamicCall("GetNameByCode(QString)", code)
        self._send_result(req_id, {"name": result})

    def _handle_get_sh_code_by_name(self, req_id, params):
        """Name -> short code."""
        name = params["name"]
        result = self._ctrl.dynamicCall("GetShCodeByName(QString)", name)
        self._send_result(req_id, {"code": result})

    def _handle_get_market_kubun(self, req_id, params):
        """Market type for code."""
        code = params["code"]
        result = self._ctrl.dynamicCall(
            "GetMarketKubun(QString, QString)", code, "",
        )
        self._send_result(req_id, {"market_kubun": result})

    def _handle_logout(self, req_id, _params):
        """Logout and terminate COM."""
        logging.info("Logout requested")
        self._ctrl.dynamicCall("CommLogout(QString)", self._user_id)
        self._ctrl.dynamicCall("CommTerminate(bool)", True)
        self._logged_in = False
        self._api_connected = False
        self._send_result(req_id, {"status": "ok"})

    def _handle_shutdown(self, req_id, _params):
        """Graceful shutdown: logout + close TCP + exit."""
        logging.info("Shutdown requested")
        self._send_result(req_id, {"status": "ok"})
        self._shutdown()

    # ============================================================
    #  COM Event Handlers (fire on main thread)
    # ============================================================

    def _on_tran_data(self, rq_id, block, block_len):
        """OnGetTranData event handler."""
        rq_key = str(rq_id)
        pending = self._pending_tr.get(rq_key)
        if pending is None:
            logging.warning("Unmatched TR response: rq_id=%s", rq_key)
            return

        request_id = pending["request_id"]
        tr_code = pending["tr_code"]
        outputs = pending["outputs"]

        # Parse output data
        result = {}
        for rec_name, fields in outputs.items():
            if rec_name == "OutRec1":
                rec_data = {}
                for field in fields:
                    val = self._ctrl.dynamicCall(
                        "GetTranOutputData(QString, QString, QString, int)",
                        rq_key, rec_name, field, 0,
                    )
                    rec_data[field] = val.strip() if isinstance(val, str) else str(val).strip()
                result[rec_name] = rec_data
            elif rec_name == "OutRec2":
                row_cnt = self._ctrl.dynamicCall(
                    "GetTranOutputRowCnt(QString, QString)", tr_code, rec_name,
                )
                rows = []
                if row_cnt > 0:
                    for i in range(row_cnt):
                        row_data = {}
                        for field in fields:
                            val = self._ctrl.dynamicCall(
                                "GetTranOutputData(QString, QString, QString, int)",
                                rq_key, rec_name, field, i,
                            )
                            row_data[field] = val.strip() if isinstance(val, str) else str(val).strip()
                        rows.append(row_data)
                result[rec_name] = rows

        # Send response
        self._send_result(request_id, result)

        # Cleanup
        self._ctrl.dynamicCall("ReleaseRqId(int)", rq_id)
        del self._pending_tr[rq_key]

    def _on_real_data(self, real_id, real_key, block, block_len):
        """OnGetRealData event handler."""
        if block_len <= 29:
            return

        rid = str(real_id)
        rkey = str(real_key)
        if rid not in self._real_output:
            return

        # Lookup fields - try direct key first, then short code
        fields = self._real_output[rid].get(rkey)
        if fields is None:
            sh_key = self._ctrl.dynamicCall("GetShCode(QString)", rkey)
            fields = self._real_output[rid].get(sh_key)
        if fields is None:
            return

        # Get field values
        data = {}
        for field in fields:
            val = self._ctrl.dynamicCall(
                "GetRealOutputData(QString, QString)", real_id, field,
            )
            data[field] = val.strip() if isinstance(val, str) else str(val).strip()

        self._send_event("real_data", {
            "real_id": rid,
            "real_key": rkey,
            "fields": data,
        })

    def _on_agent_event(self, event_type, n_param, str_param):
        """OnAgentEventHandler event handler."""
        logging.info(
            "Agent event: type=%d, n_param=%d, str_param=%s",
            event_type, n_param, str_param,
        )
        self._send_event("agent_event", {
            "event_type": event_type,
            "n_param": n_param,
            "str_param": str_param,
        })

        # Special handling
        if event_type == 50:
            logging.warning("Multi-login disconnect")
            self._logged_in = False
        elif event_type == 51:
            logging.warning("Socket closed by server")
            self._logged_in = False
            self._api_connected = False

    # ============================================================
    #  Shutdown
    # ============================================================

    def _shutdown(self):
        """Graceful shutdown sequence."""
        if self._shutting_down:
            return
        self._shutting_down = True
        logging.info("Shutting down...")

        # Unregister all real-time
        try:
            self._ctrl.dynamicCall("AllUnRegisterReal()")
        except Exception:
            pass

        # Logout
        if self._logged_in:
            try:
                self._ctrl.dynamicCall("CommLogout(QString)", self._user_id)
            except Exception:
                pass
            self._logged_in = False

        # Terminate COM
        try:
            self._ctrl.dynamicCall("CommTerminate(bool)", True)
        except Exception:
            pass
        self._api_connected = False

        # Close TCP
        self._close_client()
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None

        # Stop QTimer
        try:
            self._timer.stop()
        except Exception:
            pass

        # Quit app
        app = QApplication.instance()
        if app is not None:
            app.quit()

        logging.info("Shutdown complete")


# ============================================================
#  Main
# ============================================================

def _ensure_admin():
    """관리자 권한 확인. 아니면 안내 메시지 출력 후 종료."""
    import ctypes
    try:
        is_admin = ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        is_admin = False

    if is_admin:
        return

    print("=" * 50)
    print("  관리자 권한이 필요합니다.")
    print("  CMD 또는 터미널을 '관리자 권한으로 실행' 후")
    print("  다시 실행해주세요.")
    print("=" * 50)
    sys.exit(1)


def main():
    # 관리자 권한 확인 (버전처리 + COM 등록에 필요)
    _ensure_admin()

    # Determine paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ini_path = os.path.join(script_dir, "setting.ini")

    # Load config
    cfg = load_config(ini_path)

    # Setup logging
    log_level = cfg.get("options", "log_level", fallback="INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info("Eugene OpenAPI TCP Proxy Server starting...")
    logging.info("Python %s (%d-bit)", sys.version, struct.calcsize("P") * 8)

    # CRITICAL: chdir to OpenAPI path before COM load
    openapi_path = cfg.get("openapi", "openapi_path")
    os.chdir(openapi_path)
    logging.info("Working directory: %s", os.getcwd())

    # Create QApplication (must be on main thread, before COM)
    app = QApplication(sys.argv)

    # ── Step 1: 버전처리 먼저 (COM 로드 전) ──
    # 공식 샘플 방식: QAxWidget 생성 전에 버전처리를 완료해야
    # "lpDllEntryPoint Fail" 오류가 발생하지 않음
    skip_version = cfg.getboolean("options", "skip_version", fallback=False)
    version_timeout = cfg.getint("options", "version_timeout", fallback=30)

    if skip_version:
        logging.info("Version process skipped (skip_version=true)")
        wparam = 0
    else:
        logging.info("Running version process BEFORE COM load...")
        wparam, lparam = run_version_process(openapi_path, version_timeout)
        logging.info("Version process result: wparam=%d, lparam=%d", wparam, lparam)
        if lparam != 1:
            raise RuntimeError(f"Version patch failed: lparam={lparam}")

    # ── Step 2: COM 로드 (버전처리 완료 후) ──
    server = EugeneProxyServer(cfg)

    # Signal handler for graceful shutdown
    def signal_handler(_sig, _frame):
        logging.info("Signal received, shutting down...")
        server._shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.init_com()

    # ── Step 3: 로그인 ──
    server.do_login(wparam)

    # ── Step 4: TCP 서버 + 이벤트 루프 ──
    server.start_tcp()
    server.start_timer()

    logging.info("Server ready. Entering event loop...")

    # Enter Qt event loop (blocks until quit)
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
