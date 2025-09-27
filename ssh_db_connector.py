import paramiko
import socket as s
import threading, select
import keyring
from sqlalchemy import create_engine
from env_editor import EnvEditor

SERVICE_NAME = "PlungeTubApp"


class SSHForwardServer(threading.Thread):
    def __init__(self, transport, local_port, remote_host, remote_port):
        super().__init__(daemon=True)
        self.transport = transport
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.sock = None
        self.running = False

    def run(self):
        try:
            self.sock = s.socket(s.AF_INET, s.SOCK_STREAM)
            self.sock.setsockopt(s.SOL_SOCKET, s.SO_REUSEADDR, 1)
            self.sock.bind(("127.0.0.1", self.local_port))
            self.sock.listen(5)
            self.running = True
            while self.running:
                try:
                    client_sock, addr = self.sock.accept()
                except OSError:
                    break

                try:
                    chan = self.transport.open_channel(
                        "direct-tcpip", (self.remote_host, self.remote_port), addr
                    )
                except Exception as e:
                    print(f"[DEBUG] Failed to open channel: {e}")
                    client_sock.close()
                    continue

                threading.Thread(
                    target=self.handler, args=(client_sock, chan), daemon=True
                ).start()
        finally:
            print("[DEBUG] ForwardServer thread exiting")

    def stop(self):
        print("[DEBUG] ForwardServer.stop() called")
        self.running = False
        if self.sock:
            try:
                tmp = s.socket(s.AF_INET, s.SOCK_STREAM)
                tmp.connect(("127.0.0.1", self.local_port))
                tmp.close()
                print("[DEBUG] Dummy connect sent to unblock accept()")
            except Exception as e:
                print(f"[DEBUG] Dummy connect failed: {e}")
            try:
                self.sock.close()
                print("[DEBUG] Socket closed in stop()")
            except Exception as e:
                print(f"[DEBUG] Socket close failed: {e}")
            self.sock = None

    def handler(self, client_sock, chan):
        try:
            while True:
                r, _, _ = select.select([client_sock, chan], [], [])
                if client_sock in r:
                    data = client_sock.recv(1024)
                    if not data:
                        break
                    chan.sendall(data)
                if chan in r:
                    data = chan.recv(1024)
                    if not data:
                        break
                    client_sock.sendall(data)
        finally:
            client_sock.close()
            chan.close()

class SSHDatabaseConnector:
    def __init__(self):
        self.engine = None
        self.client = None
        self.transport = None
        self.forwarder = None
        self.params = {}

        # Reserve a free local port for forwarding
        sock = s.socket()
        sock.bind(("127.0.0.1", 0))
        self.local_port = sock.getsockname()[1]
        sock.close()

    def get_env_params(self):
        """Load params from keyring into self.params"""
        required = [
            "SSH_HOST", "SSH_PORT", "SSH_USER", "SSH_PASSWORD",
            "REMOTE_BIND_HOST", "REMOTE_BIND_PORT",
            "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DB"
        ]
        self.params = {}
        for k in required:
            val = keyring.get_password(SERVICE_NAME, k)
            self.params[k] = val
        return required

    def connect_over_ssh(self, parent=None):
        required = self.get_env_params()
        missing = [k for k in required if not self.params.get(k)]

        # If missing, open EnvEditor
        if missing and parent is not None:
            editor = EnvEditor(parent, required)
            parent.wait_window(editor)  # wait until Save or Cancel
            self.get_env_params()
            missing = [k for k in required if not self.params.get(k)]
            if missing:
                raise RuntimeError(f"Missing required fields after editor: {', '.join(missing)}")
        # --- SSH client setup ---
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.client.connect(
                hostname=self.params["SSH_HOST"],
                port=int(self.params["SSH_PORT"]),
                username=self.params["SSH_USER"],
                password=self.params["SSH_PASSWORD"],
                look_for_keys=False,
                allow_agent=False,
                timeout=15,
            )
        except paramiko.AuthenticationException:
            raise RuntimeError("SSH authentication failed – please check your username/password")
        except paramiko.SSHException as e:
            raise RuntimeError(f"SSH error: {e}")

        self.transport = self.client.get_transport()
        self.transport.set_keepalive(30)

        # --- Start forwarder ---
        self.forwarder = SSHForwardServer(
            self.transport,
            self.local_port,
            self.params["REMOTE_BIND_HOST"],
            int(self.params["REMOTE_BIND_PORT"])
        )
        self.forwarder.start()

        # --- SQLAlchemy engine ---
        db_url = (
            f"mysql+mysqldb://{self.params['MYSQL_USER']}:{self.params['MYSQL_PASSWORD']}"
            f"@127.0.0.1:{self.local_port}/{self.params['MYSQL_DB']}"
        )


        self.engine = create_engine(
            db_url,
            pool_size=5,  # keep 5 connections ready
            max_overflow=10,  # allow 10 extra if needed
            pool_recycle=1800,  # recycle every 30 min to avoid "server has gone away"
            pool_pre_ping=True,  # validate connection before using it
        )


        return self.engine

    def disconnect(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
        if self.forwarder:
            self.forwarder.stop()
            self.forwarder.join(timeout=2.0)
            self.forwarder = None
        if self.transport:
            self.transport.close()
            self.transport = None
        if self.client:
            self.client.close()
            self.client = None
        print("[DEBUG] Disconnected cleanly")
