from dotenv import load_dotenv
from sqlalchemy import create_engine, text, event
import paramiko
import os
import socket as s
import threading
import select
from pathlib import Path


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
        print("[DEBUG] ForwardServer thread started")
        try:
            self.sock = s.socket(s.AF_INET, s.SOCK_STREAM)
            self.sock.setsockopt(s.SOL_SOCKET, s.SO_REUSEADDR, 1)
            self.sock.bind(("127.0.0.1", self.local_port))
            self.sock.listen(5)
            self.running = True
            while self.running:
                try:
                    client_sock, addr = self.sock.accept()
                    print(f"[DEBUG] ForwardServer accepted connection from {addr}")
                except OSError:
                    print("[DEBUG] ForwardServer socket closed, exiting run()")
                    break

                try:
                    chan = self.transport.open_channel(
                        "direct-tcpip", (self.remote_host, self.remote_port), addr
                    )
                except Exception as e:
                    print(f"[DEBUG] Failed to open channel: {e}")
                    client_sock.close()
                    continue

                # 🔑 this was missing:
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

        self.ssh_host = None
        self.ssh_port = None
        self.ssh_user = None
        self.ssh_password = None

        self.remote_bind_host = None
        self.remote_bind_port = None
        self.mysql_user = None
        self.mysql_password = None
        self.mysql_db = None
        self.db_columns = None

        # reserve free local port
        sock = s.socket()
        sock.bind(("127.0.0.1", 0))
        self.local_port = sock.getsockname()[1]
        sock.close()

    @staticmethod
    def create_env_template(filepath=".env"):
        """Create a .env file with placeholder values"""
        content = """# SSH connection
SSH_HOST=54.204.114.213
SSH_PORT=22
SSH_USER=
SSH_PASSWORD=

# Remote DB (host is as seen from SSH server)
REMOTE_BIND_HOST=
REMOTE_BIND_PORT=3306

# Database credentials
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DB=cparchivedb
"""
        path = Path(filepath)
        if not path.exists():
            with open(path, "w") as f:
                f.write(content)
            print(f"[INFO] Created {filepath} template. Please edit with your settings.")

    def get_env_params(self):
        try:
            load_dotenv()
            self.ssh_host = os.getenv("SSH_HOST")
            self.ssh_port = int(os.getenv("SSH_PORT", 22))
            self.ssh_user = os.getenv("SSH_USER")
            self.ssh_password = os.getenv("SSH_PASSWORD")
            self.remote_bind_host = os.getenv("REMOTE_BIND_HOST")
            self.remote_bind_port = int(os.getenv("REMOTE_BIND_PORT", 3306))
            self.mysql_user = os.getenv("MYSQL_USER")
            self.mysql_password = os.getenv("MYSQL_PASSWORD")
            self.mysql_db = os.getenv("MYSQL_DB")
        except Exception:
            print("Error: Could not load .env data.")

    def connect_over_ssh(self):
        try:
            self.get_env_params()

            # SSH client
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            connect_kwargs = dict(
                hostname=self.ssh_host,
                port=self.ssh_port,
                username=self.ssh_user,
                look_for_keys=False,
                allow_agent=False,
                timeout=15,
            )
            if self.ssh_password:
                connect_kwargs["password"] = self.ssh_password
            else:
                raise ValueError("SSH_PASSWORD not set in .env (required for password auth)")

            self.client.connect(**connect_kwargs)
            self.transport = self.client.get_transport()
            self.transport.set_keepalive(30)

            # Forwarder
            self.forwarder = SSHForwardServer(
                self.transport,
                self.local_port,
                self.remote_bind_host,
                self.remote_bind_port,
            )
            self.forwarder.start()

            # SQLAlchemy engine
            conn_str = (
                f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
                f"@127.0.0.1:{self.local_port}/{self.mysql_db}"
            )
            self.engine = create_engine(
                conn_str,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    "connect_timeout": 10,
                    "read_timeout": 10,
                    "write_timeout": 10,
                    "charset": "utf8mb4",
                },
            )

            @event.listens_for(self.engine, "connect")
            def set_session_settings(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("SET SESSION MAX_EXECUTION_TIME=29999;")
                cursor.close()

            # quick health check
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                self.db_columns = conn.execute(
                    text("SHOW COLUMNS FROM cp_device_metrics;")
                )

            return self.engine
        except Exception as e:
            print(f"[ERROR] Connection failed: {e}")
            SSHDatabaseConnector.create_env_template()
            raise

    def disconnect(self):
        print("[DEBUG] Disconnect called")
        if self.engine:
            print("[DEBUG] Disposing engine")
            self.engine.dispose()
            self.engine = None
        if self.forwarder:
            print("[DEBUG] Stopping forwarder thread")
            self.forwarder.stop()
            self.forwarder.join(timeout=2.0)
            print(f"[DEBUG] Forwarder alive after join? {self.forwarder.is_alive()}")
            self.forwarder = None
        if self.transport:
            print("[DEBUG] Closing transport")
            self.transport.close()
            self.transport = None
        if self.client:
            print("[DEBUG] Closing SSH client")
            self.client.close()
            self.client = None


