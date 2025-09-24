import pandas as pd
import pytz
from sqlalchemy import text
from datetime import datetime
from ssh_db_connector import SSHDatabaseConnector

LA_TZ = pytz.timezone("America/Los_Angeles")

class QueryManager:
    def __init__(self, logger=None, units="f", root=None):
        """
        :param logger: optional logging function
        :param units: "c" or "f" for temperature output
        """
        self.connector = None
        self.engine = None
        self.logger = logger or (lambda msg: print(msg))
        self.units = units.lower()
        self.root = root
        # (earliest_local_naive_datetime, latest_local_naive_datetime)
        self.timestamp_range = (None, None)

    def warm_up(self):
        """
        Run a cheap query to force engine/tunnel init and warm MySQL's buffer pool.
        """
        if not self.connect():
            return

        today = datetime.now().date()
        start = f"{today} 00:00:00"
        end = f"{today} 23:59:59"

        query = text("""
                SELECT updated_at
                FROM cp_device_metrics
                WHERE updated_at IS NOT NULL
                LIMIT 1;
        """)
        import time
        st = time.time()
        try:
            with self.engine.connect() as conn:
                conn.execute(query, {"s": start, "e": end})
            self.logger("✅ Warm-up query executed.")
        except Exception as e:
            self.logger(f"⚠️ Warm-up failed: {e}")
        print(f"Warm-up query execution time: {time.time()-st} sec.")
    # -------------------------------
    # Connection handling
    # -------------------------------
    def connect(self):
        if not self.engine:
            self.logger("Connecting to database...")
            self.connector = SSHDatabaseConnector()
            engine = self.connector.connect_over_ssh(parent=self.root)
            if engine is None:  # cancelled
                self.logger("⚠️ Connection cancelled by user.")
                return False
            self.engine = engine
            self.logger("Database connection established.")
        return True

    # -------------------------------
    # Main query
    # -------------------------------
    def run_query(self, filter_type, filter_value, start_date_str, end_date_str, selected_columns):
        self.connect()
        if not self.engine:
            return pd.DataFrame()

        # Parse inputs as LA-local calendar days, then convert to UTC for SQL
        start_la = LA_TZ.localize(datetime.fromisoformat(start_date_str)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_la = LA_TZ.localize(datetime.fromisoformat(end_date_str)).replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        params = {
            "filter_value": filter_value,
            "start_date": start_la.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end_la.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
        }

        # --- Always get global earliest/latest timestamps ---
        range_query = text(f"""
            SELECT MIN(updated_at) AS earliest, MAX(updated_at) AS latest
            FROM cp_device_metrics
            WHERE {filter_type} = :filter_value
        """)
        with self.engine.connect() as conn:
            row = conn.execute(range_query, {"filter_value": filter_value}).first()
            if row and row[0] and row[1]:
                earliest = pd.to_datetime(row[0]).tz_localize("UTC").tz_convert(LA_TZ).tz_localize(None)
                latest = pd.to_datetime(row[1]).tz_localize("UTC").tz_convert(LA_TZ).tz_localize(None)
                self.timestamp_range = (earliest, latest)
                self.logger(f"[RANGE] Earliest={earliest}, Latest={latest}")
            else:
                self.timestamp_range = (None, None)
                self.logger("[RANGE] No data at all for this filter")

        # --- Then check inside the requested window ---
        check_query = text(f"""
            SELECT EXISTS (
                SELECT 1
                FROM cp_device_metrics
                WHERE {filter_type} = :filter_value
                  AND updated_at BETWEEN :start_date AND :end_date
            ) AS has_data;
        """)
        with self.engine.connect() as conn:
            has_data = conn.execute(check_query, params).scalar()

        if not has_data:
            range_query = text(f"""
                SELECT MIN(updated_at) AS earliest, MAX(updated_at) AS latest
                FROM cp_device_metrics
                WHERE {filter_type} = :filter_value
            """)
            with self.engine.connect() as conn:
                row = conn.execute(range_query, {"filter_value": filter_value}).first()

            if row and row[0] and row[1]:
                earliest = (
                    pd.to_datetime(row[0])
                    .tz_localize("UTC").tz_convert(LA_TZ).tz_localize(None)
                )
                latest = (
                    pd.to_datetime(row[1])
                    .tz_localize("UTC").tz_convert(LA_TZ).tz_localize(None)
                )
                raise NoDataInWindow(earliest, latest)
            else:
                raise NoDataInWindow()  # no data at all

        # --- Main data query ---
        cols = ", ".join(selected_columns) if selected_columns else "*"
        query = text(f"""
            SELECT *
            FROM cp_device_metrics
            WHERE {filter_type} = :filter_value
              AND updated_at BETWEEN :start_date AND :end_date
            LIMIT 30000;
        """)

        with self.engine.connect().execution_options(stream_results=True) as conn:
            result = conn.execute(query, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # --- Continue with normalization ---
        if df.empty:
            self.logger("⚠️ No metrics: query returned 0 rows (unexpected).")
            return pd.DataFrame()

        df.columns = [c.lower() for c in df.columns]

        # alias fix
        if "updated_at" not in df.columns:
            alias_map = {
                "updatedat": "updated_at",
                "update_at": "updated_at",
                "timestamp": "updated_at",
                "ts": "updated_at",
                "time": "updated_at",
                "created_at": "updated_at",
            }
            for src, dst in alias_map.items():
                if src in df.columns:
                    df.rename(columns={src: dst}, inplace=True)
                    break

        if "updated_at" not in df.columns:
            self.logger(f"⚠️ No metrics: 'updated_at' column not found. Columns: {list(df.columns)}")
            return pd.DataFrame()

        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
        df = df.dropna(subset=["updated_at"])
        if df.empty:
            self.logger("⚠️ No metrics: all timestamps invalid (NaT) or filtered out.")
            return pd.DataFrame()

        df = df.sort_values("updated_at")
        df = df.dropna(axis=1, how="all")

        # Convert to tz-naive LA time for plotting
        df["updated_at"] = df["updated_at"].dt.tz_convert(LA_TZ).dt.tz_localize(None)

        # --- Unit normalization ---
        if "fan_tach_rpm" in df.columns:
            df["fan_tach_rpm"] = df["fan_tach_rpm"] / 100.0

        temp_cols = [c for c in df.columns if c.endswith("_temp_c")]
        if self.units == "f":
            for col in temp_cols:
                new_col = col.replace("_temp_c", "_temp_f")
                df[new_col] = (df[col] * 9.0 / 5.0 + 32.0).round(3)
            if temp_cols:
                df.drop(columns=temp_cols, inplace=True)

        self.logger(f"updated_at dtype: {df['updated_at'].dtype}")
        if not df.empty:
            self.logger(f"updated_at sample: {df['updated_at'].head(3).to_list()}")
            self.logger(f"Columns after normalization: {list(df.columns)}")

        # ✅ Always return a DataFrame, never None
        return df if df is not None else pd.DataFrame()



    # -------------------------------
    # Optional: expose a getter
    # -------------------------------
    def get_timestamp_range(self):
        """Return (earliest_local_naive, latest_local_naive) or (None, None)."""
        return self.timestamp_range

    # -------------------------------
    # Close connections
    # -------------------------------
    def close(self):
        """Close database connections cleanly."""
        if self.connector:
            try:
                self.connector.disconnect()
            except Exception:
                pass
        if self.engine:
            try:
                self.engine.dispose()
            except Exception:
                pass

class NoDataInWindow(Exception):
    def __init__(self, earliest=None, latest=None):
        if earliest is None or latest is None:
            msg = "No data exists for this user/device at all."
        else:
            msg = f"No data in requested window.\nData detected in ranges outside your provided range.\n{earliest:%Y-%m-%d} to {latest:%Y-%m-%d}"
        super().__init__(msg)
        self.earliest = earliest
        self.latest = latest
