import pandas as pd
import pytz
from sqlalchemy import text
from datetime import datetime
from ssh_db_connector import SSHDatabaseConnector
import time
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


        query = text(f"""
            SELECT *
            FROM cp_device_metrics
            WHERE {filter_type} = :filter_value
              AND updated_at BETWEEN :start_date AND :end_date
            LIMIT 30000;
        """)

        t0 = time.time()
        with self.engine.connect().execution_options(stream_results=True) as conn:
            t1 = time.time()
            result = conn.execute(query, params)
            self.logger(f"[TIMING] Query executed in {time.time() - t1:.2f} sec")

            t2 = time.time()
            rows = result.fetchall()
            self.logger(f"[TIMING] Rows fetched in {time.time() - t2:.2f} sec")

            t3 = time.time()
            df = pd.DataFrame(rows, columns=result.keys())
            self.logger(f"[TIMING] DataFrame built in {time.time() - t3:.2f} sec")

        self.logger(f"[TIMING] Total run_query time: {time.time() - t0:.2f} sec")

        # --- Now safe to process df as before ---
        if df.empty:
            self.logger("⚠️ No metrics: query returned 0 rows.")
            return df

        df.columns = [c.lower() for c in df.columns]

        # try a few common aliases if 'updated_at' is missing
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
            return df

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

        # Safe logging
        self.logger(f"updated_at dtype: {df['updated_at'].dtype}")
        if not df.empty:
            self.logger(f"updated_at sample: {df['updated_at'].head(3).to_list()}")
            self.logger(f"Columns after normalization: {list(df.columns)}")

        return df

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
