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
        """Run query and return DataFrame with normalized units and clean column labels."""
        self.connect()
        if not self.engine:
            return pd.DataFrame()
        # Parse user inputs and expand to full days
        start_date = datetime.fromisoformat(start_date_str).replace(hour=0, minute=0, second=0)
        end_date = datetime.fromisoformat(end_date_str).replace(hour=23, minute=59, second=59)

        query = text(f"""
            SELECT *
            FROM cp_device_metrics
            WHERE {filter_type} = :filter_value
              AND updated_at BETWEEN :start_date AND :end_date
            LIMIT 30000;
        """)

        params = {
            "filter_value": filter_value,
            "start_date": start_date.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end_date.astimezone(pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"),
        }

        df = pd.read_sql(query, self.engine, params=params)

        # ✅ Ensure updated_at is tz-aware UTC
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)

        # Drop rows with invalid timestamps
        df = df.dropna(subset=["updated_at"])

        # Sort chronologically
        df = df.sort_values("updated_at")

        # Drop columns that are entirely NaN
        df = df.dropna(axis=1, how="all")

        # ✅ Convert to tz-naive Los Angeles time for Matplotlib
        df["updated_at"] = df["updated_at"].dt.tz_convert(LA_TZ).dt.tz_localize(None)

        # -------------------------------
        # Unit Normalization
        # -------------------------------
        # Fan tach scaling
        if "fan_tach_rpm" in df.columns:
            df["fan_tach_rpm (RPM/100)"] = df["fan_tach_rpm"] / 100.0
            df.drop(columns=["fan_tach_rpm"], inplace=True)

        # Temperature conversions
        temp_cols = [c for c in df.columns if c.endswith("_temp_c")]
        for col in temp_cols:
            if self.units == "f":
                new_col = col.replace("_temp_c", "_temp_f")
                df[new_col] = (df[col] * 9.0 / 5.0 + 32.0).round(3)
                df.drop(columns=[col], inplace=True)

        # ✅ Final debug logging
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
