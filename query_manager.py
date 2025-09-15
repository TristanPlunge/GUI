import pandas as pd
import pytz
from sqlalchemy import text
from datetime import datetime
from ssh_db_connector import SSHDatabaseConnector

LA_TZ = pytz.timezone("America/Los_Angeles")

class QueryManager:
    def __init__(self, logger=None):
        self.connector = None
        self.engine = None
        self.logger = logger or (lambda msg: print(msg))

    def connect(self):
        """Ensure a database connection is established."""
        if not self.engine:
            self.logger("Connecting to database...")
            self.connector = SSHDatabaseConnector()
            self.engine = self.connector.connect_over_ssh()
            self.logger("Database connection established.")

    def run_query(self, filter_type, filter_value, start_date_str, end_date_str, selected_columns):
        """Run query and return DataFrame with clean LA-time updated_at datetimes."""
        self.connect()

        # Parse user inputs and expand to full days
        start_date = datetime.fromisoformat(start_date_str).replace(hour=0, minute=0, second=0)
        end_date = datetime.fromisoformat(end_date_str).replace(hour=23, minute=59, second=59)

        cols = ["updated_at"] + selected_columns
        cols_str = ", ".join(cols)

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

        # --- Debug prints ---
        self.logger(f"updated_at dtype: {df['updated_at'].dtype}")
        if not df.empty:
            self.logger(f"updated_at sample: {df['updated_at'].head(3).to_list()}")
            self.logger(f"Non-null counts:\n{df.notna().sum()}")

        return df

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
