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

    def warm_up(self):
        """
        Run cheap queries to warm MySQL's buffer pool and engine.
        """
        if not self.connect():
            return


        st = time.time()
        try:
            with self.engine.connect() as conn:
                self.logger("Warm-up queries executed.")
                # force scan of earliest and latest index entries
                conn.execute(text("SELECT 1 from cp_device_metrics m JOIN (SELECT device_name FROM cp_device LIMIT 1) d ON m.device_name = d.device_name LIMIT 1;"))

        except Exception as e:
            self.logger(f"Warm-up failed: {e}")
        print(f"Warm-up query execution time: {time.time() - st:.3f} sec.")

    # -------------------------------
    # Connection handling
    # -------------------------------
    def connect(self):
        if not self.engine:
            self.connector = SSHDatabaseConnector()
            engine = self.connector.connect_over_ssh(parent=self.root)
            if engine is None:  # cancelled
                self.logger("Connection cancelled by user.")
                return False
            self.engine = engine
        return True

    # -------------------------------
    # Main query
    # -------------------------------
    def run_query(self, filter_type, filter_value, start_date_str, end_date_str):
        self.connect()
        if not self.engine:
            return pd.DataFrame()

        # --- Parse dates ---
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

        # --- Check inside requested window ---
        if filter_type == "esp_ble_id":
            check_query = text("""
                               SELECT EXISTS (SELECT 1
                                              FROM cp_device_metrics m
                                                       JOIN (SELECT DISTINCT device_name
                                                             FROM cparchivedb.cp_device
                                                             WHERE esp_ble_id = :filter_value) d
                                                            ON m.device_name = d.device_name
                                              WHERE m.updated_at BETWEEN :start_date AND :end_date) AS has_data;
                               """)
        else:
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
            with self.engine.connect() as conn:
                # before window
                q_before = text("""
                    SELECT m.updated_at
                    FROM cp_device_metrics m
                    WHERE {cond}
                      AND m.updated_at < :start_date
                    ORDER BY m.updated_at DESC
                    LIMIT 1
                """.format(
                    cond="m.device_name IN (SELECT DISTINCT device_name FROM cp_device WHERE esp_ble_id = :filter_value)"
                    if filter_type == "esp_ble_id" else f"m.{filter_type} = :filter_value"))

                row = conn.execute(q_before, params).scalar()
                if row:
                    before = (pd.to_datetime(row)
                              .tz_localize("UTC").tz_convert(LA_TZ).tz_localize(None))

                # after window
                q_after = text("""
                    SELECT m.updated_at
                    FROM cp_device_metrics m
                    WHERE {cond}
                      AND m.updated_at > :end_date
                    ORDER BY m.updated_at ASC
                    LIMIT 1
                """.format(
                    cond="m.device_name IN (SELECT DISTINCT device_name FROM cp_device WHERE esp_ble_id = :filter_value)"
                    if filter_type == "esp_ble_id" else f"m.{filter_type} = :filter_value"))

                row = conn.execute(q_after, params).scalar()
                if row:
                    after = (pd.to_datetime(row)
                             .tz_localize("UTC").tz_convert(LA_TZ).tz_localize(None))

        if filter_type == "esp_ble_id":
            query = text(f"""
                WITH device AS (
                    SELECT DISTINCT device_name
                    FROM cparchivedb.cp_device
                    WHERE esp_ble_id = :filter_value
                )
                SELECT m.*
                FROM cp_device_metrics m
                JOIN device d ON m.device_name = d.device_name
                WHERE m.updated_at BETWEEN :start_date AND :end_date
                ORDER BY m.updated_at DESC
                LIMIT 60000;
            """)
        else:
            query = text(f"""
                SELECT *
                FROM cp_device_metrics
                WHERE {filter_type} = :filter_value
                  AND updated_at BETWEEN :start_date AND :end_date
                ORDER BY updated_at DESC
                LIMIT 60000;
            """)

        with self.engine.connect().execution_options(stream_results=True) as conn:
            result = conn.execute(query, params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # --- Normalize ---
        if df.empty:
            self.logger("No metrics: query returned 0 rows (unexpected).")
            raise NoDataFound(
                f"No data found for {filter_type}={filter_value} "
                f"between {start_date_str} and {end_date_str}"
            )
            return pd.DataFrame()

        df.columns = [c.lower() for c in df.columns]

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
            self.logger(f"No metrics: 'updated_at' column not found. Columns: {list(df.columns)}")
            return pd.DataFrame()

        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
        df = df.dropna(subset=["updated_at"])
        if df.empty:
            self.logger("No metrics: all timestamps invalid (NaT) or filtered out.")
            return pd.DataFrame()

        df = df.sort_values("updated_at")
        df = df.dropna(axis=1, how="all")

        # Convert to tz-naive LA time for plotting
        df["updated_at"] = df["updated_at"].dt.tz_convert(LA_TZ).dt.tz_localize(None)

        # Unit normalization
        if "fan_tach_rpm" in df.columns:
            df["fan_tach_rpm"] = df["fan_tach_rpm"] / 100.0

        temp_cols = [c for c in df.columns if c.endswith("_temp_c")]
        if self.units == "f":
            for col in temp_cols:
                new_col = col.replace("_temp_c", "_temp_f")
                df[new_col] = (df[col] * 9.0 / 5.0 + 32.0).round(3)
            if temp_cols:
                df.drop(columns=temp_cols, inplace=True)

        return df

    def get_date_ranges(self, filter_type, filter_value):
        if not self.connect():
            return []

        if not filter_value:
            return []

        if filter_type == "esp_ble_id":
            sql = text("""
                       SELECT DISTINCT DATE (m.updated_at) AS day
                       FROM cp_device_metrics m
                           JOIN cparchivedb.cp_device d
                       ON m.device_name = d.device_name
                       WHERE d.esp_ble_id = :filter_value
                       ORDER BY day
                       """)
        else:
            sql = text(f"""
                SELECT DISTINCT DATE(updated_at) AS day
                FROM cp_device_metrics
                WHERE {filter_type} = :filter_value
                ORDER BY day
            """)

        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"filter_value": filter_value}).fetchall()

        if not rows:
            return []

        days = pd.Series(pd.to_datetime([r[0] for r in rows])).sort_values().reset_index(drop=True)

        # Build contiguous ranges
        ranges = []
        start = end = days.iloc[0]
        for d in days.iloc[1:]:
            if (d - end).days == 1:
                end = d
            else:
                ranges.append((start, end))
                start = end = d
        ranges.append((start, end))

        return ranges

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


class NoDataFound(Exception):
    """Raised when no rows were found for the given filter/range."""
    pass
