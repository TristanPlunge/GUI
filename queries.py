# queries.py
import pandas as pd
from sqlalchemy import text

def run_query(engine, filter_type, filter_value, start_date, end_date, selected_columns):
    cols = ["updated_at"] + selected_columns
    cols_str = ", ".join(cols)

    query = text(f"""
        SELECT {cols_str}
        FROM cp_device_metrics
        WHERE {filter_type} = :filter_value
          AND updated_at BETWEEN :start_date AND :end_date
        LIMIT 30000;
    """)

    params = {
        "filter_value": filter_value,
        "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S"),
    }

    return pd.read_sql(query, engine, params=params)
