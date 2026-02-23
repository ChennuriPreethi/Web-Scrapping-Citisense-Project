import pandas as pd
import numpy as np
from scipy import stats
import pyodbc
import toml

# ----------------------------
# SQL SERVER CONFIG
# ----------------------------
def get_sql_connection_strings():
    secrets = toml.load(".streamlit/secrets.toml")["db1"]

    SERVER   = secrets["server"]
    DB_NAME  = secrets["database"]
    USER     = secrets["username"]
    PASS     = secrets["password"]
    DRIVER   = secrets["driver"]

    # Linux + ODBC Driver 18 + SQL Authentication
    CONN_STR_MASTER = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        "DATABASE=master;"
        f"UID={USER};"
        f"PWD={PASS};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    CONN_STR_DB = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={USER};"
        f"PWD={PASS};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    return CONN_STR_MASTER, CONN_STR_DB, DB_NAME

CONN_STR_MASTER, CONN_STR_DB, DATABASE = get_sql_connection_strings()
TABLE_NAME = "traffic_data"
DB_NAME = "trafficDB"

# PREPROCESSING FUNCTION

def load_and_preprocess_data():
    # Load datasets (from current folder)
    site_df = pd.read_csv("site_details.csv")
    aadf_df = pd.read_csv("aadf_details.csv")

    # --- Missing Data Handling ---
    # Sort then forward fill AADF
    aadf_df = aadf_df.sort_values(['count_point_id', 'year']).ffill()
    # Drop rows with missing key in site data
    site_df = site_df.dropna(subset=['count_point_id'])

    # --- Data Type Conversions ---
    site_df['count_point_id'] = site_df['count_point_id'].astype(int)
    aadf_df['count_point_id'] = aadf_df['count_point_id'].astype(int)

    # Keep year as int (good for SQL + YoY)
    aadf_df['year'] = aadf_df['year'].astype(int)

    if 'count_method' in aadf_df.columns:
        aadf_df['count_method'] = (
            aadf_df['count_method']
            .astype(str)
            .str.lower()
            .str.strip()
            .astype('category')
        )

    # --- Remove Duplicates ---
    site_df = site_df.drop_duplicates()
    aadf_df = aadf_df.drop_duplicates()

    # --- Identify vehicle count columns ---
    drop_cols = {'year', 'count_method', 'count_point_id'}
    count_cols = [c for c in aadf_df.columns if c not in drop_cols]

    # --- Feature Engineering: YoY Growth ---
    aadf_df = aadf_df.sort_values(['count_point_id', 'year'])
    for col in count_cols:
        aadf_df[f'{col}_yoy_growth'] = (
            aadf_df
            .groupby('count_point_id')[col]
            .pct_change()
        )

    aadf_df['total_traffic'] = aadf_df[count_cols].sum(axis=1)
    aadf_df['total_traffic_yoy_growth'] = (
        aadf_df
        .groupby('count_point_id')['total_traffic']
        .pct_change()
    )

    # --- Merge with site details ---
    merged_df = aadf_df.merge(site_df, on='count_point_id', how='left')

    # --- Outlier Detection + Scaling ---
    for col in count_cols + ['total_traffic']:
        # Z-score for outlier detection (fill NaNs with mean first)
        filled = merged_df[col].fillna(merged_df[col].mean())
        merged_df[f'{col}_zscore'] = np.abs(stats.zscore(filled))

        # Standard scaling
        mean_val = merged_df[col].mean()
        std_val = merged_df[col].std()
        if std_val == 0 or pd.isna(std_val):
            merged_df[f'{col}_scaled'] = 0
        else:
            merged_df[f'{col}_scaled'] = (merged_df[col] - mean_val) / std_val

    # --- Reset index and tidy ---
    merged_df = merged_df.sort_values(['count_point_id', 'year'])
    merged_df = merged_df.set_index(['count_point_id', 'year'])
    processed_df = merged_df.reset_index()

    # --- Fill numeric nulls with 0 ---
    numeric_cols = processed_df.select_dtypes(include=[np.number]).columns
    processed_df[numeric_cols] = processed_df[numeric_cols].fillna(0)

    # --- Handle categorical and object columns ---
    cat_cols = processed_df.select_dtypes(include=['category']).columns
    if len(cat_cols) > 0:
        processed_df[cat_cols] = processed_df[cat_cols].astype(object)

    obj_cols = processed_df.select_dtypes(include=['object']).columns
    processed_df[obj_cols] = processed_df[obj_cols].fillna('unknown')

    # Ensure year is int (for SQL)
    if np.issubdtype(processed_df['year'].dtype, np.datetime64):
        processed_df['year'] = processed_df['year'].dt.year.astype(int)

    # Ensure count_method is plain string (no category) if present
    if 'count_method' in processed_df.columns:
        processed_df['count_method'] = processed_df['count_method'].astype(str)

    # Ensure all floats are real floats (replace 'unknown' or '' with 0)
    for col in processed_df.columns:
        if processed_df[col].dtype == object:
            # Try convert to numeric
            processed_df[col] = pd.to_numeric(processed_df[col], errors='ignore')

    # Convert any remaining objects that should be numeric
    numeric_like_cols = [
        c for c in processed_df.columns
        if any(x in c for x in ["yoy", "zscore", "scaled", "traffic"])
    ]

    for col in numeric_like_cols:
        processed_df[col] = pd.to_numeric(processed_df[col], errors="coerce").fillna(0)

    processed_df.replace([np.inf, -np.inf], 0, inplace=True)

    return processed_df

# SQL SERVER HELPERS

def ensure_database():
    conn = pyodbc.connect(CONN_STR_MASTER)
    cur = conn.cursor()
    cur.execute(f"""
        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{DB_NAME}')
        BEGIN
            CREATE DATABASE {DB_NAME};
        END;
    """)
    conn.commit()
    conn.close()


def map_dtype_to_sql(dtype):
    """Map pandas dtype to a SQL Server column type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    if pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2"
    # Fallback for strings, categories, mixed, etc.
    return "NVARCHAR(255)"


def create_table_for_dataframe(df, table_name):
    """Drop and recreate a SQL Server table based on DataFrame schema."""
    conn = pyodbc.connect(CONN_STR_DB)
    cur = conn.cursor()

    # Drop if exists
    cur.execute(f"""
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        BEGIN
            DROP TABLE {table_name};
        END;
    """)

    # Build CREATE TABLE
    col_defs = []
    for col, dtype in df.dtypes.items():
        sql_type = map_dtype_to_sql(dtype)
        col_defs.append(f"[{col}] {sql_type}")

    create_stmt = f"CREATE TABLE {table_name} ({', '.join(col_defs)});"
    cur.execute(create_stmt)
    conn.commit()
    conn.close()


def insert_dataframe(df, table_name):
    """Insert all rows from DataFrame into SQL Server table."""
    conn = pyodbc.connect(CONN_STR_DB)
    cur = conn.cursor()

    cols = list(df.columns)
    col_list = ", ".join(f"[{c}]" for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    # Insert row by row
    for row in df.itertuples(index=False, name=None):
        cur.execute(insert_sql, row)

    conn.commit()
    conn.close()


# MAIN EXECUTION

if __name__ == "__main__":
    print("Loading and preprocessing data...")
    processed_df = load_and_preprocess_data()
    print(f"Preprocessing complete. Final shape: {processed_df.shape}")

    print("Ensuring SQL Server database exists...")
    ensure_database()
    print("Database check complete.")

    print(f"Creating table [{TABLE_NAME}] in SQL Server...")
    create_table_for_dataframe(processed_df, TABLE_NAME)
    print("Table created.")

    print(f"Inserting data into [{TABLE_NAME}]...")
    insert_dataframe(processed_df, TABLE_NAME)
    print("Traffic Data successfully uploaded to SQL Server.")
