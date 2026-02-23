import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine
import os
import streamlit as st

# ------------------------- DATABASE SETUP -------------------------

@st.cache_resource
def init_engine():
    connect_args = {"sslmode": "require"}
    engine1 = create_engine(st.secrets["db1"]["uri"],connect_args=connect_args)
    return engine1

engine1 = init_engine()


def load_and_preprocess_data():
    # Load datasets
    site_df = pd.read_csv("site_details.csv")
    aadf_df = pd.read_csv("aadf_details.csv")

    # Missing Data Handling
    aadf_df = aadf_df.sort_values(['count_point_id', 'year']).ffill()
    site_df = site_df.dropna(subset=['count_point_id'])

    # Data Type Conversions 
    site_df['count_point_id'] = site_df['count_point_id'].astype(int)
    aadf_df['count_point_id'] = aadf_df['count_point_id'].astype(int)
    aadf_df['year'] = pd.to_datetime(aadf_df['year'], format='%Y', errors='coerce')
    if 'count_method' in aadf_df.columns:
        aadf_df['count_method'] = aadf_df['count_method'].str.lower().str.strip().astype('category')

    # Remove Duplicates
    site_df = site_df.drop_duplicates()
    aadf_df = aadf_df.drop_duplicates()

    # Identify vehicle count columns
    drop_cols = {'year', 'count_method', 'count_point_id'}
    count_cols = [c for c in aadf_df.columns if c not in drop_cols]

    # Feature Engineering: Year-over-Year Growth for each vehicle type
    aadf_df = aadf_df.sort_values(['count_point_id', 'year'])
    for col in count_cols:
        aadf_df[f'{col}_yoy_growth'] = aadf_df.groupby('count_point_id')[col].pct_change()
    aadf_df['total_traffic'] = aadf_df[count_cols].sum(axis=1)
    aadf_df['total_traffic_yoy_growth'] = aadf_df.groupby('count_point_id')['total_traffic'].pct_change()

    # Merge datasets
    merged_df = aadf_df.merge(site_df, on='count_point_id', how='left')

    # Outlier Detection and Normalization/Scaling for each vehicle type
    for col in count_cols + ['total_traffic']:
        # Z-score for outlier detection
        merged_df[f'{col}_zscore'] = np.abs(stats.zscore(merged_df[col].fillna(merged_df[col].mean())))
        # Standard scaling
        mean_val = merged_df[col].mean()
        std_val = merged_df[col].std()
        merged_df[f'{col}_scaled'] = (merged_df[col] - mean_val) / std_val

    # Reset & fill missing values
    merged_df = merged_df.sort_values(['count_point_id', 'year']).set_index(['count_point_id', 'year'])
    processed_df = merged_df.reset_index()

    # Fill numeric columns nulls with 0
    dtype_nums = processed_df.select_dtypes(include=[np.number]).columns
    processed_df[dtype_nums] = processed_df[dtype_nums].fillna(0)

    # Convert categorical columns to object and fill nulls with 'unknown'
    cat_cols = processed_df.select_dtypes(include=['category']).columns
    if len(cat_cols) > 0:
        processed_df[cat_cols] = processed_df[cat_cols].astype(object)
    obj_cols = processed_df.select_dtypes(include=['object']).columns
    processed_df[obj_cols] = processed_df[obj_cols].fillna('unknown')

    # Upload to PostgreSQL
    processed_df.to_sql("traffic_data", con=engine1, if_exists="replace", index=False)
    print(" Traffic Data successfully uploaded to PostgreSQL")

if __name__ == "__main__":
    load_and_preprocess_data()

