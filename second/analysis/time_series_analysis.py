# analysis/time_series_analysis.py
import pandas as pd

def add_time_features(df):
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    df['month'] = df['purchase_date'].dt.month
    df['quarter'] = df['purchase_date'].dt.quarter
    df['weekday'] = df['purchase_date'].dt.dayofweek
    return df

def monthly_category_trend(df):
    df = df.explode('categories')
    return df.groupby(['month', 'categories']).size().unstack(fill_value=0)

def sequential_category_patterns(df):
    """探索先后购买模式，适用于用户-时间顺序排序"""
    df = df.sort_values(['id', 'purchase_date'])
    print(df)
    df_print = df.groupby('id')['categories']
    df['next_categories'] = df.groupby('id')['categories'].shift(-1)
    df = df.dropna(subset=['categories', 'next_categories'])
    sequences = df.explode('categories').merge(
        df.explode('next_categories'), left_index=True, right_index=True,
        suffixes=('_curr', '_next'))
    return sequences.groupby(['categories_curr', 'next_categories']).size().reset_index(name='count')