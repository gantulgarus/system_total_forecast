# -*- coding: utf-8 -*-
"""
df_load датафрэймийг шалгах
"""
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

query = """
SELECT
    TIMESTAMP_S,
    CAST(VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion
WHERE VAR = 'SYSTEM_TOTAL_P'
ORDER BY TIMESTAMP_S
"""

df_raw = pd.read_sql(query, engine)
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')
df_raw['hour_group'] = df_raw['time_'].dt.floor('h')

# Цаг бүрийн сүүлийн timestamp-тай утгыг авах
df_load = df_raw.loc[df_raw.groupby('hour_group')['TIMESTAMP_S'].idxmax()][['hour_group', 'value']].reset_index(drop=True)
df_load.columns = ['time_', 'load']
df_load = df_load.sort_values('time_').reset_index(drop=True)

print(f"📊 df_load датафрэйм:")
print(f"   Нийт: {len(df_load)} цаг")
print(f"   Хугацаа: {df_load['time_'].min()} → {df_load['time_'].max()}")

# Өнөөдрийн өгөгдөл
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
df_today = df_load[df_load['time_'].dt.date == today.date()].copy()

print(f"\n📅 Өнөөдөр: {today.date()}")
print(f"📊 Өнөөдрийн өгөгдөл (df_today):")
print(f"   Нийт: {len(df_today)} цаг")

if len(df_today) > 0:
    print(f"\n{df_today.to_string(index=False)}")

# Сүүлийн 20 мөр харуулах
print(f"\n📊 df_load-ын сүүлийн 20 мөр:")
print(df_load.tail(20).to_string(index=False))
