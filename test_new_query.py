# -*- coding: utf-8 -*-
"""
Шинэ query-г туршиж үзэх
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
    t.max_ts AS TIMESTAMP_S,
    CAST(z.VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion z
JOIN (
    SELECT
        MAX(TIMESTAMP_S) AS max_ts
    FROM z_conclusion
    WHERE VAR = 'SYSTEM_TOTAL_P'
    GROUP BY FROM_UNIXTIME(TIMESTAMP_S, '%%Y-%%m-%%d %%H')
) t ON z.TIMESTAMP_S = t.max_ts
WHERE z.VAR = 'SYSTEM_TOTAL_P'
ORDER BY t.max_ts
"""

print("⏳ Query ажиллаж байна...")
df_raw = pd.read_sql(query, engine)
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')

print(f"\n📊 Нийт: {len(df_raw)} цаг")
print(f"   Хугацаа: {df_raw['time_'].min()} → {df_raw['time_'].max()}")

# Өнөөдрийн өгөгдөл
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
df_today = df_raw[df_raw['time_'].dt.date == today.date()]

print(f"\n📅 Өнөөдрийн өгөгдөл ({today.date()}):")
print(f"   Нийт: {len(df_today)} цаг")

if len(df_today) > 0:
    print(f"\n{df_today[['time_', 'value']].to_string(index=False)}")
