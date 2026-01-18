# -*- coding: utf-8 -*-
"""
Өнөөдрийн бүх өгөгдөл шалгах
"""
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
today_str = today.strftime('%Y-%m-%d')
print(f"📅 Өнөөдөр: {today_str}")
print(f"⏰ Одоо: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Өнөөдрийн бүх өгөгдлийг татах
query = f"""
SELECT
    TIMESTAMP_S,
    CAST(VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion
WHERE VAR = 'SYSTEM_TOTAL_P'
    AND FROM_UNIXTIME(TIMESTAMP_S) >= '{today_str} 00:00:00'
ORDER BY TIMESTAMP_S
"""

df_raw = pd.read_sql(query, engine)
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')

print(f"\n📊 Өнөөдрийн бүх өгөгдөл:")
print(f"   Нийт: {len(df_raw)} бичлэг")

if len(df_raw) > 0:
    print(f"   Хугацаа: {df_raw['time_'].min()} → {df_raw['time_'].max()}")
    print(f"   Утга: {df_raw['value'].min():.0f} → {df_raw['value'].max():.0f} МВт")

    # Цаг бүрээр харуулах
    df_raw['hour_group'] = df_raw['time_'].dt.floor('h')
    df_hourly = df_raw.groupby('hour_group')['value'].max().reset_index()
    df_hourly.columns = ['Цаг', 'Хэрэглээ (МВт)']

    print(f"\n⏰ Өнөөдрийн цаг бүрийн өгөгдөл:")
    print(df_hourly.to_string(index=False))

    print(f"\n📊 Нийт {len(df_hourly)} цаг байна")
else:
    print("   ❌ Өгөгдөл олдсонгүй!")
