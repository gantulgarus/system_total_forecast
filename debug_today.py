# -*- coding: utf-8 -*-
"""
Өнөөдрийн өгөгдөл шалгах
"""
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
print(f"📅 Өнөөдөр: {today.strftime('%Y-%m-%d')}")

# MySQL-ээс өнөөдрийн өгөгдөл татах
query = """
SELECT
    TIMESTAMP_S,
    CAST(VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion
WHERE VAR = 'SYSTEM_TOTAL_P'
ORDER BY TIMESTAMP_S DESC
LIMIT 100
"""

df_raw = pd.read_sql(query, engine)
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')

print(f"\n📊 MySQL-ээс татсан сүүлийн өгөгдөл:")
print(f"   Нийт: {len(df_raw)} бичлэг")
print(f"   Хугацаа: {df_raw['time_'].min()} → {df_raw['time_'].max()}")

# Өнөөдрийн өгөгдөл
df_today_raw = df_raw[df_raw['time_'].dt.date == today.date()]
print(f"\n🔍 Өнөөдрийн түүхий өгөгдөл (MySQL):")
print(f"   Мөр тоо: {len(df_today_raw)}")
if len(df_today_raw) > 0:
    print(f"   Хугацаа: {df_today_raw['time_'].min()} → {df_today_raw['time_'].max()}")
    print(f"   Утга: {df_today_raw['value'].min():.0f} → {df_today_raw['value'].max():.0f} МВт")
    print(f"\n   Сүүлийн 5 бичлэг:")
    print(df_today_raw[['time_', 'value']].head().to_string(index=False))
else:
    print("   ❌ Өнөөдрийн өгөгдөл олдсонгүй!")

# Цаг бүрээр group хийх
df_raw['hour_group'] = df_raw['time_'].dt.floor('h')
df_hourly = df_raw.groupby('hour_group')['value'].max().reset_index()
df_hourly.columns = ['time_', 'load']

df_today_hourly = df_hourly[df_hourly['time_'].dt.date == today.date()]
print(f"\n⏰ Өнөөдрийн цагийн өгөгдөл:")
print(f"   Мөр тоо: {len(df_today_hourly)}")
if len(df_today_hourly) > 0:
    print(f"   Хугацаа: {df_today_hourly['time_'].min()} → {df_today_hourly['time_'].max()}")
    print(f"\n   Бүх цагууд:")
    print(df_today_hourly.to_string(index=False))
