# -*- coding: utf-8 -*-
"""
Хоёр графикийн өгөгдлийг харьцуулах
"""
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
print(f"📅 Өнөөдөр: {today.date()}\n")

# 1️⃣ today_actual.py-н query
print("=" * 60)
print("1️⃣ TODAY_ACTUAL.PY-н query:")
print("=" * 60)

query1 = """
SELECT
    FROM_UNIXTIME(t.max_ts, '%%Y-%%m-%%d %%H:00:00') AS time_,
    z.VALUE AS load_value
FROM z_conclusion z
JOIN (
    SELECT
        MAX(TIMESTAMP_S) AS max_ts
    FROM z_conclusion
    WHERE VAR = 'SYSTEM_TOTAL_P'
      AND TIMESTAMP_S >= UNIX_TIMESTAMP(CURDATE())
      AND TIMESTAMP_S < UNIX_TIMESTAMP(CURDATE() + INTERVAL 1 DAY)
    GROUP BY FROM_UNIXTIME(TIMESTAMP_S, '%%Y-%%m-%%d %%H')
) t ON z.TIMESTAMP_S = t.max_ts
WHERE z.VAR = 'SYSTEM_TOTAL_P'
ORDER BY time_
"""

df1 = pd.read_sql(query1, engine)
df1['time_'] = pd.to_datetime(df1['time_'])
df1['load_value'] = pd.to_numeric(df1['load_value'])

print(f"Өгөгдөл: {len(df1)} цаг")
print(f"\n{df1.to_string(index=False)}")

# 2️⃣ main_system_total.py-н query (өнөөдрийн хэсэг)
print("\n" + "=" * 60)
print("2️⃣ MAIN_SYSTEM_TOTAL.PY-н query (бүх түүх):")
print("=" * 60)

query2 = """
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

df2_raw = pd.read_sql(query2, engine)
df2_raw['time_'] = pd.to_datetime(df2_raw['TIMESTAMP_S'], unit='s')

# Зөвхөн өнөөдрийн өгөгдөл
df2 = df2_raw[df2_raw['time_'].dt.date == today.date()].copy()
df2 = df2[['time_', 'value']].copy()
df2.columns = ['time_', 'load_value']

print(f"Нийт түүх: {len(df2_raw)} цаг")
print(f"Өнөөдрийн өгөгдөл: {len(df2)} цаг")
print(f"\n{df2.to_string(index=False)}")

# 3️⃣ Харьцуулах
print("\n" + "=" * 60)
print("3️⃣ ХАРЬЦУУЛАЛТ:")
print("=" * 60)

if len(df1) == len(df2):
    print(f"✅ Мөр тоо тохирч байна: {len(df1)} цаг")
else:
    print(f"❌ Мөр тоо ялгаатай: today_actual={len(df1)}, main_system_total={len(df2)}")

# Утгуудыг харьцуулах
merged = pd.merge(df1, df2, on='time_', suffixes=('_today', '_main'))

if len(merged) > 0:
    merged['diff'] = merged['load_value_today'] - merged['load_value_main']

    print(f"\nНэгдсэн өгөгдөл: {len(merged)} цаг")

    max_diff = merged['diff'].abs().max()
    if max_diff < 0.01:
        print(f"✅ Бүх утга яг адилхан байна! (хамгийн их зөрүү: {max_diff:.4f})")
    else:
        print(f"⚠️ Зарим утга өөр байна (хамгийн их зөрүү: {max_diff:.2f})")

        # Зөрүүтэй цагууд
        diff_rows = merged[merged['diff'].abs() > 0.01]
        if len(diff_rows) > 0:
            print(f"\nЗөрүүтэй цагууд ({len(diff_rows)}):")
            print(diff_rows[['time_', 'load_value_today', 'load_value_main', 'diff']].to_string(index=False))
else:
    print("❌ Нэгдсэн өгөгдөл байхгүй - цагууд таарахгүй байна")
