# -*- coding: utf-8 -*-
"""
Groupby хийхэд өгөгдөл алдагдаж байгааг шалгах
"""
import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

# Өнөөдрийн өгөгдлийг шууд татах
query = """
SELECT
    TIMESTAMP_S,
    CAST(VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion
WHERE VAR = 'SYSTEM_TOTAL_P'
  AND TIMESTAMP_S >= UNIX_TIMESTAMP(CURDATE())
  AND TIMESTAMP_S < UNIX_TIMESTAMP(CURDATE() + INTERVAL 1 DAY)
ORDER BY TIMESTAMP_S
"""

df_raw = pd.read_sql(query, engine)
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')
df_raw['hour_group'] = df_raw['time_'].dt.floor('h')

print(f"📊 Өнөөдрийн түүхий өгөгдөл: {len(df_raw)} бичлэг")
print(f"\nБүх бичлэгүүд:")
print(df_raw[['time_', 'hour_group', 'value']].to_string(index=False))

# Цаг бүрээр group хийх
print(f"\n" + "="*60)
print("Цаг бүрт хэдэн бичлэг байгаа:")
print(df_raw.groupby('hour_group').size())

# Манай аргаар group хийх
df_load = df_raw.loc[df_raw.groupby('hour_group')['TIMESTAMP_S'].idxmax()][['hour_group', 'value']].reset_index(drop=True)
df_load.columns = ['time_', 'load']

print(f"\n" + "="*60)
print(f"Group хийсний дараа: {len(df_load)} цаг")
print(df_load.to_string(index=False))
