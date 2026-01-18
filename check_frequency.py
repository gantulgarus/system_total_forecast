# -*- coding: utf-8 -*-
"""
Өгөгдлийн давтамж шалгах
"""
import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

print("📊 Өгөгдлийн давтамж шалгаж байна...\n")

# Сүүлийн 100 бичлэг авах
query = """
SELECT
    TIMESTAMP_S,
    VAR,
    CAST(VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion
WHERE VAR IN ('SYSTEM_TOTAL_P', 'ERDENE_SPP_BHB_TOTAL_P', 'BAGANUUR_BESS_TOTAL_P_T', 'SONGINO_BESS_TOTAL_P')
ORDER BY TIMESTAMP_S DESC
LIMIT 200
"""

df_raw = pd.read_sql(query, engine)
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')
df_raw['hour_group'] = df_raw['time_'].dt.floor('H')

print(f"✅ Нийт: {len(df_raw)} бичлэг\n")

# VAR бүрээр шалгах
for var in df_raw['VAR'].unique():
    df_var = df_raw[df_raw['VAR'] == var].copy()
    print(f"{'='*60}")
    print(f"📌 {var}")
    print(f"{'='*60}")

    # Сүүлийн 5 цаг авах
    unique_hours = df_var['hour_group'].unique()[:5]

    for hour in unique_hours:
        df_hour = df_var[df_var['hour_group'] == hour]
        print(f"\n⏰ {hour}")
        print(f"   Бичлэгийн тоо: {len(df_hour)}")
        print(f"   Утга: min={df_hour['value'].min():.2f}, max={df_hour['value'].max():.2f}, mean={df_hour['value'].mean():.2f}")

        if len(df_hour) <= 5:
            print(f"   Бүх утгууд: {df_hour['value'].tolist()}")
        else:
            print(f"   Эхний 5 утга: {df_hour['value'].head().tolist()}")

    print()

# Нэг цагт хэдэн бичлэг байгааг харах
print(f"\n{'='*60}")
print("📊 Цаг бүрт дундаж бичлэгийн тоо:")
print(f"{'='*60}")

for var in df_raw['VAR'].unique():
    df_var = df_raw[df_raw['VAR'] == var]
    counts_per_hour = df_var.groupby('hour_group').size()
    print(f"{var}: {counts_per_hour.mean():.1f} бичлэг/цаг (min={counts_per_hour.min()}, max={counts_per_hour.max()})")
