# -*- coding: utf-8 -*-
"""
MySQL өгөгдөл татах тест
"""
import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG

print("🔗 MySQL холбогдож байна...")
try:
    engine = create_engine(
        "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
    )

    query = """
    SELECT
        TIMESTAMP_S,
        VAR,
        CAST(VALUE AS DECIMAL(10,2)) AS value
    FROM z_conclusion
    WHERE VAR IN ('SYSTEM_TOTAL_P', 'ERDENE_SPP_BHB_TOTAL_P', 'BAGANUUR_BESS_TOTAL_P_T', 'SONGINO_BESS_TOTAL_P')
    ORDER BY TIMESTAMP_S DESC
    LIMIT 100
    """

    df_raw = pd.read_sql(query, engine)

    print(f"✅ Амжилттай холбогдлоо!")
    print(f"   Өгөгдөл: {len(df_raw)} мөр")
    print(f"   VAR төрлүүд: {df_raw['VAR'].unique().tolist()}")

    if len(df_raw) > 0:
        df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')
        print(f"   Хугацаа: {df_raw['time_'].min()} → {df_raw['time_'].max()}")

        # VAR бүрийн тоо
        print("\n📊 VAR төрөл бүрийн тоо:")
        print(df_raw['VAR'].value_counts())

except Exception as e:
    print(f"❌ Алдаа: {e}")
