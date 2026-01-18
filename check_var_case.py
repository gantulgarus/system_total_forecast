# -*- coding: utf-8 -*-
"""
VAR-ын бичгийн хэлбэрийг шалгах
"""
import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

# VAR-ын бүх хувилбаруудыг олох
query = """
SELECT DISTINCT VAR
FROM z_conclusion
WHERE UPPER(VAR) LIKE '%%SYSTEM%%TOTAL%%'
ORDER BY VAR
"""

df = pd.read_sql(query, engine)
print("📊 SYSTEM_TOTAL-тай холбоотой VAR утгууд:")
print(df['VAR'].tolist())

# Өнөөдрийн өгөгдлийг хоёр хувилбараар шалгах
queries = {
    "SYSTEM_TOTAL_P (том үсэг)": """
        SELECT COUNT(*) as cnt
        FROM z_conclusion
        WHERE VAR = 'SYSTEM_TOTAL_P'
          AND DATE(FROM_UNIXTIME(TIMESTAMP_S)) = CURDATE()
    """,
    "system_total_p (жижиг үсэг)": """
        SELECT COUNT(*) as cnt
        FROM z_conclusion
        WHERE VAR = 'system_total_p'
          AND DATE(FROM_UNIXTIME(TIMESTAMP_S)) = CURDATE()
    """
}

print("\n📊 Өнөөдрийн өгөгдөл:")
for name, query in queries.items():
    result = pd.read_sql(query, engine)
    count = result['cnt'].iloc[0]
    print(f"   {name}: {count} бичлэг")
