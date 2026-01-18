# -*- coding: utf-8 -*-
"""
Хэрэглэгчийн өгсөн query-г туршиж үзэх
"""
import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

# Хэрэглэгчийн өгсөн query
query = """
SELECT
    FROM_UNIXTIME(t.max_ts, '%%Y-%%m-%%d %%H:00:00') AS time_,
    z.VALUE AS load_value
FROM z_conclusion z
JOIN (
    SELECT
        MAX(TIMESTAMP_S) AS max_ts
    FROM z_conclusion
    WHERE VAR = 'system_total_p'
      AND TIMESTAMP_S >= UNIX_TIMESTAMP(CURDATE())
      AND TIMESTAMP_S < UNIX_TIMESTAMP(CURDATE() + INTERVAL 1 DAY)
    GROUP BY FROM_UNIXTIME(TIMESTAMP_S, '%%Y-%%m-%%d %%H')
) t ON z.TIMESTAMP_S = t.max_ts
WHERE z.VAR = 'system_total_p'
ORDER BY time_
"""

df = pd.read_sql(query, engine)
print(f"📊 Өнөөдрийн системийн нийт хэрэглээ:")
print(f"   Нийт: {len(df)} цаг")
print(f"\n{df.to_string(index=False)}")
