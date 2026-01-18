# -*- coding: utf-8 -*-
"""
main_system_total.py-н график хэсгийг туршиж үзэх
"""
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from config import DB_CONFIG, PLOT_CONFIG

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

print("📊 Өнөөдрийн бодит өгөгдөл татаж байна...")
query_today = """
SELECT
    FROM_UNIXTIME(t.max_ts, '%%Y-%%m-%%d %%H:00:00') AS time_,
    CAST(z.VALUE AS DECIMAL(10,2)) AS load_value
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

df_today_actual = pd.read_sql(query_today, engine)
df_today_actual['time_'] = pd.to_datetime(df_today_actual['time_'])
df_today_actual['load'] = pd.to_numeric(df_today_actual['load_value'])

print(f"✅ Өнөөдрийн бодит: {len(df_today_actual)} цаг")
print(f"   Системийн хэрэглээ: {df_today_actual['load'].min():.0f} - {df_today_actual['load'].max():.0f} МВт")
print(f"\n{df_today_actual[['time_', 'load']].to_string(index=False)}")

# График зурах
fig, ax = plt.subplots(figsize=PLOT_CONFIG['figsize'])

if len(df_today_actual) > 0:
    ax.plot(df_today_actual['time_'], df_today_actual['load'],
            color='red', linewidth=3.5, label='Системийн нийт хэрэглээ (бодит)',
            marker='o', markersize=6, zorder=5)

ax.set_xlabel('Цаг', fontsize=14, fontweight='bold')
ax.set_ylabel('Хэрэглээ, МВт', fontsize=14, fontweight='bold')
ax.set_title(f"Системийн нийт хэрэглээ - {today.strftime('%Y-%m-%d')}",
             fontsize=16, fontweight='bold', pad=20)
ax.grid(True, linestyle='--', alpha=0.4, zorder=0)
ax.legend(fontsize=11, loc='best', framealpha=0.95, edgecolor='black')

ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_xlim(today - timedelta(minutes=30), today + timedelta(hours=24))

ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('test_main_system_graph.png', dpi=PLOT_CONFIG['dpi'], bbox_inches='tight')
plt.close()

print(f"\n✅ График: test_main_system_graph.png")
