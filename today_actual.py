# -*- coding: utf-8 -*-
"""
Өнөөдрийн системийн нийт хэрэглээний бодит утга
"""
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from config import DB_CONFIG, PLOT_CONFIG

# MySQL холболт
engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

print("📊 Өнөөдрийн системийн нийт хэрэглээ татаж байна...")

# Өнөөдрийн өгөгдлийг татах
query = """
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

df = pd.read_sql(query, engine)
df['time_'] = pd.to_datetime(df['time_'])
df['load_value'] = pd.to_numeric(df['load_value'])

print(f"✅ Өнөөдрийн өгөгдөл: {len(df)} цаг")
if len(df) > 0:
    print(f"   Хугацаа: {df['time_'].min()} → {df['time_'].max()}")
    print(f"   Хэрэглээ: {df['load_value'].min():.0f} - {df['load_value'].max():.0f} МВт")
    print(f"\n📊 Өгөгдөл:")
    print(df.to_string(index=False))

# График зурах
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

fig, ax = plt.subplots(figsize=PLOT_CONFIG['figsize'])

if len(df) > 0:
    ax.plot(df['time_'], df['load_value'],
            color='red', linewidth=3.5, label='Системийн нийт хэрэглээ (бодит)',
            marker='o', markersize=8, zorder=5)

# График тохиргоо
ax.set_xlabel('Цаг', fontsize=14, fontweight='bold')
ax.set_ylabel('Хэрэглээ, МВт', fontsize=14, fontweight='bold')
ax.set_title(f"Системийн нийт хэрэглээ - {today.strftime('%Y-%m-%d')}",
             fontsize=16, fontweight='bold', pad=20)
ax.grid(True, linestyle='--', alpha=0.4, zorder=0)
ax.legend(fontsize=11, loc='best', framealpha=0.95, edgecolor='black')

ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
ax.set_xlim(today - timedelta(minutes=30), today + timedelta(hours=24))

ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('today_actual.png', dpi=PLOT_CONFIG['dpi'], bbox_inches='tight')
plt.close()

print(f"\n✅ График хадгалагдлаа: today_actual.png")
print(f"   🔴 Өнөөдрийн бодит: {len(df)} цаг")
