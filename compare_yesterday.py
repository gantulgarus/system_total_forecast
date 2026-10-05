# -*- coding: utf-8 -*-
"""
Өчигдрийн таамаглал vs Бодит хэрэглээ харьцуулалт
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error

from config import DB_CONFIG, LOCATION, MODEL_CONFIG, PLOT_CONFIG

# ==========================
# MySQL холболт
# ==========================
engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

# ==========================
# Өчигдөр, уржигдар
# ==========================
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - timedelta(days=1)
day_before = today - timedelta(days=2)

print(f"📅 Өнөөдөр: {today.strftime('%Y-%m-%d')}")
print(f"📅 Өчигдөр: {yesterday.strftime('%Y-%m-%d')}")
print(f"📅 Уржигдар: {day_before.strftime('%Y-%m-%d')}")
print("=" * 60)

# ==========================
# Өгөгдөл татах
# ==========================
print("📊 MySQL-ээс өгөгдөл татаж байна...")

# 7 хоногийн өмнөөс өгөгдөл татах (өчигдрөөс 7 хоногийн өмнө)
start_from = (yesterday - timedelta(days=7)).strftime('%Y-%m-%d')

query = """
SELECT
    TIMESTAMP_S,
    VAR,
    CAST(VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion
WHERE VAR IN ('SYSTEM_TOTAL_P', 'Tom_Nar_O_G1_ANALOG_P', 'BAGANUUR_BESS_TOTAL_P_T', 'SONGINO_BESS_TOTAL_P')
  AND CALCULATION = 50
  AND TIMESTAMP_S >= UNIX_TIMESTAMP('{start_date}')
ORDER BY TIMESTAMP_S
""".format(start_date=start_from)

print(f"   Өгөгдөл татах хугацаа: {start_from} - {yesterday.strftime('%Y-%m-%d')}")

df_raw = pd.read_sql(query, engine)

def adjust_battery_value(value):
    if value >= 0:
        return 0
    else:
        return -value

def excel_weekday(dt):
    wd = (dt.weekday() + 2) % 7
    return 7 if wd == 0 else wd

# Өгөгдөл боловсруулах
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'] + 8*3600, unit='s')
df_raw['hour_group'] = df_raw['time_'].dt.floor('h')

df_system = df_raw[df_raw['VAR'].str.upper() == 'SYSTEM_TOTAL_P'].copy()
df_tomnar = df_raw[df_raw['VAR'].str.upper() == 'TOM_NAR_O_G1_ANALOG_P'].copy()
df_baganuur = df_raw[df_raw['VAR'].str.upper() == 'BAGANUUR_BESS_TOTAL_P_T'].copy()
df_songino = df_raw[df_raw['VAR'].str.upper() == 'SONGINO_BESS_TOTAL_P'].copy()

df_tomnar['value_adjusted'] = df_tomnar['value'].apply(adjust_battery_value)
df_baganuur['value_adjusted'] = df_baganuur['value'].apply(adjust_battery_value)
df_songino['value_adjusted'] = df_songino['value'].apply(adjust_battery_value)

df_system_hourly = df_system.groupby('hour_group')['value'].max().reset_index()
df_system_hourly.columns = ['time_', 'system_load']

df_tomnar_hourly = df_tomnar.groupby('hour_group')['value_adjusted'].mean().reset_index()
df_tomnar_hourly.columns = ['time_', 'tomnar_bess']

df_baganuur_hourly = df_baganuur.groupby('hour_group')['value_adjusted'].mean().reset_index()
df_baganuur_hourly.columns = ['time_', 'baganuur_bess']

df_songino_hourly = df_songino.groupby('hour_group')['value_adjusted'].mean().reset_index()
df_songino_hourly.columns = ['time_', 'songino_bess']

df_load = df_system_hourly.copy()
for df_temp, col_name in [(df_tomnar_hourly, 'tomnar_bess'), (df_baganuur_hourly, 'baganuur_bess'), (df_songino_hourly, 'songino_bess')]:
    if not df_temp.empty:
        df_load = pd.merge(df_load, df_temp, on='time_', how='left')

df_load['tomnar_bess'] = df_load['tomnar_bess'].fillna(0)
df_load['baganuur_bess'] = df_load['baganuur_bess'].fillna(0)
df_load['songino_bess'] = df_load['songino_bess'].fillna(0)
df_load['load'] = df_load['system_load'] - df_load['tomnar_bess'] - df_load['baganuur_bess'] - df_load['songino_bess']
df_load = df_load.sort_values('time_').reset_index(drop=True)

print(f"✅ Нийт өгөгдөл: {len(df_load)} цаг")

# ==========================
# Өчигдрийн бодит өгөгдөл
# ==========================
df_yesterday_actual = df_load[df_load['time_'].dt.date == yesterday.date()].copy()
print(f"✅ Өчигдрийн бодит өгөгдөл: {len(df_yesterday_actual)} цаг")

# ==========================
# Температур татах
# ==========================
import requests

def get_temperature_openmeteo(start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LOCATION['latitude'],
        "longitude": LOCATION['longitude'],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m",
        "timezone": LOCATION['timezone']
    }
    response = requests.get(url, params=params)
    data = response.json()
    df = pd.DataFrame({
        'time_': pd.to_datetime(data['hourly']['time']),
        'temp': data['hourly']['temperature_2m']
    })
    return df

print("🌡️ Температур татаж байна...")
df_temp = get_temperature_openmeteo(start_from, yesterday.strftime('%Y-%m-%d'))
print(f"✅ Температур: {len(df_temp)} цаг")

# ==========================
# df + temp merge
# ==========================
df = pd.merge(df_load, df_temp, on='time_', how='inner')
df['wd'] = ((df['time_'].dt.weekday + 2) % 7).replace(0, 7)
df['year'] = df['time_'].dt.year
df['month'] = df['time_'].dt.month
df['day'] = df['time_'].dt.day
df['hour'] = df['time_'].dt.hour

print(f"✅ Merge хийсний дараа: {len(df)} бичлэг")

# ==========================
# Таамаглалын арга: Weighted Average
# ==========================
print("\n🔮 Өчигдрийн таамаглал тооцоолж байна...")
print("   Арга: Уржигдрийн утга (70%) + 7 хоногийн дундаж (30%)")

# Өчигдрөөс өмнөх өгөгдөл
df_before_yesterday = df[df['time_'] < yesterday].copy()

# Уржигдрийн өгөгдөл (өчигдрөөс 1 өдрийн өмнө)
day_before = yesterday - timedelta(days=1)
df_day_before = df_before_yesterday[df_before_yesterday['time_'].dt.date == day_before.date()].copy()

# 7 хоногийн цаг бүрийн дундаж
hourly_avg = df_before_yesterday.groupby('hour')['load'].mean().to_dict()

# Долоо хоногийн ижил гаригийн өгөгдөл (өмнөх долоо хоног)
same_weekday = yesterday - timedelta(days=7)
df_same_weekday = df_before_yesterday[df_before_yesterday['time_'].dt.date == same_weekday.date()].copy()

print(f"   Уржигдрийн өгөгдөл: {len(df_day_before)} цаг")
print(f"   Өмнөх ижил гаригийн өгөгдөл: {len(df_same_weekday)} цаг")

forecast_results = []

for hour in range(1, 25):
    future_time = yesterday + timedelta(hours=hour)
    h = future_time.hour

    # 1. Уржигдрийн ижил цагийн утга (хамгийн чухал)
    yesterday_same_hour = df_day_before[df_day_before['hour'] == h]['load'].values
    load_yesterday = yesterday_same_hour[0] if len(yesterday_same_hour) > 0 else None

    # 2. 7 хоногийн дундаж
    load_weekly_avg = hourly_avg.get(h, df_before_yesterday['load'].mean())

    # 3. Өмнөх долоо хоногийн ижил гариг, ижил цаг
    same_wd_same_hour = df_same_weekday[df_same_weekday['hour'] == h]['load'].values
    load_same_weekday = same_wd_same_hour[0] if len(same_wd_same_hour) > 0 else None

    # Weighted average тооцоолох
    if load_yesterday is not None and load_same_weekday is not None:
        # Уржигдар 50% + Ижил гариг 30% + 7 хоногийн дундаж 20%
        pred = load_yesterday * 0.5 + load_same_weekday * 0.3 + load_weekly_avg * 0.2
    elif load_yesterday is not None:
        # Уржигдар 70% + 7 хоногийн дундаж 30%
        pred = load_yesterday * 0.7 + load_weekly_avg * 0.3
    else:
        # Зөвхөн 7 хоногийн дундаж
        pred = load_weekly_avg

    forecast_results.append({
        'time_': future_time,
        'forecast': round(pred, 0)
    })

df_forecast = pd.DataFrame(forecast_results)
print(f"✅ Таамаглал: {len(df_forecast)} цаг")

# ==========================
# Харьцуулалт
# ==========================
df_compare = pd.merge(df_yesterday_actual, df_forecast, on='time_', how='inner')

print("\n" + "=" * 60)
print(f"📊 ӨЧИГДРИЙН ХАРЬЦУУЛАЛТ ({yesterday.strftime('%Y-%m-%d')})")
print("=" * 60)

if len(df_compare) > 0:
    mae = mean_absolute_error(df_compare['load'], df_compare['forecast'])
    rmse = np.sqrt(mean_squared_error(df_compare['load'], df_compare['forecast']))
    mape = mean_absolute_percentage_error(df_compare['load'], df_compare['forecast']) * 100

    print(f"\n📈 Үнэлгээ:")
    print(f"   MAE:  {mae:.1f} МВт")
    print(f"   RMSE: {rmse:.1f} МВт")
    print(f"   MAPE: {mape:.2f}%")

    print(f"\n📊 Цаг тутмын харьцуулалт:")
    print("-" * 70)
    print(f"{'Цаг':<8} {'Систем':<12} {'Бодит':<12} {'Таамаглал':<12} {'Зөрүү':<12}")
    print("-" * 70)

    for _, row in df_compare.iterrows():
        hour_str = row['time_'].strftime('%H:%M')
        system = row['system_load']
        actual = row['load']
        forecast = row['forecast']
        diff = forecast - actual
        diff_str = f"+{diff:.0f}" if diff > 0 else f"{diff:.0f}"
        print(f"{hour_str:<8} {system:>10.0f}  {actual:>10.0f}  {forecast:>10.0f}  {diff_str:>10}")

    print("-" * 70)

# ==========================
# График
# ==========================
fig, ax = plt.subplots(figsize=(16, 8))

# Системийн нийт хэрэглээ
ax.plot(df_compare['time_'], df_compare['system_load'],
        color='purple', linewidth=2, label='Системийн нийт хэрэглээ',
        linestyle=':', marker='s', markersize=4, alpha=0.6)

# Бодит хэрэглээ
ax.plot(df_compare['time_'], df_compare['load'],
        color='red', linewidth=3, label='Бодит хэрэглээ (батарей хассан)',
        marker='o', markersize=6)

# Таамаглал
ax.plot(df_compare['time_'], df_compare['forecast'],
        color='dodgerblue', linewidth=2.5, label='Өдрийн таамаглал',
        linestyle='--', marker='s', markersize=5)

ax.set_xlabel('Цаг', fontsize=14, fontweight='bold')
ax.set_ylabel('Хэрэглээ, МВт', fontsize=14, fontweight='bold')
ax.set_title(f"Өчигдрийн таамаглал vs Бодит хэрэглээ - {yesterday.strftime('%Y-%m-%d')}",
             fontsize=16, fontweight='bold', pad=20)
ax.grid(True, linestyle='--', alpha=0.4)
ax.legend(fontsize=12, loc='upper left')

# Үнэлгээний мэдээлэл
if len(df_compare) > 0:
    eval_text = (
        f"📊 Үнэлгээ\n"
        f"━━━━━━━━━━━━\n"
        f"MAE:  {mae:.1f} МВт\n"
        f"RMSE: {rmse:.1f} МВт\n"
        f"MAPE: {mape:.2f}%"
    )
    props = dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.9, edgecolor='gray')
    ax.text(0.98, 0.97, eval_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', horizontalalignment='right',
            bbox=props, family='monospace')

ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

plt.xticks(rotation=45)
plt.tight_layout()

output_file = 'compare_yesterday.png'
plt.savefig(output_file, dpi=200, bbox_inches='tight')
plt.show()

print(f"\n📊 График хадгалагдлаа: {output_file}")
print("🎉 Дууслаа!")
