# -*- coding: utf-8 -*-
"""
Системийн нийт хэрэглээний таамаглал (батарей хасахгүй)
- MySQL өгөгдлийн сангаас SYSTEM_TOTAL_P татах
- Open-Meteo API-аас температур татах
- Feature engineering
- AdaBoost forecast
- График гаргах + CSV хадгалах
"""

import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
from datetime import datetime, timedelta
import warnings
import time
import numpy as np

# Тохиргоо импортлох
from config import DB_CONFIG, LOCATION, MODEL_CONFIG, PLOT_CONFIG

warnings.filterwarnings("ignore")

# ==========================
# 1️⃣ MySQL холболт
# ==========================
engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

# ==========================
# 2️⃣ Системийн нийт хэрэглээ татах (батарей хасахгүй)
# ==========================
print("📊 MySQL-ээс системийн нийт хэрэглээ татаж байна...")

query = """
SELECT
    t.max_ts AS TIMESTAMP_S,
    CAST(z.VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion z
JOIN (
    SELECT
        MAX(TIMESTAMP_S) AS max_ts
    FROM z_conclusion
    WHERE VAR = 'SYSTEM_TOTAL_P'
      AND CALCULATION = 50
    GROUP BY FROM_UNIXTIME(TIMESTAMP_S, '%%Y-%%m-%%d %%H')
) t ON z.TIMESTAMP_S = t.max_ts
WHERE z.VAR = 'SYSTEM_TOTAL_P'
  AND z.CALCULATION = 50
ORDER BY t.max_ts
"""

df_raw = pd.read_sql(query, engine)

if df_raw.empty:
    print("❌ Алдаа: Өгөгдөл олдсонгүй!")
    exit(1)

print(f"✅ Түүхийн өгөгдөл: {len(df_raw)} мөр")

# UNIX timestamp-ыг datetime болгох
df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'], unit='s')

# Query-д цаг бүрийн сүүлийн timestamp авсан учир шууд ашиглана
df_load = df_raw[['time_', 'value']].copy()
df_load.columns = ['time_', 'load']

print(f"\n✅ Цагийн өгөгдөл бэлэн: {len(df_load)} цаг")
print(f"   Хугацаа: {df_load['time_'].min()} - {df_load['time_'].max()}")
print(f"   Системийн хэрэглээ: {df_load['load'].min():.0f} - {df_load['load'].max():.0f} МВт")

# Өнөөдрийн бодит өгөгдлийг MySQL-ээс шууд татах (найдвартай)
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
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

print(f"\n✅ Өнөөдрийн бодит: {len(df_today_actual)} цаг")
if len(df_today_actual) > 0:
    print(f"   Хугацаа: {df_today_actual['time_'].min()} → {df_today_actual['time_'].max()}")

# ==========================
# 3️⃣ Temperature Open-Meteo API-аас татах
# ==========================
def get_temperature_openmeteo(start_date, end_date):
    """Open-Meteo API - ҮНЭГҮЙ, API key шаардлагагүй!"""
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

# Load датаны хугацааг шалгаж температур татах
load_start = df_load['time_'].min().strftime("%Y-%m-%d")
load_end = df_load['time_'].max().strftime("%Y-%m-%d")

print("\n🌡️ Температур татаж байна (Open-Meteo API)...")
print(f"   Хугацаа: {load_start} → {load_end}")

# Хугацааг жилээр хувааж татах
all_temp_data = []
current_year = datetime.strptime(load_start, "%Y-%m-%d").year
end_year = datetime.strptime(load_end, "%Y-%m-%d").year

for year in range(current_year, end_year + 1):
    try:
        year_start = f"{year}-01-01" if year > current_year else load_start
        year_end = f"{year}-12-31" if year < end_year else load_end

        print(f"   → {year_start} ~ {year_end}")
        df_temp_year = get_temperature_openmeteo(year_start, year_end)
        all_temp_data.append(df_temp_year)
        time.sleep(1)

    except Exception as e:
        print(f"   ⚠️ Алдаа {year}: {e}")

# Температурын датаг нэгтгэх
df_temp = pd.concat(all_temp_data, ignore_index=True)
df_temp = df_temp.drop_duplicates(subset=['time_']).sort_values('time_')

print(f"✅ {len(df_temp)} цагийн температур бэлэн боллоо!")
print(f"   Температур: {df_temp['temp'].min():.1f}°C → {df_temp['temp'].max():.1f}°C")
print("=" * 60)

# ==========================
# 4️⃣ Load + Temperature merge
# ==========================
df = pd.merge(df_load, df_temp, on='time_', how='inner')
df['wd'] = df['time_'].dt.weekday

print(f"📊 Merge хийсний дараа: {len(df)} бичлэг")

# ==========================
# 5️⃣ Feature engineering
# ==========================
for i in range(1, 4):
    df[f'load-{i}h'] = df['load'].shift(i)

for i in range(1, 8):
    df[f'load-{i}d'] = df['load'].shift(i*24)

df['year'] = df['time_'].dt.year
df['month'] = df['time_'].dt.month
df['day'] = df['time_'].dt.day
df['hour'] = df['time_'].dt.hour

df = df.dropna()
print(f"📊 Feature engineering хийсний дараа: {len(df)} бичлэг")
print("=" * 60)

# ==========================
# 6️⃣ Train-test split
# ==========================
X_daily = df[['year','month','day','hour','temp','wd',
              'load-1d','load-2d','load-3d','load-4d',
              'load-5d','load-6d','load-7d']]
y_daily = df['load']

x_train, x_test, y_train, y_test = train_test_split(
    X_daily, y_daily, test_size=MODEL_CONFIG['test_size'], shuffle=False
)

X_hourly = df[['month','day','hour','temp','wd','load-1h','load-2h','load-3h']]
y_hourly = df['load']

x_train_h, x_test_h, y_train_h, y_test_h = train_test_split(
    X_hourly, y_hourly, test_size=MODEL_CONFIG['test_size'], shuffle=False
)

print(f"🎯 Training дата: {len(x_train)} бичлэг")
print(f"🎯 Test дата: {len(x_test)} бичлэг")
print("=" * 60)

# ==========================
# 7️⃣ Модель үүсгэх
# ==========================
print("🤖 Модель сургаж байна...")

model_daily = AdaBoostRegressor(
    DecisionTreeRegressor(max_depth=MODEL_CONFIG['daily']['max_depth']),
    n_estimators=MODEL_CONFIG['daily']['n_estimators'],
    random_state=MODEL_CONFIG['daily']['random_state']
)
model_daily.fit(x_train, y_train)

model_hourly = AdaBoostRegressor(
    DecisionTreeRegressor(max_depth=MODEL_CONFIG['hourly']['max_depth']),
    n_estimators=MODEL_CONFIG['hourly']['n_estimators'],
    random_state=MODEL_CONFIG['hourly']['random_state']
)
model_hourly.fit(x_train_h, y_train_h)

print("✅ Модель бэлэн боллоо!")

# ==========================
# 8️⃣ Forecast хийх + үнэлгээ
# ==========================
df['forecast_daily'] = model_daily.predict(X_daily).round(0)
df['forecast_hourly'] = model_hourly.predict(X_hourly).round(0)

# ==========================
# 🔮 ӨДРИЙН ТААМАГЛАЛ
# ==========================
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow = today + timedelta(days=1)
future_hours_daily = []

for hour in range(24):
    future_time = today + timedelta(hours=hour)
    lag_data = df[df['time_'] < future_time].tail(24*7)

    if len(lag_data) < 24*7:
        continue

    temp_current = df_temp[df_temp['time_'] == future_time]['temp'].values
    if len(temp_current) == 0:
        temp_current = df[df['time_'].dt.date == today.date()]['temp'].mean()
    else:
        temp_current = temp_current[0]

    feature_daily = {
        'year': future_time.year,
        'month': future_time.month,
        'day': future_time.day,
        'hour': hour,
        'temp': temp_current,
        'wd': future_time.weekday(),
        'load-1d': lag_data.iloc[-24]['load'] if len(lag_data) >= 24 else lag_data['load'].mean(),
        'load-2d': lag_data.iloc[-48]['load'] if len(lag_data) >= 48 else lag_data['load'].mean(),
        'load-3d': lag_data.iloc[-72]['load'] if len(lag_data) >= 72 else lag_data['load'].mean(),
        'load-4d': lag_data.iloc[-96]['load'] if len(lag_data) >= 96 else lag_data['load'].mean(),
        'load-5d': lag_data.iloc[-120]['load'] if len(lag_data) >= 120 else lag_data['load'].mean(),
        'load-6d': lag_data.iloc[-144]['load'] if len(lag_data) >= 144 else lag_data['load'].mean(),
        'load-7d': lag_data.iloc[-168]['load'] if len(lag_data) >= 168 else lag_data['load'].mean(),
    }

    pred_daily = model_daily.predict(pd.DataFrame([feature_daily]))[0]
    future_hours_daily.append({'time_': future_time, 'forecast_daily': round(pred_daily, 0)})

df_daily_forecast = pd.DataFrame(future_hours_daily)
print(f"\n🔮 Өдрийн таамаглал: {len(df_daily_forecast)} цаг (өнөөдөр: {today.strftime('%Y-%m-%d')})")

# ==========================
# ⚡ ЦАГИЙН ТААМАГЛАЛ
# ==========================
future_hours_hourly = []

# Сүүлийн бодит цагийг df_today_actual-аас авах (графикт зурагдсан өгөгдөл)
if len(df_today_actual) > 0:
    last_actual_load = df_today_actual.tail(1)
    last_time = last_actual_load['time_'].values[0]
    last_load = last_actual_load['load'].values[0]
    last_hour = pd.to_datetime(last_time)
else:
    # Хэрэв өнөөдрийн өгөгдөл байхгүй бол df_load-ын сүүлийн утгыг авах
    last_actual_load = df_load.tail(1)
    last_time = last_actual_load['time_'].values[0]
    last_load = last_actual_load['load'].values[0]
    last_hour = pd.to_datetime(last_time)

print(f"⚡ Цагийн таамаглал:")
print(f"   Сүүлийн бодит: {last_hour.strftime('%Y-%m-%d %H:%M')} = {last_load:.0f} МВт")

# Өнөөдрийн 00:00-ээс сүүлийн бодит + 3 цаг хүртэл таамаглах
end_time = last_hour + timedelta(hours=3)
current_time = today

while current_time <= end_time:
    lag_data = df[df['time_'] < current_time].tail(24)

    if len(lag_data) < 3:
        current_time += timedelta(hours=1)
        continue

    temp_current = df_temp[df_temp['time_'] == current_time]['temp'].values
    if len(temp_current) == 0:
        temp_current = lag_data['temp'].mean()
    else:
        temp_current = temp_current[0]

    feature_hourly = {
        'month': current_time.month,
        'day': current_time.day,
        'hour': current_time.hour,
        'temp': temp_current,
        'wd': current_time.weekday(),
        'load-1h': lag_data.iloc[-1]['load'] if len(lag_data) >= 1 else lag_data['load'].mean(),
        'load-2h': lag_data.iloc[-2]['load'] if len(lag_data) >= 2 else lag_data['load'].mean(),
        'load-3h': lag_data.iloc[-3]['load'] if len(lag_data) >= 3 else lag_data['load'].mean(),
    }

    pred_hourly = model_hourly.predict(pd.DataFrame([feature_hourly]))[0]
    future_hours_hourly.append({
        'time_': current_time,
        'forecast_hourly': round(pred_hourly, 0)
    })

    current_time += timedelta(hours=1)

df_hourly_forecast = pd.DataFrame(future_hours_hourly)
if len(df_hourly_forecast) > 0:
    start_hour = df_hourly_forecast['time_'].min().strftime('%H:%M')
    end_hour = df_hourly_forecast['time_'].max().strftime('%H:%M')
    print(f"   → Нийт: {len(df_hourly_forecast)} цэг ({start_hour} → {end_hour})")
else:
    print(f"   → Нийт: 0 цэг")

# Test дата дээр үнэлгээ
pred_daily = model_daily.predict(x_test)
pred_hourly = model_hourly.predict(x_test_h)

rmse_daily = np.sqrt(mean_squared_error(y_test, pred_daily))
rmse_hourly = np.sqrt(mean_squared_error(y_test_h, pred_hourly))

print("=" * 60)
print("📈 DAILY FORECAST үнэлгээ (Test дата):")
print(f"   MAE:  {mean_absolute_error(y_test, pred_daily):.2f} МВт")
print(f"   RMSE: {rmse_daily:.2f} МВт")
print(f"   R²:   {r2_score(y_test, pred_daily):.4f}")

print("\n📈 HOURLY FORECAST үнэлгээ (Test дата):")
print(f"   MAE:  {mean_absolute_error(y_test_h, pred_hourly):.2f} МВт")
print(f"   RMSE: {rmse_hourly:.2f} МВт")
print(f"   R²:   {r2_score(y_test_h, pred_hourly):.4f}")
print("=" * 60)

# ==========================
# 9️⃣ График гаргах
# ==========================
print(f"\n📊 График зурах өгөгдөл:")
print(f"   Өнөөдрийн бодит: {len(df_today_actual)} мөр")
if len(df_today_actual) > 0:
    print(f"   Системийн хэрэглээ: {df_today_actual['load'].min():.0f} - {df_today_actual['load'].max():.0f} МВт")

fig, ax = plt.subplots(figsize=PLOT_CONFIG['figsize'])

# 1️⃣ Бодит хэрэглээ (улаан)
if len(df_today_actual) > 0:
    ax.plot(df_today_actual['time_'], df_today_actual['load'],
            color='red', linewidth=3.5, label='Системийн нийт хэрэглээ (бодит)',
            marker='o', markersize=6, zorder=5)

# 2️⃣ Өдрийн таамаглал (цэнхэр)
if len(df_daily_forecast) > 0:
    ax.plot(df_daily_forecast['time_'], df_daily_forecast['forecast_daily'],
            color='dodgerblue', linestyle='--', linewidth=2.5,
            label='Өдрийн таамаглал (24 цаг)',
            marker='s', markersize=4, alpha=0.7, zorder=3)

# 3️⃣ Цагийн таамаглал (ногоон)
if len(df_hourly_forecast) > 0:
    ax.plot(df_hourly_forecast['time_'], df_hourly_forecast['forecast_hourly'],
            color='green', linestyle='-', linewidth=2.5,
            label='Цагийн таамаглал',
            marker='o', markersize=4, alpha=0.8, zorder=4)

# График тохиргоо
ax.set_xlabel('Цаг', fontsize=14, fontweight='bold')
ax.set_ylabel('Хэрэглээ, МВт', fontsize=14, fontweight='bold')
ax.set_title(f"Системийн нийт хэрэглээний таамаглал - {today.strftime('%Y-%m-%d')}",
             fontsize=16, fontweight='bold', pad=20)
ax.grid(True, linestyle='--', alpha=0.4, zorder=0)
ax.legend(fontsize=11, loc='best', framealpha=0.95, edgecolor='black')

ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_xlim(today - timedelta(minutes=30), today + timedelta(hours=24))

ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('forecast_system_total.png', dpi=PLOT_CONFIG['dpi'], bbox_inches='tight')
plt.show()

print(f"\n📊 График хадгалагдлаа: forecast_system_total.png")
print(f"   🔴 Бодит дата: {len(df_today_actual)} цаг")
print(f"   🔵 Өдрийн таамаглал: {len(df_daily_forecast)} цаг")
print(f"   🟢 Цагийн таамаглал: {len(df_hourly_forecast)} цэг")

# ==========================
# 🔟 CSV хадгалах
# ==========================
df_daily_forecast.to_csv('forecast_system_total_daily.csv', index=False)
df_hourly_forecast.to_csv('forecast_system_total_hourly.csv', index=False)
df.to_csv('forecast_system_total_history.csv', index=False)

print("\n✅ CSV файлууд хадгалагдлаа:")
print(f"   📁 forecast_system_total_daily.csv - Өдрийн таамаглал (24 цаг)")
print(f"   📁 forecast_system_total_hourly.csv - Цагийн таамаглал")
print(f"   📁 forecast_system_total_history.csv - Түүхэн өгөгдөл")

print("\n" + "=" * 60)
print("🎉 Бүх ажил дууслаа!")
print(f"\n📊 Хураангуй:")
print(f"   Сүүлийн бодит цаг: {last_hour.strftime('%Y-%m-%d %H:%M')}")
print(f"   Сүүлийн бодит хэрэглээ: {last_load:.0f} МВт")
print(f"   Цагийн таамаглал: {len(df_hourly_forecast)} цэг")
