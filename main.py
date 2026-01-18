# -*- coding: utf-8 -*-
"""
Forecast системийн хэрэглээ (цаг тутмын) - ЗАСВАРЛАСАН
- MySQL өгөгдлийн сангаас load татах
- Open-Meteo API-аас Ulaanbaatar temperature татах (API key шаардлагагүй!)
- Feature engineering (hourly / daily lag)
- AdaBoost forecast
- График гаргах + CSV хадгалах
"""

import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score, mean_absolute_percentage_error
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
from datetime import datetime, timedelta
import warnings
import time
import numpy as np

# Тохиргоо импортлох
from config import DB_CONFIG, LARAVEL_API_URL, LARAVEL_LAST_HISTORY_URL, LOCATION, MODEL_CONFIG, FILES, PLOT_CONFIG

warnings.filterwarnings("ignore")

# ==========================
# 1️⃣ MySQL холболт
# ==========================
engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(**DB_CONFIG)
)

# ==========================
# 2️⃣ Цаг тутмын системийн хэрэглээ татах - ШИНЭЧЛЭГДСЭН
# ==========================
print("📊 MySQL-ээс өгөгдөл татаж байна...")

# Бүх өгөгдлийг нэг дор авъя (2024-01-05 00:00:00 цагаас одоо хүртэл)
# Энэ query нь түүхэн дата + өнөөдрийн датаг хамтад нь татна
query = """
SELECT
    TIMESTAMP_S,
    VAR,
    CAST(VALUE AS DECIMAL(10,2)) AS value
FROM z_conclusion
WHERE VAR IN ('SYSTEM_TOTAL_P', 'ERDENE_SPP_BHB_TOTAL_P', 'BAGANUUR_BESS_TOTAL_P_T', 'SONGINO_BESS_TOTAL_P')
  AND CALCULATION = 50
  AND TIMESTAMP_S >= UNIX_TIMESTAMP('2024-01-05 00:00:00')
ORDER BY TIMESTAMP_S
"""

df_raw = pd.read_sql(query, engine)

# Excel WEEKDAY() форматаар долоо хоногийн өдрийг буцаах
# Ням=1, Даваа=2, Мягмар=3, Лхагва=4, Пүрэв=5, Баасан=6, Бямба=7
def excel_weekday(dt):
    """Python weekday (Даваа=0) -> Excel WEEKDAY (Ням=1)"""
    wd = (dt.weekday() + 2) % 7
    return 7 if wd == 0 else wd

# Батарейны утгыг тохируулах функц
# Логик:
# - Эерэг (өгч байна) → 0 (хасахгүй)
# - Сөрөг (цэнэглэж байна) → |утга| (хасах)
def adjust_battery_value(value):
    """
    Батарейны утгыг тохируулах:
    - Хэрэв утга >= 0 бол (системд эрчим хүч өгч байгаа) → хасахгүй
    - Хэрэв утга < 0 бол (системээс цэнэглэж байгаа) → сөрөг утгыг эерэг болгож хасах
    """
    if value >= 0:  # Системд өгч байна
        return 0  # Хасахгүй
    else:  # Системээс авч байна (цэнэглэж байна)
        return -value  # Сөрөг утгыг эерэг болгож хасах

# Хэрэв өгөгдөл байхгүй бол
if df_raw.empty:
    print("❌ Алдаа: Өгөгдөл олдсонгүй!")
    df_load = pd.DataFrame(columns=['time_', 'system_load', 'erdene_bess', 'baganuur_bess', 'songino_bess', 'load'])
else:
    print(f"✅ Түүхийн өгөгдөл: {len(df_raw)} мөр")
    print(f"   VAR төрлүүд: {df_raw['VAR'].unique().tolist()}")
    
    # UNIX timestamp-ыг хүснэгтэд нэмэх (UTC+8 Монголын цаг)
    df_raw['time_'] = pd.to_datetime(df_raw['TIMESTAMP_S'] + 8*3600, unit='s')
    df_raw['hour_group'] = df_raw['time_'].dt.floor('H')
    
    # Өгөгдлийг тусдаа хэсгүүдэд хуваах
    df_system = df_raw[df_raw['VAR'].str.upper() == 'SYSTEM_TOTAL_P'].copy()
    df_erdene = df_raw[df_raw['VAR'].str.upper() == 'ERDENE_SPP_BHB_TOTAL_P'].copy()
    df_baganuur = df_raw[df_raw['VAR'].str.upper() == 'BAGANUUR_BESS_TOTAL_P_T'].copy()
    df_songino = df_raw[df_raw['VAR'].str.upper() == 'SONGINO_BESS_TOTAL_P'].copy()
    
    # Батарейны утгуудыг тохируулах
    df_erdene['value_adjusted'] = df_erdene['value'].apply(adjust_battery_value)
    df_baganuur['value_adjusted'] = df_baganuur['value'].apply(adjust_battery_value)
    df_songino['value_adjusted'] = df_songino['value'].apply(adjust_battery_value)
    
    # Цаг бүрт дундаж утгыг тооцоолох
    df_system_hourly = df_system.groupby('hour_group')['value'].max().reset_index()
    df_system_hourly.columns = ['time_', 'system_load']
    
    df_erdene_hourly = df_erdene.groupby('hour_group')['value_adjusted'].mean().reset_index()
    df_erdene_hourly.columns = ['time_', 'erdene_bess']
    
    df_baganuur_hourly = df_baganuur.groupby('hour_group')['value_adjusted'].mean().reset_index()
    df_baganuur_hourly.columns = ['time_', 'baganuur_bess']
    
    df_songino_hourly = df_songino.groupby('hour_group')['value_adjusted'].mean().reset_index()
    df_songino_hourly.columns = ['time_', 'songino_bess']
    
    # Бүх өгөгдлийг нэгтгэх
    df_load = df_system_hourly.copy()
    
    merge_dfs = [
        (df_erdene_hourly, 'erdene_bess'),
        (df_baganuur_hourly, 'baganuur_bess'),
        (df_songino_hourly, 'songino_bess')
    ]
    
    for df_temp, col_name in merge_dfs:
        if not df_temp.empty:
            df_load = pd.merge(df_load, df_temp, on='time_', how='left')
    
    # NULL утгуудыг 0 болгох
    df_load['erdene_bess'] = df_load['erdene_bess'].fillna(0)
    df_load['baganuur_bess'] = df_load['baganuur_bess'].fillna(0)
    df_load['songino_bess'] = df_load['songino_bess'].fillna(0)
    
    # Бодит хэрэглээг тооцоолох
    df_load['load'] = df_load['system_load'] - df_load['erdene_bess'] - df_load['baganuur_bess'] - df_load['songino_bess']
    
    # Дарааллаар эрэмбэлэх
    df_load = df_load.sort_values('time_').reset_index(drop=True)
    
    print(f"\n✅ Цагийн өгөгдөл бэлэн: {len(df_load)} цаг")
    print(f"   Хугацаа: {df_load['time_'].min()} - {df_load['time_'].max()}")
    
    # Батарейны статистик
    print(f"\n📊 Батарейны утгууд (тохируулсан):")
    for battery in ['erdene_bess', 'baganuur_bess', 'songino_bess']:
        min_val = df_load[battery].min()
        max_val = df_load[battery].max()
        mean_val = df_load[battery].mean()
        count_positive = (df_load[battery] > 0).sum()
        count_zero = (df_load[battery] == 0).sum()
        
        print(f"   {battery.upper()}:")
        print(f"     Утга: {min_val:.1f} → {max_val:.1f} МВт")
        print(f"     Дундаж: {mean_val:.1f} МВт")
        print(f"     Эерэг утгатай: {count_positive} цаг")
        print(f"     Тэг утгатай: {count_zero} цаг")
    
    print(f"\n📊 Хэрэглээний статистик:")
    print(f"   Системийн хэрэглээ: {df_load['system_load'].min():.0f} - {df_load['system_load'].max():.0f} МВт")
    print(f"   Бодит хэрэглээ: {df_load['load'].min():.0f} - {df_load['load'].max():.0f} МВт")
    print(f"   Батарейнуудын нийт хасагдсан: {df_load[['erdene_bess', 'baganuur_bess', 'songino_bess']].sum().sum():.0f} МВт")

# ==========================
# 3️⃣ Temperature Open-Meteo API-аас татах
# ==========================
def get_temperature_openmeteo(start_date, end_date):
    """Open-Meteo Archive API - Түүхийн температур"""
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

def get_temperature_forecast():
    """Open-Meteo Forecast API - Өнөөдрийн температур"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LOCATION['latitude'],
        "longitude": LOCATION['longitude'],
        "hourly": "temperature_2m",
        "timezone": LOCATION['timezone'],
        "past_days": 1,
        "forecast_days": 1
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

print("🌡️ Температур татаж байна (Open-Meteo API)...")
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

# Өнөөдрийн температурыг forecast API-аас татах
try:
    print("   → Өнөөдрийн температур (Forecast API)...")
    df_temp_today = get_temperature_forecast()
    df_temp = pd.concat([df_temp, df_temp_today], ignore_index=True)
    df_temp = df_temp.drop_duplicates(subset=['time_'], keep='last').sort_values('time_')
    print(f"   ✅ Өнөөдрийн температур нэмэгдлээ")
except Exception as e:
    print(f"   ⚠️ Өнөөдрийн температур алдаа: {e}")

# Хадгалах
df_temp.to_excel(FILES['temperature'], index=False)
print(f"✅ {len(df_temp)} цагийн температур бэлэн боллоо!")
print(f"   Температур: {df_temp['temp'].min():.1f}°C → {df_temp['temp'].max():.1f}°C")
print("=" * 60)

# ==========================
# 4️⃣ Load + Temperature merge
# ==========================
df = pd.merge(df_load, df_temp, on='time_', how='inner')
# Excel WEEKDAY() форматаар: Ням=1, Даваа=2, ..., Бямба=7
# Python weekday(): Даваа=0, ..., Ням=6
# Хөрвүүлэлт: (weekday + 2) % 7, 0 бол 7 болгох
df['wd'] = ((df['time_'].dt.weekday + 2) % 7).replace(0, 7)

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
# 🔮 ӨДРИЙН ТААМАГЛАЛ (01:00 - 00:00)
# ==========================
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
future_hours_daily = []

for hour in range(1, 25):  # 01:00 - 00:00 (маргааш)
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
        'hour': future_time.hour,
        'temp': temp_current,
        'wd': excel_weekday(future_time),
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
print(f"\n🔮 Өдрийн таамаглал: {len(df_daily_forecast)} цаг (01:00 → 00:00)")

# ==========================
# ⚡ ЦАГИЙН ТААМАГЛАЛ - ЭНГИЙН
# ==========================
future_hours_hourly = []

# Сүүлийн бодит цагийг df-ээс авах (одоогоор)
# df дотор өнөөдрийн өгөгдөл дутуу байж магадгүй гэдгийг анхаарах
last_actual = df[df['time_'].dt.date == today.date()].tail(1)
if len(last_actual) == 0:
    last_actual = df.tail(1)

last_time = last_actual['time_'].values[0]
last_load = last_actual['load'].values[0]
last_hour = pd.to_datetime(last_time)

print(f"⚡ Цагийн таамаглал:")
print(f"   Сүүлийн бодит (df-ээс): {last_hour.strftime('%Y-%m-%d %H:%M')} = {last_load:.0f} МВт")

# Өнөөдрийн 01:00-өөс дараагийн 3 цаг хүртэл
end_time = last_hour + timedelta(hours=3)

current_time = today + timedelta(hours=1)  # 01:00-оос эхлэх
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
        'wd': excel_weekday(current_time),
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

print(f"   → Нийт: {len(df_hourly_forecast)} цэг (01:00 → {end_time.strftime('%H:%M')})")

# Test дата дээр үнэлгээ
pred_daily = model_daily.predict(x_test)
pred_hourly = model_hourly.predict(x_test_h)

rmse_daily = np.sqrt(mean_squared_error(y_test, pred_daily))
rmse_hourly = np.sqrt(mean_squared_error(y_test_h, pred_hourly))
mape_daily = mean_absolute_percentage_error(y_test, pred_daily) * 100
mape_hourly = mean_absolute_percentage_error(y_test_h, pred_hourly) * 100

print("=" * 60)
print("📈 DAILY FORECAST үнэлгээ (Test дата):")
print(f"   MAE:  {mean_absolute_error(y_test, pred_daily):.2f} МВт")
print(f"   RMSE: {rmse_daily:.2f} МВт")
print(f"   MAPE: {mape_daily:.2f}%")
print(f"   R²:   {r2_score(y_test, pred_daily):.4f}")

print("\n📈 HOURLY FORECAST үнэлгээ (Test дата):")
print(f"   MAE:  {mean_absolute_error(y_test_h, pred_hourly):.2f} МВт")
print(f"   RMSE: {rmse_hourly:.2f} МВт")
print(f"   MAPE: {mape_hourly:.2f}%")
print(f"   R²:   {r2_score(y_test_h, pred_hourly):.4f}")
print("=" * 60)

# ==========================
# 9️⃣ График гаргах - df_load-оос өнөөдрийн датаг авах
# ==========================
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# df_load-оос өнөөдрийн датаг авах (нэмэлт query шаардлагагүй)
df_today_actual = df_load[df_load['time_'].dt.date == today.date()].copy()

# Хэрэв өнөөдрийн өгөгдөл байхгүй бол, df-ээс сүүлийн 24 цагийг авах
if len(df_today_actual) == 0:
    df_today_actual = df.tail(24).copy()
    print(f"⚠️ Өнөөдрийн өгөгдөл олдсонгүй, df-ээс сүүлийн 24 цагийг авлаа")

# Debug мэдээлэл
print(f"\n📊 График зурах өгөгдөл:")
print(f"   df_today_actual: {len(df_today_actual)} мөр")
if len(df_today_actual) > 0:
    print(f"   'system_load' багана: {'Тийм' if 'system_load' in df_today_actual.columns else 'Үгүй'}")
    if 'system_load' in df_today_actual.columns:
        print(f"   system_load утга: {df_today_actual['system_load'].min():.0f} - {df_today_actual['system_load'].max():.0f} МВт")
    print(f"   load утга: {df_today_actual['load'].min():.0f} - {df_today_actual['load'].max():.0f} МВт")

# Цагийн таамаглалыг df_today_actual ашиглаж дахин тооцоолох
if len(df_today_actual) > 0:
    last_actual_new = df_today_actual.tail(1)
    last_time_new = last_actual_new['time_'].values[0]
    last_load_new = last_actual_new['load'].values[0]
    last_hour_new = pd.to_datetime(last_time_new)

    print(f"\n⚡ Цагийн таамаглал дахин тооцоолж байна:")
    print(f"   Сүүлийн бодит (df_today_actual-аас): {last_hour_new.strftime('%Y-%m-%d %H:%M')} = {last_load_new:.0f} МВт")

    # Хэрэв шинэ last_hour өмнөхөөс өөр бол дахин forecast хий
    if last_hour_new > last_hour:
        print(f"   Өмнөх: {last_hour.strftime('%H:%M')}, Одоо: {last_hour_new.strftime('%H:%M')} → Дахин тооцоолж байна...")

        future_hours_hourly_new = []
        end_time_new = last_hour_new + timedelta(hours=3)
        current_time_new = today + timedelta(hours=1)  # 01:00-оос эхлэх

        while current_time_new <= end_time_new:
            lag_data = df[df['time_'] < current_time_new].tail(24)

            if len(lag_data) < 3:
                current_time_new += timedelta(hours=1)
                continue

            temp_current = df_temp[df_temp['time_'] == current_time_new]['temp'].values
            if len(temp_current) == 0:
                temp_current = lag_data['temp'].mean() if 'temp' in lag_data.columns else 0
            else:
                temp_current = temp_current[0]

            feature_hourly = {
                'month': current_time_new.month,
                'day': current_time_new.day,
                'hour': current_time_new.hour,
                'temp': temp_current,
                'wd': excel_weekday(current_time_new),
                'load-1h': lag_data.iloc[-1]['load'] if len(lag_data) >= 1 else lag_data['load'].mean(),
                'load-2h': lag_data.iloc[-2]['load'] if len(lag_data) >= 2 else lag_data['load'].mean(),
                'load-3h': lag_data.iloc[-3]['load'] if len(lag_data) >= 3 else lag_data['load'].mean(),
            }

            pred_hourly = model_hourly.predict(pd.DataFrame([feature_hourly]))[0]
            future_hours_hourly_new.append({
                'time_': current_time_new,
                'forecast_hourly': round(pred_hourly, 0)
            })

            current_time_new += timedelta(hours=1)

        # Шинэ forecast ашиглах
        df_hourly_forecast = pd.DataFrame(future_hours_hourly_new)
        print(f"   ✅ Шинэчилсэн: {len(df_hourly_forecast)} цаг (00:00 → {end_time_new.strftime('%H:%M')})")

fig, ax = plt.subplots(figsize=PLOT_CONFIG['figsize'])

# 1️⃣ Системийн нийт хэрэглээ (ягаан - батарейг хасаагүй)
if len(df_today_actual) > 0 and 'system_load' in df_today_actual.columns:
    ax.plot(df_today_actual['time_'], df_today_actual['system_load'],
            color='purple', linewidth=2.5, label='Системийн нийт хэрэглээ',
            linestyle=':', marker='s', markersize=4, alpha=0.6, zorder=2)
    print("   ✅ Системийн нийт хэрэглээ зурагдлаа")

# 2️⃣ Бодит хэрэглээ (улаан - батарей хассан)
if len(df_today_actual) > 0 and 'load' in df_today_actual.columns:
    ax.plot(df_today_actual['time_'], df_today_actual['load'],
            color=PLOT_CONFIG['colors']['actual'], linewidth=3.5, label='Бодит хэрэглээ (батарей хассан)',
            marker='o', markersize=6, zorder=5)
    print("   ✅ Бодит хэрэглээ зурагдлаа")

# 3️⃣ Өдрийн таамаглал (цэнхэр)
if len(df_daily_forecast) > 0:
    ax.plot(df_daily_forecast['time_'], df_daily_forecast['forecast_daily'],
            color=PLOT_CONFIG['colors']['daily'], linestyle='--', linewidth=2.5,
            label='Өдрийн таамаглал (24 цаг)',
            marker='s', markersize=4, alpha=0.7, zorder=3)

# 4️⃣ Цагийн таамаглал (ногоон) - НЭГ ШУГАМ
if len(df_hourly_forecast) > 0:
    ax.plot(df_hourly_forecast['time_'], df_hourly_forecast['forecast_hourly'],
            color=PLOT_CONFIG['colors']['hourly_today'], linestyle='-', linewidth=2.5,
            label='Цагийн таамаглал',
            marker='o', markersize=4, alpha=0.8, zorder=4)

# График тохиргоо
ax.set_xlabel('Цаг', fontsize=14, fontweight='bold')
ax.set_ylabel('Хэрэглээ, МВт', fontsize=14, fontweight='bold')
ax.set_title(f"Системийн хэрэглээний таамаглал - {today.strftime('%Y-%m-%d')}",
             fontsize=16, fontweight='bold', pad=20)
ax.grid(True, linestyle='--', alpha=0.4, zorder=0)
ax.legend(fontsize=11, loc='upper left', framealpha=0.95, edgecolor='black')

# Үнэлгээний мэдээлэл (textbox)
eval_text = (
    f"📊 Модель үнэлгээ (Test)\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"Өдрийн таамаглал:\n"
    f"  MAE:  {mean_absolute_error(y_test, pred_daily):.1f} МВт\n"
    f"  RMSE: {rmse_daily:.1f} МВт\n"
    f"  MAPE: {mape_daily:.2f}%\n"
    f"  R²:   {r2_score(y_test, pred_daily):.4f}\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"Цагийн таамаглал:\n"
    f"  MAE:  {mean_absolute_error(y_test_h, pred_hourly):.1f} МВт\n"
    f"  RMSE: {rmse_hourly:.1f} МВт\n"
    f"  MAPE: {mape_hourly:.2f}%\n"
    f"  R²:   {r2_score(y_test_h, pred_hourly):.4f}"
)
props = dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.9, edgecolor='gray')
ax.text(0.98, 0.97, eval_text, transform=ax.transAxes, fontsize=9,
        verticalalignment='top', horizontalalignment='right',
        bbox=props, family='monospace')

ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
# X тэнхлэг: 01:00 - 00:00 (маргааш)
ax.set_xlim(today + timedelta(hours=1) - timedelta(minutes=30),
            today + timedelta(hours=24) + timedelta(minutes=30))

ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(FILES['plot'], dpi=PLOT_CONFIG['dpi'], bbox_inches='tight')
plt.show()

print(f"\n📊 График хадгалагдлаа: {FILES['plot']}")
print(f"   🟣 Системийн нийт хэрэглээ: {len(df_today_actual)} цаг")
print(f"   🔴 Бодит дата (батарей хассан): {len(df_today_actual)} цаг")
print(f"   🔵 Өдрийн таамаглал: {len(df_daily_forecast)} цаг")
print(f"   🟢 Цагийн таамаглал: {len(df_hourly_forecast)} цэг")

# ==========================
# 🔟 CSV хадгалах
# ==========================
df_daily_forecast.to_csv(FILES['daily_forecast'], index=False)
df_hourly_forecast.to_csv(FILES['hourly_forecast'], index=False)
df.to_csv(FILES['history'], index=False)

print("\n✅ CSV файлууд хадгалагдлаа:")
print(f"   📁 {FILES['daily_forecast']} - Өдрийн таамаглал (24 цаг)")
print(f"   📁 {FILES['hourly_forecast']} - Цагийн таамаглал")
print(f"   📁 {FILES['history']} - Түүхэн өгөгдөл")

# ==========================
# 🌐 Laravel-руу өгөгдөл илгээх
# ==========================
def send_to_laravel(data_type, data_list):
    """Laravel API-руу өгөгдөл илгээх"""
    try:
        payload = {'type': data_type, 'data': []}
        
        for item in data_list:
            entry = {
                'time': item['time'].strftime('%Y-%m-%d %H:%M:%S'),
                'value': float(item['value'])
            }

            # system_load байвал нэмэх
            if 'system_load' in item:
                entry['system_load'] = float(item['system_load'])

            # forecast_daily байвал нэмэх
            if 'forecast_daily' in item:
                entry['forecast_daily'] = float(item['forecast_daily'])

            # forecast_hourly байвал нэмэх
            if 'forecast_hourly' in item:
                entry['forecast_hourly'] = float(item['forecast_hourly'])

            payload['data'].append(entry)
        
        response = requests.post(
            LARAVEL_API_URL,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"   ✅ {data_type}: {len(data_list)} бичлэг")
            return True
        else:
            print(f"   ⚠️ {data_type} алдаа: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ {data_type} алдаа: {e}")
        return False

def send_metrics_to_laravel():
    """Үнэлгээний мэдээллийг Laravel руу илгээх"""
    try:
        metrics = {
            'type': 'metrics',
            'data': {
                'daily': {
                    'mae': round(mean_absolute_error(y_test, pred_daily), 2),
                    'rmse': round(rmse_daily, 2),
                    'mape': round(mape_daily, 2),
                    'r2': round(r2_score(y_test, pred_daily), 4)
                },
                'hourly': {
                    'mae': round(mean_absolute_error(y_test_h, pred_hourly), 2),
                    'rmse': round(rmse_hourly, 2),
                    'mape': round(mape_hourly, 2),
                    'r2': round(r2_score(y_test_h, pred_hourly), 4)
                },
                'training_size': len(x_train),
                'test_size': len(x_test),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        }

        response = requests.post(
            LARAVEL_API_URL,
            json=metrics,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )

        if response.status_code == 200:
            print(f"   ✅ metrics: үнэлгээний мэдээлэл")
            return True
        else:
            print(f"   ⚠️ metrics алдаа: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ metrics алдаа: {e}")
        return False

print("\n" + "=" * 60)
print("🌐 Laravel-руу өгөгдөл илгээж байна...")
print("=" * 60)

# Үнэлгээний мэдээлэл илгээх
send_metrics_to_laravel()

# Бодит хэрэглээ илгээх
actual_data = [
    {
        'time': row['time_'], 
        'value': row['load'],
        'system_load': row['system_load']  # 🔴 НЭМ
    } 
    for _, row in df_today_actual.iterrows()
]
if actual_data:
    send_to_laravel('actual', actual_data)
    

# Өдрийн таамаглал илгээх
daily_data = [{'time': row['time_'], 'value': row['forecast_daily']} 
              for _, row in df_daily_forecast.iterrows()]
if daily_data:
    send_to_laravel('daily', daily_data)

# Цагийн таамаглал илгээх
hourly_data = [{'time': row['time_'], 'value': row['forecast_hourly']}
               for _, row in df_hourly_forecast.iterrows()]
if hourly_data:
    send_to_laravel('hourly', hourly_data)

# Түүхэн өгөгдөл илгээх (зөвхөн шинэ дата)
try:
    # Laravel дээрх сүүлийн цагийг авах
    response = requests.get(LARAVEL_LAST_HISTORY_URL, timeout=10)
    last_time_data = response.json()

    if last_time_data.get('success') and last_time_data.get('last_time'):
        last_history_time = pd.to_datetime(last_time_data['last_time']).tz_localize(None)
        # Зөвхөн шинэ датаг шүүх
        df_new_history = df[df['time_'] > last_history_time].copy()
        print(f"   📊 Түүхэн дата: Laravel-д {last_history_time} хүртэл байна")
        print(f"      Шинэ дата: {len(df_new_history)} мөр")
    else:
        # Анх удаа - бүх датаг илгээх
        df_new_history = df
        print(f"   📊 Түүхэн дата: Анх удаа илгээж байна ({len(df_new_history)} мөр)")

    if len(df_new_history) > 0:
        history_data = [
            {
                'time': row['time_'],
                'value': row['load'],
                'system_load': row['system_load'],
                'forecast_daily': row['forecast_daily'],
                'forecast_hourly': row['forecast_hourly']
            }
            for _, row in df_new_history.iterrows()
        ]

        # Хэсэгчилж илгээх (1000 мөр тутамд)
        batch_size = 1000
        total_batches = (len(history_data) + batch_size - 1) // batch_size

        for i in range(0, len(history_data), batch_size):
            batch = history_data[i:i + batch_size]
            batch_num = i // batch_size + 1
            print(f"      Batch {batch_num}/{total_batches}: {len(batch)} мөр илгээж байна...")
            send_to_laravel('history', batch)
    else:
        print(f"   ✅ Түүхэн дата: Шинэ дата байхгүй")

except Exception as e:
    print(f"   ⚠️ Түүхэн дата илгээх алдаа: {e}")

print("=" * 60)
print("🎉 Бүх ажил дууслаа!")
print(f"\n📊 Хураангуй:")
print(f"   Сүүлийн бодит цаг: {last_hour.strftime('%Y-%m-%d %H:%M')}")
print(f"   Сүүлийн бодит хэрэглээ: {last_load:.0f} МВт")
print(f"   Цагийн таамаглал: {len(df_hourly_forecast)} цэг")
print(f"\n🌐 Web хаяг: http://localhost:8000/forecast")