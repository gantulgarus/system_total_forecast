# -*- coding: utf-8 -*-
"""
Forecast системийн хэрэглээ (цаг тутмын) - ЗАСВАРЛАСАН
- Laravel API-аас load татах (/api/forecast/actual-load — Хянах самбартай ижил станцуудын нийлбэр)
- Open-Meteo API-аас Ulaanbaatar temperature татах (API key шаардлагагүй!)
- Feature engineering (hourly / daily lag)
- AdaBoost forecast
- График гаргах + CSV хадгалах
"""

import os
import pandas as pd
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
from config import LARAVEL_API_URL, LARAVEL_LAST_HISTORY_URL, LOCATION, MODEL_CONFIG, FILES, PLOT_CONFIG

warnings.filterwarnings("ignore")

# ==========================
# 1️⃣-2️⃣ Цаг тутмын системийн ачаалал — Laravel API (Хянах самбартай ижил)
# ==========================
# Өмнө нь SYSTEM_TOTAL_P − (Том Нар, Багануур, Сонгино БХ цэнэглэлт)-ийг энд тооцдог байсан.
# Одоо Хянах самбарын графиктай яг ижил байлгахын тулд Laravel-ийн SystemLoad-оос авна:
#   system_load — станцуудын нийлбэр (БХ цэнэглэлт тооцохгүй) = Хянах самбарын үндсэн шугам
#   load        — нийлбэр − БХ цэнэглэлт = Хянах самбарын «хэрэглээ» (таамаглалын бодит хэрэглээ)
# Импортын тэмдэг, Эрдэнэ БХ, гараас оруулсан утга зэрэг дүрэм Laravel талд нэг газар байна.
import config as _cfg
LARAVEL_ACTUAL_LOAD_URL = getattr(
    _cfg, 'LARAVEL_ACTUAL_LOAD_URL',
    LARAVEL_API_URL.replace('/forecast/store', '/forecast/actual-load')
)
HISTORY_START = '2024-01-05'
# Станцуудын нийлбэр СКАДА-аас үүнээс их зөрвөл (станцын телеметр дутуу — ж: Бөөрөлжүүт ЦС 2025.11-ээс өмнө,
# телеметрийн тасалдал) сургалтад СКАДА-д суурилсан утгыг авна. Илгээх/харуулах утга нь Хянах самбарынх хэвээр.
TRAIN_FALLBACK_MW = 50

print("📊 Laravel-аас цаг тутмын ачаалал татаж байна...")
print(f"   URL: {LARAVEL_ACTUAL_LOAD_URL}")

df_load = pd.DataFrame(columns=['time_', 'system_load', 'load', 'load_dashboard'])
try:
    resp = requests.get(LARAVEL_ACTUAL_LOAD_URL, params={'from': HISTORY_START}, timeout=180)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get('success') and payload.get('data'):
        df_load = pd.DataFrame(payload['data'])
        # time — цагийн эхлэлийн орон нутгийн цаг (z_conclusion-ий тэмдэглэгээтэй ижил)
        df_load['time_'] = pd.to_datetime(df_load['time'])
        df_load = df_load[['time_', 'system_load', 'load', 'scada_load']].astype(
            {'system_load': float, 'load': float, 'scada_load': float})
        df_load = df_load.sort_values('time_').reset_index(drop=True)

        # load_dashboard — Хянах самбарын «хэрэглээ» (Laravel-д илгээж харуулна)
        # load           — загварт (сургалт, lag) ашиглах цэвэрлэсэн утга
        df_load['load_dashboard'] = df_load['load']
        charge = df_load['system_load'] - df_load['load']          # БХ цэнэглэлт (≥ 0)
        bad = df_load['scada_load'].notna() & ((df_load['system_load'] - df_load['scada_load']).abs() > TRAIN_FALLBACK_MW)
        df_load.loc[bad, 'load'] = df_load.loc[bad, 'scada_load'] - charge[bad]
        df_load = df_load.drop(columns=['scada_load'])
        print(f"   Сургалтад СКАДА-аар орлуулсан цаг: {int(bad.sum())} (станцын нийлбэр {TRAIN_FALLBACK_MW} МВт-аас их зөрсөн)")
except Exception as e:
    print(f"❌ Алдаа: ачааллын өгөгдөл татаж чадсангүй: {e}")

if df_load.empty:
    print("❌ Алдаа: Өгөгдөл олдсонгүй!")
else:
    print(f"\n✅ Цагийн өгөгдөл бэлэн: {len(df_load)} цаг")
    print(f"   Хугацаа: {df_load['time_'].min()} - {df_load['time_'].max()}")
    print(f"\n📊 Хэрэглээний статистик:")
    print(f"   Станцуудын нийлбэр: {df_load['system_load'].min():.0f} - {df_load['system_load'].max():.0f} МВт")
    print(f"   Бодит хэрэглээ (Хянах самбар): {df_load['load_dashboard'].min():.0f} - {df_load['load_dashboard'].max():.0f} МВт")
    print(f"   Сургалтын хэрэглээ (цэвэрлэсэн): {df_load['load'].min():.0f} - {df_load['load'].max():.0f} МВт")
    print(f"   БХ цэнэглэлт нийт: {(df_load['system_load'] - df_load['load_dashboard']).sum():.0f} МВт.ц")

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
# Өдөрт нэг удаа л тооцоолж, файлд хадгална
# ==========================
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
daily_forecast_file = FILES['daily_forecast']

# Өмнөх таамаглал байгаа эсэхийг шалгах
need_new_forecast = True

if os.path.exists(daily_forecast_file):
    try:
        df_daily_existing = pd.read_csv(daily_forecast_file)
        df_daily_existing['time_'] = pd.to_datetime(df_daily_existing['time_'])

        # Файл дахь таамаглал өнөөдрийнх эсэхийг шалгах
        if len(df_daily_existing) > 0:
            forecast_date = df_daily_existing['time_'].iloc[0].date()
            if forecast_date == today.date():
                # Өнөөдрийн таамаглал аль хэдийн байна - файлаас унших
                df_daily_forecast = df_daily_existing
                need_new_forecast = False
                print(f"\n🔮 Өдрийн таамаглал: файлаас уншлаа ({len(df_daily_forecast)} цаг)")
                print(f"   Тооцоолсон огноо: {forecast_date}")
    except Exception as e:
        print(f"   ⚠️ Файл унших алдаа: {e}")
        need_new_forecast = True

if need_new_forecast:
    print(f"\n🔮 Өдрийн таамаглал шинээр тооцоолж байна...")
    print("   Арга: Weighted Average (Уржигдар 50% + Ижил гариг 30% + 7 хоногийн дундаж 20%)")
    future_hours_daily = []

    # Өнөөдрийн 00:00 хүртэлх өгөгдлийг авах
    baseline_time = today  # Өнөөдрийн 00:00
    df_historical = df[df['time_'] < baseline_time].copy()

    print(f"   Суурь өгөгдөл: {baseline_time.strftime('%Y-%m-%d %H:%M')} хүртэл")
    print(f"   Түүхэн өгөгдөл: {len(df_historical)} цаг")

    if len(df_historical) < 24*7:
        print(f"   ⚠️ Өгөгдөл хүрэлцэхгүй байна (хамгийн багадаа {24*7} цаг хэрэгтэй)")
    else:
        # Уржигдрийн өгөгдөл
        yesterday = today - timedelta(days=1)
        df_yesterday = df_historical[df_historical['time_'].dt.date == yesterday.date()].copy()

        # 7 хоногийн цаг бүрийн дундаж
        hourly_avg = df_historical.groupby('hour')['load'].mean().to_dict()

        # Долоо хоногийн өмнөх ижил гаригийн өгөгдөл
        same_weekday = today - timedelta(days=7)
        df_same_weekday = df_historical[df_historical['time_'].dt.date == same_weekday.date()].copy()

        print(f"   Уржигдрийн өгөгдөл: {len(df_yesterday)} цаг")
        print(f"   Өмнөх ижил гаригийн өгөгдөл: {len(df_same_weekday)} цаг")

        for hour in range(1, 25):  # 01:00 - 00:00 (маргааш)
            future_time = today + timedelta(hours=hour)
            h = future_time.hour

            # 1. Уржигдрийн ижил цагийн утга
            yesterday_same_hour = df_yesterday[df_yesterday['hour'] == h]['load'].values
            load_yesterday = yesterday_same_hour[0] if len(yesterday_same_hour) > 0 else None

            # 2. 7 хоногийн дундаж
            load_weekly_avg = hourly_avg.get(h, df_historical['load'].mean())

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

            future_hours_daily.append({'time_': future_time, 'forecast_daily': round(pred, 0)})

    df_daily_forecast = pd.DataFrame(future_hours_daily)

    # Шинэ таамаглалыг файлд хадгалах
    df_daily_forecast.to_csv(daily_forecast_file, index=False)
    print(f"   ✅ Шинэ таамаглал хадгалагдлаа: {len(df_daily_forecast)} цаг (01:00 → 00:00)")

# ==========================
# ⚡ ЦАГИЙН ТААМАГЛАЛ - AdaBoost модель
# ==========================
future_hours_hourly = []

# Сүүлийн бодит цагийг df-ээс авах
last_actual = df[df['time_'].dt.date == today.date()].tail(1)
if len(last_actual) == 0:
    last_actual = df.tail(1)

last_time = last_actual['time_'].values[0]
last_load = last_actual['load'].values[0]
last_hour = pd.to_datetime(last_time)

print(f"⚡ Цагийн таамаглал:")
print(f"   Арга: AdaBoost модель (load-1h, load-2h, load-3h)")
print(f"   Сүүлийн бодит (df-ээс): {last_hour.strftime('%Y-%m-%d %H:%M')} = {last_load:.0f} МВт")

# 01:00-оос сүүлийн бодит + 3 цаг хүртэл таамаглах
start_time = today + timedelta(hours=1)  # 01:00
end_time = last_hour + timedelta(hours=3)

# Сүүлийн 3 цагийн утгыг хадгалах (rolling forecast)
recent_loads = list(df[df['time_'] <= last_hour].tail(3)['load'].values)

current_time = start_time
while current_time <= end_time:
    # Бодит утга байвал ашиглах
    actual_data = df[df['time_'] == current_time]['load'].values
    if len(actual_data) > 0:
        # Бодит утга байна - list-д нэмээд үргэлжлүүлэх
        recent_loads.append(actual_data[0])
        if len(recent_loads) > 3:
            recent_loads.pop(0)
        future_hours_hourly.append({
            'time_': current_time,
            'forecast_hourly': round(actual_data[0], 0)
        })
        current_time += timedelta(hours=1)
        continue

    if len(recent_loads) < 3:
        current_time += timedelta(hours=1)
        continue

    temp_current = df_temp[df_temp['time_'] == current_time]['temp'].values
    if len(temp_current) == 0:
        temp_current = df_temp['temp'].mean()
    else:
        temp_current = temp_current[0]

    feature_hourly = {
        'month': current_time.month,
        'day': current_time.day,
        'hour': current_time.hour,
        'temp': temp_current,
        'wd': excel_weekday(current_time),
        'load-1h': recent_loads[-1],  # Сүүлийн утга (бодит эсвэл таамаглал)
        'load-2h': recent_loads[-2],
        'load-3h': recent_loads[-3],
    }

    pred_hourly = model_hourly.predict(pd.DataFrame([feature_hourly]))[0]
    future_hours_hourly.append({
        'time_': current_time,
        'forecast_hourly': round(pred_hourly, 0)
    })

    # Таамаглалын утгыг list-д нэмэх (дараагийн цагт ашиглах)
    recent_loads.append(pred_hourly)
    if len(recent_loads) > 3:
        recent_loads.pop(0)

    current_time += timedelta(hours=1)

df_hourly_forecast = pd.DataFrame(future_hours_hourly)

print(f"   → Нийт: {len(df_hourly_forecast)} цэг ({start_time.strftime('%H:%M')} → {end_time.strftime('%H:%M')})")

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
        start_time_new = today + timedelta(hours=1)  # 01:00-оос эхлэх
        end_time_new = last_hour_new + timedelta(hours=3)

        # Rolling forecast - сүүлийн 3 цагийн утга
        recent_loads_new = list(df[df['time_'] <= last_hour_new].tail(3)['load'].values)

        current_time_new = start_time_new
        while current_time_new <= end_time_new:
            # Бодит утга байвал ашиглах
            actual_data_new = df[df['time_'] == current_time_new]['load'].values
            if len(actual_data_new) > 0:
                recent_loads_new.append(actual_data_new[0])
                if len(recent_loads_new) > 3:
                    recent_loads_new.pop(0)
                future_hours_hourly_new.append({
                    'time_': current_time_new,
                    'forecast_hourly': round(actual_data_new[0], 0)
                })
                current_time_new += timedelta(hours=1)
                continue

            if len(recent_loads_new) < 3:
                current_time_new += timedelta(hours=1)
                continue

            temp_current = df_temp[df_temp['time_'] == current_time_new]['temp'].values
            if len(temp_current) == 0:
                temp_current = df_temp['temp'].mean()
            else:
                temp_current = temp_current[0]

            feature_hourly = {
                'month': current_time_new.month,
                'day': current_time_new.day,
                'hour': current_time_new.hour,
                'temp': temp_current,
                'wd': excel_weekday(current_time_new),
                'load-1h': recent_loads_new[-1],
                'load-2h': recent_loads_new[-2],
                'load-3h': recent_loads_new[-3],
            }

            pred_hourly = model_hourly.predict(pd.DataFrame([feature_hourly]))[0]
            future_hours_hourly_new.append({
                'time_': current_time_new,
                'forecast_hourly': round(pred_hourly, 0)
            })

            # Таамаглалыг дараагийн цагт ашиглах
            recent_loads_new.append(pred_hourly)
            if len(recent_loads_new) > 3:
                recent_loads_new.pop(0)

            current_time_new += timedelta(hours=1)

        # Шинэ forecast ашиглах
        df_hourly_forecast = pd.DataFrame(future_hours_hourly_new)
        print(f"   ✅ Шинэчилсэн: {len(df_hourly_forecast)} цаг ({start_time_new.strftime('%H:%M')} → {end_time_new.strftime('%H:%M')})")

fig, ax = plt.subplots(figsize=PLOT_CONFIG['figsize'])

# 1️⃣ Системийн нийт хэрэглээ (ягаан - батарейг хасаагүй)
if len(df_today_actual) > 0 and 'system_load' in df_today_actual.columns:
    ax.plot(df_today_actual['time_'], df_today_actual['system_load'],
            color='purple', linewidth=2.5, label='Системийн нийт хэрэглээ',
            linestyle=':', marker='s', markersize=4, alpha=0.6, zorder=2)
    print("   ✅ Системийн нийт хэрэглээ зурагдлаа")

# 2️⃣ Бодит хэрэглээ (улаан - батарей хассан)
if len(df_today_actual) > 0 and 'load_dashboard' in df_today_actual.columns:
    ax.plot(df_today_actual['time_'], df_today_actual['load_dashboard'],
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
        'value': row['load_dashboard'],      # Хянах самбарын «хэрэглээ»
        'system_load': row['system_load']    # Хянах самбарын станцуудын нийлбэр
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

# Цагийн таамаглал илгээх — зөвхөн бодит утга хараахан ороогүй цагууд.
# Бодит утгатай цагийг (df_hourly_forecast-д бодит утгаар бөглөсөн) илгээвэл өмнө нь хадгалсан
# жинхэнэ таамаглалыг дарж, таамаглалын нарийвчлалыг тооцох боломжгүй болгодог.
_actual_times = set(df_load['time_'])
hourly_data = [{'time': row['time_'], 'value': row['forecast_hourly']}
               for _, row in df_hourly_forecast.iterrows()
               if row['time_'] not in _actual_times]
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
                'value': row['load_dashboard'],
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