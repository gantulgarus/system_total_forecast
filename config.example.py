# -*- coding: utf-8 -*-
"""
Тохиргооны файл - config.py
Энэ файлыг config.py гэж хуулаад өөрийн тохиргоогоор бөглөнө үү
"""

# ==========================
# MySQL Database тохиргоо
# ==========================
DB_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'database': 'your_database',
    'port': 3306
}

# ==========================
# Laravel API тохиргоо
# ==========================
LARAVEL_API_URL = "http://localhost:8000/api/forecast/store"
LARAVEL_LAST_HISTORY_URL = "http://localhost:8000/api/forecast/last-history-time"

# ==========================
# Ulaanbaatar координат
# ==========================
LOCATION = {
    'latitude': 47.8864,
    'longitude': 106.9057,
    'timezone': 'Asia/Ulaanbaatar'
}

# ==========================
# Модель тохиргоо
# ==========================
MODEL_CONFIG = {
    'daily': {
        'max_depth': 12,
        'n_estimators': 50,
        'random_state': 42
    },
    'hourly': {
        'max_depth': 15,
        'n_estimators': 50,
        'random_state': 42
    },
    'test_size': 0.33
}

# ==========================
# Файлын нэрс
# ==========================
FILES = {
    'temperature': 'temperature_full.xlsx',
    'daily_forecast': 'forecast_daily_24h.csv',
    'hourly_forecast': 'forecast_hourly_3h.csv',
    'history': 'forecast_history.csv',
    'plot': 'forecast_today.png'
}

# ==========================
# График тохиргоо
# ==========================
PLOT_CONFIG = {
    'figsize': (16, 8),
    'dpi': 200,
    'colors': {
        'actual': 'red',
        'daily': 'dodgerblue',
        'hourly_today': 'green',
        'hourly_future': 'gold',
        'actual_point': 'darkgreen'
    }
}
