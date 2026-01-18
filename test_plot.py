# -*- coding: utf-8 -*-
"""
График зураах тестийн скрипт
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

# Туршилтын өгөгдөл үүсгэх
today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
times = [today + timedelta(hours=i) for i in range(24)]
system_load = [1000 + i * 10 + (i % 3) * 50 for i in range(24)]
load = [900 + i * 10 + (i % 3) * 45 for i in range(24)]

df_test = pd.DataFrame({
    'time_': times,
    'system_load': system_load,
    'load': load
})

print("📊 Тестийн өгөгдөл:")
print(f"   Мөр тоо: {len(df_test)}")
print(f"   Баганууд: {df_test.columns.tolist()}")
print(f"\n   system_load: {df_test['system_load'].min():.0f} - {df_test['system_load'].max():.0f}")
print(f"   load: {df_test['load'].min():.0f} - {df_test['load'].max():.0f}")

# График зурах
fig, ax = plt.subplots(figsize=(16, 8))

# Системийн нийт хэрэглээ
ax.plot(df_test['time_'], df_test['system_load'],
        color='purple', linewidth=2.5, label='Системийн нийт хэрэглээ',
        linestyle=':', marker='s', markersize=4, alpha=0.6)

# Бодит хэрэглээ
ax.plot(df_test['time_'], df_test['load'],
        color='red', linewidth=3.5, label='Бодит хэрэглээ (батарей хассан)',
        marker='o', markersize=6)

# График тохиргоо
ax.set_xlabel('Цаг', fontsize=14, fontweight='bold')
ax.set_ylabel('Хэрэглээ, МВт', fontsize=14, fontweight='bold')
ax.set_title(f"Тестийн график - {today.strftime('%Y-%m-%d')}",
             fontsize=16, fontweight='bold', pad=20)
ax.grid(True, linestyle='--', alpha=0.4)
ax.legend(fontsize=11, loc='best', framealpha=0.95, edgecolor='black')

ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('test_forecast.png', dpi=200, bbox_inches='tight')
plt.close()

print(f"\n✅ Тестийн график хадгалагдлаа: test_forecast.png")
