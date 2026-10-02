import time
import random
from datetime import datetime
from prometheus_client import start_http_server, Counter, Gauge

# Підваріант Б: Управління навантаженням

# 1. Поточне активне навантаження району (кВт)
DISTRICT_POWER = Gauge(
    'smartgrid_district_power_kw',
    'Поточне активне споживання електроенергії районом м. Львова (кВт)',
    ['district']
)

# 2. Прогнозоване навантаження на наступну годину (кВт)
FORECAST_POWER = Gauge(
    'smartgrid_forecast_power_kw',
    'Розраховане прогнозне навантаження району на наступну годину (кВт)',
    ['district']
)

# 3. Відсоток завантаження опорних трансформаторних підстанцій 110/10 кВ (%)
SUBSTATION_LOAD = Gauge(
    'smartgrid_critical_node_load_percent',
    'Коефіцієнт завантаження силових трансформаторів опорних підстанцій (%)',
    ['district', 'substation']
)

# 4. Коефіцієнт форми навантаження (Load Factor) = P_avg / P_max
LOAD_FACTOR = Gauge(
    'smartgrid_load_factor',
    'Коефіцієнт заповнення графіка навантаження (Load Factor = P_avg / P_max)',
    ['district']
)

# 5. Індекс ризику аварійного знеструмлення (0.0 - 1.0)
OUTAGE_RISK = Gauge(
    'smartgrid_outage_risk_index',
    'Ймовірність перевантаження та спрацювання автоматичного захисту лінії',
    ['district']
)

# 6. Лічильник телеметричних пакетів
TELEMETRY_COUNT = Counter(
    'smartgrid_telemetry_packets_total',
    'Загальна кількість отриманих пакетів телеметрії від розумних лічильників',
    ['district', 'status']
)

# Лінійні параметри мережі
VOLTAGE_GAUGE = Gauge('smartgrid_grid_voltage_v', 'Напруга в розподільчій мережі (В)', ['district'])
FREQUENCY_GAUGE = Gauge('smartgrid_grid_frequency_hz', 'Частота струму в мережі (Гц)', ['district'])

DISTRICTS = {
    'Сихівський': {'substation': 'ПС-110/10кВ "Сихів"', 'base_kw': 340, 'capacity': 600},
    'Галицький': {'substation': 'ПС-110/10кВ "Центральна"', 'base_kw': 390, 'capacity': 550},
    'Франківський': {'substation': 'ПС-110/10кВ "Франківська"', 'base_kw': 310, 'capacity': 500},
    'Шевченківський': {'substation': 'ПС-110/10кВ "Шевченківська"', 'base_kw': 330, 'capacity': 550},
    'Личаківський': {'substation': 'ПС-110/10кВ "Личаківська"', 'base_kw': 280, 'capacity': 480}
}

def simulate_smart_grid_load():
    history_windows = {d: [] for d in DISTRICTS}
    cycle = 0

    print("=" * 70)
    print("[*] Сервіс моніторингу Smart Grid Львова запущено!")
    print("    Метрики доступні за адресою: http://localhost:8000/metrics")
    print("=" * 70)

    while True:
        cycle += 1
        now = datetime.now()
        current_hour = now.hour

        # Симуляція добового профілю або штучного піку для демонстрації алерту
        # Перші 10 циклів - базове споживання, наступні - пікове навантаження 18:00-21:00
        is_peak_time = (18 <= current_hour <= 21) or (cycle % 12 >= 7)

        for district, cfg in DISTRICTS.items():
            base = cfg['base_kw']
            substation = cfg['substation']
            capacity = cfg['capacity']

            # Генерація фактичного споживання
            if is_peak_time:
                # Вечірній пік з можливим перевищенням критичного порогу 500 кВт
                load_multiplier = random.uniform(1.35, 1.65)
            else:
                load_multiplier = random.uniform(0.85, 1.15)

            actual_power = base * load_multiplier + random.uniform(-15, 20)
            actual_power = round(max(actual_power, 150.0), 2)

            # Прогнозоване навантаження на наступну годину (базується на ковзному середньому)
            hist = history_windows[district]
            hist.append(actual_power)
            if len(hist) > 10:
                hist.pop(0)

            avg_p = sum(hist) / len(hist)
            max_p = max(hist)
            forecast_power = round(avg_p * (1.10 if is_peak_time else 0.95) + random.uniform(-10, 10), 2)

            # Розрахунок Load Factor (P_avg / P_max)
            load_factor = round(avg_p / max_p, 3) if max_p > 0 else 1.0

            # Відсоток завантаження трансформаторної підстанції
            substation_load_pct = round((actual_power / capacity) * 100, 1)

            # Розрахунок індексу ризику аварії
            # Ризик зростає експоненційно при наближенні до 100% трансформатора
            risk = round(min(1.0, max(0.05, (substation_load_pct - 60) / 40.0)), 2)

            # Напруга і частота
            voltage = round(random.gauss(221.5, 3.2), 1)
            frequency = round(random.gauss(50.00, 0.05), 3)

            # Оновлення Gauge та Counter у Prometheus
            DISTRICT_POWER.labels(district=district).set(actual_power)
            FORECAST_POWER.labels(district=district).set(forecast_power)
            SUBSTATION_LOAD.labels(district=district, substation=substation).set(substation_load_pct)
            LOAD_FACTOR.labels(district=district).set(load_factor)
            OUTAGE_RISK.labels(district=district).set(risk)
            VOLTAGE_GAUGE.labels(district=district).set(voltage)
            FREQUENCY_GAUGE.labels(district=district).set(frequency)
            TELEMETRY_COUNT.labels(district=district, status='success').inc(random.randint(10, 25))

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Сихівський: {actual_power} кВт (Прогноз: {forecast_power} кВт) | "
              f"Завантаження ПС: {substation_load_pct}% | Load Factor: {load_factor} | Пік: {is_peak_time}")

        time.sleep(5)

if __name__ == '__main__':
    # Старт внутрішнього HTTP сервера Prometheus на порту 8000
    start_http_server(8000)
    simulate_smart_grid_load()
