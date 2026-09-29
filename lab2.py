import datetime
import random
from gevent import monkey
monkey.patch_all()
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
from cassandra.cluster import Cluster


def init_database():
  """Підключення до Cassandra та створення структури даних"""
  print("Підключення до Apache Cassandra...")
  cluster = Cluster(["127.0.0.1"], port=9042)
  session = cluster.connect()

  # Створення Keyspace
  session.execute("""
        CREATE KEYSPACE IF NOT EXISTS lviv_smart_grid 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
  session.set_keyspace("lviv_smart_grid")

  # Створення таблиць
  session.execute("""
        CREATE TABLE IF NOT EXISTS meter_readings_asc (
            meter_id text,
            reading_date date,
            reading_time timestamp,
            active_power_kw double,
            reactive_power_kvar double,
            voltage double,
            current double,
            tariff_zone text,
            PRIMARY KEY ((meter_id, reading_date), reading_time)
        ) WITH CLUSTERING ORDER BY (reading_time ASC);
    """)

  session.execute("""
        CREATE TABLE IF NOT EXISTS district_hourly_metrics (
            district text,
            stat_date date,
            stat_hour int,
            avg_active_power double,
            peak_active_power double,
            total_consumption_kwh double,
            meters_reporting int,
            PRIMARY KEY ((district, stat_date), stat_hour)
        ) WITH CLUSTERING ORDER BY (stat_hour ASC);
    """)

  session.execute("""
        CREATE TABLE IF NOT EXISTS abnormal_consumption_history (
            district text,
            event_date date,
            event_time timestamp,
            meter_id text,
            anomaly_type text,
            measured_value double,
            threshold_value double,
            PRIMARY KEY ((district, event_date), event_time, meter_id)
        ) WITH CLUSTERING ORDER BY (event_time DESC, meter_id ASC);
    """)

  print("Схема даних (Keyspace та таблиці) успішно ініціалізована!")
  return cluster, session


def populate_daily_telemetry(session, target_meter_id, target_date):
  """Генерація 24-годинного профілю споживання для смарт-лічильника Львова"""
  print(
      f"\nГенерація добових даних для {target_meter_id} за {target_date}..."
  )

  insert_query = session.prepare("""
        INSERT INTO meter_readings_asc (
            meter_id, reading_date, reading_time, active_power_kw, 
            reactive_power_kvar, voltage, current, tariff_zone
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

  # Моделювання реалістичного добового навантаження
  # Нічний мінімум (02:00-05:00), ранковий пік (08:00), вечірній максимум (19:00-21:00)
  hourly_load_weights = {
      0: 1.4,
      1: 1.1,
      2: 0.9,
      3: 0.8,
      4: 0.7,
      5: 0.9,
      6: 1.8,
      7: 3.6,
      8: 4.8,
      9: 4.2,
      10: 3.5,
      11: 3.2,
      12: 3.0,
      13: 2.9,
      14: 3.1,
      15: 3.4,
      16: 4.0,
      17: 5.6,
      18: 7.8,
      19: 9.6,
      20: 11.4,
      21: 8.9,
      22: 5.1,
      23: 2.8,
  }

  for hour in range(24):
    timestamp = datetime.datetime.combine(
        target_date, datetime.time(hour=hour, minute=0)
    )

    base_power = hourly_load_weights[hour]
    active_power = round(base_power + random.uniform(-0.3, 0.4), 2)
    reactive_power = round(active_power * random.uniform(0.15, 0.25), 2)
    voltage = round(random.uniform(216.0, 224.0), 1)
    current = round((active_power * 1000) / voltage, 2)

    # Визначення зони
    if 23 <= hour or hour < 7:
      tariff = "night"
    elif 18 <= hour < 22:
      tariff = "peak"
    else:
      tariff = "day"

    session.execute(
        insert_query,
        (
            target_meter_id,
            target_date,
            timestamp,
            active_power,
            reactive_power,
            voltage,
            current,
            tariff,
        ),
    )

  print("✅ Успішно записано 24 погодинні точки вимірювань.")


def execute_dynamic_analysis_subvariant_c(
    session, target_meter_id, target_date
):
  """Виконання завдання ПІДВАРІАНТУ В (Динамічний аналіз)"""
  print("\n" + "=" * 70)
  print(
      f"📊 ДИНАМІЧНИЙ АНАЛІЗ ДОБОВОГО ПРОФІЛЮ (Підваріант В) | {target_meter_id}"
  )
  print(f"📅 Дата спостереження: {target_date}")
  print("=" * 70)

  # Вибірка даних по Partition Key за обрану добу
  query = """
        SELECT reading_time, active_power_kw, voltage, tariff_zone 
        FROM meter_readings_asc 
        WHERE meter_id = %s AND reading_date = %s
    """
  rows = list(session.execute(query, (target_meter_id, target_date)))

  if not rows:
    print("❌ Дані не знайдено!")
    return

  min_record = None
  max_record = None

  print(
      f"{'Година':<10} | {'Потужність (кВт)':<18} | {'Напруга (В)':<14} |"
      f" {'Тарифна зона':<12}"
  )
  print("-" * 65)

  for row in rows:
    hour_str = row.reading_time.strftime("%H:%M")
    power = row.active_power_kw
    voltage = row.voltage
    zone = row.tariff_zone.upper()

    print(f"{hour_str:<10} | {power:<18.2f} | {voltage:<14.1f} | {zone:<12}")

    # Пошук піків
    if min_record is None or power < min_record["power"]:
      min_record = {"hour": hour_str, "power": power}
    if max_record is None or power > max_record["power"]:
      max_record = {"hour": hour_str, "power": power}

  print("-" * 65)
  print("\n📈 ПІДСУМКОВІ АНАЛІТИЧНІ РЕЗУЛЬТАТИ:")
  print(
      f" 🟢 Добовий мінімум споживання:  {min_record['power']:.2f} кВт (зафіксовано"
      f" о {min_record['hour']})"
  )
  print(
      f" 🔴 Добовий максимум (ПІК):       {max_record['power']:.2f} кВт"
      f" (зафіксовано о {max_record['hour']})"
  )

  # Формулювання висновку за підваріантом В
  print("\n📝 АНАЛІТИЧНИЙ ВИСНОВОК ДЛЯ ЗВІТУ (Підваріант В):")
  print(
      f'   "Аналіз добової динаміки лічильника {target_meter_id} мережі м.'
      f' Львів за {target_date} демонструє виражену нерівномірність графіка'
      f' навантаження. \nПік навантаження спостерігається о {max_record["hour"]}'
      f' ({max_record["power"]:.2f} кВт) під час дії вечірньої тарифної зони'
      ' PEAK\n, що обумовлено масовим включенням побутових приладів. Мінімум'
      f' зафіксовано о {min_record["hour"]} ({min_record["power"]:.2f} кВт) у'
      ' період дії зони NIGHT\n, що свідчить про спад електроспоживання в нічні'
      ' години."'
  )
  print("=" * 70)


def insert_sample_aggregates_and_anomalies(session, target_date):
  """Наповнення інших двох обов'язкових таблиць для повного звіту варіанту 4"""
  # Агрегація по Галицькому району Львова
  session.execute("""
        INSERT INTO district_hourly_metrics (
            district, stat_date, stat_hour, avg_active_power, 
            peak_active_power, total_consumption_kwh, meters_reporting
        ) VALUES (
            'Галицький', '2026-09-22', 20, 8.45, 12.10, 42250.0, 5000
        );
    """)

  # Фіксація аномалії
  session.execute("""
        INSERT INTO abnormal_consumption_history (
            district, event_date, event_time, meter_id, 
            anomaly_type, measured_value, threshold_value
        ) VALUES (
            'Сихівський', '2026-09-22', toTimestamp(now()), 'METER_LV_0142', 
            'OVERLOAD_SPIKE', 14.85, 12.00
        );
    """)


def main():
  cluster, session = init_database()
  target_meter = "METER_LV_0042"
  target_date = datetime.date(2026, 9, 22)

  try:
    populate_daily_telemetry(session, target_meter, target_date)
    insert_sample_aggregates_and_anomalies(session, target_date)
    execute_dynamic_analysis_subvariant_c(session, target_meter, target_date)
  finally:
    session.shutdown()
    cluster.shutdown()
    print("\n🔌 З'єднання з Cassandra закрито.")


if __name__ == "__main__":
  main()
