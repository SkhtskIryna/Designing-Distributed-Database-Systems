import json
import time
from collections import defaultdict
from kafka import KafkaConsumer


def safe_json_deserializer(raw_bytes):
  """Безпечне декодування JSON: ігнорує порожні повідомлення та звичайний текст"""
  if not raw_bytes:
    return None
  try:
    return json.loads(raw_bytes.decode('utf-8'))
  except Exception:
    return {'_raw_text': raw_bytes.decode('utf-8', errors='ignore')}


def create_consumer():
  """Створюємо Kafka consumer для аналітики Smart Grid Львова"""
  print('🔌 Підключаємося до Kafka як Analytics Consumer...')

  try:
    consumer = KafkaConsumer(
        'smart-grid-main',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Читаємо всі накопичені дані
        group_id=f'smart-grid-analytics-{int(time.time())}',
        value_deserializer=safe_json_deserializer,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        fetch_min_bytes=1024,
        fetch_max_wait_ms=1000,
        max_poll_records=1000,
    )

    print('✅ Consumer для Smart Grid (Analytics) готовий до роботи!\n')
    return consumer

  except Exception as e:
    print(f'❌ Помилка підключення: {e}')
    return None


def main():
  consumer = create_consumer()
  if not consumer:
    return

  print('📊 Очікуємо IoT-потік показників лічильників Львова...')
  print('🛑 Натисніть Ctrl+C для формування фінального звіту\n')

  total_messages = 0
  tariff_consumption = defaultdict(float)
  tariff_counts = defaultdict(int)
  power_factors = []
  voltages = []

  try:
    for message in consumer:
      data = message.value

      # Ігноруємо порожні повідомлення
      if not data:
        continue

      # Якщо це тестовий текст із консолі продюсера — просто показуємо його й не ламаємо скрипт
      if '_raw_text' in data:
        print(f"⚠️ [ТЕКСТОВЕ ПОВІДОМЛЕННЯ]: {data['_raw_text']}")
        continue

      total_messages += 1

      # Читаємо метрики Варіанту 4
      meter_id = data.get('device_id', 'METER_UNKNOWN')
      power = float(data.get('power_output', 0.0))
      voltage = float(data.get('voltage', 220.0))
      pf = float(data.get('power_factor', 0.9))
      frequency = float(data.get('frequency', 50.0))
      tariff = str(data.get('tariff_zone', 'day'))

      # Агрегація показників для Підваріанту B
      tariff_consumption[tariff] += power
      tariff_counts[tariff] += 1
      power_factors.append(pf)
      voltages.append(voltage)

      # Формування аналітичного зрізу (Batch Ingestion)
      if total_messages % 10 == 0:
        print(
            f'📦 [MICRO-BATCH] Оброблено коректних записів: {total_messages} |'
            f' Останній: {meter_id}'
        )
        print(
            f'   ⚡ Потужність: {power} кВт | Напруга: {voltage} В | Cos φ: {pf}'
            f' | Частота: {frequency} Гц'
        )
        print('   📈 Споживання за тарифами:')
        for t_zone, total_p in tariff_consumption.items():
          count = tariff_counts[t_zone]
          avg_p = total_p / count if count else 0
          print(
              f'      🔹 {t_zone.upper()}: сумарно {total_p:.2f} кВт·год |'
              f' сер.: {avg_p:.2f} кВт (записів: {count})'
          )
        avg_pf = sum(power_factors) / len(power_factors)
        print(f'   ⚙️ Середній коефіцієнт потужності (cos φ): {avg_pf:.3f}')
        print('-' * 65)

  except KeyboardInterrupt:
    print(f'\n🛑 Обробку зупинено.')
    print(f'📊 ПІДСУМКОВА АНАЛІТИКА ДЛЯ DATA WAREHOUSE:')
    print(f' • Всього оброблено телеметрії: {total_messages} записів')
    for t_zone, total_p in tariff_consumption.items():
      print(f' • {t_zone.upper()}: {total_p:.2f} кВт·год')
    if power_factors:
      print(
          f' • Середній cos φ мережі:'
          f' {sum(power_factors)/len(power_factors):.3f}'
      )
  finally:
    consumer.close()
    print("🔌 З'єднання з Kafka закрите.")


if __name__ == '__main__':
  main()
