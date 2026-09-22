import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer


def create_producer():
  """Створюємо Kafka producer під Варіант 4, Підваріант B (Analytics focus)"""
  print("🔌 Підключаємося до Kafka як Smart Grid Producer...")

  try:
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        # Серіалізація об'єктів у JSON UTF-8
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
            "utf-8"
        ),
        # Налаштування надійності
        acks=1,  # Для високої пропускної здатності аналітики достатньо 1
        retries=3,
        request_timeout_ms=30000,
        retry_backoff_ms=500,
        # Специфіка Підваріанту B (максимальний Throughput та буферизація для DWH)
        batch_size=1048576,  # 1MB розмір батчу для агрегації
        linger_ms=100,  # Чекаємо 100мс для накопичення великих пакетів
        compression_type="zstd",  # Zstandard — найкращий коефіцієнт стискання для аналітики
        max_in_flight_requests_per_connection=5,
    )

    print("✅ Підключення до Kafka успішне (Analytics Producer готовий)!")
    return producer

  except Exception as e:
    print(f"❌ Помилка підключення: {e}")
    print("Перевірте, чи запущено Kafka на localhost:9092")
    return None


def generate_smart_meter_data():
  """Генеруємо телеметрію смарт-лічильника м. Львів (Варіант 4)"""
  meter_num = random.randint(1, 5000)
  tariff_zones = ["day", "night", "peak"]
  statuses = ["active", "inactive", "error"]

  data = {
      # Базові поля
      "device_id": f"METER_LV_{meter_num:04d}",
      "timestamp": datetime.now().isoformat(),
      "power_output": round(
          random.uniform(0.5, 15.0), 2
      ),  # споживання у кВт (0.5-15.0)
      "efficiency": round(
          random.uniform(95.0, 100.0), 2
      ),  # точність вимірювання % (95-100)
      "temperature": round(
          random.uniform(-5.0, 35.0), 1
      ),  # температура довкілля °C
      "voltage": round(random.uniform(210.0, 240.0), 1),  # напруга В (210-240)
      "current": round(random.uniform(2.0, 65.0), 2),  # сила струму А (2-65)
      "status": random.choices(statuses, weights=[0.96, 0.03, 0.01])[0],
      "location": {
          "lat": round(random.uniform(49.80, 49.90), 4),  # координати Львова
          "lon": round(random.uniform(24.00, 24.10), 4),
      },
      "maintenance_hours": random.randint(6000, 12000),  # годин до ТО
      # Специфічні поля Варіанту 4:
      "power_factor": round(
          random.uniform(0.70, 1.00), 2
      ),  # коефіцієнт потужності cos φ
      "frequency": round(
          random.uniform(49.80, 50.20), 2
      ),  # частота мережі Гц
      "tariff_zone": random.choice(tariff_zones),  # тарифна зона
      "reserved": "smart_grid_lviv_telemetry_pad_bytes_256",
  }

  return data


def main():
  """Основна функція відправки потоку телеметрії"""
  producer = create_producer()
  if not producer:
    return

  print("🚀 Починаємо надсилання потоку Smart Grid Львова в Kafka...")
  print("📊 Натисніть Ctrl+C для зупинки\n")

  message_count = 0
  topic_name = "smart-grid-main"

  try:
    while True:
      meter_data = generate_smart_meter_data()

      try:
        # Key-based партиціонування за tariff_zone (для аналітичної DWH-сегментації)
        partition_key = meter_data["tariff_zone"].encode("utf-8")

        future = producer.send(
            topic_name, key=partition_key, value=meter_data
        )

        record_metadata = future.get(timeout=10)
        message_count += 1

        print(
            f"📤 [{message_count}] {meter_data['device_id']} | Зона:"
            f" {meter_data['tariff_zone'].upper()} | Споживання:"
            f" {meter_data['power_output']} кВт"
        )
        print(
            f"   Partition: {record_metadata.partition}, Offset:"
            f" {record_metadata.offset}, Cos φ: {meter_data['power_factor']},"
            f" Частота: {meter_data['frequency']} Гц"
        )

      except Exception as e:
        print(f"❌ Помилка відправки: {e}")

      time.sleep(0.5)  # Затримка пів секунди для наочного виводу в терміналі

  except KeyboardInterrupt:
    print(
        f"\n🛑 Генерацію зупинено. Всього надіслано {message_count} повідомлень"
    )

  finally:
    producer.flush()
    producer.close()
    print("🔌 З'єднання з Kafka закрито.")


if __name__ == "__main__":
  main()