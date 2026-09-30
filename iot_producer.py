import time
import json
import random
from kafka import KafkaProducer

KAFKA_BROKERS = ['localhost:9092']
TOPIC_RAW = 'smartgrid.meters.raw'

DISTRICTS = ['Сихівський', 'Галицький', 'Франківський', 'Шевченківський']
METERS = ['METER_LV_0001', 'METER_LV_0002', 'METER_LV_0003', 'METER_LV_0004']


def run_producer():
    print(f"[*] Підключення до Kafka ({KAFKA_BROKERS[0]})...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    print("[*] Старт генерації телеметрії лічильників...")
    # Симуляція 3 хвиль генерації з паузами для спрацювання Session Window
    for cycle in range(1, 4):
        print(f"\n--- [ХВИЛЯ СПОЖИВАННЯ #{cycle}]: Генерація активних сесій ---")
        for step in range(8):
            for meter_id in METERS:
                district = DISTRICTS[METERS.index(meter_id)]
                # Різний рівень споживання для residential та commercial
                is_commercial = 'COM' in meter_id or meter_id in ['METER_LV_0002', 'METER_LV_0004']
                base_power = random.uniform(8.0, 18.0) if is_commercial else random.uniform(2.0, 8.5)

                # Додаємо періодичний пік навантаження
                if random.random() < 0.25:
                    base_power *= 1.4

                reading = {
                    'meter_id': meter_id,
                    'district': district,
                    'timestamp': int(time.time() * 1000),
                    'active_power_kw': round(base_power, 2),
                    'reactive_power_kvar': round(base_power * random.uniform(0.18, 0.28), 2),
                    'voltage': round(random.gauss(222, 3), 1),
                    'frequency': round(random.gauss(50.0, 0.08), 3),
                    'power_factor': round(random.uniform(0.90, 0.98), 2),
                    'tariff_zone': 'PEAK' if step >= 4 else 'DAY'
                }

                producer.send(TOPIC_RAW, key=meter_id, value=reading)
            producer.flush()
            print(f" -> Відправлено пакет телеметрії (крок {step + 1}/8)")
            time.sleep(1)

        print("[*] Пауза активності для закриття сесійного вікна (GAP timeout)...")
        time.sleep(12)  # Пауза перевищує тестовий GAP у 10 секунд

    producer.close()
    print("[+] Генерацію тестових даних завершено!")


if __name__ == '__main__':
    run_producer()
