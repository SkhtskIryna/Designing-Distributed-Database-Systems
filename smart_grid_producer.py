import json
import random
from datetime import datetime
from kafka import KafkaProducer


def get_smart_meter_payload(meter_num):
  return {
      "device_id": f"METER_LV_{meter_num:04d}",
      "timestamp": datetime.utcnow().isoformat() + "Z",
      "power_output": round(random.uniform(0.5, 15.0), 2),
      "efficiency": round(random.uniform(95.0, 100.0), 2),
      "temperature": round(random.uniform(-5.0, 35.0), 1),
      "voltage": round(random.uniform(210.0, 240.0), 1),
      "current": round(random.uniform(2.0, 65.0), 2),
      "status": random.choices(
          ["active", "inactive", "error"], weights=[0.96, 0.03, 0.01]
      )[0],
      "location": {
          "lat": round(random.uniform(49.80, 49.90), 4),
          "lon": round(random.uniform(24.00, 24.10), 4),
      },
      "maintenance_hours": random.randint(6000, 12000),
      # Специфічні метрики Варіанту 4:
      "power_factor": round(random.uniform(0.70, 1.00), 2),
      "frequency": round(random.uniform(49.80, 50.20), 2),
      "tariff_zone": random.choice(["day", "night", "peak"]),
      "reserved": "smart_grid_lviv_telemetry_pad_bytes_256",
  }


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    batch_size=1048576,  # 1MB для Підваріанту B
    linger_ms=100,  # 100ms
    compression_type="zstd",  # Zstandard для DWH
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(
    "Генерація та відправка 500 записів Smart Grid Львів (Підваріант B: ZSTD +"
    " 1MB batch)..."
)
for i in range(1, 501):
  data = get_smart_meter_payload(random.randint(1, 5000))
  # Key-based партиціонування за тарифною зоною для аналітики
  producer.send(
      "smart-grid-main", key=data["tariff_zone"].encode("utf-8"), value=data
  )

producer.flush()
print("Дані успішно записані в топік!")