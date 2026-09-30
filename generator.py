import time
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

DISTRICTS = ['Галицький', 'Франківський', 'Сихівський', 'Шевченківський', 'Личаківський', 'Залізничний']


def get_tariff_zone(hour: int) -> str:
    if 23 <= hour or hour < 7:
        return 'NIGHT'
    elif 18 <= hour < 22:
        return 'PEAK'
    else:
        return 'DAY'


def run_generator(total_target=100000, batch_size=5000, days_back=30):
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('lviv_smart_grid_opt')

    insert_simple = session.prepare("""
        INSERT INTO meter_readings_simple 
        (meter_id, reading_time, district, active_power_kw, reactive_power_kvar, voltage, frequency, power_factor, tariff_zone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    insert_hourly = session.prepare("""
        INSERT INTO meter_readings_hourly 
        (district, meter_id, bucket_hour, reading_time, active_power_kw, reactive_power_kvar, voltage, frequency, power_factor, tariff_zone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    insert_daily = session.prepare("""
        INSERT INTO meter_readings_daily 
        (district, meter_id, bucket_date, reading_time, active_power_kw, reactive_power_kvar, voltage, frequency, power_factor, tariff_zone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    print(f"[*] Генерація {total_target} записів у 3 схеми одночасно...")
    start_time = time.time()
    inserted = 0
    now = datetime.now()
    meters = [f"METER_LV_{i:04d}" for i in range(1, 201)]

    while inserted < total_target:
        chunk = min(batch_size, total_target - inserted)
        args_simple = []
        args_hourly = []
        args_daily = []

        for _ in range(chunk):
            meter = random.choice(meters)
            district = DISTRICTS[hash(meter) % len(DISTRICTS)]

            # Генерація часової мітки у межах останніх days_back днів
            offset_sec = random.randint(0, days_back * 86400)
            rec_time = now - timedelta(seconds=offset_sec)
            bucket_hr = rec_time.replace(minute=0, second=0, microsecond=0)
            bucket_dt = rec_time.date()

            hour = rec_time.hour
            tz = get_tariff_zone(hour)

            # Профіль навантаження за тарифами
            if tz == 'PEAK':
                p_kw = round(random.uniform(5.5, 11.5), 2)
            elif tz == 'DAY':
                p_kw = round(random.uniform(1.8, 5.5), 2)
            else:
                p_kw = round(random.uniform(0.4, 2.0), 2)

            q_kvar = round(p_kw * random.uniform(0.15, 0.30), 2)
            u_v = round(random.uniform(217.0, 233.0), 1)

            # Частота 50 Гц з аномаліями у 4% записів (для Materialized Views)
            if random.random() < 0.04:
                freq = round(random.choice([random.uniform(49.4, 49.78), random.uniform(50.22, 50.48)]), 3)
            else:
                freq = round(random.uniform(49.85, 50.15), 3)

            cos_phi = round(random.uniform(0.88, 0.98), 2)

            args_simple.append((meter, rec_time, district, p_kw, q_kvar, u_v, freq, cos_phi, tz))
            args_hourly.append((district, meter, bucket_hr, rec_time, p_kw, q_kvar, u_v, freq, cos_phi, tz))
            args_daily.append((district, meter, bucket_dt, rec_time, p_kw, q_kvar, u_v, freq, cos_phi, tz))

        # Асинхронна вставка
        execute_concurrent_with_args(session, insert_hourly, args_hourly, concurrency=50)
        execute_concurrent_with_args(session, insert_simple, args_simple, concurrency=50)
        execute_concurrent_with_args(session, insert_daily, args_daily, concurrency=50)

        inserted += chunk
        rate = inserted / (time.time() - start_time)
        print(f" -> Оброблено {inserted}/{total_target} | Пропускна здатність: {rate:.1f} оп/сек")

    # Генерація попередньо агрегованих добових метрик
    print("[*] Генерація даних для district_tariff_daily_aggregates...")
    insert_agg = session.prepare("""
        INSERT INTO district_tariff_daily_aggregates 
        (district, stat_date, tariff_zone, avg_active_power, peak_active_power, avg_frequency, total_consumption_kwh, records_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    for d in DISTRICTS:
        for offset in range(days_back):
            d_date = (now - timedelta(days=offset)).date()
            for zone in ['NIGHT', 'DAY', 'PEAK']:
                avg_p = round(random.uniform(1.5, 8.0), 2)
                peak_p = round(avg_p * random.uniform(1.3, 1.8), 2)
                session.execute(insert_agg, (d, d_date, zone, avg_p, peak_p, 50.002, round(avg_p * 8 * 200, 2), 240))

    cluster.shutdown()
    print(f"[+] Успішно завершено за {time.time() - start_time:.2f} с.")


if __name__ == '__main__':
    run_generator(total_target=100000, batch_size=5000)