import time
import statistics
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

class PerformanceBenchmark:
    def __init__(self, keyspace='lviv_smart_grid_opt'):
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect(keyspace)

    def measure(self, query, params=None, iterations=50):
        stmt = self.session.prepare(query) if params else query
        latencies = []
        for _ in range(iterations):
            start = time.perf_counter()
            if params:
                res = self.session.execute(stmt, params)
            else:
                res = self.session.execute(stmt)
            _ = list(res)
            end = time.perf_counter()
            latencies.append((end - start) * 1000.0)

        latencies.sort()
        return {
            'avg': statistics.mean(latencies),
            'p50': statistics.median(latencies),
            'p95': latencies[int(len(latencies) * 0.95)],
            'p99': latencies[int(len(latencies) * 0.99)],
            'min': min(latencies),
            'max': max(latencies)
        }

    def close(self):
        self.cluster.shutdown()

def print_row(schema, q_type, res):
    print(f"{schema:<18} | {q_type:<18} | {res['avg']:>8.2f} ms | {res['p95']:>8.2f} ms | {res['p99']:>8.2f} ms")

def main():
    bench = PerformanceBenchmark()
    target_meter = 'METER_LV_0042'
    target_district = 'Сихівський'
    now = datetime.now()
    bucket_hr = now.replace(minute=0, second=0, microsecond=0)
    bucket_dt = now.date()

    print("\n" + "="*80)
    print(f"{'Схема':<18} | {'Тип запиту':<18} | {'Avg':>11} | {'P95':>11} | {'P99':>11}")
    print("="*80)

    # 1. Latest 100
    q_s1 = "SELECT * FROM meter_readings_simple WHERE meter_id = ? LIMIT 100;"
    q_h1 = "SELECT * FROM meter_readings_hourly WHERE district = ? AND meter_id = ? AND bucket_hour = ? LIMIT 100;"
    q_d1 = "SELECT * FROM meter_readings_daily WHERE district = ? AND meter_id = ? AND bucket_date = ? LIMIT 100;"

    print_row("Simple (Wide Row)", "Latest 100", bench.measure(q_s1, [target_meter]))
    print_row("Hourly Bucketing",  "Latest 100", bench.measure(q_h1, [target_district, target_meter, bucket_hr]))
    print_row("Daily Bucketing",   "Latest 100", bench.measure(q_d1, [target_district, target_meter, bucket_dt]))

    # 2. 6 Hour Range
    t_start = now - timedelta(hours=6)
    q_s2 = "SELECT * FROM meter_readings_simple WHERE meter_id = ? AND reading_time >= ?;"
    q_h2 = "SELECT * FROM meter_readings_hourly WHERE district = ? AND meter_id = ? AND bucket_hour = ?;"
    q_d2 = "SELECT * FROM meter_readings_daily WHERE district = ? AND meter_id = ? AND bucket_date = ? AND reading_time >= ?;"

    print_row("Simple (Wide Row)", "6 Hour Range", bench.measure(q_s2, [target_meter, t_start]))
    print_row("Hourly Bucketing",  "6 Hour Range", bench.measure(q_h2, [target_district, target_meter, bucket_hr]))
    print_row("Daily Bucketing",   "6 Hour Range", bench.measure(q_d2, [target_district, target_meter, bucket_dt, t_start]))

    # 3. Daily Aggregation
    q_s3 = "SELECT * FROM meter_readings_simple WHERE meter_id = ? AND reading_time >= ?;"
    q_d3 = "SELECT * FROM district_tariff_daily_aggregates WHERE district = ? AND stat_date = ?;"

    print_row("Simple (Wide Row)", "Daily Aggregation", bench.measure(q_s3, [target_meter, now - timedelta(days=1)]))
    print_row("Daily Pre-agg",     "Daily Aggregation", bench.measure(q_d3, [target_district, bucket_dt]))
    print("="*80)

    # 4. Порівняння Materialized View та ALLOW FILTERING
    print("\n" + "="*80)
    print("ДОСЛІДЖЕННЯ MATERIALIZED VIEWS VS ALLOW FILTERING")
    print("="*80)

    # Тест 1: Аномалії частоти
    q_af_freq = """
        SELECT * FROM meter_readings_hourly 
        WHERE district = ? AND meter_id = ? AND bucket_hour = ? AND frequency > 50.20 
        ALLOW FILTERING;
    """
    q_mv_freq = """
        SELECT * FROM mv_high_frequency_anomalies 
        WHERE district = ? AND meter_id = ? AND bucket_hour = ? AND frequency > 50.20;
    """
    res_af_f = bench.measure(q_af_freq, [target_district, target_meter, bucket_hr])
    res_mv_f = bench.measure(q_mv_freq, [target_district, target_meter, bucket_hr])

    print(f"Частотні аномалії (f > 50.2 Гц):")
    print(f"  * ALLOW FILTERING:   Avg = {res_af_f['avg']:.2f} ms | P95 = {res_af_f['p95']:.2f} ms")
    print(f"  * Materialized View: Avg = {res_mv_f['avg']:.2f} ms | P95 = {res_mv_f['p95']:.2f} ms")
    print(f"  -> Прискорення (Speedup): {res_af_f['avg'] / res_mv_f['avg']:.1f}x швидше!\n")

    # Тест 2: Пікова потужність
    q_af_power = """
        SELECT * FROM meter_readings_hourly 
        WHERE district = ? AND meter_id = ? AND bucket_hour = ? AND active_power_kw > 9.0 
        ALLOW FILTERING;
    """
    q_mv_power = """
        SELECT * FROM mv_peak_load 
        WHERE district = ? AND meter_id = ? AND bucket_hour = ? AND active_power_kw > 9.0;
    """
    res_af_p = bench.measure(q_af_power, [target_district, target_meter, bucket_hr])
    res_mv_p = bench.measure(q_mv_power, [target_district, target_meter, bucket_hr])

    print(f"Пікова потужність (P > 9.0 кВт):")
    print(f"  * ALLOW FILTERING:   Avg = {res_af_p['avg']:.2f} ms | P95 = {res_af_p['p95']:.2f} ms")
    print(f"  * Materialized View: Avg = {res_mv_p['avg']:.2f} ms | P95 = {res_mv_p['p95']:.2f} ms")
    print(f"  -> Прискорення (Speedup): {res_af_p['avg'] / res_mv_p['avg']:.1f}x швидше!")
    print("="*80 + "\n")

    bench.close()

if __name__ == '__main__':
    main()