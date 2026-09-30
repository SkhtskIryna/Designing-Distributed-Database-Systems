import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

def test_consistency_levels():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('lviv_smart_grid_opt')

    levels = [
        ('ONE', ConsistencyLevel.ONE),
        ('QUORUM', ConsistencyLevel.QUORUM),
        ('ALL', ConsistencyLevel.ALL)
    ]

    print("\n" + "="*60)
    print("ТЕСТУВАННЯ РІВНІВ УЗГОДЖЕНОСТІ (CONSISTENCY LEVEL)")
    print("="*60)

    query = "SELECT * FROM meter_readings_hourly LIMIT 100;"

    for name, level in levels:
        stmt = SimpleStatement(query, consistency_level=level)
        latencies = []
        try:
            for _ in range(50):
                t0 = time.perf_counter()
                _ = list(session.execute(stmt))
                latencies.append((time.perf_counter() - t0) * 1000.0)

            avg_l = sum(latencies) / len(latencies)
            p95_l = sorted(latencies)[int(len(latencies) * 0.95)]
            print(f"Рівень: {name:<8} | Середня: {avg_l:>6.2f} ms | P95: {p95_l:>6.2f} ms | Статус: Успішно")
        except Exception as e:
            print(f"Рівень: {name:<8} | Помилка: {e}")

    print("="*60 + "\n")
    cluster.shutdown()

if __name__ == '__main__':
    test_consistency_levels()