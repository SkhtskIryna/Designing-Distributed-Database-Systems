import time
import json
import uuid
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

KAFKA_BROKERS = ['localhost:9092']
TOPIC_RAW = 'smartgrid.meters.raw'
KEYSPACE = 'lviv_smart_grid_billing'

# Тестовий GAP таймаут: 10 секунд (симулює 10 хвилин у real-time)
SESSION_GAP_SEC = 10.0

# Тарифи Львова
RATES = {
    'residential': 4.32,  # грн / кВт·год
    'commercial': 8.60  # грн / кВт·год
}


class TwoPhaseCommitCoordinator:
    """Координатор розподілених транзакцій 2PC між двома таблицями Cassandra"""

    def __init__(self, session):
        self.session = session

        # Prepared statements
        self.prep_account_check = session.prepare("""
            SELECT account_id, balance_uah, credit_limit_uah, status, consumer_type 
            FROM customer_accounts WHERE meter_id = ?
        """)

        # ВИПРАВЛЕНО: передаємо готове нове значення балансу
        self.prep_account_update = session.prepare("""
            UPDATE customer_accounts 
            SET balance_uah = ?, last_updated = toTimestamp(now()) 
            WHERE meter_id = ?
        """)

        self.prep_session_insert = session.prepare("""
            INSERT INTO billing_sessions 
            (district, meter_id, session_id, start_time, end_time, duration_sec, readings_count,
             total_kwh, peak_power_kw, rate_uah_per_kwh, peak_demand_charge_uah, total_amount_uah, tx_id, tx_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        self.prep_tx_log = session.prepare("""
            INSERT INTO transaction_log 
            (tx_id, tx_timestamp, meter_id, session_id, phase, status, total_amount_uah, participant_votes, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

    def execute_transaction(self, session_data):
        tx_id = f"TX_2PC_{uuid.uuid4().hex[:8].upper()}"
        meter_id = session_data['meter_id']
        total_amount = session_data['total_amount_uah']
        now = datetime.now()

        print(f"\n[2PC COORDINATOR] Запуск транзакції {tx_id} для {meter_id} (Сума: {total_amount:.2f} грн)")

        # ФАЗА 1: PREPARE
        votes = {}

        # Учасник 1: Валідація сесії
        if session_data['total_kwh'] > 0 and session_data['readings_count'] > 0:
            votes['Participant_1_BillingSessions'] = 'VOTE_COMMIT'
        else:
            votes['Participant_1_BillingSessions'] = 'VOTE_ABORT'

        # Учасник 2: Перевірка балансу та статусу рахунку
        account_row = self.session.execute(self.prep_account_check, [meter_id]).one()
        if not account_row:
            votes['Participant_2_CustomerAccounts'] = 'VOTE_ABORT (Account Not Found)'
        elif account_row.status != 'ACTIVE':
            votes['Participant_2_CustomerAccounts'] = 'VOTE_ABORT (Account Suspended)'
        else:
            available_funds = account_row.balance_uah + account_row.credit_limit_uah
            if available_funds >= total_amount:
                votes['Participant_2_CustomerAccounts'] = 'VOTE_COMMIT'
            else:
                votes[
                    'Participant_2_CustomerAccounts'] = f'VOTE_ABORT (Deficit: {total_amount - available_funds:.2f} UAH)'

        # Логування фази PREPARE в transaction_log
        prepare_ok = all(v == 'VOTE_COMMIT' for v in votes.values())
        self.session.execute(self.prep_tx_log, (
            tx_id, now, meter_id, session_data['session_id'],
            '1_PREPARE', 'SUCCESS' if prepare_ok else 'REJECTED',
            total_amount, json.dumps(votes),
            'Всі учасники підтвердили готовність' if prepare_ok else 'Один із учасників відхилив підготовку'
        ))

        # ФАЗА 2: COMMIT або ABORT
        if prepare_ok and account_row:
            # Обчислення нового балансу в коді програми
            new_balance = round(account_row.balance_uah - total_amount, 2)

            # Атомарне застосування змін в обох таблицях
            self.session.execute(self.prep_account_update, [new_balance, meter_id])
            self.session.execute(self.prep_session_insert, (
                session_data['district'], meter_id, session_data['session_id'],
                datetime.fromtimestamp(session_data['start_ts']),
                datetime.fromtimestamp(session_data['end_ts']),
                int(session_data['end_ts'] - session_data['start_ts']),
                session_data['readings_count'],
                round(session_data['total_kwh'], 4),
                round(session_data['peak_power_kw'], 2),
                round(session_data['rate'], 2),
                round(session_data['peak_charge'], 2),
                round(total_amount, 2),
                tx_id, 'COMMITTED'
            ))
            self.session.execute(self.prep_tx_log, (
                tx_id, datetime.now(), meter_id, session_data['session_id'],
                '2_COMMIT', 'SUCCESS', total_amount, json.dumps(votes),
                f'Кошти успішно списано. Новий баланс: {new_balance:.2f} грн'
            ))
            print(
                f" -> [2PC COMMIT]: Транзакція {tx_id} УСПІШНА! Списано {total_amount:.2f} грн. Новий баланс: {new_balance:.2f} грн.")
            return True
        else:
            # Фіксація відхилення (ABORT)
            self.session.execute(self.prep_session_insert, (
                session_data['district'], meter_id, session_data['session_id'],
                datetime.fromtimestamp(session_data['start_ts']),
                datetime.fromtimestamp(session_data['end_ts']),
                int(session_data['end_ts'] - session_data['start_ts']),
                session_data['readings_count'],
                round(session_data['total_kwh'], 4),
                round(session_data['peak_power_kw'], 2),
                round(session_data['rate'], 2),
                round(session_data['peak_charge'], 2),
                round(total_amount, 2),
                tx_id, 'ABORTED'
            ))
            self.session.execute(self.prep_tx_log, (
                tx_id, datetime.now(), meter_id, session_data['session_id'],
                '2_ABORT', 'REJECTED_INSUFFICIENT_FUNDS', total_amount, json.dumps(votes),
                'Транзакцію відхилено через брак ліміту/коштів'
            ))
            print(f" -> [2PC ABORT]: Транзакцію {tx_id} ВІДХИЛЕНО! {votes.get('Participant_2_CustomerAccounts')}")
            return False


class StreamProcessor:
    def __init__(self):
        print("[*] Ініціалізація підключень до Cassandra та Kafka...")
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.cassandra_session = self.cluster.connect(KEYSPACE)
        self.coordinator = TwoPhaseCommitCoordinator(self.cassandra_session)

        # State Store в пам'яті (активні сесії для кожного лічильника)
        self.active_sessions = {}
        # Профілі споживачів
        self.consumer_profiles = {
            'METER_LV_0001': 'residential',
            'METER_LV_0002': 'commercial',
            'METER_LV_0003': 'residential',
            'METER_LV_0004': 'commercial'
        }

    def process_reading(self, reading):
        meter_id = reading['meter_id']
        current_time = reading['timestamp'] / 1000.0
        power_kw = reading['active_power_kw']
        district = reading['district']

        session = self.active_sessions.get(meter_id)

        # Перевірка на закриття попередньої сесії через перевищення GAP таймауту
        if session and (current_time - session['last_event_ts'] > SESSION_GAP_SEC):
            self.finalize_session(session)
            session = None

        # Створення нової сесії
        if not session:
            c_type = self.consumer_profiles.get(meter_id, 'residential')
            session = {
                'session_id': f"SES_{meter_id}_{int(current_time)}",
                'meter_id': meter_id,
                'district': district,
                'consumer_type': c_type,
                'start_ts': current_time,
                'last_event_ts': current_time,
                'end_ts': current_time,
                'readings_count': 0,
                'powers': [],
                'peak_power_kw': 0.0,
                'total_kwh': 0.0
            }
            self.active_sessions[meter_id] = session

        # Оновлення поточної сесії
        session['readings_count'] += 1
        session['last_event_ts'] = current_time
        session['end_ts'] = current_time
        session['powers'].append(power_kw)
        if power_kw > session['peak_power_kw']:
            session['peak_power_kw'] = power_kw
        # Споживання енергії за інтервал між вимірюваннями (1 сек)
        session['total_kwh'] += (power_kw * (1.0 / 3600.0))

    def finalize_session(self, session):
        c_type = session['consumer_type']
        rate = RATES.get(c_type, 4.32)
        base_cost = session['total_kwh'] * rate

        # Розрахунок Peak Demand Charges: поріг 8.0 кВт для residential і 16.0 кВт для commercial
        threshold = 8.0 if c_type == 'residential' else 16.0
        peak_charge = 0.0
        if session['peak_power_kw'] > threshold:
            peak_charge = (session['peak_power_kw'] - threshold) * 15.0  # 15 грн за кожен надлишковий кВт піку

        total_amount = base_cost + peak_charge
        session['rate'] = rate
        session['peak_charge'] = peak_charge
        session['total_amount_uah'] = total_amount

        # Виклик транзакційного координатора 2PC
        self.coordinator.execute_transaction(session)

    def check_expired_sessions(self):
        now = time.time()
        expired = [m for m, s in self.active_sessions.items() if now - s['last_event_ts'] > SESSION_GAP_SEC]
        for m in expired:
            self.finalize_session(self.active_sessions.pop(m))

    def run(self):
        consumer = KafkaConsumer(
            TOPIC_RAW,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            isolation_level='read_committed',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"[*] Stream Processor запущено. Очікування потоку з {TOPIC_RAW}...")

        try:
            while True:
                msg_pack = consumer.poll(timeout_ms=1000, max_records=50)
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        self.process_reading(msg.value)
                self.check_expired_sessions()
        except KeyboardInterrupt:
            print("\nЗупинка процесора...")
        finally:
            self.check_expired_sessions()
            self.cluster.shutdown()


if __name__ == '__main__':
    processor = StreamProcessor()
    processor.run()