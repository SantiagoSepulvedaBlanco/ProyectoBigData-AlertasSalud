#!/usr/bin/env python3
"""
Generador de datos biométricos de wearables (simulación).

- Emite un JSON por sensor cada EMIT_INTERVAL_SECONDS (0.5 s por defecto).
- Cada ciclo de 15 s (alineado con las ventanas TUMBLE de Flink) inyecta:
    * una CRISIS real (taquicardia + hipoxemia)  -> debe generar alerta
    * un DISTRACTOR (solo taquicardia, p. ej. ejercicio) -> NO debe alertar
- Crea los tópicos si no existen, por lo que el orden de arranque no importa.
"""
import json
import logging
import os
import random
import signal
import time

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SENSOR_TOPIC = os.getenv("SENSOR_TOPIC", "sensor-vital-data")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "alertas-emergencia")
NUM_SENSORS = max(2, int(os.getenv("NUM_SENSORS", "1000")))
EMIT_INTERVAL = float(os.getenv("EMIT_INTERVAL_SECONDS", "0.5"))

CYCLE_SECONDS = 15                 # = tamaño de la ventana de Flink
CRISIS_FROM, CRISIS_TO = 3.0, 8.0  # segundos dentro del ciclo (queda dentro de una sola ventana)

# Centro urbano de Bucaramanga y radio aproximado de 2 km
CENTER_LAT, CENTER_LON = 7.12539, -73.11980
RADIUS_DEG = 0.018
STEP_DEG = 0.00003                 # ~3 m de desplazamiento por lectura

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("generator")


def clamp(value, low, high):
    return max(low, min(high, value))


class Patient:
    __slots__ = ("patient_id", "lat", "lon", "base_hr", "base_spo2", "base_temp")

    def __init__(self, idx):
        self.patient_id = f"patient-{idx:04d}"
        self.lat = CENTER_LAT + random.uniform(-RADIUS_DEG, RADIUS_DEG)
        self.lon = CENTER_LON + random.uniform(-RADIUS_DEG, RADIUS_DEG)
        self.base_hr = random.gauss(74, 7)
        self.base_spo2 = random.gauss(97, 0.8)
        self.base_temp = random.gauss(36.6, 0.25)

    def move(self):
        self.lat = clamp(self.lat + random.uniform(-STEP_DEG, STEP_DEG),
                         CENTER_LAT - RADIUS_DEG, CENTER_LAT + RADIUS_DEG)
        self.lon = clamp(self.lon + random.uniform(-STEP_DEG, STEP_DEG),
                         CENTER_LON - RADIUS_DEG, CENTER_LON + RADIUS_DEG)

    def reading(self, mode, now_ms):
        if mode == "crisis":      # taquicardia severa + hipoxemia
            hr = random.randint(140, 175)
            spo2 = random.randint(78, 88)
            temp = round(random.uniform(37.5, 39.2), 1)
        elif mode == "decoy":     # solo taquicardia: NO cumple la regla compuesta
            hr = random.randint(135, 150)
            spo2 = random.randint(95, 99)
            temp = round(random.uniform(36.8, 37.6), 1)
        else:                     # lectura normal
            hr = int(clamp(random.gauss(self.base_hr, 3), 50, 115))
            spo2 = int(clamp(round(random.gauss(self.base_spo2, 0.7)), 93, 100))
            temp = round(clamp(random.gauss(self.base_temp, 0.1), 35.8, 37.4), 1)
        self.move()
        return {
            "patient_id": self.patient_id,
            "heart_rate": hr,
            "oxygen_saturation": spo2,
            "temperature": temp,
            "latitude": round(self.lat, 6),
            "longitude": round(self.lon, 6),
            "event_ts": now_ms,
        }


def ensure_topics():
    """Espera a Kafka y crea los tópicos si no existen (idempotente)."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    deadline = time.time() + 120
    while True:
        try:
            admin.list_topics(timeout=5)
            break
        except KafkaException as exc:
            if time.time() > deadline:
                raise
            log.warning("Kafka no disponible aún (%s). Reintentando...", exc)
            time.sleep(3)

    futures = admin.create_topics([
        NewTopic(SENSOR_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(ALERT_TOPIC, num_partitions=1, replication_factor=1),
    ])
    for topic, future in futures.items():
        try:
            future.result()
            log.info("Tópico creado: %s", topic)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def main():
    running = True

    def _stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info("Conectando a Kafka en %s", BOOTSTRAP)
    ensure_topics()

    stats = {"sent": 0, "errors": 0}

    def on_delivery(err, _msg):
        if err is not None:
            stats["errors"] += 1

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 50,
        "batch.num.messages": 5000,
        "acks": "1",
    })
    patients = [Patient(i) for i in range(1, NUM_SENSORS + 1)]
    log.info("Generando datos de %d sensores cada %.1f s", NUM_SENSORS, EMIT_INTERVAL)

    cycle, victim, decoy, announced = -1, None, None, False
    last_report, next_tick = time.time(), time.time()

    while running:
        now = time.time()
        cycle_id = int(now // CYCLE_SECONDS)
        if cycle_id != cycle:
            cycle = cycle_id
            victim, decoy = random.sample(patients, 2)
            announced = False

        active = CRISIS_FROM <= (now % CYCLE_SECONDS) < CRISIS_TO
        if active and not announced:
            log.info("Crisis inyectada en %s (distractor: %s)", victim.patient_id, decoy.patient_id)
            announced = True

        now_ms = int(now * 1000)
        for p in patients:
            mode = "normal"
            if active:
                if p is victim:
                    mode = "crisis"
                elif p is decoy:
                    mode = "decoy"
            payload = json.dumps(p.reading(mode, now_ms)).encode()
            try:
                producer.produce(SENSOR_TOPIC, key=p.patient_id.encode(),
                                 value=payload, on_delivery=on_delivery)
            except BufferError:
                producer.poll(0.5)
                producer.produce(SENSOR_TOPIC, key=p.patient_id.encode(),
                                 value=payload, on_delivery=on_delivery)
            stats["sent"] += 1
        producer.poll(0)

        if time.time() - last_report >= 10:
            log.info("Mensajes enviados: %d | errores de entrega: %d", stats["sent"], stats["errors"])
            last_report = time.time()

        next_tick += EMIT_INTERVAL
        delay = next_tick - time.time()
        if delay > 0:
            time.sleep(delay)
        else:
            next_tick = time.time()   # nos atrasamos: reiniciar el reloj

    log.info("Deteniendo generador...")
    producer.flush(10)


if __name__ == "__main__":
    main()
