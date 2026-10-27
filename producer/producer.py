"""Producer de RapidGo V6: publica en Kafka los eventos del simulador.

Variables de entorno:
  KAFKA_BOOTSTRAP   servidor Kafka            (default kafka:29092)
  TOPIC             topic de entrada          (default rapidgo.preparacion.eventos)
  VELOCIDAD         minutos simulados por segundo real (default 2)
  DURACION_SEG      segundos reales a ejecutar, 0 = infinito (default 0)
  SEED              semilla aleatoria         (default 42)
"""
import json
import os
import signal
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from generador import Simulador

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("TOPIC", "rapidgo.preparacion.eventos")
VELOCIDAD = float(os.getenv("VELOCIDAD", "2"))
DURACION_SEG = float(os.getenv("DURACION_SEG", "0"))
SEED = int(os.getenv("SEED", "42"))
TICK_SEG = 1.0

detener = False


def _salir(*_):
    global detener
    detener = True


def conectar(intentos=20):
    for i in range(1, intentos + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                linger_ms=50,
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka no disponible ({i}/{intentos}), reintentando en 3 s...", flush=True)
            time.sleep(3)
    raise SystemExit("[producer] No fue posible conectar con Kafka")


def main():
    signal.signal(signal.SIGTERM, _salir)
    signal.signal(signal.SIGINT, _salir)

    producer = conectar()
    reloj_sim = datetime.now(timezone.utc).replace(microsecond=0)
    sim = Simulador(inicio=reloj_sim, seed=SEED)
    perfiles = Counter(c.perfil for c in sim.comercios)
    print(f"[producer] Conectado a {BOOTSTRAP} · topic={TOPIC} · velocidad={VELOCIDAD} min/s "
          f"· comercios={dict(perfiles)}", flush=True)

    por_particion, por_tipo, total = Counter(), Counter(), 0

    def ok(meta):
        por_particion[meta.partition] += 1

    inicio_real = time.time()
    ultimo_log = inicio_real
    while not detener:
        reloj_sim += timedelta(minutes=VELOCIDAD * TICK_SEG)
        for evento in sim.avanzar_hasta(reloj_sim):
            producer.send(TOPIC, key=evento["comercio_id"], value=evento).add_callback(ok)
            por_tipo[evento["event_type"]] += 1
            total += 1
        producer.flush()

        ahora = time.time()
        if ahora - ultimo_log >= 10:
            print(f"[producer] reloj_sim={reloj_sim:%H:%M} · enviados={total} "
                  f"· por_particion={dict(sorted(por_particion.items()))} · por_tipo={dict(por_tipo)}",
                  flush=True)
            ultimo_log = ahora
        if DURACION_SEG and ahora - inicio_real >= DURACION_SEG:
            break
        time.sleep(max(0.0, TICK_SEG - (time.time() - ahora)))

    producer.flush()
    producer.close()
    print(f"[producer] Fin · enviados={total} · por_particion={dict(sorted(por_particion.items()))}",
          flush=True)


if __name__ == "__main__":
    main()
