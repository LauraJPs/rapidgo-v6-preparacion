"""Producer de RapidGo V6: publica en Kafka los eventos del simulador.

Variables de entorno:
  KAFKA_BOOTSTRAP   servidor Kafka            (default kafka:29092)
  TOPIC             topic de entrada          (default rapidgo.preparacion.eventos)
  VELOCIDAD         minutos simulados por segundo real (default 2)
  DURACION_SEG      segundos reales a ejecutar, 0 = infinito (default 0)
  SEED              semilla aleatoria         (default 42)

Semana 4: si el topic ya tiene eventos (por ejemplo tras `docker compose restart producer`),
el reloj simulado continúa desde el último event_ts publicado. Antes volvía a "ahora" y Spark
descartaba los eventos nuevos por estar detrás del watermark.
"""
import json
import os
import signal
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import NoBrokersAvailable

from generador import Simulador, reloj_inicial

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


def ultimo_event_ts():
    """Mayor event_ts entre el último mensaje de cada partición del topic (None si está vacío)."""
    consumer = None
    try:
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, enable_auto_commit=False,
                                 value_deserializer=lambda v: json.loads(v.decode("utf-8")))
        particiones = [TopicPartition(TOPIC, p) for p in sorted(consumer.partitions_for_topic(TOPIC) or [])]
        if not particiones:
            return None
        consumer.assign(particiones)
        finales = consumer.end_offsets(particiones)
        con_datos = [tp for tp in particiones if finales[tp] > 0]
        for tp in con_datos:
            consumer.seek(tp, finales[tp] - 1)
        maximo, pendientes = None, set(con_datos)
        for _ in range(10):
            if not pendientes:
                break
            for tp, registros in consumer.poll(timeout_ms=1000).items():
                pendientes.discard(tp)
                for r in registros:
                    ts = datetime.strptime(r.value["event_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    maximo = ts if maximo is None or ts > maximo else maximo
        return maximo
    except Exception as error:  # nunca impedir que el producer arranque
        print(f"[producer] No se pudo leer el último evento del topic ({error}); se usa la hora actual",
              flush=True)
        return None
    finally:
        if consumer is not None:
            consumer.close()


def main():
    signal.signal(signal.SIGTERM, _salir)
    signal.signal(signal.SIGINT, _salir)

    producer = conectar()
    ultimo = ultimo_event_ts()
    reloj_sim = reloj_inicial(datetime.now(timezone.utc), ultimo)
    if ultimo:
        print(f"[producer] El topic ya tenía eventos hasta {ultimo:%Y-%m-%d %H:%M}; "
              f"el reloj simulado continúa desde {reloj_sim:%Y-%m-%d %H:%M}", flush=True)
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
