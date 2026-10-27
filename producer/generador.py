"""Simulador de comercios de RapidGo - Variante 6 (Preparación de pedidos).

Lógica pura (sin Kafka) para poder probarla con pytest en GitHub Actions.

Cada comercio tiene un perfil:
  - normal:   pocos pedidos y prepara a tiempo.
  - lento:    prepara mucho más lento de lo que promete  -> dispara A1.
  - saturado: recibe muchos pedidos y se le acumula la cola -> dispara A2.

El tiempo es SIMULADO: el reloj avanza `velocidad` veces más rápido que el
real (con velocidad=2, 1 segundo real = 2 minutos simulados). Todos los
timestamps (event_ts, inicio/fin) usan ese reloj, que es el tiempo de evento
que Spark usa para las ventanas.
"""
import heapq
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

EQUIPO = "equipo3-v6"
SCHEMA_VERSION = "1.0"
TIPOS_COMERCIO = ["restaurante", "comida_rapida", "panaderia", "farmacia", "supermercado"]
ZONAS = ["norte", "sur", "centro", "occidente"]

# pedidos/minuto, (min, max) minutos de preparación como factor del estimado
PERFILES = {
    "normal":   {"pedidos_min": 0.15, "factor_prep": (0.6, 1.15), "prob_cancelacion": 0.03},
    "lento":    {"pedidos_min": 0.15, "factor_prep": (1.4, 2.2),  "prob_cancelacion": 0.05},
    "saturado": {"pedidos_min": 0.70, "factor_prep": (0.9, 1.3),  "prob_cancelacion": 0.08},
}


@dataclass
class Comercio:
    comercio_id: str
    tipo_comercio: str
    zona: str
    perfil: str
    cola: int = 0           # pedidos recibidos o en preparación sin terminar
    proximo_pedido: datetime = None


def crear_comercios(rng, n_normal=20, n_lento=6, n_saturado=4):
    perfiles = ["normal"] * n_normal + ["lento"] * n_lento + ["saturado"] * n_saturado
    comercios = []
    for i, perfil in enumerate(perfiles, start=1):
        comercios.append(Comercio(
            comercio_id=f"COM-{i:03d}",
            tipo_comercio=rng.choice(TIPOS_COMERCIO),
            zona=rng.choice(ZONAS),
            perfil=perfil,
        ))
    return comercios


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Simulador:
    """Genera eventos en orden de tiempo simulado."""

    def __init__(self, inicio: datetime, seed: int = 42, comercios=None):
        self.rng = random.Random(seed)
        self.reloj = inicio
        self.comercios = comercios or crear_comercios(self.rng)
        self._agenda = []        # heap de (ts, secuencia, accion, datos)
        self._seq = 0
        self._n_pedido = 0
        for c in self.comercios:
            c.proximo_pedido = inicio + self._espera_entre_pedidos(c)

    # ---------- utilidades ----------
    def _espera_entre_pedidos(self, c):
        tasa = PERFILES[c.perfil]["pedidos_min"]
        return timedelta(minutes=self.rng.expovariate(tasa))

    def _agendar(self, ts, accion, datos):
        self._seq += 1
        heapq.heappush(self._agenda, (ts, self._seq, accion, datos))

    def _evento(self, tipo, ts, c, pedido):
        return {
            "schema_version": SCHEMA_VERSION,
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "event_type": tipo,
            "event_ts": iso(ts),
            "equipo": EQUIPO,
            "comercio_id": c.comercio_id,
            "tipo_comercio": c.tipo_comercio,
            "zona": c.zona,
            "pedido_id": pedido["pedido_id"],
            "items": pedido["items"],
            "tiempo_estimado_min": pedido["estimado"],
            "cola_actual": c.cola,
            "inicio_preparacion_ts": pedido.get("inicio"),
            "fin_preparacion_ts": pedido.get("fin"),
        }

    # ---------- simulación ----------
    def _nuevo_pedido(self, c, ts):
        self._n_pedido += 1
        items = self.rng.randint(1, 8)
        estimado = self.rng.randint(10, 20) + items  # el comercio promete más tiempo si son más ítems
        pedido = {"pedido_id": f"P-{self._n_pedido:06d}", "items": items, "estimado": estimado}
        c.cola += 1
        evento = self._evento("PEDIDO_RECIBIDO", ts, c, pedido)

        perfil = PERFILES[c.perfil]
        espera_inicio = timedelta(minutes=self.rng.uniform(0.5, 3) + 0.2 * c.cola)
        if self.rng.random() < perfil["prob_cancelacion"]:
            self._agendar(ts + espera_inicio, "cancelar", (c, pedido))
        else:
            self._agendar(ts + espera_inicio, "iniciar", (c, pedido))
        return evento

    def _procesar(self, ts, accion, c, pedido):
        perfil = PERFILES[c.perfil]
        if accion == "iniciar":
            pedido["inicio"] = iso(ts)
            factor = self.rng.uniform(*perfil["factor_prep"])
            duracion = timedelta(minutes=pedido["estimado"] * factor)
            self._agendar(ts + duracion, "finalizar", (c, pedido))
            return self._evento("PREPARACION_INICIADA", ts, c, pedido)
        if accion == "finalizar":
            pedido["fin"] = iso(ts)
            c.cola = max(0, c.cola - 1)
            self._agendar(ts + timedelta(minutes=self.rng.uniform(1, 6)), "recoger", (c, pedido))
            return self._evento("PREPARACION_FINALIZADA", ts, c, pedido)
        if accion == "recoger":
            return self._evento("PEDIDO_RECOGIDO", ts, c, pedido)
        if accion == "cancelar":
            c.cola = max(0, c.cola - 1)
            return self._evento("PEDIDO_CANCELADO", ts, c, pedido)
        raise ValueError(f"Acción desconocida: {accion}")

    def avanzar_hasta(self, limite: datetime):
        """Devuelve, en orden de tiempo, todos los eventos que ocurren hasta `limite`."""
        eventos = []
        while True:
            prox_pedido = min(self.comercios, key=lambda c: c.proximo_pedido)
            prox_agenda = self._agenda[0][0] if self._agenda else None
            if prox_agenda is not None and prox_agenda <= prox_pedido.proximo_pedido:
                if prox_agenda > limite:
                    break
                ts, _, accion, (c, pedido) = heapq.heappop(self._agenda)
                eventos.append(self._procesar(ts, accion, c, pedido))
            else:
                if prox_pedido.proximo_pedido > limite:
                    break
                ts = prox_pedido.proximo_pedido
                eventos.append(self._nuevo_pedido(prox_pedido, ts))
                prox_pedido.proximo_pedido = ts + self._espera_entre_pedidos(prox_pedido)
        self.reloj = limite
        return eventos
