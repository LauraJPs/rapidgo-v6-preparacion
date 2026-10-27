"""Pruebas del simulador de comercios (sin Kafka)."""
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from jsonschema import Draft202012Validator

from producer.generador import Simulador

RAIZ = Path(__file__).resolve().parent.parent
SCHEMA = json.loads((RAIZ / "samples" / "schema_evento_preparacion.json").read_text(encoding="utf-8"))
VALIDADOR = Draft202012Validator(SCHEMA)
INICIO = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)


def simular(horas=3, seed=7):
    sim = Simulador(inicio=INICIO, seed=seed)
    return sim, sim.avanzar_hasta(INICIO + timedelta(hours=horas))


def test_genera_eventos_que_cumplen_el_contrato():
    _, eventos = simular()
    assert len(eventos) > 200
    for evento in eventos:
        VALIDADOR.validate(evento)


def test_eventos_identificados_por_equipo_y_ordenados_en_el_tiempo():
    _, eventos = simular()
    assert {e["equipo"] for e in eventos} == {"equipo3-v6"}
    tiempos = [e["event_ts"] for e in eventos]
    assert tiempos == sorted(tiempos)


def test_ciclo_de_vida_en_orden_y_mismo_comercio_por_pedido():
    _, eventos = simular()
    por_pedido = defaultdict(list)
    for e in eventos:
        por_pedido[e["pedido_id"]].append(e)
    orden = ["PEDIDO_RECIBIDO", "PREPARACION_INICIADA", "PREPARACION_FINALIZADA", "PEDIDO_RECOGIDO"]
    for lista in por_pedido.values():
        tipos = [e["event_type"] for e in lista]
        assert len({e["comercio_id"] for e in lista}) == 1     # misma key en Kafka
        if "PEDIDO_CANCELADO" in tipos:
            assert tipos[0] == "PEDIDO_RECIBIDO" and tipos[-1] == "PEDIDO_CANCELADO"
        else:
            assert tipos == orden[: len(tipos)]


def test_finalizados_traen_inicio_y_fin():
    _, eventos = simular()
    finalizados = [e for e in eventos if e["event_type"] == "PREPARACION_FINALIZADA"]
    assert finalizados
    for e in finalizados:
        assert e["inicio_preparacion_ts"] and e["fin_preparacion_ts"]
        assert e["fin_preparacion_ts"] >= e["inicio_preparacion_ts"]


def test_perfiles_generan_los_casos_de_alerta():
    sim, eventos = simular(horas=4)
    perfil = {c.comercio_id: c.perfil for c in sim.comercios}

    # A1: los comercios lentos tardan > 1.5x lo prometido en buena parte de sus pedidos
    def excede(e):
        ini = datetime.fromisoformat(e["inicio_preparacion_ts"].replace("Z", "+00:00"))
        fin = datetime.fromisoformat(e["fin_preparacion_ts"].replace("Z", "+00:00"))
        return (fin - ini).total_seconds() / 60 > 1.5 * e["tiempo_estimado_min"]

    fin_lentos = [e for e in eventos if e["event_type"] == "PREPARACION_FINALIZADA"
                  and perfil[e["comercio_id"]] == "lento"]
    assert sum(excede(e) for e in fin_lentos) / len(fin_lentos) > 0.4

    # A2: los saturados acumulan cola >= 8; los normales casi nunca
    max_cola = defaultdict(int)
    for e in eventos:
        max_cola[e["comercio_id"]] = max(max_cola[e["comercio_id"]], e["cola_actual"])
    saturados = [cid for cid, p in perfil.items() if p == "saturado"]
    assert all(max_cola[cid] >= 8 for cid in saturados)
