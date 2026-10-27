"""Valida que los ejemplos de samples/ cumplan el contrato JSON del evento."""
import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

RAIZ = Path(__file__).resolve().parent.parent
SAMPLES = RAIZ / "samples"
SCHEMA = json.loads((SAMPLES / "schema_evento_preparacion.json").read_text(encoding="utf-8"))
VALIDADOR = Draft202012Validator(SCHEMA)
EJEMPLO = json.loads((SAMPLES / "evento_preparacion.json").read_text(encoding="utf-8"))


def test_schema_es_valido():
    Draft202012Validator.check_schema(SCHEMA)


def test_evento_de_ejemplo_cumple_contrato():
    VALIDADOR.validate(EJEMPLO)


def test_ciclo_completo_cumple_contrato_y_orden():
    lineas = (SAMPLES / "eventos_ciclo_pedido.jsonl").read_text(encoding="utf-8").splitlines()
    eventos = [json.loads(l) for l in lineas if l.strip()]
    for evento in eventos:
        VALIDADOR.validate(evento)
    tipos = [e["event_type"] for e in eventos]
    assert tipos == ["PEDIDO_RECIBIDO", "PREPARACION_INICIADA",
                     "PREPARACION_FINALIZADA", "PEDIDO_RECOGIDO"]
    assert len({e["pedido_id"] for e in eventos}) == 1
    assert len({e["comercio_id"] for e in eventos}) == 1  # misma key en Kafka


@pytest.mark.parametrize("campo", ["comercio_id", "event_ts", "pedido_id", "cola_actual"])
def test_evento_sin_campo_obligatorio_es_rechazado(campo):
    evento = copy.deepcopy(EJEMPLO)
    del evento[campo]
    with pytest.raises(ValidationError):
        VALIDADOR.validate(evento)


@pytest.mark.parametrize("campo, valor", [
    ("event_type", "PEDIDO_PERDIDO"),     # tipo no permitido
    ("comercio_id", "14"),                # no cumple COM-000
    ("items", 0),                         # mínimo 1
    ("cola_actual", -1),                  # no puede ser negativa
    ("equipo", "otro-equipo"),            # eventos de otro equipo
])
def test_evento_con_valor_invalido_es_rechazado(campo, valor):
    evento = copy.deepcopy(EJEMPLO)
    evento[campo] = valor
    with pytest.raises(ValidationError):
        VALIDADOR.validate(evento)
