import pytest

from spark.reglas import (
    ALERTA_A1,
    ALERTA_A2,
    es_demorado,
    evaluar_acumulacion,
    evaluar_preparacion,
    tiempo_preparacion_min,
)


# ---------- Tiempo de preparación ----------

def test_tiempo_preparacion_del_ejemplo():
    # samples/evento_preparacion.json: 18:21:40 -> 18:42:10
    assert tiempo_preparacion_min("2026-10-22T18:21:40Z", "2026-10-22T18:42:10Z") == 20.5


def test_tiempo_preparacion_fin_antes_de_inicio():
    with pytest.raises(ValueError):
        tiempo_preparacion_min("2026-10-22T18:42:10Z", "2026-10-22T18:21:40Z")


def test_tiempo_preparacion_sin_fin():
    with pytest.raises(ValueError):
        tiempo_preparacion_min("2026-10-22T18:21:40Z", None)


# ---------- KPI K3: pedido demorado ----------

def test_demorado_cuando_supera_lo_prometido():
    assert es_demorado(20.5, 15) is True


def test_no_demorado_en_el_limite():
    assert es_demorado(15, 15) is False


# ---------- A1: preparación excedida ----------

def test_a1_normal_sin_alerta():
    assert evaluar_preparacion(18, 15) is None


def test_a1_limite_exacto_1_5x_no_alerta():
    # 15 x 1.5 = 22.5 -> la regla usa ">" estricto
    assert evaluar_preparacion(22.5, 15) is None


def test_a1_alerta_por_factor():
    assert evaluar_preparacion(23, 15) == (ALERTA_A1, "ALTA")


def test_a1_alerta_por_tope_de_30_min():
    # Prometió 25 (1.5x = 37.5) pero 31 min ya supera el tope absoluto
    assert evaluar_preparacion(31, 25) == (ALERTA_A1, "ALTA")


def test_a1_estimado_invalido():
    with pytest.raises(ValueError):
        evaluar_preparacion(10, 0)


# ---------- A2: acumulación de pedidos ----------

def test_a2_cola_normal_sin_alerta():
    assert evaluar_acumulacion(3.2, 5) is None


def test_a2_limite_8_es_alerta_media():
    assert evaluar_acumulacion(8, 10) == (ALERTA_A2, "MEDIA")


def test_a2_maximo_12_sube_a_alta():
    assert evaluar_acumulacion(9.5, 12) == (ALERTA_A2, "ALTA")


def test_a2_cola_negativa_invalida():
    with pytest.raises(ValueError):
        evaluar_acumulacion(-1, 3)
