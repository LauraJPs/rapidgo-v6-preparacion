"""Pruebas de la lógica del DAG (rapidgo_lib.calidad), sin Airflow."""
import os

import pandas as pd
import pytest

from rapidgo_lib.calidad import (
    consolidar_alertas,
    contar_archivos_parquet,
    fuentes_faltantes,
    generar_reporte_md,
    kpis_diarios,
    validar_eventos,
    validar_kpis,
)


def eventos(**cambios):
    base = {
        "event_id": ["e1", "e2", "e3", "e4"],
        "event_type": ["PEDIDO_RECIBIDO", "PREPARACION_INICIADA", "PREPARACION_FINALIZADA", "PREPARACION_FINALIZADA"],
        "event_ts": pd.to_datetime(["2026-11-03 10:00", "2026-11-03 10:02", "2026-11-03 10:20", "2026-11-03 10:40"]),
        "equipo": ["equipo3-v6"] * 4,
        "comercio_id": ["COM-001", "COM-001", "COM-001", "COM-002"],
        "zona": ["norte", "norte", "norte", "sur"],
        "pedido_id": ["P-000001", "P-000001", "P-000001", "P-000002"],
        "cola_actual": [1, 1, 0, 9],
        "tiempo_real_min": [None, None, 18.0, 40.0],
        "demorado": [None, None, True, True],
        "alerta_a1": [False, False, False, True],
    }
    base.update(cambios)
    return pd.DataFrame(base)


# ---------- verificar_fuentes ----------

def test_contar_archivos_ignora_metadata_de_spark(tmp_path):
    proc = tmp_path / "processed" / "eventos" / "fecha=2026-11-03"
    proc.mkdir(parents=True)
    (proc / "part-0001.parquet").write_bytes(b"x")
    meta = tmp_path / "processed" / "eventos" / "_spark_metadata"
    meta.mkdir()
    (meta / "0.parquet").write_bytes(b"x")
    conteo = contar_archivos_parquet(str(tmp_path))
    assert conteo["processed"] == 1
    assert conteo["raw"] == 0


def test_fuentes_faltantes_detecta_raw_vacio():
    assert fuentes_faltantes({"raw": 0, "processed": 3, "kpis_5min": 2}) == ["raw"]
    assert fuentes_faltantes({"raw": 1, "processed": 3, "kpis_5min": 2}) == []


# ---------- validar_calidad_processed ----------

def test_eventos_validos_sin_errores():
    r = validar_eventos(eventos())
    assert r["errores"] == []
    assert r["metricas"]["filas"] == 4 and r["metricas"]["comercios"] == 2


def test_cola_negativa_es_error():
    r = validar_eventos(eventos(cola_actual=[1, -1, 0, 9]))
    assert any("cola_actual negativa" in e for e in r["errores"])


def test_tipo_de_evento_invalido_es_error():
    r = validar_eventos(eventos(event_type=["PEDIDO_RECIBIDO", "X", "PREPARACION_FINALIZADA", "PEDIDO_RECOGIDO"]))
    assert any("event_type" in e for e in r["errores"])


def test_duplicados_por_encima_del_limite():
    r = validar_eventos(eventos(event_id=["e1", "e1", "e3", "e4"]))
    assert any("duplicados" in e for e in r["errores"])


def test_columna_obligatoria_faltante():
    r = validar_eventos(eventos().drop(columns=["cola_actual"]))
    assert r["errores"] and "Columnas faltantes" in r["errores"][0]


# ---------- agregar_kpis_diarios ----------

def test_kpis_diarios_por_comercio():
    k = kpis_diarios(eventos()).set_index("comercio_id")
    assert k.loc["COM-001", "pedidos_recibidos"] == 1
    assert k.loc["COM-001", "tiempo_prep_promedio_min"] == 18.0
    assert k.loc["COM-001", "pct_demorados"] == 100.0
    assert bool(k.loc["COM-002", "critico"]) is True       # cola 9 >= 8
    assert k.loc["COM-002", "alertas_a1"] == 1


def test_validar_kpis_ok_y_con_error():
    k = kpis_diarios(eventos())
    assert validar_kpis(k) == []
    k.loc[0, "pct_demorados"] = 150
    assert "pct_demorados fuera de [0, 100]" in validar_kpis(k)


def test_validar_kpis_vacio():
    assert validar_kpis(pd.DataFrame(columns=["fecha", "comercio_id"])) != []


# ---------- consolidar_alertas (no repetir en 60 min) ----------

def alertas(momentos, comercio="COM-027", codigo="A2_ACUMULACION_PEDIDOS"):
    return pd.DataFrame({
        "comercio_id": [comercio] * len(momentos),
        "codigo": [codigo] * len(momentos),
        "momento_evento": pd.to_datetime(momentos),
    })


def test_alertas_seguidas_se_consolidan_en_una():
    a = alertas(["2026-11-03 10:00", "2026-11-03 10:05", "2026-11-03 10:10", "2026-11-03 10:55"])
    c = consolidar_alertas(a)
    assert len(c) == 1
    assert c.loc[0, "repeticiones_suprimidas"] == 3


def test_alerta_despues_de_la_ventana_se_conserva():
    a = alertas(["2026-11-03 10:00", "2026-11-03 10:05", "2026-11-03 11:00", "2026-11-03 11:10"])
    c = consolidar_alertas(a)
    assert len(c) == 2
    assert c["repeticiones_suprimidas"].tolist() == [1, 1]


def test_comercios_y_codigos_distintos_no_se_mezclan():
    a = pd.concat([alertas(["2026-11-03 10:00"]), alertas(["2026-11-03 10:01"], comercio="COM-028"),
                   alertas(["2026-11-03 10:02"], codigo="A1_PREPARACION_EXCEDIDA")])
    assert len(consolidar_alertas(a)) == 3


def test_consolidar_sin_alertas():
    vacio = pd.DataFrame(columns=["comercio_id", "codigo", "momento_evento"])
    assert consolidar_alertas(vacio).empty


# ---------- generar_reporte ----------

def test_reporte_incluye_resumen_y_criticos():
    k = kpis_diarios(eventos())
    texto = generar_reporte_md("manual__prueba", {"filas": 4, "comercios": 2, "pedidos": 2},
                               k, {"originales": 10, "consolidadas": 3, "por_codigo": {"A2": 3}},
                               {"errores": []})
    assert "manual__prueba" in texto
    assert "10 originales" in texto and "3 consolidadas" in texto
    assert "COM-002" in texto


# ---------- Semana 4: frescura del Data Lake ----------

def test_frescura_none_si_no_hay_parquet(tmp_path):
    from rapidgo_lib.calidad import minutos_desde_ultimo_parquet
    (tmp_path / "processed" / "eventos").mkdir(parents=True)
    assert minutos_desde_ultimo_parquet(str(tmp_path)) is None


def test_frescura_usa_el_archivo_mas_reciente_e_ignora_metadata(tmp_path):
    from rapidgo_lib.calidad import minutos_desde_ultimo_parquet
    carpeta = tmp_path / "processed" / "eventos" / "fecha=2026-11-09"
    carpeta.mkdir(parents=True)
    viejo, nuevo = carpeta / "part-0.parquet", carpeta / "part-1.parquet"
    viejo.write_bytes(b"x")
    nuevo.write_bytes(b"x")
    meta = tmp_path / "processed" / "eventos" / "_spark_metadata"
    meta.mkdir()
    (meta / "0.parquet").write_bytes(b"x")
    ahora = 1_800_000_000.0
    os.utime(viejo, (ahora - 3600, ahora - 3600))
    os.utime(nuevo, (ahora - 120, ahora - 120))
    os.utime(meta / "0.parquet", (ahora, ahora))
    assert minutos_desde_ultimo_parquet(str(tmp_path), ahora=ahora) == 2.0


def test_reporte_avisa_si_spark_dejo_de_escribir():
    from rapidgo_lib.calidad import FRESCURA_MAX_MIN
    k = kpis_diarios(eventos())
    reciente = generar_reporte_md("run", {"filas": 4, "frescura_min": 1.5}, k, {}, {"errores": []})
    vieja = generar_reporte_md("run", {"filas": 4, "frescura_min": FRESCURA_MAX_MIN + 5}, k, {}, {"errores": []})
    sin_dato = generar_reporte_md("run", {"filas": 4}, k, {}, {"errores": []})
    assert "hace 1.5 min" in reciente and "⚠️" not in reciente
    assert "⚠️" in vieja
    assert "Frescura" not in sin_dato
