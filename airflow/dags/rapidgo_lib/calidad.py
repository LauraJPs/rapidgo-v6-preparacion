"""Lógica del DAG de RapidGo V6 (calidad, agregación, consolidación y reporte).

Funciones puras sobre pandas para poder probarlas con pytest en GitHub Actions
sin levantar Airflow. El DAG (dag_preparacion.py) solo lee/escribe archivos y
llama a estas funciones.
"""
from __future__ import annotations

import os

import pandas as pd

EQUIPO = "equipo3-v6"
TIPOS_EVENTO = {
    "PEDIDO_RECIBIDO", "PREPARACION_INICIADA", "PREPARACION_FINALIZADA",
    "PEDIDO_RECOGIDO", "PEDIDO_CANCELADO",
}
COLUMNAS_OBLIGATORIAS = ["event_id", "event_type", "event_ts", "comercio_id", "pedido_id", "cola_actual"]
FUENTES = {
    "raw": "raw/eventos",
    "processed": "processed/eventos",
    "kpis_5min": "curated/kpis_comercio_5min",
    "alertas": "curated/alertas",
}
MAX_PCT_DUPLICADOS = 1.0          # % máximo de event_id repetidos aceptado
PCT_DEMORADOS_CRITICO = 50.0      # comercio crítico del día si más de la mitad sale tarde
COLA_PROMEDIO_CRITICA = 8.0       # mismo umbral de la regla A2
VENTANA_CONSOLIDACION_MIN = 60    # ADR-002 pendiente: no repetir la misma alerta en 60 min


# ---------------------------------------------------------------- fuentes ---
def contar_archivos_parquet(lake: str) -> dict[str, int]:
    """Cantidad de archivos .parquet por fuente (ignora _spark_metadata y ocultos)."""
    conteo = {}
    for nombre, relativa in FUENTES.items():
        total = 0
        for raiz, carpetas, archivos in os.walk(os.path.join(lake, relativa)):
            carpetas[:] = [c for c in carpetas if not c.startswith(("_", "."))]
            total += sum(1 for a in archivos if a.endswith(".parquet") and not a.startswith(("_", ".")))
        conteo[nombre] = total
    return conteo


def fuentes_faltantes(conteo: dict[str, int], requeridas=("raw", "processed")) -> list[str]:
    return [f for f in requeridas if conteo.get(f, 0) == 0]


# ---------------------------------------------------------------- calidad ---
def validar_eventos(df: pd.DataFrame) -> dict:
    """Calidad de processed/eventos. Devuelve métricas y lista de errores (vacía = OK)."""
    errores = []
    faltan = [c for c in COLUMNAS_OBLIGATORIAS if c not in df.columns]
    if faltan:
        return {"metricas": {"filas": int(len(df))}, "errores": [f"Columnas faltantes: {faltan}"]}
    if df.empty:
        return {"metricas": {"filas": 0}, "errores": ["processed/eventos está vacío"]}

    nulos = {c: int(df[c].isna().sum()) for c in COLUMNAS_OBLIGATORIAS}
    tipos_invalidos = int((~df["event_type"].isin(TIPOS_EVENTO)).sum())
    otro_equipo = int((df["equipo"] != EQUIPO).sum()) if "equipo" in df.columns else 0
    cola_negativa = int((df["cola_actual"] < 0).sum())
    tiempo_negativo = int((df["tiempo_real_min"] < 0).sum()) if "tiempo_real_min" in df.columns else 0
    duplicados = int(df["event_id"].duplicated().sum())
    pct_duplicados = round(100.0 * duplicados / len(df), 3)

    if any(nulos.values()):
        errores.append(f"Nulos en columnas obligatorias: { {c: n for c, n in nulos.items() if n} }")
    if tipos_invalidos:
        errores.append(f"{tipos_invalidos} eventos con event_type no permitido")
    if otro_equipo:
        errores.append(f"{otro_equipo} eventos de otro equipo")
    if cola_negativa:
        errores.append(f"{cola_negativa} eventos con cola_actual negativa")
    if tiempo_negativo:
        errores.append(f"{tiempo_negativo} eventos con tiempo_real_min negativo")
    if pct_duplicados > MAX_PCT_DUPLICADOS:
        errores.append(f"{pct_duplicados}% de event_id duplicados (máx {MAX_PCT_DUPLICADOS}%)")

    metricas = {
        "filas": int(len(df)),
        "comercios": int(df["comercio_id"].nunique()),
        "pedidos": int(df["pedido_id"].nunique()),
        "duplicados": duplicados,
        "pct_duplicados": pct_duplicados,
        "por_tipo": {str(k): int(v) for k, v in df["event_type"].value_counts().items()},
    }
    return {"metricas": metricas, "errores": errores}


# -------------------------------------------------------------- agregación ---
def kpis_diarios(df: pd.DataFrame) -> pd.DataFrame:
    """KPIs por día y comercio (K1, K2, K3) y marca de comercio crítico."""
    d = df.copy()
    d["fecha"] = pd.to_datetime(d["event_ts"]).dt.strftime("%Y-%m-%d")
    d["zona"] = d["zona"].astype(str)
    d["recibido"] = (d["event_type"] == "PEDIDO_RECIBIDO").astype(int)
    d["finalizado"] = (d["event_type"] == "PREPARACION_FINALIZADA").astype(int)
    d["demorado_int"] = d["demorado"].fillna(False).astype(bool).astype(int)
    d["a1_int"] = d["alerta_a1"].fillna(False).astype(bool).astype(int)

    k = (d.groupby(["fecha", "comercio_id", "zona"], observed=True)
         .agg(pedidos_recibidos=("recibido", "sum"),
              pedidos_finalizados=("finalizado", "sum"),
              tiempo_prep_promedio_min=("tiempo_real_min", "mean"),
              pedidos_demorados=("demorado_int", "sum"),
              cola_promedio=("cola_actual", "mean"),
              cola_maxima=("cola_actual", "max"),
              alertas_a1=("a1_int", "sum"))
         .reset_index())
    k["pct_demorados"] = (100.0 * k["pedidos_demorados"] / k["pedidos_finalizados"].where(k["pedidos_finalizados"] > 0)).round(1)
    k["tiempo_prep_promedio_min"] = k["tiempo_prep_promedio_min"].round(2)
    k["cola_promedio"] = k["cola_promedio"].round(2)
    k["critico"] = (k["pct_demorados"] > PCT_DEMORADOS_CRITICO) | (k["cola_promedio"] >= COLA_PROMEDIO_CRITICA)
    return k.sort_values(["fecha", "comercio_id"]).reset_index(drop=True)


def consolidar_alertas(alertas: pd.DataFrame, ventana_min: int = VENTANA_CONSOLIDACION_MIN) -> pd.DataFrame:
    """No repetir la misma alerta (comercio + código) dentro de `ventana_min` minutos.

    Conserva la primera alerta de cada racha y cuenta cuántas repeticiones absorbió.
    """
    columnas = list(alertas.columns) + ["repeticiones_suprimidas"]
    if alertas.empty:
        return pd.DataFrame(columns=columnas)
    a = alertas.copy()
    a["momento_evento"] = pd.to_datetime(a["momento_evento"])
    a = a.sort_values(["comercio_id", "codigo", "momento_evento"])
    ventana = pd.Timedelta(minutes=ventana_min)

    filas = []
    for _, grupo in a.groupby(["comercio_id", "codigo"], sort=False):
        actual, suprimidas = None, 0
        for _, fila in grupo.iterrows():
            if actual is None or fila["momento_evento"] - actual["momento_evento"] >= ventana:
                if actual is not None:
                    filas.append({**actual.to_dict(), "repeticiones_suprimidas": suprimidas})
                actual, suprimidas = fila, 0
            else:
                suprimidas += 1
        filas.append({**actual.to_dict(), "repeticiones_suprimidas": suprimidas})
    return pd.DataFrame(filas, columns=columnas).reset_index(drop=True)


def validar_kpis(kpis: pd.DataFrame) -> list[str]:
    """Calidad de la salida curated. Lista de errores (vacía = OK)."""
    if kpis.empty:
        return ["La tabla de KPIs diarios está vacía"]
    errores = []
    pct = kpis["pct_demorados"].dropna()
    if ((pct < 0) | (pct > 100)).any():
        errores.append("pct_demorados fuera de [0, 100]")
    if (kpis["tiempo_prep_promedio_min"].dropna() < 0).any():
        errores.append("tiempo_prep_promedio_min negativo")
    if (kpis["cola_promedio"] < 0).any():
        errores.append("cola_promedio negativa")
    if (~kpis["comercio_id"].astype(str).str.fullmatch(r"COM-\d{3}")).any():
        errores.append("comercio_id con formato inválido")
    if kpis.duplicated(["fecha", "comercio_id"]).any():
        errores.append("Más de una fila por fecha y comercio")
    return errores


# ----------------------------------------------------------------- reporte ---
def generar_reporte_md(run_id: str, metricas: dict, kpis: pd.DataFrame,
                       alertas_info: dict, validacion: dict) -> str:
    criticos = kpis[kpis["critico"]].sort_values(["cola_promedio", "pct_demorados"], ascending=False)
    lineas = [
        "# Reporte RapidGo V6 – Preparación de pedidos",
        "",
        f"- Ejecución del DAG: `{run_id}`",
        f"- Eventos procesados: **{metricas.get('filas', 0)}** · comercios: {metricas.get('comercios', 0)} "
        f"· pedidos: {metricas.get('pedidos', 0)} · duplicados: {metricas.get('pct_duplicados', 0)} %",
        f"- KPIs diarios: {len(kpis)} filas · comercios críticos: "
        f"**{kpis.loc[kpis['critico'], 'comercio_id'].nunique()}** de {kpis['comercio_id'].nunique()}",
        f"- Alertas: {alertas_info.get('originales', 0)} originales → "
        f"**{alertas_info.get('consolidadas', 0)} consolidadas** "
        f"(sin repetir en {VENTANA_CONSOLIDACION_MIN} min)",
        f"- Validación curated: {'OK' if not validacion.get('errores') else validacion['errores']}",
        "",
        "## Comercios críticos (top 10)",
        "",
        "| Fecha | Comercio | Zona | Prep. prom. (min) | % demorados | Cola prom. | Alertas A1 |",
        "|---|---|---|---|---|---|---|",
    ]
    for _, r in criticos.head(10).iterrows():
        lineas.append(f"| {r['fecha']} | {r['comercio_id']} | {r['zona']} | {r['tiempo_prep_promedio_min']} "
                      f"| {r['pct_demorados']} | {r['cola_promedio']} | {int(r['alertas_a1'])} |")
    lineas += ["", "## Alertas consolidadas por código", ""]
    for codigo, n in sorted(alertas_info.get("por_codigo", {}).items()):
        lineas.append(f"- {codigo}: {n}")
    return "\n".join(lineas) + "\n"
