"""
### RapidGo V6 – Control de calidad y reporte de preparación de pedidos

Orquesta el control batch sobre el Data Lake que escribe Spark Structured Streaming:

1. **verificar_fuentes** – hay archivos Parquet en raw y processed
   (reintentos con espera exponencial, ADR-003).
2. **validar_calidad_processed** – nulos, tipos de evento, equipo, cola y duplicados.
3. **agregar_kpis_diarios** – K1, K2, K3 por día y comercio → `curated/kpis_diarios`.
4. **consolidar_alertas** – no repetir la misma alerta en 60 min → `curated/alertas_consolidadas`.
5. **validar_calidad_curated** – rangos y unicidad de la salida.
6. **generar_reporte** – reporte Markdown en `curated/reportes/`.

La información entre tareas viaja por **XCom** (valores de retorno de cada `@task`).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # para importar rapidgo_lib

LAKE = os.getenv("DATALAKE", "/opt/datalake")

default_args = {
    "owner": "equipo3-v6",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}


@dag(
    dag_id="rapidgo_v6_preparacion",
    description="Calidad, KPIs diarios, consolidación de alertas y reporte (Variante 6)",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=default_args,
    tags=["rapidgo", "variante6", "semana3"],
    doc_md=__doc__,
)
def rapidgo_v6_preparacion():

    # ADR-003: si Spark aún no ha escrito datos (arranque en frío o Kafka detenido), esta tarea
    # reintenta con espera exponencial (20 s, 40 s, 80 s, 120 s) en vez de fallar en 30 s.
    # Las tareas de calidad mantienen 2 reintentos: si los datos están mal, reintentar no lo arregla.
    @task(
        retries=4,
        retry_delay=timedelta(seconds=20),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )
    def verificar_fuentes() -> dict:
        from rapidgo_lib.calidad import contar_archivos_parquet, fuentes_faltantes

        conteo = contar_archivos_parquet(LAKE)
        print(f"[verificar_fuentes] archivos Parquet por fuente: {conteo}")
        faltan = fuentes_faltantes(conteo)
        if faltan:
            raise RuntimeError(f"Fuentes sin archivos Parquet: {faltan} · conteo={conteo}. "
                               "¿Están corriendo el producer y Spark?")
        return conteo

    @task
    def validar_calidad_processed(conteo: dict) -> dict:
        import pandas as pd
        from rapidgo_lib.calidad import validar_eventos

        df = pd.read_parquet(f"{LAKE}/processed/eventos")
        resultado = validar_eventos(df)
        print(f"[validar_calidad_processed] archivos={conteo['processed']} métricas={resultado['metricas']}")
        if resultado["errores"]:
            raise ValueError(f"Calidad de processed NO superada: {resultado['errores']}")
        return resultado["metricas"]

    @task
    def agregar_kpis_diarios(metricas: dict) -> dict:
        import pandas as pd
        from rapidgo_lib.calidad import kpis_diarios

        df = pd.read_parquet(f"{LAKE}/processed/eventos")
        kpis = kpis_diarios(df)
        destino = f"{LAKE}/curated/kpis_diarios"
        os.makedirs(destino, exist_ok=True)
        ruta = f"{destino}/kpis_diarios.parquet"
        kpis.to_parquet(ruta, index=False)
        info = {
            "ruta": ruta,
            "filas": int(len(kpis)),
            "comercios_criticos": sorted(kpis.loc[kpis["critico"], "comercio_id"].unique().tolist()),
        }
        print(f"[agregar_kpis_diarios] {info}")
        return info

    @task
    def consolidar_alertas(metricas: dict) -> dict:
        import glob
        import pandas as pd
        from rapidgo_lib.calidad import consolidar_alertas as consolidar

        carpetas = sorted(glob.glob(f"{LAKE}/curated/alertas/*/"))
        partes = [pd.read_parquet(c) for c in carpetas]
        alertas = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
        consolidadas = consolidar(alertas)
        destino = f"{LAKE}/curated/alertas_consolidadas"
        os.makedirs(destino, exist_ok=True)
        ruta = f"{destino}/alertas_consolidadas.parquet"
        consolidadas.to_parquet(ruta, index=False)
        info = {
            "ruta": ruta,
            "originales": int(len(alertas)),
            "consolidadas": int(len(consolidadas)),
            "por_codigo": {str(k): int(v) for k, v in consolidadas["codigo"].value_counts().items()}
            if not consolidadas.empty else {},
        }
        print(f"[consolidar_alertas] {info}")
        return info

    @task
    def validar_calidad_curated(kpis_info: dict, alertas_info: dict) -> dict:
        import pandas as pd
        from rapidgo_lib.calidad import validar_kpis

        kpis = pd.read_parquet(kpis_info["ruta"])
        errores = validar_kpis(kpis)
        if alertas_info["consolidadas"] > alertas_info["originales"]:
            errores.append("La consolidación generó más alertas de las que había")
        print(f"[validar_calidad_curated] filas={len(kpis)} errores={errores}")
        if errores:
            raise ValueError(f"Calidad de curated NO superada: {errores}")
        return {"filas": int(len(kpis)), "errores": errores}

    @task
    def generar_reporte(metricas: dict, kpis_info: dict, alertas_info: dict, validacion: dict) -> str:
        import pandas as pd
        from rapidgo_lib.calidad import generar_reporte_md

        run_id = get_current_context()["run_id"]
        kpis = pd.read_parquet(kpis_info["ruta"])
        texto = generar_reporte_md(run_id, metricas, kpis, alertas_info, validacion)
        destino = f"{LAKE}/curated/reportes"
        os.makedirs(destino, exist_ok=True)
        ruta = f"{destino}/reporte_{datetime.now():%Y%m%d_%H%M%S}.md"
        with open(ruta, "w", encoding="utf-8") as archivo:
            archivo.write(texto)
        print(texto)
        print(f"[generar_reporte] reporte guardado en {ruta}")
        return ruta

    conteo = verificar_fuentes()
    metricas = validar_calidad_processed(conteo)
    kpis_info = agregar_kpis_diarios(metricas)
    alertas_info = consolidar_alertas(metricas)
    validacion = validar_calidad_curated(kpis_info, alertas_info)
    generar_reporte(metricas, kpis_info, alertas_info, validacion)


rapidgo_v6_preparacion()
