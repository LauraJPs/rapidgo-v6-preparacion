"""Consulta rápida del Data Lake para obtener valores reales de la ejecución.

Uso (con el pipeline levantado):
  docker compose exec spark /opt/spark/bin/spark-submit /opt/app/consultar_lake.py
"""
import os

from pyspark.sql import SparkSession, functions as F

LAKE = os.getenv("DATALAKE", "/opt/datalake")
spark = SparkSession.builder.appName("consultar-lake").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def leer(ruta):
    try:
        return spark.read.parquet(f"{LAKE}/{ruta}")
    except Exception as exc:  # carpeta aún vacía
        print(f"(sin datos en {ruta}: {type(exc).__name__})")
        return None


raw = leer("raw/eventos")
if raw is not None:
    print("\n=== RAW: mensajes por partición de Kafka ===")
    raw.groupBy("partition").agg(F.count("*").alias("mensajes"),
                                 F.countDistinct("key").alias("comercios")).orderBy("partition").show()

proc = leer("processed/eventos")
if proc is not None:
    print("=== PROCESSED: eventos por tipo ===")
    proc.groupBy("event_type").count().orderBy("event_type").show(truncate=False)
    print("=== K1: tiempo real de preparación (min) ===")
    proc.filter("tiempo_real_min IS NOT NULL").select(
        F.round(F.avg("tiempo_real_min"), 2).alias("promedio"),
        F.round(F.expr("percentile_approx(tiempo_real_min, 0.5)"), 2).alias("mediana"),
        F.round(F.max("tiempo_real_min"), 2).alias("maximo"),
        F.round(100 * F.avg(F.col("demorado").cast("int")), 1).alias("pct_demorados")).show()

kpis = leer("curated/kpis_comercio_5min")
if kpis is not None:
    print("=== CURATED: top 10 comercios por cola promedio (K2 / comercios críticos) ===")
    (kpis.groupBy("comercio_id", "zona")
     .agg(F.round(F.avg("cola_promedio"), 2).alias("cola_prom"), F.max("cola_maxima").alias("cola_max"),
          F.round(F.avg("tiempo_prep_promedio_min"), 2).alias("prep_prom_min"),
          F.sum(F.col("alerta_a2").cast("int")).alias("ventanas_a2"), F.sum("alertas_a1").alias("alertas_a1"))
     .orderBy(F.desc("cola_prom")).show(10))

alertas = leer("curated/alertas/*")
if alertas is not None:
    print("=== ALERTAS por código y severidad ===")
    alertas.groupBy("codigo", "severidad").count().orderBy("codigo").show(truncate=False)
