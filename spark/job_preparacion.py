"""Spark Structured Streaming - RapidGo V6 Preparación de pedidos.

Kafka (rapidgo.preparacion.eventos)
  -> raw/eventos          : mensajes tal como llegan + partición/offset   (Parquet)
  -> processed/eventos    : parseados, limpios, tipados y enriquecidos     (Parquet)
  -> curated/kpis_comercio_5min : KPIs por comercio en ventanas de 5 min   (Parquet)
  -> curated/alertas/A1_*, A2_*  : alertas                                 (Parquet + topic de alertas)

Transformaciones:
  T1 parseo JSON con el contrato (from_json) y conversión de timestamps.
  T2 limpieza: solo eventos del equipo, campos válidos y sin duplicados (event_id).
  T3 enriquecimiento: tiempo_real_min, demorado (K3) y bandera de alerta A1.
  Agregación por ventana tumbling de 5 min con watermark (K1, K2, K3 y regla A2).
"""
import os

from pyspark.sql import SparkSession, functions as F, types as T

from reglas import (
    ALERTA_A1,
    ALERTA_A2,
    FACTOR_EXCESO_PREPARACION,
    TOPE_PREPARACION_MIN,
    UMBRAL_COLA_MAXIMA_ALTA,
    UMBRAL_COLA_PROMEDIO,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("TOPIC", "rapidgo.preparacion.eventos")
TOPIC_ALERTAS = os.getenv("TOPIC_ALERTAS", "rapidgo.preparacion.alertas")
LAKE = os.getenv("DATALAKE", "/opt/datalake")
CHK = f"{LAKE}/_checkpoints"
EQUIPO = "equipo3-v6"

VENTANA = os.getenv("VENTANA", "5 minutes")
WATERMARK = os.getenv("WATERMARK", "10 minutes")
TRIGGER = os.getenv("TRIGGER", "10 seconds")

ESQUEMA = T.StructType([
    T.StructField("schema_version", T.StringType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("equipo", T.StringType()),
    T.StructField("comercio_id", T.StringType()),
    T.StructField("tipo_comercio", T.StringType()),
    T.StructField("zona", T.StringType()),
    T.StructField("pedido_id", T.StringType()),
    T.StructField("items", T.IntegerType()),
    T.StructField("tiempo_estimado_min", T.IntegerType()),
    T.StructField("cola_actual", T.IntegerType()),
    T.StructField("inicio_preparacion_ts", T.StringType()),
    T.StructField("fin_preparacion_ts", T.StringType()),
])

COLUMNAS_ALERTA = ["alerta_id", "codigo", "severidad", "comercio_id", "zona", "pedido_id",
                   "valor", "umbral", "detalle", "momento_evento", "detectada_en"]


def crear_spark():
    return (SparkSession.builder
            .appName("rapidgo-v6-preparacion")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())


def leer_kafka(spark):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("key").cast("string").alias("key"),
                    F.col("value").cast("string").alias("value"),
                    "topic", "partition", "offset",
                    F.col("timestamp").alias("kafka_ts")))


def transformar(kafka_df, deduplicar=True):
    # T1: parseo con el contrato y tipos de fecha
    df = (kafka_df
          .withColumn("e", F.from_json("value", ESQUEMA))
          .select("key", "partition", "offset", "kafka_ts", "e.*")
          .withColumn("event_ts", F.try_to_timestamp(F.col("event_ts")))
          .withColumn("inicio_ts", F.try_to_timestamp(F.col("inicio_preparacion_ts")))
          .withColumn("fin_ts", F.try_to_timestamp(F.col("fin_preparacion_ts"))))

    # T2: limpieza / calidad
    df = (df.filter(F.col("event_id").isNotNull()
                    & F.col("event_ts").isNotNull()
                    & (F.col("equipo") == EQUIPO)
                    & F.col("comercio_id").rlike("^COM-[0-9]{3}$")
                    & (F.col("items") >= 1)
                    & (F.col("cola_actual") >= 0))
          .withWatermark("event_ts", WATERMARK))
    if deduplicar:
        df = df.dropDuplicatesWithinWatermark(["event_id"])

    # T3: enriquecimiento con las reglas de negocio
    finalizado = F.col("event_type") == "PREPARACION_FINALIZADA"
    tiempo_real = F.round((F.col("fin_ts").cast("long") - F.col("inicio_ts").cast("long")) / 60.0, 2)
    df = (df
          .withColumn("tiempo_real_min", F.when(finalizado, tiempo_real))
          .withColumn("demorado", F.when(finalizado, F.col("tiempo_real_min") > F.col("tiempo_estimado_min")))
          .withColumn("alerta_a1", F.coalesce(
              finalizado & ((F.col("tiempo_real_min") > FACTOR_EXCESO_PREPARACION * F.col("tiempo_estimado_min"))
                            | (F.col("tiempo_real_min") > TOPE_PREPARACION_MIN)),
              F.lit(False)))
          .withColumn("fecha", F.to_date("event_ts")))
    return df


def kpis_por_ventana(procesado):
    finalizado = F.col("event_type") == "PREPARACION_FINALIZADA"
    return (procesado
            .groupBy(F.window("event_ts", VENTANA), "comercio_id", "zona", "tipo_comercio")
            .agg(F.count("*").alias("eventos"),
                 F.sum(F.when(F.col("event_type") == "PEDIDO_RECIBIDO", 1).otherwise(0)).alias("pedidos_recibidos"),
                 F.sum(F.when(finalizado, 1).otherwise(0)).alias("pedidos_finalizados"),
                 F.round(F.avg("tiempo_real_min"), 2).alias("tiempo_prep_promedio_min"),        # K1
                 F.sum(F.when(F.col("demorado"), 1).otherwise(0)).alias("pedidos_demorados"),
                 F.round(F.avg("cola_actual"), 2).alias("cola_promedio"),                      # K2
                 F.max("cola_actual").alias("cola_maxima"),                                     # K2
                 F.sum(F.when(F.col("alerta_a1"), 1).otherwise(0)).alias("alertas_a1"))
            .withColumn("pct_demorados", F.when(F.col("pedidos_finalizados") > 0,               # K3
                        F.round(100.0 * F.col("pedidos_demorados") / F.col("pedidos_finalizados"), 1)))
            .withColumn("alerta_a2", F.col("cola_promedio") >= UMBRAL_COLA_PROMEDIO)
            .withColumn("severidad_a2", F.when(F.col("alerta_a2"),
                        F.when(F.col("cola_maxima") >= UMBRAL_COLA_MAXIMA_ALTA, "ALTA").otherwise("MEDIA")))
            .select(F.col("window.start").alias("ventana_inicio"), F.col("window.end").alias("ventana_fin"),
                    "comercio_id", "zona", "tipo_comercio", "eventos", "pedidos_recibidos",
                    "pedidos_finalizados", "tiempo_prep_promedio_min", "pedidos_demorados",
                    "pct_demorados", "cola_promedio", "cola_maxima", "alertas_a1",
                    "alerta_a2", "severidad_a2"))


def publicar_alertas(alertas_df, carpeta):
    """Guarda las alertas en Parquet, las publica en Kafka y las muestra en el log."""
    if alertas_df.isEmpty():
        return
    alertas_df = alertas_df.select(*COLUMNAS_ALERTA).cache()
    alertas_df.write.mode("append").parquet(f"{LAKE}/curated/alertas/{carpeta}")
    (alertas_df
     .select(F.col("comercio_id").alias("key"), F.to_json(F.struct(*COLUMNAS_ALERTA)).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP)
     .option("topic", TOPIC_ALERTAS)
     .save())
    print(f"[spark] {alertas_df.count()} alertas {carpeta}", flush=True)
    alertas_df.select("codigo", "severidad", "comercio_id", "pedido_id", "valor", "umbral").show(10, truncate=False)
    alertas_df.unpersist()


def procesar_lote_a1(batch_df, batch_id):
    alertas = (batch_df.filter("alerta_a1")
               .select(F.expr("uuid()").alias("alerta_id"),
                       F.lit(ALERTA_A1).alias("codigo"),
                       F.lit("ALTA").alias("severidad"),
                       "comercio_id", "zona", "pedido_id",
                       F.col("tiempo_real_min").cast("double").alias("valor"),
                       F.concat(F.lit(f"> {FACTOR_EXCESO_PREPARACION} x "), F.col("tiempo_estimado_min").cast("string"),
                                F.lit(f" min o > {TOPE_PREPARACION_MIN} min")).alias("umbral"),
                       F.concat(F.lit("Preparación de "), F.col("tiempo_real_min").cast("string"), F.lit(" min; prometido "),
                                F.col("tiempo_estimado_min").cast("string"), F.lit(" min")).alias("detalle"),
                       F.col("event_ts").alias("momento_evento"),
                       F.current_timestamp().alias("detectada_en")))
    publicar_alertas(alertas, ALERTA_A1)


def procesar_lote_kpis(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df = batch_df.cache()
    batch_df.write.mode("append").parquet(f"{LAKE}/curated/kpis_comercio_5min")
    print(f"[spark] lote {batch_id}: {batch_df.count()} ventanas de KPIs cerradas", flush=True)
    alertas = (batch_df.filter("alerta_a2")
               .select(F.expr("uuid()").alias("alerta_id"),
                       F.lit(ALERTA_A2).alias("codigo"),
                       F.col("severidad_a2").alias("severidad"),
                       "comercio_id", "zona",
                       F.lit(None).cast("string").alias("pedido_id"),
                       F.col("cola_promedio").cast("double").alias("valor"),
                       F.lit(f">= {UMBRAL_COLA_PROMEDIO} promedio (ALTA si max >= {UMBRAL_COLA_MAXIMA_ALTA})").alias("umbral"),
                       F.concat(F.lit("Cola promedio "), F.col("cola_promedio").cast("string"), F.lit(", máxima "),
                                F.col("cola_maxima").cast("string"), F.lit(" entre "), F.col("ventana_inicio").cast("string"),
                                F.lit(" y "), F.col("ventana_fin").cast("string")).alias("detalle"),
                       F.col("ventana_inicio").alias("momento_evento"),
                       F.current_timestamp().alias("detectada_en")))
    publicar_alertas(alertas, ALERTA_A2)
    batch_df.unpersist()


def main():
    spark = crear_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. RAW: copia fiel de Kafka
    raw = leer_kafka(spark).withColumn("fecha", F.to_date("kafka_ts"))
    (raw.writeStream.format("parquet")
     .option("path", f"{LAKE}/raw/eventos")
     .option("checkpointLocation", f"{CHK}/raw")
     .partitionBy("fecha")
     .trigger(processingTime=TRIGGER)
     .queryName("raw").start())

    # 2. PROCESSED: eventos limpios y enriquecidos
    procesado = transformar(leer_kafka(spark))
    (procesado.drop("value")
     .writeStream.format("parquet")
     .option("path", f"{LAKE}/processed/eventos")
     .option("checkpointLocation", f"{CHK}/processed")
     .partitionBy("fecha", "zona")
     .trigger(processingTime=TRIGGER)
     .queryName("processed").start())

    # 3. ALERTA A1 por pedido
    (procesado.writeStream
     .foreachBatch(procesar_lote_a1)
     .option("checkpointLocation", f"{CHK}/alertas_a1")
     .trigger(processingTime=TRIGGER)
     .queryName("alertas_a1").start())

    # 4. KPIs por ventana de 5 min + ALERTA A2
    kpis = kpis_por_ventana(transformar(leer_kafka(spark), deduplicar=False))
    (kpis.writeStream
     .outputMode("append")
     .foreachBatch(procesar_lote_kpis)
     .option("checkpointLocation", f"{CHK}/kpis")
     .trigger(processingTime=TRIGGER)
     .queryName("kpis_a2").start())

    print(f"[spark] Streaming iniciado · topic={TOPIC} · ventana={VENTANA} · watermark={WATERMARK}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
