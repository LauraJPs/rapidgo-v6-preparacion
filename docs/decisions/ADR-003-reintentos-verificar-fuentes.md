# ADR-003 – Recuperación ante fuentes vacías: reintentos con espera exponencial

- **Estado:** Aceptado
- **Fecha:** 2026-11-04
- **Autores:** Juan Quiroga, Laura Posada, Daniel Solano
- **Issue relacionado:** #13
- **PR del DAG:** #27 · **PR que aplica el cambio:** #29

## Contexto / problema
El DAG `rapidgo_v6_preparacion` necesita que Spark ya haya escrito Parquet en `raw/` y
`processed/`. Para el fallo controlado lo ejecutamos **con el Data Lake vacío y Kafka, el
producer y Spark detenidos** (equivalente a "Kafka/Spark caído" o "raw vacío"):

```bash
docker compose down
rm -rf datalake/raw datalake/processed datalake/curated datalake/_checkpoints
docker compose up -d airflow          # solo Airflow
# UI → rapidgo_v6_preparacion → Trigger
```

Con la configuración inicial (`retries=2`, `retry_delay=15 s` para todas las tareas), en el run
`manual__2026-11-05T00:44:15` (hora Colombia 19:44):

| Medida | Valor real |
|---|---|
| Tarea que falló | `verificar_fuentes` |
| Intentos | 3 (1 + 2 reintentos), el último a las 19:44:51 |
| Tiempo hasta el fallo definitivo | **38,3 s** (19:44:16 → 19:44:55) |
| Tareas afectadas | 5 en `upstream_failed`: `validar_calidad_processed`, `agregar_kpis_diarios`, `consolidar_alertas`, `validar_calidad_curated`, `generar_reporte` |

Línea esencial del log (intento 3):
```
[2026-11-04 19:44:51] INFO - [verificar_fuentes] archivos Parquet por fuente: {'raw': 0, 'processed': 0, 'kpis_5min': 0, 'alertas': 0}
[2026-11-04 19:44:51] ERROR - Task failed with exception
RuntimeError: Fuentes sin archivos Parquet: ['raw', 'processed'] · conteo={'raw': 0, 'processed': 0, 'kpis_5min': 0, 'alertas': 0}. ¿Están corriendo el producer y Spark?
  File "/opt/airflow/dags/dag_preparacion.py", line 54 in verificar_fuentes
```

**Recuperación manual:** levantamos el pipeline (`docker compose up -d kafka kafka-init producer spark`).
La primera vez tardó ≈ 6 min en haber datos (incluye descargar la imagen de Kafka y el conector de
Spark). Con `processed` = 128 archivos hicimos **Clear** sobre `verificar_fuentes` con *Downstream*:
el intento 4 pasó (20:00:05) con conteo `{'raw': 36, 'processed': 192, 'kpis_5min': 40, 'alertas': 87}`.
Ahí apareció un segundo fallo real (permisos, ver abajo); tras corregirlo y hacer *Clear* de las
tareas fallidas, las 6 tareas terminaron en `success` (12 s) y se generó
`curated/reportes/reporte_20261105_011039.md`.

El problema: cuando el pipeline arranca junto con el DAG, Spark tarda entre 1 y varios minutos en
escribir los primeros archivos, pero el DAG se rinde a los ~38 s. La recuperación dependía de que
alguien hiciera *Clear* a mano.

## Opciones consideradas
| Opción | Ventaja | Desventaja |
|---|---|---|
| A. Mantener 2 reintentos de 15 s | Falla rápido | No tolera el arranque normal de Spark; recuperación siempre manual |
| B. Subir los reintentos de **todas** las tareas | Simple | Una tarea de calidad que falla por datos malos reintentaría inútilmente y retrasaría la alerta |
| **C. Reintentos con espera exponencial solo en `verificar_fuentes`** | Espera a que lleguen los datos (~4–5 min) sin ocultar errores de calidad | El fallo real de fuentes se reporta más tarde |
| D. Sensor que espere archivos | Patrón de Airflow para esperar datos | Ocupa un worker mientras espera; más complejo para el alcance |

## Decisión tomada
Opción **C**:
- `verificar_fuentes`: `retries=4`, `retry_delay=20 s`, `retry_exponential_backoff=True`,
  `max_retry_delay=2 min` → espera ≈ 20 s, 40 s, 80 s y 120 s entre intentos.
- Las demás tareas mantienen `retries=2` / `15 s`: si la calidad falla, el problema son los datos
  y reintentar no lo arregla.
- `tests/test_dag.py` comprueba ambas configuraciones en el CI (job `validar-dag`).

## Evidencia utilizada
Capturas en [`docs/evidencias/Semana3/`](../evidencias/Semana3/).

- **Antes (fallo):** `01_fallo_tabla_intentos.png`, `02_fallo_grafo.png`, `03_fallo_log_runtimeerror.png`.
- **Recuperación manual:** `04_recuperacion_parcial.png` … `08_reporte.png`.
- **Después, pipeline realmente caído** (run `manual__2026-11-05T01:24:41`, sin levantar Spark):
  `verificar_fuentes` falló en el **intento 5** y el run terminó en `failed` a los **5 min 51 s**
  (20:24:42 → 20:30:33). El aviso llega, pero más tarde (`09_backoff_pipeline_caido.png`).
- **Después, pipeline arrancando junto con el DAG** (run `manual__2026-11-05T01:33:11`):
  - `verificar_fuentes` falló en los intentos **1 y 2** (sin los Parquet requeridos) y pasó en el
    **intento 3** a las 20:34:17 con conteo `{'raw': 6, 'processed': 32, 'kpis_5min': 4, 'alertas': 4}`.
  - Las 6 tareas terminaron en `success` en **1 min 09 s** (20:33:12 → 20:34:22) **sin intervención manual**
    (`10_backoff_recuperado_grafo.png`, `11_backoff_recuperado_log.png`).
- CI del PR: `validar` y `validar-dag` en verde.

## Hallazgo adicional durante la recuperación: permisos del Data Lake
Al hacer *Clear*, `agregar_kpis_diarios` y `consolidar_alertas` fallaron 3 veces con:
```
PermissionError: [Errno 13] Permission denied: '/opt/datalake/curated/alertas_consolidadas'
```
Spark corre como `root` (`user: "0"`) y, al vaciar el Data Lake, volvió a crear `curated/` con
`drwxr-xr-x root root`; Airflow corre como `uid=50000(airflow)` y no puede crear carpetas ahí.
- Arreglo inmediato: `docker exec -u root rapidgo-v6-airflow chmod -R a+rwX /opt/datalake/curated` + *Clear*.
- Arreglo permanente (este PR): servicio `datalake-init` en `docker-compose.yml` que crea
  `curated/kpis_diarios`, `curated/alertas_consolidadas` y `curated/reportes` con permiso de escritura
  antes de arrancar Airflow (`depends_on: service_completed_successfully`). En el run de las 20:33
  ambas tareas pasaron al primer intento.

## Consecuencias / trade-offs
- ✅ El DAG se recupera solo cuando Spark arranca tarde (medido: intento 3, 1 min 09 s en total).
- ✅ Los errores de calidad siguen fallando rápido (2 reintentos de 15 s).
- ⚠️ Si Spark/Kafka están realmente caídos, el aviso llega a los ≈ 5,9 min en vez de 38 s.
- ⚠️ El reintento no distingue "todavía no hay datos" de "el pipeline está caído"; en la Semana 4
  podría agregarse una tarea que revise la antigüedad del último archivo.
