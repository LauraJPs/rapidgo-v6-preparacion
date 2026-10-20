# Data Lake local

| Zona | Contenido | Formato |
|---|---|---|
| `raw/` | Eventos de Kafka tal como llegan, más offset/partición | Parquet, particionado por `fecha` |
| `processed/` | Eventos limpios y tipados con `tiempo_real_min` y `demorado` | Parquet, particionado por `fecha` y `zona` |
| `curated/` | KPIs por ventana y tabla de alertas | Parquet |
| `_checkpoints/` | Checkpoints de Spark Structured Streaming | — |

Los datos generados **no se suben a Git** (ver `.gitignore`); solo este README y los `.gitkeep`.
