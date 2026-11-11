# RapidGo · Variante 6 – Preparación de pedidos

Pipeline Big Data end-to-end para detectar **demoras y acumulación de pedidos en comercios** (restaurantes, panaderías, farmacias, supermercados) antes de que el pedido llegue al repartidor.

**Curso:** Línea de Profundización I – Big Data · Ingeniería de Sistemas y Computación – Facatativá
**Equipo 3**

| Integrante | GitHub | Rol Semana 2 |
|---|---|---|
| Juan Quiroga | @JuanD1P | Kafka / Producer |
| Laura Posada | @LauraJPs | Spark / Data Lake |
| Daniel Solano | @DanielSolano7 | Tests / CI |

**Estado actual:** `v0.3.0-orchestration` – pipeline Kafka → Spark → Data Lake, orquestación con Airflow y CI ampliado.

## Problema
Cuando un comercio se demora preparando o acumula pedidos, el repartidor espera, la comida se enfría y el cliente recibe tarde. RapidGo necesita indicadores y alertas casi en tiempo real por comercio para actuar a tiempo.

## Arquitectura

![Arquitectura](docs/arquitectura.png)

```
Producer (simulador de comercios)
   │  key = comercio_id
   ▼
Kafka  topic: rapidgo.preparacion.eventos (3 particiones)
   ▼
Spark Structured Streaming  → parseo, tiempo_real_min, flag demora, ventanas 5 min, alertas
   ▼
Data Lake  raw → processed → curated (Parquet + checkpoint)
   ▼
Airflow  validar fuentes → calidad raw → agregación → calidad curated → reporte
   ▼
KPIs y alertas por comercio
```

## Eventos, KPIs y alertas
- **Topic:** `rapidgo.preparacion.eventos` · **Key:** `comercio_id` · **Particiones:** 3 → [ADR-001](docs/decisions/ADR-001-key-y-particiones-kafka.md)
- **Contrato:** [`samples/schema_evento_preparacion.json`](samples/schema_evento_preparacion.json)
- **KPIs:** tiempo promedio de preparación, backlog por comercio, % pedidos demorados, comercios críticos.
- **Alertas:** `A1_PREPARACION_EXCEDIDA` (real > 1.5× estimado o > 30 min) y `A2_ACUMULACION_PEDIDOS` (cola promedio ≥ 8 en 5 min).

Detalle: [docs/diseno_variante6.md](docs/diseno_variante6.md)

## Ejecución local
Requisitos: Docker Desktop (o Docker Engine + Compose v2) y Git. Se recomiendan 6 GB de RAM libres para Docker.

```bash
git clone https://github.com/LauraJPs/rapidgo-v6-preparacion.git
cd rapidgo-v6-preparacion
docker compose up -d --build
docker compose logs kafka-init          # topic con PartitionCount: 3
docker compose logs -f producer         # eventos enviados por partición
docker compose logs -f spark            # lotes, KPIs y alertas
```

| Servicio | URL |
|---|---|
| Kafka UI | http://localhost:8085 |
| Spark UI | http://localhost:4040 |
| Airflow UI | http://localhost:8080 (sin login en local) |

La primera vez Spark descarga el conector de Kafka (1–3 min). El simulador usa un reloj acelerado: `VELOCIDAD=2` → 1 segundo real = 2 minutos simulados, así las ventanas de 5 min se cierran en segundos.

Consultar los resultados del Data Lake:
```bash
docker compose exec spark /opt/spark/bin/spark-submit /opt/app/consultar_lake.py
```

Verificar todo de punta a punta (contenedores → Data Lake → DAG → reporte):
```bash
bash scripts/verificar_pipeline.sh
```

Detener todo: `docker compose down`. Al volver a levantar, Kafka arranca vacío y `kafka-init` limpia
el Data Lake automáticamente (los offsets y checkpoints viejos ya no sirven). Para pausar sin perder
datos: `docker compose stop` / `docker compose start`; el producer continúa su reloj simulado desde
el último evento del topic.

### Orquestación con Airflow
El DAG `rapidgo_v6_preparacion` (`airflow/dags/dag_preparacion.py`) corre sobre el Data Lake que escribe Spark:

```
verificar_fuentes → validar_calidad_processed → ┬→ agregar_kpis_diarios ─┬→ validar_calidad_curated → generar_reporte
                                                └→ consolidar_alertas  ──┘
```

| Tarea | Qué hace | Salida / XCom |
|---|---|---|
| `verificar_fuentes` | Exige archivos Parquet en raw y processed (cuenta también curated). Reintenta con espera exponencial (ADR-003) | conteo por fuente |
| `validar_calidad_processed` | Nulos, tipos de evento, equipo, cola ≥ 0, duplicados ≤ 1 % | métricas |
| `agregar_kpis_diarios` | K1, K2, K3 por día y comercio + comercio crítico | `curated/kpis_diarios/` |
| `consolidar_alertas` | No repite la misma alerta del mismo comercio en 60 min (pendiente del ADR-002) | `curated/alertas_consolidadas/` |
| `validar_calidad_curated` | Rangos y unicidad de la salida | errores |
| `generar_reporte` | Reporte Markdown con resumen y comercios críticos | `curated/reportes/reporte_*.md` |

Ejecutarlo:
1. Con el pipeline corriendo (`docker compose up -d --build`), esperar ~3 min a que Spark escriba datos.
2. Abrir http://localhost:8080 → **Dags** → `rapidgo_v6_preparacion` → **Trigger**.
3. Ver el grafo en la pestaña **Graph** y los logs de cada tarea.

Antes de Airflow arranca `datalake-init`, que deja `curated/kpis_diarios`, `curated/alertas_consolidadas` y `curated/reportes` con permiso de escritura.

Fallo controlado y recuperación: ver [ADR-003](docs/decisions/ADR-003-reintentos-verificar-fuentes.md) y [checkpoint semana 3](docs/checkpoints/semana3.md).

### Data Lake generado
| Ruta | Contenido |
|---|---|
| `datalake/raw/eventos` | Mensajes de Kafka con partición y offset |
| `datalake/processed/eventos` | Eventos limpios + `tiempo_real_min`, `demorado`, `alerta_a1` (por `fecha`, `zona`) |
| `datalake/curated/kpis_comercio_5min` | K1, K2, K3 y alerta A2 por comercio y ventana de 5 min |
| `datalake/curated/alertas/` | Alertas A1 y A2 (también publicadas en `rapidgo.preparacion.alertas`) |
| `datalake/curated/kpis_diarios/` · `alertas_consolidadas/` · `reportes/` | Salidas del DAG de Airflow |


### Problemas conocidos (Windows / Git Bash)
- `docker run ... /opt/spark/bin/spark-submit` falla con `/opt/spark/work-dir/C:/Program Files/Git/opt/...: No such file or directory`: Git Bash convierte las rutas que empiezan por `/`. Solución: `echo 'export MSYS_NO_PATHCONV=1' >> ~/.bashrc && source ~/.bashrc`.
- `Conflict. The container name "/rapidgo-kafka" is already in use`: hay un contenedor viejo con ese nombre. Solución: `docker rename rapidgo-kafka rapidgo-kafka-viejo`.
- Al pegar comandos con Ctrl+V en Git Bash aparece `^[[200~`: escribir los comandos o pegarlos con clic derecho → Paste, una línea a la vez.
- Tareas del DAG fallan con `PermissionError: [Errno 13] Permission denied: '/opt/datalake/curated/...'`: Spark (root) creó `curated/` y Airflow (uid 50000) no puede escribir. Lo evita el servicio `datalake-init` del compose; arreglo manual: `MSYS_NO_PATHCONV=1 docker exec -u root rapidgo-v6-airflow chmod -R a+rwX /opt/datalake/curated` y *Clear* de las tareas.
- Spark no escribe después de `docker compose down` (offsets viejos en el checkpoint): desde la Semana 4 `kafka-init` limpia el Data Lake cuando el topic es nuevo; si se borró solo Kafka, ejecutar `docker compose down` y volver a levantar.

## Pruebas y CI
```bash
pip install -r requirements.txt
pytest -v
```
El workflow `.github/workflows/ci.yml` corre en cada push, Pull Request y manualmente (`workflow_dispatch`): sintaxis Python, pruebas del contrato, reglas A1/A2 y simulador, y `docker compose config`.

## Flujo de trabajo
`Issue → Branch → PR → GitHub Actions → Review → Merge → Release`. No se hacen cambios funcionales directamente sobre `main`.

## Releases
| Versión | Contenido |
|---|---|
| v0.1.0-foundation | Arquitectura, contrato, KPIs, alertas, Kanban, ADR-001 |
| v0.2.0-streaming | Producer, Spark Structured Streaming, Data Lake Parquet, alertas A1/A2 (A2 con 2 ventanas seguidas, ADR-002), CI |
| v0.3.0-orchestration | DAG de Airflow (6 tareas, retries, XCom), CI con validación del DAG, fallo controlado (ADR-003) |
| v1.0.0 | _pendiente_ |
