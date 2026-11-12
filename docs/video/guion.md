# Guion del video final (máx. 10 min)

Estructura según la guía del proyecto. Cada integrante explica una parte técnica que implementó.
Grabar con la pantalla compartida (Meet, Teams o OBS) y la cámara encendida.

**Antes de grabar:** `docker compose up -d --build`, esperar ~3 min y tener abiertas las pestañas
Kafka UI (8085), Spark UI (4040), Airflow (8080), el repo, el Kanban y Actions.

| Tiempo | Contenido | Quién | Qué mostrar / decir |
|---|---|---|---|
| 0:00–1:00 | Problema, variante y objetivo | **Laura** | README arriba: demoras y acumulación de pedidos en comercios; KPIs K1–K3 y alertas A1/A2 |
| 1:00–2:30 | Arquitectura y decisiones | **Juan** | Diagrama `docs/arquitectura.png`; ADR-001 (key `comercio_id`, 3 particiones), ADR-002 (A2 en 2 ventanas), ADR-003 (reintentos) |
| 2:30–3:45 | Demo: Docker, Kafka y producer | **Juan** | `docker compose ps`; Kafka UI con el topic y sus 3 particiones; `docker compose logs producer --tail 5` |
| 3:45–5:00 | Demo: Spark y Data Lake | **Laura** | `docker compose logs spark --tail 20` (lotes, A2 confirmadas); Spark UI; carpetas `datalake/raw`, `processed`, `curated` |
| 5:00–6:30 | Demo: Airflow | **Daniel** | `bash scripts/verificar_pipeline.sh` o Trigger en la UI; grafo de 6 tareas en verde; logs con XCom; reporte generado; capturas del fallo controlado (`docs/evidencias/Semana3`) |
| 6:30–8:00 | GitHub: Kanban, Issues, PR, Actions y releases | **Daniel** | Kanban con todo en Done; un PR con review y checks; Actions con `pipeline-e2e` y su Summary; releases v0.1.0 → v1.0.0 |
| 8:00–9:00 | Resultados | **Laura** | Tabla *Resultados y evidencias* del README: 7–10 comercios críticos, 3.265 → 435 alertas consolidadas, recuperación en 1 min 09 s |
| 9:00–10:00 | Aprendizajes | **Los tres** | Uno cada uno (≈20 s): p. ej. watermark y checkpoints, permisos entre contenedores, CI como quality gate |

## Después de grabar
1. Subir el video a YouTube (**No listado**) o a Google Drive (**cualquier persona con el enlace puede ver**).
2. Reemplazar `_ENLACE_DEL_VIDEO_` en el README por el enlace y hacer un PR pequeño (o incluirlo antes del merge del PR #32).
3. Comprobar que el enlace abre en una ventana de incógnito.
