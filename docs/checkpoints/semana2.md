# Checkpoint Semana 2 – v0.2.0-streaming

**Equipo 3 · Variante 6 – Preparación de pedidos**
Juan Quiroga · Laura Posada · Daniel Solano

## 1. Qué quedó funcionando
- **Producer** con 30 comercios simulados (20 normales, 6 lentos, 4 saturados) que publica el ciclo de vida de cada pedido en `rapidgo.preparacion.eventos` con key `comercio_id` (PR #22).
- **Spark Structured Streaming**: parseo con el contrato, limpieza y deduplicación, `tiempo_real_min`, `demorado`, alerta A1 y ventana de 5 min con watermark de 10 min para K1, K2, K3 y A2 (PR #23).
- **Data Lake** en Parquet con checkpoint: `raw/` → `processed/` → `curated/` (KPIs y alertas). Las alertas también se publican en `rapidgo.preparacion.alertas`.
- **CI** en GitHub Actions en cada PR y obligatorio para fusionar en `main`: sintaxis, 35 pruebas (contrato, reglas A1/A2, simulador) y `docker compose config` (PR #21).

## 2. Valor real observado en la ejecución (2026-10-27, ≈20 min)
| Métrica | Valor | Fuente |
|---|---|---|
| Mensajes RAW por partición (0 / 1 / 2) | 16.673 / 16.366 / 10.372 (10 / 13 / 7 comercios) | `consultar_lake.py` |
| Tiempo promedio de preparación (K1) | 21,3 min (mediana 19,88 · máx. 59,05) | `consultar_lake.py` |
| % pedidos demorados (K3) | 56,7 % | `consultar_lake.py` |
| Comercios críticos (K2) | COM-027..030, cola promedio ≈ 17 (máx. 33) | `consultar_lake.py` |
| Alertas A1 / A2 | 1.760 / 1.918 | `consultar_lake.py` |
| Producer (prueba de Juan) | 741 eventos en ~20 s · particiones 290 / 282 / 169 | log del producer, PR #22 |

La partición 2 recibió ≈ 24 % de los mensajes frente a ≈ 38 % de las otras porque le correspondieron solo 7 de los 30 comercios. Esto confirma el riesgo de partición desbalanceada anotado en el ADR-001.

## 3. Decisión técnica tomada
A2 producía 1.918 alertas, incluidas alertas por picos aislados en comercios lentos, cuya cola promedio es ≈ 5,5. Decidimos confirmar A2 solo cuando la condición se cumple en **2 ventanas seguidas**, sin cambiar el umbral de cola ≥ 8. → [ADR-002](../decisions/ADR-002-a2-ventanas-consecutivas.md) · PR #24

## 4. Problema real encontrado y cómo se resolvió
Al verificar la imagen de Spark desde Git Bash en Windows, el comando
`docker run --rm spark:4.2.0-python3 /opt/spark/bin/spark-submit --version` falló con:

```
/opt/entrypoint.sh: line 128: /opt/spark/work-dir/C:/Program Files/Git/opt/spark/bin/spark-submit: No such file or directory
```

**Causa:** Git Bash (MSYS) convierte automáticamente las rutas que empiezan por `/` en rutas de Windows antes de pasarlas a Docker.
**Solución:** desactivar la conversión con `export MSYS_NO_PATHCONV=1` en `~/.bashrc`. Con eso el mismo comando mostró `version 4.2.0 · Scala 2.13.18`, compatible con el conector `spark-sql-kafka-0-10_2.13:4.2.0`. La solución quedó documentada en el README (sección *Problemas conocidos*) en el **PR #24**.

## 5. Enlaces
- PR CI y pruebas (Daniel): #21
- PR Producer (Juan): #22
- PR Spark + Data Lake (Laura): #23
- PR ADR-002 y checkpoint (Daniel): #24
- Action de ejemplo en PR: https://github.com/LauraJPs/rapidgo-v6-preparacion/pull/24/checks
- Release: v0.2.0-streaming
