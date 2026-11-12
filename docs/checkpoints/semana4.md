# Checkpoint Semana 4 – v1.0.0

**Equipo 3 · Variante 6 – Preparación de pedidos**
Juan Quiroga · Laura Posada · Daniel Solano

## 1. Qué quedó funcionando
- **Arranque con un solo comando** (`docker compose up -d --build`): `kafka-init` limpia el Data Lake cuando Kafka arranca vacío, `datalake-init` deja listas las carpetas de Airflow y el producer continúa su reloj simulado si se reinicia (PR #30).
- **Frescura en el DAG**: `verificar_fuentes` mide hace cuántos minutos escribió Spark y el reporte avisa si pasan más de 15 min (pendiente del ADR-003, PR #30).
- **`scripts/verificar_pipeline.sh`**: verifica contenedores → Data Lake → dispara el DAG → muestra el reporte (PR #30).
- **Quality gate final**: job `pipeline-e2e` que levanta el pipeline completo dentro de GitHub Actions y `release.yml`, que solo publica el release si todo el CI queda en verde (PR #31).
- **README final**, tabla de contribuciones y guion del video (PR #32).

## 2. Valor real observado
Resumen del job `pipeline-e2e` del PR #31, con el pipeline levantado desde cero en GitHub Actions (2 min 05 s):

| Medida | Valor |
|---|---|
| Archivos Parquet raw / processed / curated | 3 / 16 / 4 |
| Eventos procesados por el DAG | 577 · 30 comercios · 239 pedidos · 0,0 % duplicados |
| Comercios críticos | 7 de 30 (COM-027..030 a la cabeza, cola promedio 8,9–15,0) |
| Validación de curated | OK |
| Frescura | último Parquet de processed escrito hace 0,2 min |

## 3. Decisión técnica tomada
**Limpiar el Data Lake automáticamente cuando el topic es nuevo**, en lugar de pedir en el README que se borre a mano. En las semanas 2 y 3, después de `docker compose down`, Kafka empezaba de cero, pero los checkpoints de Spark guardaban offsets viejos (`offset was changed from 799274 to 235`) y Spark dejaba de escribir KPIs. Como esos datos ya no corresponden al topic, `kafka-init` los borra antes de crear el topic. Si Kafka conserva el topic (`docker compose stop/start`), el Data Lake se conserva.

## 4. Problema encontrado y cómo se resolvió
- **Problema:** al reiniciar solo el producer, su reloj simulado volvía a la hora actual, mientras que el watermark de Spark ya iba varias horas simuladas adelante: los eventos nuevos llegaban "tarde" y Spark los descartaba (0 lotes nuevos de KPIs).
- **Solución (PR #30):** al arrancar, el producer lee el último `event_ts` de cada partición del topic y continúa un minuto después (`reloj_inicial`, con 3 pruebas nuevas en `tests/test_producer.py`).

## 5. Enlaces
- Issues: #14 (integración) · #15 (quality gate) · #16 (README y video)
- PR integración final (Juan): #30
- PR quality gate (Laura): #31
- PR README final (Daniel): #32
- Action `pipeline-e2e`: https://github.com/LauraJPs/rapidgo-v6-preparacion/pull/31/checks
- Release: v1.0.0
