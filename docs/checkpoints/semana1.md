# Checkpoint Semana 1 – v0.1.0-foundation

**Equipo 3 · Variante 6 – Preparación de pedidos**
Juan Quiroga · Laura Posada · Daniel Solano

## 1. Qué quedó listo esta semana
- Repositorio con estructura base, README inicial y `docker-compose.yml` preliminar (Kafka + creación automática del topic).
- Contrato JSON del evento (`samples/schema_evento_preparacion.json`) y ejemplos de un ciclo completo de pedido.
- 4 KPIs y 2 reglas de alerta con umbrales iniciales (`docs/diseno_variante6.md`).
- Kanban con ≥12 Issues distribuidos entre las 4 semanas y asignados.

## 2. Decisión más importante
**Key = `comercio_id` con 3 particiones** (ADR-001). Las dos alertas dependen de procesar en orden los eventos de cada comercio; usar `pedido_id` o no usar key rompía ese orden.

## 3. Riesgo principal
Integrar Spark Structured Streaming con Kafka dentro de Docker (versiones del conector `spark-sql-kafka`, red entre contenedores y memoria del equipo local). **Mitigación:** probar la conexión Spark→Kafka el primer día de la Semana 2 con un job mínimo de lectura antes de escribir transformaciones.

## 4. Plan Semana 2
| Integrante | Rol | Entregable |
|---|---|---|
| Juan Quiroga | Kafka / Producer | Generador de eventos con comercios normales, lentos y saturados |
| Laura Posada | Spark / Data Lake | Job de streaming, 3 transformaciones, ventana, Parquet + checkpoint |
| Daniel Solano | Tests / CI | `ci.yml`, pruebas del contrato y de las reglas de alerta |

## 5. Enlaces
- Issues semana 1: https://github.com/LauraJPs/rapidgo-v6-preparacion/milestone/1
- PR #17 (estructura base): https://github.com/LauraJPs/rapidgo-v6-preparacion/pull/17
- PR #18 (contrato + ADR-001): https://github.com/LauraJPs/rapidgo-v6-preparacion/pull/18
- Kanban: https://github.com/users/LauraJPs/projects/1
- Release: v0.1.0-foundation
