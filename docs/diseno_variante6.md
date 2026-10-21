# Diseño de la Variante 6 – Preparación de pedidos

> Valores **iniciales** (Semana 1). Se ajustarán en la Semana 2 con datos reales de ejecución y el cambio quedará documentado en ADR-002.

## 1. Problema de negocio

Los pedidos se demoran **dentro del comercio** (restaurante, panadería, farmacia, etc.) antes de entregarse al repartidor. Eso genera repartidores esperando, comida fría y clientes insatisfechos. RapidGo necesita detectar **en casi tiempo real** qué comercios están preparando lento o acumulando pedidos para actuar (pausar el comercio, reasignar repartidores, ajustar el tiempo prometido al cliente).

## 2. Ciclo de vida del pedido (eventos)

```
PEDIDO_RECIBIDO -> PREPARACION_INICIADA -> PREPARACION_FINALIZADA -> PEDIDO_RECOGIDO
        \______________________________________________ PEDIDO_CANCELADO
```

| Campo | Uso |
|---|---|
| `comercio_id` | Key de Kafka; agrupación de KPIs y alertas |
| `pedido_id` | Seguimiento del pedido a través de sus eventos |
| `event_ts` | Tiempo de evento (ventanas + watermark en Spark) |
| `inicio_preparacion_ts` / `fin_preparacion_ts` | Cálculo del tiempo real de preparación |
| `tiempo_estimado_min` | Lo prometido por el comercio; base de la alerta de demora |
| `cola_actual` | Backlog del comercio en ese momento |
| `zona`, `tipo_comercio` | Dimensiones de análisis |
| `equipo` | Identifica eventos del equipo (`equipo3-v6`) |

Contrato formal: [`samples/schema_evento_preparacion.json`](../samples/schema_evento_preparacion.json) · Ejemplo: [`samples/evento_preparacion.json`](../samples/evento_preparacion.json)

## 3. KPIs (mínimo 3)

| # | KPI | Cálculo | Ventana |
|---|---|---|---|
| K1 | **Tiempo promedio de preparación** por comercio | `avg(fin_preparacion_ts - inicio_preparacion_ts)` en minutos, sobre eventos `PREPARACION_FINALIZADA` | Tumbling 5 min |
| K2 | **Backlog** por comercio | `max(cola_actual)` y `avg(cola_actual)` | Tumbling 5 min |
| K3 | **% de pedidos demorados** | pedidos con `tiempo_real > tiempo_estimado_min` / pedidos finalizados × 100 | Tumbling 5 min |
| K4 | **Comercios críticos** | Comercios con al menos una alerta en la ventana (ranking) | Tumbling 15 min |

## 4. Reglas de alerta (mínimo 2)

| Código | Regla | Umbral inicial | Severidad |
|---|---|---|---|
| `A1_PREPARACION_EXCEDIDA` | El tiempo real de preparación de un pedido supera lo prometido | `tiempo_real_min > 1.5 × tiempo_estimado_min` **o** `tiempo_real_min > 30` | ALTA |
| `A2_ACUMULACION_PEDIDOS` | El comercio acumula pedidos sin despachar | `avg(cola_actual) >= 8` en ventana de 5 min | MEDIA (ALTA si `max >= 12`) |

**Justificación de umbrales iniciales**
- **1.5×** el estimado: tolera variaciones normales (un pedido de 15 min que sale en 20 no es crítico), pero detecta desviaciones claras (15 min → más de 22,5 min).
- **30 min** como tope absoluto: aunque el comercio haya prometido mucho, más de media hora en cocina ya afecta la entrega.
- **Cola ≥ 8** en promedio de 5 min: un valor puntual alto puede ser un pico normal (ej. hora de almuerzo); el promedio evita falsas alarmas.
- En el generador se simularán comercios "lentos" y "saturados" para garantizar que ambas alertas se disparen y poder medir cuántas salen con estos umbrales.

## 5. Diseño de Kafka

| Elemento | Decisión |
|---|---|
| Topic de entrada | `rapidgo.preparacion.eventos` |
| Topic de alertas (opcional, Semana 2) | `rapidgo.preparacion.alertas` |
| Key | `comercio_id` |
| Particiones | **3** |
| Replication factor | 1 (entorno local de un solo broker) |
| Retención | 7 días (por defecto) |

La justificación completa está en [ADR-001](decisions/ADR-001-key-y-particiones-kafka.md).

## 6. Data Lake

```
datalake/
├── raw/        eventos tal como llegan de Kafka (JSON parseado → Parquet), particionado por fecha
├── processed/  eventos limpios, tipados, con tiempo_real_min y flag de demora
└── curated/    KPIs por ventana y tabla de alertas (lo que consume Airflow y el reporte)
```
