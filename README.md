# RapidGo · Variante 6 – Preparación de pedidos

Pipeline Big Data end-to-end para detectar **demoras y acumulación de pedidos en comercios** (restaurantes, panaderías, farmacias, supermercados) antes de que el pedido llegue al repartidor.

**Curso:** Línea de Profundización I – Big Data · Ingeniería de Sistemas y Computación – Facatativá
**Equipo 3**

| Integrante | GitHub | Rol Semana 2 |
|---|---|---|
| Juan Quiroga | @JuanD1P | Kafka / Producer |
| Laura Posada | @LauraJPs | Spark / Data Lake |
| Daniel Solano | @DanielSolano7 | Tests / CI |

**Estado actual:** `v0.1.0-foundation` – diseño, contrato y organización.

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

## Ejecución local (preliminar)
Requisitos: Docker Desktop (o Docker Engine + Compose v2) y Git.

```bash
git clone https://github.com/LauraJPs/rapidgo-v6-preparacion.git
cd rapidgo-v6-preparacion
docker compose up -d
docker compose logs kafka-init      # debe mostrar el topic con PartitionCount: 3
```
Kafka UI: http://localhost:8085

## Flujo de trabajo
`Issue → Branch → PR → GitHub Actions → Review → Merge → Release`. No se hacen cambios funcionales directamente sobre `main`.

## Releases
| Versión | Contenido |
|---|---|
| v0.1.0-foundation | Arquitectura, contrato, KPIs, alertas, Kanban, ADR-001 |
| v0.2.0-streaming | _pendiente_ |
| v0.3.0-orchestration | _pendiente_ |
| v1.0.0 | _pendiente_ |
