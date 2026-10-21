# ADR-001 – Key y número de particiones del topic de preparación

- **Estado:** Aceptado
- **Fecha:** 2026-10-22
- **Autores:** Juan Quiroga, Laura Posada, Daniel Solano
- **Issue relacionado:** #4

## Contexto / problema

Los eventos de un mismo pedido (`RECIBIDO → INICIADA → FINALIZADA → RECOGIDO`) deben procesarse en orden para calcular correctamente el tiempo de preparación. Además, la alerta de acumulación depende de la secuencia de `cola_actual` de **cada comercio**. Necesitamos elegir la key del mensaje y el número de particiones del topic `rapidgo.preparacion.eventos`.

Kafka solo garantiza orden **dentro de una partición**, y todos los mensajes con la misma key van a la misma partición.

## Opciones consideradas

| Opción | Ventaja | Desventaja |
|---|---|---|
| A. Sin key (round-robin) | Distribución perfecta | Se pierde el orden por comercio y por pedido |
| B. Key = `pedido_id` | Orden por pedido | Los eventos de un comercio quedan repartidos; la cola del comercio llega desordenada |
| C. Key = `zona` | Pocas keys, fácil de entender | Solo 4 zonas → particiones desbalanceadas; la zona centro concentraría la carga |
| **D. Key = `comercio_id`** | Orden por comercio **y** por pedido (un pedido pertenece a un solo comercio) | Un comercio muy grande podría generar una partición "caliente" |

Particiones: 1 (sin paralelismo), **3**, 6 o más.

## Decisión tomada

- **Key = `comercio_id`**.
- **3 particiones**.

## Evidencia utilizada

- El cálculo de K1 y la alerta A1 necesitan el par inicio/fin de un mismo pedido; con la opción D ambos eventos quedan en la misma partición y en orden.
- La alerta A2 analiza la evolución de `cola_actual` por comercio; con la opción D esa secuencia no se mezcla.
- El simulador tendrá ~30 comercios; con 3 particiones quedan ~10 comercios por partición, una distribución razonable.
- 3 particiones permiten hasta 3 consumidores en paralelo (o 3 tareas de lectura en Spark) en un entorno local con recursos limitados, y coincide con el número de integrantes para demostrar paralelismo.
- _(Semana 2: agregar aquí la distribución real de mensajes por partición, obtenida con `kafka-get-offsets` o la UI de Kafka.)_

## Consecuencias / trade-offs

- ✅ Orden garantizado por comercio y por pedido.
- ✅ Paralelismo suficiente para el entorno local.
- ⚠️ Si un comercio genera muchísimos eventos, su partición quedará más cargada (hot partition). Se monitoreará en la Semana 2 comparando offsets por partición.
- ⚠️ Aumentar particiones después cambia el mapeo key→partición; por eso se decide desde el inicio.
- ⚠️ Replication factor 1: no hay tolerancia a fallos del broker (aceptable solo para el entorno académico local).
