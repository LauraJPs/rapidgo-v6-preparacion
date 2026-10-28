# ADR-002 – La alerta A2 se confirma solo tras 2 ventanas seguidas

- **Estado:** Aceptado
- **Fecha:** 2026-10-28
- **Autores:** Juan Quiroga, Laura Posada, Daniel Solano
- **Issue relacionado:** #10
- **PR que observó el problema:** #23 (Spark + Data Lake) · **PR que aplica el cambio:** #24

## Contexto / problema
Con el pipeline completo funcionando (PR #23) dejamos correr el producer y Spark ≈20 min
y consultamos el Data Lake con `spark/consultar_lake.py`. La regla A2 inicial
(cola promedio ≥ 8 en **una** ventana de 5 min) generó más alertas que A1:

| Medida (ejecución del 2026-10-27) | Valor |
|---|---|
| Mensajes procesados (RAW) | 43.411 |
| Alertas A1 | 1.760 |
| Alertas A2 | **1.918** (371 MEDIA · 1.547 ALTA) |
| Ventanas con A2 por comercio saturado (COM-027..030) | 364 – 369 |
| Ventanas con A2 por comercio **lento** (COM-021..026) | 38 – 68 |
| Cola promedio de los comercios lentos | 5,29 – 5,96 (máx. 12–17) |

Los comercios lentos tienen cola promedio ≈ 5,5, muy por debajo de 8, pero A2 se les
disparaba en picos aislados de una sola ventana. Esas alertas no indican una
acumulación sostenida y generan ruido para el operador.

## Opciones consideradas
| Opción | Ventaja | Desventaja |
|---|---|---|
| A. Mantener la regla (1 ventana) | Detecta de inmediato | Picos aislados generan falsas alarmas |
| B. Subir el umbral de cola (8 → 12) | Menos alertas | Detecta tarde a los saturados reales; cambia el diseño validado en Semana 1 |
| **C. Exigir 2 ventanas seguidas** | Elimina picos aislados sin cambiar el umbral | Retrasa la alerta 5 min (una ventana) |
| D. No repetir la alerta del mismo comercio por X min | Reduce repeticiones de los saturados | No distingue picos de acumulación real |

## Decisión tomada
Opción **C**. La condición de A2 sigue siendo `avg(cola_actual) >= 8`, pero la alerta solo
se emite si el mismo comercio la cumplió también en la ventana de 5 min inmediatamente
anterior.
- `spark/reglas.py`: nuevo parámetro `VENTANAS_CONSECUTIVAS_A2 = 2` y función `confirmar_acumulacion()`.
- `spark/job_preparacion.py`: en cada lote se cruzan las ventanas candidatas con las ventanas
  guardadas en `curated/kpis_comercio_5min` para confirmar la anterior.
- `tests/test_reglas.py`: 4 pruebas nuevas (pico aislado, 2 seguidas, interrumpida, sin historial).

## Evidencia utilizada
- **Antes:** salida de `consultar_lake.py` del 2026-10-27 (tabla de arriba).
- **Simulación previa** con el mismo generador (40 h simuladas): A2 en normales 40 → 13 (−67 %),
  lentos 406 → 288 (−29 %), saturados 1.905 → 1.897 (≈ 0 %).
- **Después** (ejecución del 2026-10-28 con este cambio, rama `docs/adr002-semana2`, ≈17 min, datalake limpio):

  | Medida | Antes (27-oct) | Después (28-oct) |
  |---|---|---|
  | Mensajes RAW | 43.411 | 68.098 |
  | % pedidos demorados (K3, control) | 56,7 % | 56,4 % |
  | Alertas A1 (regla sin cambios) | 1.760 | 2.505 |
  | Alertas A2 | 1.918 | 2.389 (307 MEDIA · 2.082 ALTA) |
  | A1 por cada 1.000 mensajes | 40,5 | 36,8 |
  | A2 por cada 1.000 mensajes | 44,2 | 35,1 |
  | Ventanas con condición A2 (top 10 comercios) | — | 2.515 (saturados 2.076 · lentos 439) |

  La segunda ejecución duró más (+57 % de mensajes), por eso se comparan tasas. K3 casi igual
  (56,7 % vs 56,4 %) indica que el simulador produjo el mismo tipo de carga.
- **Log de Spark** con la nueva regla, p. ej. `lote 1: A2 candidatas=30 confirmadas=25`,
  `lote 38: A2 candidatas=63 confirmadas=54`. En los lotes registrados: **1.586 candidatas →
  1.476 confirmadas (−7 %)**.
- Al final del log: `WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger
  interval is 10000 milliseconds, but spent 18546 milliseconds`.
- CI del PR #24: check `validar` en verde (35 passed).

### Lectura de los resultados
- La regla descarta los picos aislados: solo se emiten como alerta las ventanas cuya ventana anterior
  también cumplía la condición (≈ 7 % menos en el log; la tasa de A2 bajó de 44,2 a 35,1 por cada
  1.000 mensajes, mientras A1, que no cambió, bajó solo de 40,5 a 36,8).
- Como se esperaba, **los comercios saturados (COM-027..030) siguen alertando en casi todas sus
  ventanas** (515–521 ventanas con condición cada uno): su cola es alta de forma sostenida.

## Consecuencias / trade-offs
- ✅ Se eliminan las alertas por picos aislados en comercios normales y lentos.
- ✅ El umbral de diseño (cola ≥ 8) no cambia; el ajuste se prueba con pytest.
- ⚠️ A2 se emite con 5 minutos (una ventana) de retraso.
- ⚠️ **No resuelve la repetición en comercios saturados**: siguen alertando en casi todas las
  ventanas porque su cola está siempre por encima de 8. Se evaluará en la Semana 3 una
  regla de "no repetir" (opción D) junto con Airflow.
- ⚠️ **Costo de rendimiento observado:** la confirmación lee todo el histórico de
  `curated/kpis_comercio_5min` en cada lote; a los ≈17 min un lote tardó 18,5 s con un trigger de
  10 s (`Current batch is falling behind`). En la Semana 3 se limitará la lectura a las ventanas de
  la última hora o se guardará solo la última ventana de cada comercio.
