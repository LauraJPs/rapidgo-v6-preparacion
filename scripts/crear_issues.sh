#!/usr/bin/env bash
# Crea etiquetas, milestones (uno por semana) y los Issues del backlog.
# Requisitos: GitHub CLI instalado y autenticado (gh auth login).
# Uso: ejecutar desde la raíz del repositorio clonado:  bash scripts/crear_issues.sh
set -euo pipefail

# ======= CAMBIAR por los usuarios reales de GitHub =======
JUAN="JuanD1P"
LAURA="LauraJPs"
DANIEL="DanielSolano7"
# =========================================================

REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
echo "Repositorio: $REPO"

# ---------- Etiquetas ----------
lbl() { gh label create "$1" --color "$2" --description "$3" --force >/dev/null; }
lbl "semana-1" "1D7A4F" "Semana 1 - Diseño"
lbl "semana-2" "1F4E79" "Semana 2 - Streaming + CI"
lbl "semana-3" "0E9F8E" "Semana 3 - Airflow"
lbl "semana-4" "D94A5C" "Semana 4 - Release"
lbl "kafka"    "F5A623" "Componente Kafka / producer"
lbl "spark"    "1F4E79" "Componente Spark"
lbl "datalake" "0E9F8E" "Componente Data Lake"
lbl "airflow"  "006837" "Componente Airflow"
lbl "ci"       "6E5494" "GitHub Actions / pruebas"
lbl "docs"     "BFD4F2" "Documentación"

# ---------- Milestones ----------
ms() { gh api "repos/$REPO/milestones" -f title="$1" -f due_on="$2" -f description="$3" >/dev/null 2>&1 || true; }
ms "v0.1.0-foundation"    "2026-10-22T23:59:00Z" "Semana 1 - Diseño, arquitectura y organización"
ms "v0.2.0-streaming"     "2026-10-29T23:59:00Z" "Semana 2 - Pipeline funcional y CI"
ms "v0.3.0-orchestration" "2026-11-05T23:59:00Z" "Semana 3 - Airflow, calidad y resiliencia"
ms "v1.0.0"               "2026-11-12T23:59:00Z" "Semana 4 - Integración, Quality Gate y release"

issue() { # titulo  labels  assignee  milestone  cuerpo
  gh issue create --title "$1" --label "$2" --assignee "$3" --milestone "$4" --body "$5"
}

# ---------- Semana 1 ----------
issue "Estructura inicial del repositorio y docker-compose preliminar" "semana-1,docs" "$DANIEL" "v0.1.0-foundation" \
"Crear carpetas recomendadas, .gitignore, requirements.txt y docker-compose.yml con Kafka y creación del topic.

**Criterio de aceptación:** \`docker compose config --quiet\` sin errores y \`docker compose up -d\` crea el topic con 3 particiones."

issue "Contrato JSON del evento de preparación + ejemplos" "semana-1,kafka" "$JUAN" "v0.1.0-foundation" \
"Definir \`samples/schema_evento_preparacion.json\` y ejemplos en \`samples/\`.

**Criterio de aceptación:** los ejemplos validan contra el schema."

issue "Definir KPIs, reglas de alerta y umbrales iniciales" "semana-1,docs,spark" "$LAURA" "v0.1.0-foundation" \
"Documentar ≥3 KPIs y ≥2 alertas con umbrales justificados en \`docs/diseno_variante6.md\`."

issue "ADR-001: key y particiones de Kafka" "semana-1,kafka,docs" "$JUAN" "v0.1.0-foundation" \
"Justificar topic, key (\`comercio_id\`) y número de particiones (3)."

issue "README inicial, diagrama de arquitectura y checkpoint semana 1" "semana-1,docs" "$LAURA" "v0.1.0-foundation" \
"README con problema, equipo, variante y arquitectura. \`docs/arquitectura.png\` y \`docs/checkpoints/semana1.md\`."

# ---------- Semana 2 ----------
issue "Producer: simulador de comercios (normales, lentos y saturados)" "semana-2,kafka" "$JUAN" "v0.2.0-streaming" \
"Producer en Python que publique el ciclo de vida de pedidos con key \`comercio_id\`. Incluir comercios lentos y saturados para disparar las alertas."

issue "Spark Structured Streaming: lectura, parseo y transformaciones" "semana-2,spark" "$LAURA" "v0.2.0-streaming" \
"Leer de Kafka, parsear con el schema, ≥3 transformaciones (tiempo_real_min, flag demorado, limpieza/dedupe) y agregación por ventana de 5 min con watermark."

issue "Data Lake: persistencia Parquet raw/processed/curated con checkpoint" "semana-2,datalake,spark" "$LAURA" "v0.2.0-streaming" \
"Escribir cada zona en Parquet con su checkpoint. Implementar alertas A1 y A2."

issue "CI básico: ci.yml + pruebas de contrato y reglas de alerta" "semana-2,ci" "$DANIEL" "v0.2.0-streaming" \
"Workflow con push, pull_request y workflow_dispatch: compileall, pytest, docker compose config. Pruebas unitarias del contrato y de A1/A2."

issue "ADR-002 y checkpoint semana 2 con valores reales" "semana-2,docs" "$DANIEL" "v0.2.0-streaming" \
"Decisión técnica tomada tras observar la ejecución (p. ej. ajuste de umbrales o watermark) con valores reales."

# ---------- Semana 3 ----------
issue "DAG de Airflow con ≥5 tasks, retries y XCom" "semana-3,airflow" "$JUAN" "v0.3.0-orchestration" \
"check fuentes → validar raw → agregar → validar curated → reporte. Retries y paso de información con XCom."

issue "Ampliar CI: validar/importar DAG + nuevas pruebas" "semana-3,ci" "$LAURA" "v0.3.0-orchestration" \
"Agregar al workflow la importación del DAG (DagBag sin errores) y pruebas de las reglas nuevas."

issue "Fallo controlado, recuperación, ADR-003 y checkpoint semana 3" "semana-3,airflow,docs" "$DANIEL" "v0.3.0-orchestration" \
"Provocar fallo (Kafka detenido / raw vacío / esquema inválido), registrar log real y recuperación."

# ---------- Semana 4 ----------
issue "Integración final y estabilización del pipeline" "semana-4,kafka,spark,airflow" "$JUAN" "v1.0.0" \
"Pipeline completo levantando con un solo docker compose up siguiendo el README."

issue "Quality gate final y release v1.0.0" "semana-4,ci" "$LAURA" "v1.0.0" \
"CI verde antes del tag, release notes y release.yml (opcional). Cada integrante ejecuta workflow_dispatch."

issue "README final, tabla de contribuciones y video" "semana-4,docs" "$DANIEL" "v1.0.0" \
"README completo según la guía (sección 10) y video ≤ 10 min enlazado."

echo "Listo. Issues creados en $REPO"
