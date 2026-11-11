#!/usr/bin/env bash
# RapidGo V6 – verificación de punta a punta del pipeline local (Semana 4).
#
# Uso (desde la carpeta del repo, con `docker compose up -d --build` ya ejecutado):
#   bash scripts/verificar_pipeline.sh
#
# 1. Revisa que kafka, producer, spark y airflow estén corriendo.
# 2. Espera (máx. 5 min) a que Spark escriba Parquet en raw y processed.
# 3. Dispara el DAG de Airflow y espera (máx. 8 min) el reporte nuevo.
# 4. Muestra el reporte. Termina con código 0 si todo pasó y 1 si algo falló.
set -u
export MSYS_NO_PATHCONV=1          # Git Bash en Windows: no convertir rutas /opt/...
cd "$(dirname "$0")/.." || exit 1

DAG_ID=rapidgo_v6_preparacion
REPORTES=datalake/curated/reportes
fallos=0

contar() {  # archivos Parquet de una capa, sin _spark_metadata ni _temporary
  find "datalake/$1" -name '*.parquet' -not -path '*/_*' 2>/dev/null | wc -l | tr -d ' '
}

echo "== 1. Contenedores"
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
for servicio in kafka producer spark airflow; do
  if docker compose ps --status running --services | grep -qx "$servicio"; then
    echo "  ✅ $servicio corriendo"
  else
    echo "  ❌ $servicio NO está corriendo (docker compose up -d --build)"
    fallos=$((fallos + 1))
  fi
done
[ "$fallos" -gt 0 ] && { echo "Resultado: ❌ faltan servicios"; exit 1; }

echo "== 2. Data Lake"
for _ in $(seq 1 30); do
  [ "$(contar raw)" -gt 0 ] && [ "$(contar processed)" -gt 0 ] && break
  echo "  esperando a Spark... raw=$(contar raw) processed=$(contar processed)"
  sleep 10
done
for capa in raw processed curated; do
  echo "  $capa: $(contar "$capa") archivos Parquet"
done
if [ "$(contar processed)" -eq 0 ]; then
  echo "  ❌ Spark no ha escrito processed (docker compose logs spark --tail 30)"
  exit 1
fi

echo "== 3. DAG $DAG_ID"
antes=$(ls "$REPORTES" 2>/dev/null | wc -l | tr -d ' ')
disparado=0
for _ in $(seq 1 12); do
  if docker compose exec -T airflow airflow dags trigger "$DAG_ID" >/dev/null 2>&1; then
    disparado=1; break
  fi
  echo "  Airflow todavía está cargando el DAG, reintento en 10 s..."
  sleep 10
done
[ "$disparado" -eq 0 ] && { echo "  ❌ no se pudo disparar el DAG"; exit 1; }
echo "  DAG disparado; puedes verlo en http://localhost:8080 (pestaña Graph)"

nuevo=""
for _ in $(seq 1 48); do
  ahora=$(ls "$REPORTES" 2>/dev/null | wc -l | tr -d ' ')
  if [ "$ahora" -gt "$antes" ]; then
    nuevo=$(ls -t "$REPORTES"/reporte_*.md | head -1); break
  fi
  sleep 10
done
if [ -z "$nuevo" ]; then
  echo "  ❌ el DAG no generó reporte en 8 min: revisar la UI de Airflow"
  exit 1
fi

echo "== 4. Reporte generado: $nuevo"
cat "$nuevo"
echo "Resultado: ✅ pipeline completo (Kafka → Spark → Data Lake → Airflow)"
