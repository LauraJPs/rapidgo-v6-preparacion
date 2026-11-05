"""Validación del DAG de Airflow con DagBag.

En el job `validar` (sin Airflow instalado) estas pruebas se omiten; en el job
`validar-dag` del CI se instala Airflow 3.1.8 y se ejecutan.
"""
import os

import pytest

# "airflow.sdk" y no "airflow": la carpeta airflow/ del repo se importaría como namespace
pytest.importorskip("airflow.sdk")
from airflow.models.dagbag import DagBag  # noqa: E402

DAGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "airflow", "dags")
DAG_ID = "rapidgo_v6_preparacion"
TAREAS = [
    "verificar_fuentes",
    "validar_calidad_processed",
    "agregar_kpis_diarios",
    "consolidar_alertas",
    "validar_calidad_curated",
    "generar_reporte",
]


@pytest.fixture(scope="module")
def dag():
    dagbag = DagBag(dag_folder=DAGS, include_examples=False)
    assert dagbag.import_errors == {}, f"Errores al importar DAGs: {dagbag.import_errors}"
    assert DAG_ID in dagbag.dags, f"No se encontró el DAG {DAG_ID}; hay: {list(dagbag.dags)}"
    return dagbag.dags[DAG_ID]


def test_dag_tiene_al_menos_5_tareas(dag):
    assert len(dag.tasks) >= 5
    assert set(TAREAS) <= set(dag.task_ids)


def test_dependencias_del_pipeline_de_calidad(dag):
    t = dag.get_task
    assert t("verificar_fuentes").upstream_task_ids == set()
    assert t("validar_calidad_processed").upstream_task_ids == {"verificar_fuentes"}
    assert {"agregar_kpis_diarios", "consolidar_alertas"} <= t("validar_calidad_processed").downstream_task_ids
    assert t("validar_calidad_curated").upstream_task_ids == {"agregar_kpis_diarios", "consolidar_alertas"}
    assert "validar_calidad_curated" in t("generar_reporte").upstream_task_ids
    assert t("generar_reporte").downstream_task_ids == set()


def test_todas_las_tareas_tienen_retries(dag):
    for tarea in dag.tasks:
        assert tarea.retries >= 1, f"{tarea.task_id} no tiene retries"


def test_dag_manual_sin_catchup(dag):
    assert dag.catchup is False


def test_verificar_fuentes_espera_con_backoff_adr003(dag):
    t = dag.get_task("verificar_fuentes")
    assert t.retries == 4
    assert t.retry_exponential_backoff
    assert t.max_retry_delay is not None


def test_tareas_de_calidad_no_reintentan_de_mas_adr003(dag):
    for task_id in ["validar_calidad_processed", "validar_calidad_curated"]:
        assert dag.get_task(task_id).retries == 2
