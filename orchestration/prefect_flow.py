#!/usr/bin/env python3
"""
prefect_flow.py
Orquesta el ciclo de vida completo del laboratorio con Prefect:

1. Valida que la infraestructura (scheduler Dask) este arriba.
2. Verifica que existan los datos crudos generados.
3. Ejecuta el pipeline de limpieza distribuido en el cluster de Dask.
4. Corre un quality gate sobre la tasa de anomalias del resultado.

Correr dentro del contenedor 'runner':
    python orchestration/prefect_flow.py
"""
import socket
import sys
from pathlib import Path

sys.path.insert(0, "/app/scripts")

from prefect import flow, task, get_run_logger  # noqa: E402
from cleaning_pipeline import run_pipeline, RAW_DIR  # noqa: E402

SCHEDULER_HOST = "dask-scheduler"
SCHEDULER_PORT = 8786
SCHEDULER_ADDRESS = f"tcp://{SCHEDULER_HOST}:{SCHEDULER_PORT}"

# Umbral maximo aceptable de filas irrecuperables (customer_code no resuelto)
MAX_ANOMALY_RATE = 0.10


@task(retries=3, retry_delay_seconds=5, name="validar-infraestructura")
def validar_infraestructura():
    """Falla (y reintenta) si el scheduler de Dask todavia no responde."""
    logger = get_run_logger()
    with socket.create_connection((SCHEDULER_HOST, SCHEDULER_PORT), timeout=5):
        logger.info(f"Scheduler accesible en {SCHEDULER_ADDRESS}")
    return True


@task(name="verificar-datos-crudos")
def verificar_datos_crudos():
    logger = get_run_logger()
    raw_path = Path(RAW_DIR)
    archivos = sorted(raw_path.glob("transactions_dirty_part_*.csv"))
    if not archivos:
        raise FileNotFoundError(
            f"No hay particiones en {RAW_DIR}. "
            "Corre antes: python scripts/generate_dirty_data.py"
        )
    logger.info(f"{len(archivos)} particiones crudas encontradas")
    return len(archivos)


@task(name="ejecutar-pipeline-dask")
def ejecutar_pipeline_dask():
    return run_pipeline(SCHEDULER_ADDRESS)


@task(name="quality-gate")
def validar_calidad(resultado: dict):
    logger = get_run_logger()
    tasa = resultado["anomaly_rate"]
    logger.info(f"Tasa de anomalias: {tasa:.2%} (umbral: {MAX_ANOMALY_RATE:.0%})")
    assert tasa < MAX_ANOMALY_RATE, (
        f"Quality gate fallido: {tasa:.2%} de filas anomalas supera el "
        f"umbral de {MAX_ANOMALY_RATE:.0%}"
    )
    return tasa


@flow(name="pipeline-limpieza-distribuida")
def pipeline_completo():
    validar_infraestructura()
    n_particiones = verificar_datos_crudos()
    print(f"Particiones crudas: {n_particiones}")
    resultado = ejecutar_pipeline_dask()
    validar_calidad(resultado)
    print("\n[OK] Flujo completo terminado correctamente.")


if __name__ == "__main__":
    pipeline_completo()
