#!/usr/bin/env python3
"""
cleaning_pipeline.py
Pipeline distribuido de limpieza sobre el dataset sucio de 300.000 filas.

- Lee los CSV particionados con dask.dataframe (lazy, out-of-core).
- Refactoriza raw_customer_code -> CUST-XXXXX via regex.
- Repara mojibake (UTF-8 mal-decodificado como Latin-1) en city_notes_corrupted.
- Usa map_partitions para correr la logica condicional compleja como Pandas
  puro por bloque (reduce overhead de serializacion Scheduler <-> Workers).
- Escribe el resultado final a Parquet (no a CSV: ver README, seccion de
  preguntas de analisis, pregunta 3).
"""
import os
import re

import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

RAW_DIR = "/shared-data/raw"
OUT_DIR = "/shared-data/processed"

# Extrae el numero base de 5 digitos sin importar el prefijo/formato de origen
CODE_PATTERN = re.compile(r"(?:CLI-|cli_|RAW#|CUST-)?(\d{5})")

ANOMALY_TOKEN = "CUST-00000-ANOMALY"


def extract_customer_code(value) -> str:
    """Mapea cualquier variante de raw_customer_code al estandar CUST-XXXXX."""
    if pd.isna(value):
        return ANOMALY_TOKEN
    match = CODE_PATTERN.search(str(value))
    if match:
        return f"CUST-{match.group(1)}"
    return ANOMALY_TOKEN


def fix_mojibake(value):
    """
    Repara texto UTF-8 que fue interpretado erroneamente como Latin-1
    (el error de encoding cruzado mas comun: 'BogotÃ¡' -> 'Bogotá').
    Si el texto ya esta bien o no es reparable, lo deja intacto.
    """
    if pd.isna(value):
        return value
    try:
        return value.encode("latin1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError, AttributeError):
        return value


def clean_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Se ejecuta una vez POR PARTICION, dentro de un worker, usando Pandas puro.
    Esto es lo que la guia llama 'patron de optimizacion en Dask' con
    map_partitions: evita mandar fila por fila al scheduler.
    """
    pdf = pdf.copy()
    pdf["customer_code"] = pdf["raw_customer_code"].apply(extract_customer_code)
    pdf["city_notes"] = pdf["city_notes_corrupted"].apply(fix_mojibake)
    return pdf


def run_pipeline(scheduler_address: str = "tcp://dask-scheduler:8786") -> dict:
    client = Client(scheduler_address)
    print("Conectado al cluster:")
    print(client)
    print(f"Workers activos: {list(client.scheduler_info()['workers'].keys())}\n")

    ddf = dd.read_csv(f"{RAW_DIR}/transactions_dirty_part_*.csv")
    print(f"Particiones leidas (lazy): {ddf.npartitions}")

    meta = ddf._meta.assign(customer_code="", city_notes="")
    ddf_clean = ddf.map_partitions(clean_partition, meta=meta)

    # --- Quality gate ---
    total = ddf_clean.shape[0].compute()
    n_anomalias = (ddf_clean["customer_code"] == ANOMALY_TOKEN).sum().compute()
    tasa = n_anomalias / total
    print(f"Filas totales:        {total:,}")
    print(f"Anomalias detectadas: {n_anomalias:,} ({tasa:.2%})")

    os.makedirs(OUT_DIR, exist_ok=True)
    ddf_clean.to_parquet(OUT_DIR, engine="pyarrow", write_index=False)
    print(f"[OK] Datos limpios escritos en {OUT_DIR}/ (formato Parquet)")

    client.close()
    return {"total_rows": int(total), "anomalies": int(n_anomalias), "anomaly_rate": tasa}


if __name__ == "__main__":
    resultado = run_pipeline()
    print("\nResumen:", resultado)
