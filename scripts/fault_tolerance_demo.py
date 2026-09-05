#!/usr/bin/env python3
"""
fault_tolerance_demo.py
Demo para la pregunta de analisis #2 (tolerancia a fallos y recomputacion).

Corre las mismas 6 particiones que cleaning_pipeline.py, pero le mete una
demora artificial a cada una para tener una ventana de varios segundos en
la que puedas matar un worker a mitad de camino:

    docker compose exec runner python scripts/fault_tolerance_demo.py

...y en otra terminal, unos segundos despues de que arranque:

    docker stop dask-worker-2

Mira la salida: vas a ver 6 lineas "partition done" (una por particion),
cada una con el hostname del worker que la proceso. Si matas
dask-worker-2 a la mitad, las particiones que le faltaban por hacer
(incluida la que estaba corriendo ahi mismo) van a terminar reportando
dask-worker-1 o dask-worker-3 -- el scheduler las reasigna solo.
"""
import socket
import time

import pandas as pd
from dask.distributed import Client

from cleaning_pipeline import extract_customer_code, fix_mojibake, RAW_DIR

SLEEP_PER_PARTITION = 10  # segundos "extra" de trabajo simulado por particion


def clean_partition_lento(pdf: pd.DataFrame) -> pd.DataFrame:
    """Igual que clean_partition, pero con sleep para poder matar un worker
    a mitad de camino y observar la reasignacion."""
    time.sleep(SLEEP_PER_PARTITION)
    pdf = pdf.copy()
    pdf["customer_code"] = pdf["raw_customer_code"].apply(extract_customer_code)
    pdf["city_notes"] = pdf["city_notes_corrupted"].apply(fix_mojibake)
    pdf["_worker"] = socket.gethostname()
    return pdf


def main(scheduler_address: str = "tcp://dask-scheduler:8786"):
    import dask.dataframe as dd

    client = Client(scheduler_address)
    print(client)
    print(f"Workers activos ahora: {list(client.scheduler_info()['workers'].keys())}")
    print(f"\n[*] Arrancando... cada particion tarda ~{SLEEP_PER_PARTITION}s. "
          f"Tienes esa ventana para hacer 'docker stop dask-worker-2' en otra terminal.\n")

    ddf = dd.read_csv(f"{RAW_DIR}/transactions_dirty_part_*.csv")
    meta = ddf._meta.assign(customer_code="", city_notes="", _worker="")

    t0 = time.time()
    ddf_out = ddf.map_partitions(clean_partition_lento, meta=meta)
    resultado = ddf_out[["transaction_id", "_worker"]].compute()
    elapsed = time.time() - t0

    print(f"\n[OK] Terminado en {elapsed:.1f}s pese a lo que haya pasado con los workers.")
    print("\nFilas procesadas por worker (contenedor):")
    print(resultado["_worker"].value_counts())

    client.close()


if __name__ == "__main__":
    main()
