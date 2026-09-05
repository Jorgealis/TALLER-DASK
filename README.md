# Taller: Computación Distribuida con Dask, Docker & Prefect

Implementación del laboratorio "Procesamiento Out-of-Core en Clúster Docker"
(Patrones Arquitectónicos Avanzados / Sistemas Distribuidos y Big Data).

## Topología

| Contenedor       | Rol                  | Puertos            | Recursos           |
|------------------|----------------------|---------------------|---------------------|
| dask-scheduler   | Master / Coordinador | 8786 (IPC), 8787 (Dashboard) | 1 vCPU, 1 GB RAM |
| dask-worker-1/2/3| Worker Node          | interno (red Docker)| 2 threads, 1.5 GB RAM c/u |
| runner           | Cliente / orquestador| interno             | -                    |

Todos comparten la red `dask-cluster-net` y el volumen `./shared-data`
(montado en `/shared-data` en cada contenedor), que emula el Data
Lake / sistema de archivos distribuido.

## Estructura del proyecto

```
taller-dask-docker/
├── docker-compose.yml
├── docker/
│   ├── Dockerfile                 # UNA sola imagen para scheduler+workers+runner
│   └── requirements.txt           # versiones pineadas (dask, distributed, pandas, ...)
├── shared-data/
│   ├── raw/                       # CSV sucios generados (6 particiones)
│   └── processed/                 # salida limpia en Parquet
├── scripts/
│   ├── generate_dirty_data.py     # genera las 300.000 filas con ruido
│   └── cleaning_pipeline.py       # limpieza distribuida con dask.dataframe
├── orchestration/
│   └── prefect_flow.py            # @flow/@task: valida, ejecuta, evalúa calidad
└── README.md
```

**Por qué una sola imagen para todo:** si el scheduler/workers usan una
imagen genérica (ej. `ghcr.io/dask/dask:latest`) y el cliente instala Dask
por su cuenta con otra versión, `distributed` puede romper la conexión a
mitad de un cómputo (`VersionMismatchWarning` -> `FutureCancelledError:
scheduler-connection-lost`). Aquí los 5 contenedores corren la misma
imagen (`docker/Dockerfile`), así que dask/distributed/pandas/numpy/python
son idénticos en cliente y clúster.

## 1. Levantar el clúster

Desde PowerShell, en esta carpeta:

```powershell
docker compose up --build -d
```

(el primer `--build` compila la imagen `docker/Dockerfile` una sola vez y
la reutiliza en scheduler, los 3 workers y el runner; si luego cambias
`docker/requirements.txt`, vuelve a correr `--build`).

Verifica que los 5 contenedores estén arriba:

```powershell
docker compose ps
```

Dashboard de Dask (para ver workers, tareas y memoria en vivo):
http://localhost:8787

UI de Prefect (historial de flow runs, tareas y logs):
http://localhost:4200

## Capturas para la entrega

1. Deja el clúster arriba y corre el flujo completo:
   ```powershell
   docker compose exec runner python orchestration/prefect_flow.py
   ```
2. Mientras corre (o justo después), abre http://localhost:8787 en el navegador
   y captura la pestaña "Workers" o "Task Stream" -> esa es la captura de Dask.
3. Abre http://localhost:4200, entra a "Flow Runs" y abre la corrida más
   reciente de "pipeline-limpieza-distribuida" -> ahí ves las 4 tareas
   (validar-infraestructura, verificar-datos-crudos, ejecutar-pipeline-dask,
   quality-gate) todas en verde (Completed), con sus logs -> esa es la
   captura de Prefect.

## 2. Generar el dataset sucio (300.000 filas)

```powershell
docker compose exec runner python scripts/generate_dirty_data.py
```

Esto escribe 6 archivos CSV (~50.000 filas c/u) en `shared-data/raw/`,
visibles también desde tu explorador de Windows en
`Documents\taller-dask-docker\shared-data\raw`.

## 3. Correr el pipeline de limpieza (solo Dask, sin Prefect)

Útil para probar rápido la lógica de limpieza:

```powershell
docker compose exec runner python scripts/cleaning_pipeline.py
```

## 4. Correr el flujo completo orquestado con Prefect

```powershell
docker compose exec runner python orchestration/prefect_flow.py
```

Esto encadena: validar que el scheduler responde -> verificar que existan
los CSV crudos -> correr el pipeline distribuido -> validar que la tasa de
anomalías (`customer_code` no resuelto) esté bajo el umbral. Si algo falla,
Prefect lo marca como `Failed` y reintenta la validación de infraestructura
automáticamente.

El resultado limpio queda en `shared-data/processed/` como archivos
`.parquet`.

## 5. Apagar todo

```powershell
docker compose down
```

(agrega `-v` si además quieres borrar el volumen nombrado, aunque aquí
`shared-data` es un bind mount a tu carpeta, así que los datos persisten
en disco de todas formas).

## Guía para las preguntas de análisis (sección 6 de la guía)

No son preguntas para responder "de memoria" — la idea es que generes la
evidencia tú mismo con este mismo clúster:

- **Cuellos de botella red vs. CPU**: compara el tiempo de
  `cleaning_pipeline.py` corriendo con 3 workers vs. corriendo con 1 solo
  (`docker compose up -d --scale dask-worker-2=0 --scale dask-worker-3=0`)
  y mira en el dashboard (pestaña "Task Stream") cuánto tiempo se va en
  transferencia vs. cómputo puro.
- **Tolerancia a fallos**: con el pipeline corriendo, ejecuta
  `docker stop dask-worker-2` a la mitad. Mira el dashboard y los logs del
  scheduler (`docker compose logs dask-scheduler`) para ver cómo reasigna
  las particiones pendientes a `dask-worker-1` y `dask-worker-3`.
- **Parquet vs. CSV**: compara `du -sh shared-data/processed` (Parquet)
  contra el tamaño que tendría el mismo dataset consolidado en un solo CSV,
  y prueba leer solo una columna con `dd.read_parquet(..., columns=[...])`
  para ver el predicate/column pushdown en acción.
- **Prefect vs. Dask**: fíjate qué te da Prefect que Dask solo no te da en
  este proyecto — reintentos automáticos en `validar_infraestructura`,
  logs estructurados por tarea, y que el flujo falle "limpio" (quality
  gate) en vez de escribir un Parquet a medias.

## Rúbrica (referencia rápida)

| Criterio                          | Peso | Dónde está en este repo |
|-----------------------------------|------|--------------------------|
| Infraestructura Docker y Clúster  | 20%  | `docker-compose.yml` |
| Script generador y datos sucios   | 20%  | `scripts/generate_dirty_data.py` |
| Refactorización y limpieza Dask   | 35%  | `scripts/cleaning_pipeline.py` |
| Orquestación Prefect y análisis   | 25%  | `orchestration/prefect_flow.py` + respuestas del informe |
