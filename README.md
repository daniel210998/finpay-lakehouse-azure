# FinPay Lakehouse — Plataforma de Detección de Fraude Transaccional

## Descripción del caso de uso

FinPay es una fintech latinoamericana que procesa pagos digitales entre comercios afiliados y usuarios finales. Opera en cinco países (Perú, Colombia, México, Chile y Argentina) y maneja tres tipos de transacciones: pagos, reversas y retiros.

Este proyecto construye una plataforma de datos sobre Databricks que resuelve el problema de visibilidad sobre patrones de fraude mediante un pipeline automatizado de ingesta, procesamiento y publicación analítica, con un modelo dimensional que permite al área de riesgo detectar patrones de fraude en tiempo casi real.

## Arquitectura

El proyecto implementa la arquitectura Medallion completa con tres capas:

- **Bronze**: ingesta raw de archivos fuente sin transformación, con columnas técnicas de auditoría (`_source_name`, `_ingestion_time`, `_file_path`). Usa Auto Loader (`cloudFiles`) para detección incremental de archivos nuevos.
- **Silver**: limpieza, estandarización, deduplicación y enriquecimiento. Aplica reglas de calidad con `@dlt.expect` y `@dlt.expect_or_drop`. Los registros rechazados se persisten en la tabla de cuarentena `fintech_finpay.default.quarantine`.
- **Gold**: KPIs de riesgo, tasas de reversa y detección de anomalías por comercio y canal.

Todo el pipeline se implementa con Lakeflow Declarative Pipelines usando Streaming Tables con `trigger=availableNow` — esto significa que el pipeline procesa todos los archivos disponibles en la landing zone y se detiene automáticamente al terminar (modo batch).

Los event logs del pipeline están habilitados y persistidos en `fintech_finpay.observability.finpay_event_log_prod`, que es la fuente del dashboard de observabilidad.

## Estructura del repositorio

```
finpay-lakehouse-azure/
├── databricks.yml                          # Configuración raíz del DAB
├── resources/
│   ├── finpay_etl_pipeline.yml             # Lakeflow Declarative Pipeline Bronze→Silver→Gold
│   ├── finpay_ingestion_job.yml            # Job 1: orquesta el pipeline ETL
│   ├── finpay_semantic_job.yml             # Job 2: refresca vistas materializadas en SQL Warehouse
│   └── finpay_observability_dashboard.yml  # Dashboard AI/BI de observabilidad
├── src/
│   ├── utils.py                            # Funciones reutilizables compartidas
│   ├── bronze.py                           # Lógica de ingesta Bronze con @dlt.table
│   ├── silver.py                           # Lógica de transformación Silver con @dlt.expect
│   └── gold.py                             # Lógica de agregación Gold
├── notebooks/
│   ├── 00_setup.ipynb                      # Aprovisionamiento inicial (ejecutar una vez)
│   ├── 01_create_materialized_views.ipynb  # Modelo dimensional (ejecutar una vez desde SQL Warehouse)
│   ├── 02_refresh_materialized_views.sql   # Refresco de vistas materializadas (ejecutado por Job 2)
│   └── 03_observability_queries.ipynb      # Consultas de validación sobre event logs de observability
├── dashboard/
│   └── observability.lvdash.json          # Dashboard AI/BI exportado
└── README.md
```

## Unity Catalog

- **Catálogo producción**: `fintech_finpay`
- **Catálogo desarrollo**: `fintech_finpay_dev`
- **Schemas**: `default`, `bronze`, `silver`, `gold`, `observability`
- **Landing zone**: `/Volumes/fintech_finpay/default/vol_landing/`
- **Event logs**: `fintech_finpay.observability.finpay_event_log_prod`

### Roles y permisos

| Rol | Permisos |
|-----|----------|
| ingenieria | USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY en todos los schemas |
| riesgo | USE CATALOG, USE SCHEMA, SELECT en silver y gold |
| auditoria | USE CATALOG, USE SCHEMA, SELECT en gold y observability |

Column masking y Row-Level Security aplicados en `silver.users` sobre campos PII (`full_name`, `document_id`, `email`, `phone`). Solo el rol `ingenieria` ve los valores reales.

## Instrucciones de despliegue

### Prerequisitos

- Python 3.12+
- Git 2.54+
- Databricks CLI v0.299+
- VS Code con extensión Databricks
- SQL Warehouse activo en el workspace (necesario para el modelo dimensional y Job 2)

### Paso 1 — Clonar el repositorio

```bash
git clone https://github.com/daniel210998/finpay-lakehouse-azure.git
cd finpay-lakehouse-azure
```

### Paso 2 — Configurar el CLI

```bash
databricks configure
# Host: https://dbc-705cf5fe-d80f.cloud.databricks.com
# Token: personal access token generado en Databricks
```

### Paso 3 — Ejecutar el notebook de setup

Ejecutar `notebooks/00_setup.ipynb` directamente en Databricks una sola vez. Crea el catálogo, schemas, volume, subdirectorios, roles, permisos, column masking y RLS.

### Paso 4 — Subir datos a la landing zone

Subir los archivos fuente al volume:
- `transactions_*.csv` → `/Volumes/fintech_finpay/default/vol_landing/transactions/`
- `merchants.json` → `/Volumes/fintech_finpay/default/vol_landing/merchants/`
- `users_*.txt` → `/Volumes/fintech_finpay/default/vol_landing/users/`
- `ingestion_archetypes.json` → `/Volumes/fintech_finpay/default/vol_landing/metadata/`

### Paso 5 — Validar y desplegar

```bash
# Validar el bundle
databricks bundle validate

# Desplegar en desarrollo
databricks bundle deploy --target dev

# Desplegar en producción
databricks bundle deploy --target prod
```

### Paso 6 — Ejecutar los jobs

```bash
# Job 1: Pipeline ETL Bronze → Silver → Gold
databricks bundle run finpay_ingestion_job --target prod

# Job 2: Refrescar modelo dimensional (después de Job 1)
databricks bundle run finpay_semantic_job --target prod
```

### Paso 7 — Crear el modelo dimensional

Ejecutar `notebooks/01_create_materialized_views.ipynb` una sola vez desde un SQL Warehouse para crear las 5 vistas materializadas del modelo dimensional (`fact_transactions`, `dim_merchant`, `dim_user`, `dim_channel`, `dim_date`). Este notebook debe ejecutarse antes de correr Job 2 por primera vez.

### Paso 8 — Validar event logs y observabilidad

Ejecutar `notebooks/03_observability_queries.ipynb` para validar los event logs del pipeline en `fintech_finpay.observability`. Contiene consultas para verificar registros procesados por capa, registros fallidos por expectativa de calidad e historial de ejecuciones.

## Retos técnicos implementados

### Reto 1 — Metadata-driven con arquetipos de ingesta

El pipeline lee `ingestion_archetypes.json` desde `vol_landing/metadata/` al inicio del módulo `bronze.py`. La función `ingest_source()` recibe el arquetipo de cada fuente y configura dinámicamente el formato, delimitador, header, ruta y schema_location sin ningún valor hardcodeado en el código.

Si se agrega una nueva fuente al JSON con `active: true`, el pipeline la procesa automáticamente sin modificar ni redesplegar el código. El campo `active: false` permite desactivar una fuente sin eliminarla del catálogo.

### Reto 2 — Tabla de cuarentena

Los registros que no superan las reglas de calidad críticas en Silver se persisten en `fintech_finpay.default.quarantine` con:
- `_source_name`: fuente de origen (transactions, merchants, users)
- `_reject_reason`: motivo del rechazo (campo crítico nulo, monto inválido, etc.)
- `_processed_at`: timestamp del procesamiento
- `_raw_record`: contenido original del registro como STRING JSON para trazabilidad completa

Esto permite al equipo de ingeniería auditar los rechazos, corregir los datos en la fuente y reprocesarlos sin perder información.

## Entregable opcional

Delta Sharing sobre las tablas Gold para exponer datos al equipo de auditoría en una cuenta Databricks externa no fue implementado en esta entrega.
