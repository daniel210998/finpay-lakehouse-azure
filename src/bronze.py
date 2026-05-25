# =============================================================
# bronze.py — Capa Bronze del pipeline FinPay
# Ingesta raw de archivos desde la landing zone sin transformación.
# Implementa el patrón metadata-driven con arquetipos de ingesta.
# =============================================================

import dlt
from pyspark.sql import functions as F
from utils import load_archetypes, add_audit_columns
 
# -------------------------------------------------------------
# RUTA AL ARCHIVO DE ARQUETIPOS
# Se lee una sola vez al inicio del pipeline para orquestar
# dinámicamente cada ingesta según las propiedades del arquetipo.
# -------------------------------------------------------------
ARCHETYPES_PATH = (
    "/Volumes/fintech_finpay/default/vol_landing/metadata/"
    "ingestion_archetypes.json"
)
 
# -------------------------------------------------------------
# CARGA DE ARQUETIPOS AL INICIO DEL MÓDULO
# DLT ejecuta esto cuando carga el archivo — antes de crear
# las tablas. Genera un dict indexado por source_name para
# acceso rápido desde cada función de tabla.
# Solo se cargan los arquetipos activos (active: true).
# -------------------------------------------------------------
_archetypes = {
    a["source_name"]: a
    for a in load_archetypes(spark, ARCHETYPES_PATH)
}
 
 
# -------------------------------------------------------------
# FUNCIÓN GENÉRICA DE INGESTA (metadata-driven — Reto 1)
# Lee cualquier fuente según su arquetipo (CSV, JSON, TXT).
# Todas las propiedades vienen del JSON — ningún valor
# está hardcodeado en el código.
# -------------------------------------------------------------
def ingest_source(spark, archetype: dict):
    """
    Ingesta una fuente de datos según su arquetipo.
 
    Propiedades leídas del JSON:
        file_format      : csv, json, text
        source_path      : ruta al directorio en vol_landing
        delimiter        : separador de campos (CSV y TXT)
        header           : si el archivo tiene cabecera
        multiline        : si el JSON es multilinea
        schema_location  : ruta para persistir el schema inferido
        checkpoint_path  : ruta para el checkpoint de streaming
 
    Soporta:
        - CSV  : transactions (header, delimiter configurable)
        - JSON : merchants (multiline configurable)
        - text : users con delimitador pipe (|)
    """
    fmt         = archetype["file_format"]
    path        = archetype["source_path"]
    source_name = archetype["source_name"]
    schema_loc  = archetype.get(
        "schema_location",
        f"/Volumes/fintech_finpay/default/vol_landing/metadata/schema/{source_name}/"
    )
 
    if fmt == "csv":
        df = (
            spark.readStream
            .format("cloudFiles")                          # Auto Loader
            .option("cloudFiles.format", "csv")
            .option("header", str(archetype.get("header", True)))
            .option("delimiter", archetype.get("delimiter", ","))
            .option("inferSchema", "false")                # Bronze: todo como STRING
            .option("cloudFiles.schemaLocation", schema_loc)
            .load(path)
        )
 
    elif fmt == "json":
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", str(archetype.get("multiline", False)))
            .option("inferSchema", "false")
            .option("cloudFiles.schemaLocation", schema_loc)
            .load(path)
        )
 
    elif fmt == "text":
        # Archivos TXT con delimitador pipe — se leen como CSV con sep=|
        delimiter = archetype.get("delimiter", "|")
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", str(archetype.get("header", True)))
            .option("delimiter", delimiter)
            .option("inferSchema", "false")
            .option("cloudFiles.schemaLocation", schema_loc)
            .load(path)
        )
 
    else:
        raise ValueError(f"Formato no soportado en arquetipo: {fmt}")
 
    # Agregar columnas técnicas de auditoría requeridas en Bronze
    return add_audit_columns(df, source_name)
 
 
# =============================================================
# TABLAS BRONZE — STREAMING TABLES
# Las propiedades de cada fuente vienen del _archetypes dict
# cargado desde ingestion_archetypes.json al inicio del módulo.
# Si se modifica el JSON (ruta, formato, delimiter), el pipeline
# toma los cambios automáticamente sin tocar el código.
# =============================================================
 
@dlt.table(
    name="raw_transactions",
    comment="Bronze: ingesta raw de transactions — configuración desde ingestion_archetypes.json",
    table_properties={"quality": "bronze"}
)
def bronze_transactions():
    """
    Lee transactions_*.csv desde la landing zone.
    Configuración: formato CSV, header=true, delimiter=,
    Todo se mantiene como STRING — los tipos se castean en Silver.
    """
    return ingest_source(spark, _archetypes["transactions"])
 
 
@dlt.table(
    name="raw_merchants",
    comment="Bronze: ingesta raw de merchants — configuración desde ingestion_archetypes.json",
    table_properties={"quality": "bronze"}
)
def bronze_merchants():
    """
    Lee merchants.json desde la landing zone.
    Configuración: formato JSON, multiline=true
    Preserva todos los campos incluyendo los no documentados
    (internal_code, legacy_id, region, source_system).
    """
    return ingest_source(spark, _archetypes["merchants"])
 
 
@dlt.table(
    name="raw_users",
    comment="Bronze: ingesta raw de users — configuración desde ingestion_archetypes.json",
    table_properties={"quality": "bronze"}
)
def bronze_users():
    """
    Lee users_*.txt desde la landing zone.
    Configuración: formato text, delimiter=|, header=true
    Los campos PII se preservan sin transformación en Bronze.
    El masking se aplica en Silver a nivel de tabla Unity Catalog.
    """
    return ingest_source(spark, _archetypes["users"])
