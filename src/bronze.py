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
# dinámicamente la ingesta de cada fuente.
# -------------------------------------------------------------
ARCHETYPES_PATH = (
    "/Volumes/fintech_finpay/default/vol_landing/metadata/"
    "ingestion_archetypes.json"
)


# -------------------------------------------------------------
# FUNCIÓN GENÉRICA DE INGESTA
# Lee cualquier fuente según su arquetipo (CSV, JSON, TXT).
# Agrega columnas técnicas de auditoría y retorna el DataFrame.
# -------------------------------------------------------------
def ingest_source(spark, archetype: dict):
    """
    Ingesta una fuente de datos según su arquetipo.

    Soporta:
        - CSV  : con header y delimitador configurables
        - JSON : con soporte multiline
        - text : archivos TXT con delimitador pipe (|)
    """
    fmt         = archetype["file_format"]
    path        = archetype["source_path"]
    source_name = archetype["source_name"]

    if fmt == "csv":
        df = (
            spark.readStream
            .format("cloudFiles")                          # Auto Loader para streaming incremental
            .option("cloudFiles.format", "csv")
            .option("header", str(archetype.get("header", True)))
            .option("delimiter", archetype.get("delimiter", ","))
            .option("inferSchema", "false")                # Bronze lee todo como STRING
            .option("cloudFiles.schemaLocation",
                    archetype.get("schema_location", f"{path}_schema"))
            .load(path)
        )

    elif fmt == "json":
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", str(archetype.get("multiline", False)))
            .option("inferSchema", "false")
            .option("cloudFiles.schemaLocation",
                    archetype.get("schema_location", f"{path}_schema"))
            .load(path)
        )

    elif fmt == "text":
        # Los archivos TXT con pipe se leen primero como texto
        # y luego se parsean dividiendo por el delimitador
        delimiter = archetype.get("delimiter", "|")
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("cloudFiles.schemaLocation",
                    archetype.get("schema_location", f"{path}_schema"))
            .load(path)
        )
        # Leer cabecera para obtener nombres de columnas
        header_df = (
            spark.read
            .text(path)
            .limit(1)
            .collect()
        )
        if header_df:
            columns = header_df[0][0].split(delimiter)
            # Dividir cada línea por el delimitador y crear columnas
            split_col = F.split(F.col("value"), r"\|")
            for i, col_name in enumerate(columns):
                df = df.withColumn(col_name.strip(), split_col.getItem(i))
            df = df.drop("value")

    else:
        raise ValueError(f"Formato no soportado: {fmt}")

    # Agregar columnas técnicas de auditoría
    return add_audit_columns(df, source_name)


# -------------------------------------------------------------
# GENERACIÓN DINÁMICA DE TABLAS BRONZE
# Itera sobre los arquetipos activos y crea una Streaming Table
# por cada fuente usando el decorador @dlt.table.
# -------------------------------------------------------------
def create_bronze_tables(spark):
    """
    Crea dinámicamente las Streaming Tables de Bronze
    basándose en los arquetipos activos del JSON de configuración.
    """
    archetypes = load_archetypes(spark, ARCHETYPES_PATH)

    for archetype in archetypes:
        source_name  = archetype["source_name"]
        target_table = archetype["target_table"]
        checkpoint   = archetype.get(
            "checkpoint_path",
            f"/Volumes/fintech_finpay/default/vol_landing/metadata/checkpoints/{source_name}/"
        )

        # Crear la tabla Bronze con decorador DLT
        @dlt.table(
            name=target_table.split(".")[-1],           # ej: "transactions"
            comment=f"Bronze: ingesta raw de {source_name} sin transformación",
            table_properties={
                "quality": "bronze",
                "pipelines.reset.allowed": "true"
            }
        )
        def bronze_table(archetype=archetype):          # closure para capturar el arquetipo
            return ingest_source(spark, archetype)


# -------------------------------------------------------------
# TABLAS BRONZE EXPLÍCITAS
# Además de la generación dinámica, se definen explícitamente
# las tres tablas para mayor claridad y control.
# -------------------------------------------------------------

@dlt.table(
    name="raw_transactions",
    comment="Bronze: ingesta raw de transactions desde CSV sin transformación",
    table_properties={"quality": "bronze"}
)
def bronze_transactions():
    """
    Lee los archivos transactions_*.csv desde la landing zone.
    Todos los campos se mantienen como STRING para preservar
    el dato original. Las transformaciones se hacen en Silver.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "false")           # todo como STRING en Bronze
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/fintech_finpay/default/vol_landing/metadata/schema/transactions/"
        )
        .load("/Volumes/fintech_finpay/default/vol_landing/transactions/")
        .withColumn("_source_name", F.lit("transactions"))
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_file_path", F.input_file_name())
    )


@dlt.table(
    name="raw_merchants",
    comment="Bronze: ingesta raw de merchants desde JSON sin transformación",
    table_properties={"quality": "bronze"}
)
def bronze_merchants():
    """
    Lee el archivo merchants.json desde la landing zone.
    Preserva todos los campos extra no documentados en el enunciado
    (internal_code, legacy_id, region, source_system) para trazabilidad.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("inferSchema", "false")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/fintech_finpay/default/vol_landing/metadata/schema/merchants/"
        )
        .load("/Volumes/fintech_finpay/default/vol_landing/merchants/")
        .withColumn("_source_name", F.lit("merchants"))
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_file_path", F.input_file_name())
    )


@dlt.table(
    name="raw_users",
    comment="Bronze: ingesta raw de users desde TXT con delimitador pipe",
    table_properties={"quality": "bronze"}
)
def bronze_users():
    """
    Lee los archivos users_*.txt desde la landing zone.
    El delimitador es pipe (|). Se leen como CSV con sep=|.
    Los campos PII se preservan sin transformación en Bronze;
    el masking se aplica en Silver a nivel de tabla.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", "|")
        .option("inferSchema", "false")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/fintech_finpay/default/vol_landing/metadata/schema/users/"
        )
        .load("/Volumes/fintech_finpay/default/vol_landing/users/")
        .withColumn("_source_name", F.lit("users"))
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_file_path", F.input_file_name())
    )
