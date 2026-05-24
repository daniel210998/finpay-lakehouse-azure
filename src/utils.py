# =============================================================
# utils.py — Funciones reutilizables compartidas por el pipeline
# FinPay Lakehouse · Azure Databricks · Arquitectura Medallion
# =============================================================

import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# -------------------------------------------------------------
# 1. LEER ARQUETIPOS DE INGESTA
# Lee el archivo ingestion_archetypes.json desde el volume y
# retorna solo los arquetipos activos (active = true).
# -------------------------------------------------------------
def load_archetypes(spark: SparkSession, metadata_path: str) -> list:
    """
    Lee ingestion_archetypes.json y retorna lista de arquetipos activos.

    Args:
        spark: SparkSession activa
        metadata_path: ruta al archivo JSON en el volume

    Returns:
        Lista de dicts con la configuración de cada fuente activa
    """
    raw = spark.read.text(metadata_path).collect()
    content = "\n".join([row[0] for row in raw])
    archetypes = json.loads(content)
    # Filtrar solo fuentes activas
    return [a for a in archetypes if a.get("active", False)]


# -------------------------------------------------------------
# 2. COLUMNAS TÉCNICAS DE AUDITORÍA (capa Bronze)
# Agrega metadatos de trazabilidad a cada registro ingestado.
# -------------------------------------------------------------
def add_audit_columns(df, source_name: str):
    """
    Agrega columnas técnicas de auditoría requeridas en Bronze.

    Columnas agregadas:
        _source_name     : nombre de la fuente (ej: transactions)
        _ingestion_time  : timestamp de cuando se procesó el registro
        _file_path       : ruta del archivo de origen (si está disponible)
    """
    return (
        df
        .withColumn("_source_name", F.lit(source_name))
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn(
            "_file_path",
            F.col("_metadata.file_path")  # ruta real del archivo procesado
        )
    )


# -------------------------------------------------------------
# 3. NORMALIZAR FECHAS
# Las fuentes mezclan formatos yyyy-MM-dd y dd/MM/yyyy.
# Esta función intenta ambos formatos y retorna DATE o null.
# -------------------------------------------------------------
def normalize_date(col_name: str):
    """
    Intenta parsear una columna de fecha con múltiples formatos.
    Retorna columna tipo DATE. Si no puede parsear, retorna null.

    Formatos soportados:
        - yyyy-MM-dd  (ISO estándar)
        - dd/MM/yyyy  (formato latinoamericano)
        - MM/dd/yyyy  (formato norteamericano)
    """
    return F.coalesce(
        F.to_date(F.col(col_name), "yyyy-MM-dd"),
        F.to_date(F.col(col_name), "dd/MM/yyyy"),
        F.to_date(F.col(col_name), "MM/dd/yyyy"),
    )


# -------------------------------------------------------------
# 4. NORMALIZAR MONTO
# Los montos llegan como STRING con posibles formatos sucios:
# comas como separador de miles, espacios, signos negativos.
# Retorna columna DOUBLE limpia.
# -------------------------------------------------------------
def normalize_amount(col_name: str):
    """
    Limpia y convierte una columna de monto a DOUBLE.

    Transformaciones:
        - Elimina espacios en blanco
        - Elimina comas de miles (1,250.50 → 1250.50)
        - Castea a DOUBLE
        - Si el resultado es null o negativo, lo marca como null
    """
    cleaned = F.regexp_replace(
        F.trim(F.col(col_name)),
        ",",   # eliminar separador de miles
        ""
    )
    amount = cleaned.cast("double")
    # Montos negativos o nulos son inválidos
    return F.when(amount > 0, amount).otherwise(F.lit(None).cast("double"))


# -------------------------------------------------------------
# 5. NORMALIZAR MERCHANT_ID
# Los merchant_id llegan con formatos inconsistentes:
# MCH00892 (sin guión) o MCH-00892 (con guión).
# Estandariza al formato MCH-NNNNN.
# -------------------------------------------------------------
def normalize_merchant_id(col_name: str):
    """
    Estandariza merchant_id al formato MCH-NNNNN.

    Ejemplos:
        MCH00892  → MCH-00892
        MCH-00892 → MCH-00892 (sin cambio)
    """
    return F.regexp_replace(
        F.upper(F.trim(F.col(col_name))),
        r"^MCH(\d+)$",   # si no tiene guión, lo agrega
        "MCH-$1"
    )


# -------------------------------------------------------------
# 6. NORMALIZAR COUNTRY CODE
# Algunos registros traen el nombre completo del país en lugar
# del código ISO de 2 letras (ej: "Peru" → "PE").
# -------------------------------------------------------------
COUNTRY_MAP = {
    "peru": "PE",
    "perú": "PE",
    "colombia": "CO",
    "mexico": "MX",
    "méxico": "PE",
    "chile": "CL",
    "argentina": "AR",
}


def normalize_country(col_name: str):
    """
    Convierte nombres de países a códigos ISO de 2 letras.
    Si ya es un código válido (PE, CO, MX, CL, AR), lo devuelve tal cual.
    """
    # Construir expresión CASE dinámica
    expr = F.when(
        F.upper(F.trim(F.col(col_name))).isin(["PE", "CO", "MX", "CL", "AR"]),
        F.upper(F.trim(F.col(col_name)))
    )
    for name, code in COUNTRY_MAP.items():
        expr = expr.when(
            F.lower(F.trim(F.col(col_name))) == name,
            F.lit(code)
        )
    return expr.otherwise(F.upper(F.trim(F.col(col_name))))


# -------------------------------------------------------------
# 7. NORMALIZAR STATUS
# Los status llegan en distintos idiomas y capitalización.
# Estandariza a los valores definidos en el enunciado.
# -------------------------------------------------------------
def normalize_status_merchant(col_name: str):
    """
    Estandariza el status de merchants.
    Valores válidos: activo, inactivo, suspendido
    """
    col = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(col.isin(["activo", "active"]), F.lit("activo"))
        .when(col.isin(["inactivo", "inactive"]), F.lit("inactivo"))
        .when(col.isin(["suspendido", "suspended"]), F.lit("suspendido"))
        .otherwise(F.lit(None).cast(StringType()))
    )


# -------------------------------------------------------------
# 8. CONSTRUIR REGISTRO DE RECHAZO (tabla de cuarentena)
# Empaqueta un registro rechazado con metadatos del error.
# Usado en Silver para poblar silver.quarantine.
# -------------------------------------------------------------
def build_quarantine_record(
    df,
    source_name: str,
    reject_reason: str
):
    """
    Prepara un DataFrame para insertar en la tabla de cuarentena.

    Columnas que agrega:
        _source_name    : nombre de la fuente de origen
        _reject_reason  : motivo del rechazo
        _processed_at   : timestamp del procesamiento
        _raw_record     : contenido original del registro como STRING
    """
    return (
        df
        .withColumn("_source_name", F.lit(source_name))
        .withColumn("_reject_reason", F.lit(reject_reason))
        .withColumn("_processed_at", F.current_timestamp())
        # Serializar toda la fila como string JSON para trazabilidad
        .withColumn(
            "_raw_record",
            F.to_json(F.struct([F.col(c) for c in df.columns]))
        )
        # Seleccionar solo las columnas de cuarentena
        .select(
            "_source_name",
            "_reject_reason",
            "_processed_at",
            "_raw_record"
        )
    )
