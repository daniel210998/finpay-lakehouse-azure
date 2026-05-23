# =============================================================
# silver.py — Capa Silver del pipeline FinPay
# Limpieza, estandarización, deduplicación y enriquecimiento.
# Aplica reglas de calidad con @dlt.expect y tabla de cuarentena.
# =============================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType
from utils import (
    normalize_date,
    normalize_amount,
    normalize_merchant_id,
    normalize_country,
    normalize_status_merchant,
    build_quarantine_record,
)


# =============================================================
# SILVER: TRANSACTIONS
# =============================================================

@dlt.table(
    name="transactions",
    comment="Silver: transactions limpias, deduplicadas y con tipos correctos",
    table_properties={"quality": "silver"}
)
@dlt.expect("transaction_id no nulo",       "transaction_id IS NOT NULL")
@dlt.expect("user_id no nulo",              "user_id IS NOT NULL")
@dlt.expect("merchant_id no nulo",          "merchant_id IS NOT NULL")
@dlt.expect("amount positivo",              "amount > 0")
@dlt.expect("transaction_date no nula",     "transaction_date IS NOT NULL")
@dlt.expect_or_drop(
    "transaction_type válido",
    "transaction_type IN ('pago', 'reversa', 'retiro')"
)
@dlt.expect_or_drop(
    "channel válido",
    "channel IN ('web', 'app', 'pos')"
)
@dlt.expect_or_drop(
    "currency válida",
    "currency IN ('PEN', 'USD', 'COP', 'MXN', 'CLP', 'ARS')"
)
@dlt.expect_or_drop(
    "reversa con reference_id",
    "NOT (transaction_type = 'reversa' AND reference_id IS NULL)"
)
def silver_transactions():
    """
    Transformaciones aplicadas:
        - Cast de amount STRING → DOUBLE, limpiando formatos sucios
        - Normalización de transaction_date (dd/MM/yyyy y yyyy-MM-dd)
        - Estandarización de merchant_id al formato MCH-NNNNN
        - Normalización de channel a minúsculas (web, app, pos)
        - Deduplicación por transaction_id manteniendo el más reciente
        - Filtrado de registros con campos críticos nulos
    """
    df = dlt.read_stream("raw_transactions")

    return (
        df
        # ── Limpiar y castear amount ──────────────────────────
        .withColumn("amount", normalize_amount("amount"))

        # ── Normalizar fecha ──────────────────────────────────
        .withColumn("transaction_date", normalize_date("transaction_date"))

        # ── Estandarizar merchant_id ──────────────────────────
        .withColumn("merchant_id", normalize_merchant_id("merchant_id"))

        # ── Normalizar channel a minúsculas ───────────────────
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))

        # ── Normalizar transaction_type a minúsculas ──────────
        .withColumn(
            "transaction_type",
            F.lower(F.trim(F.col("transaction_type")))
        )

        # ── Normalizar status a minúsculas ────────────────────
        .withColumn("status", F.lower(F.trim(F.col("status"))))

        # ── Normalizar currency a mayúsculas ──────────────────
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))

        # ── Deduplicar por transaction_id ─────────────────────
        # En streaming usamos dropDuplicatesWithinWatermark
        .withWatermark("_ingestion_time", "1 hour")
        .dropDuplicates(["transaction_id"])

        # ── Seleccionar columnas finales ──────────────────────
        .select(
            "transaction_id",
            "user_id",
            "merchant_id",
            "channel",
            "transaction_type",
            "amount",
            "currency",
            "transaction_date",
            "status",
            "reference_id",
            "_source_name",
            "_ingestion_time",
            "_file_path",
        )
    )


# =============================================================
# SILVER: MERCHANTS
# =============================================================

@dlt.table(
    name="merchants",
    comment="Silver: merchants limpios con categorías y status estandarizados",
    table_properties={"quality": "silver"}
)
@dlt.expect("merchant_id no nulo",   "merchant_id IS NOT NULL")
@dlt.expect("merchant_name no nulo", "merchant_name IS NOT NULL")
@dlt.expect_or_drop(
    "country válido",
    "country IN ('PE', 'CO', 'MX', 'CL', 'AR')"
)
@dlt.expect_or_drop(
    "category válida",
    """category IN (
        'retail', 'restaurante', 'farmacia', 'supermercado',
        'tecnologia', 'transporte', 'educacion', 'salud',
        'entretenimiento', 'moda'
    )"""
)
def silver_merchants():
    """
    Transformaciones aplicadas:
        - Estandarización de merchant_id al formato MCH-NNNNN
        - Normalización de country (nombres completos → códigos ISO)
        - Normalización de status (inglés/mayúsculas → español/minúsculas)
        - Normalización de category a minúsculas
        - Normalización de affiliation_date
        - Deduplicación por merchant_id
        - Descarte de columnas extra no documentadas
    """
    df = dlt.read_stream("raw_merchants")

    return (
        df
        # ── Estandarizar merchant_id ──────────────────────────
        .withColumn("merchant_id", normalize_merchant_id("merchant_id"))

        # ── Normalizar country ────────────────────────────────
        .withColumn("country", normalize_country("country"))

        # ── Normalizar status ─────────────────────────────────
        .withColumn("status", normalize_status_merchant("status"))

        # ── Normalizar category a minúsculas ──────────────────
        .withColumn("category", F.lower(F.trim(F.col("category"))))

        # ── Normalizar affiliation_date ───────────────────────
        .withColumn("affiliation_date", normalize_date("affiliation_date"))

        # ── Normalizar risk_level a minúsculas (puede ser nulo) ─
        .withColumn(
            "risk_level",
            F.when(
                F.col("risk_level").isNotNull(),
                F.lower(F.trim(F.col("risk_level")))
            )
        )

        # ── Deduplicar por merchant_id ────────────────────────
        .withWatermark("_ingestion_time", "1 hour")
        .dropDuplicates(["merchant_id"])

        # ── Seleccionar solo columnas documentadas ────────────
        # Se descartan: internal_code, legacy_id, region, source_system
        .select(
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "affiliation_date",
            "status",
            "risk_level",
            "_source_name",
            "_ingestion_time",
            "_file_path",
        )
    )


# =============================================================
# SILVER: USERS
# =============================================================

@dlt.table(
    name="users",
    comment="Silver: users limpios con campos PII bajo masking y RLS",
    table_properties={"quality": "silver"}
)
@dlt.expect("user_id no nulo",            "user_id IS NOT NULL")
@dlt.expect("email no nulo",              "email IS NOT NULL")
@dlt.expect_or_drop(
    "country válido",
    "country IN ('PE', 'CO', 'MX', 'CL', 'AR')"
)
@dlt.expect_or_drop(
    "registration_date no nula",
    "registration_date IS NOT NULL"
)
def silver_users():
    """
    Transformaciones aplicadas:
        - Normalización de country (nombres completos → códigos ISO)
        - Normalización de registration_date
        - Normalización de segment a minúsculas
          (VIP → premium por mapeo, estandar, nuevo)
        - Deduplicación por user_id
        - Los campos PII (full_name, document_id, email, phone)
          están protegidos por column masking y RLS definidos
          en el notebook 00_setup.ipynb
    """
    df = dlt.read_stream("raw_users")

    return (
        df
        # ── Normalizar country ────────────────────────────────
        .withColumn("country", normalize_country("country"))

        # ── Normalizar registration_date ──────────────────────
        .withColumn(
            "registration_date",
            normalize_date("registration_date")
        )

        # ── Normalizar segment ────────────────────────────────
        # VIP no es un valor válido según el enunciado → se mapea a premium
        .withColumn(
            "segment",
            F.when(
                F.lower(F.trim(F.col("segment"))) == "vip",
                F.lit("premium")
            ).when(
                F.lower(F.trim(F.col("segment"))).isin(
                    ["premium", "estandar", "nuevo"]
                ),
                F.lower(F.trim(F.col("segment")))
            ).otherwise(F.lit(None))   # null si no está clasificado
        )

        # ── Limpiar user_id ───────────────────────────────────
        .withColumn("user_id", F.trim(F.col("user_id")))

        # ── Deduplicar por user_id ────────────────────────────
        .withWatermark("_ingestion_time", "1 hour")
        .dropDuplicates(["user_id"])

        # ── Seleccionar columnas finales ──────────────────────
        .select(
            "user_id",
            "full_name",       # PII — column masking activo
            "document_id",     # PII — column masking activo
            "email",           # PII — column masking activo
            "phone",           # PII — column masking activo
            "country",
            "segment",
            "registration_date",
            "_source_name",
            "_ingestion_time",
            "_file_path",
        )
    )


# =============================================================
# SILVER: QUARANTINE (Reto 2)
# Registros que no superan reglas críticas de calidad.
# Se persisten con metadatos para auditoría y reprocesamiento.
# =============================================================

@dlt.table(
    name="quarantine",
    comment="Silver: registros rechazados por reglas de calidad críticas",
    table_properties={"quality": "silver"}
)
def silver_quarantine():
    """
    Captura registros inválidos de las tres fuentes:
        - Transactions sin transaction_id, user_id o merchant_id
        - Transactions con amount nulo o negativo
        - Users sin user_id o email
        - Merchants sin merchant_id

    Cada registro en cuarentena incluye:
        _source_name   : fuente de origen
        _reject_reason : motivo del rechazo
        _processed_at  : timestamp del procesamiento
        _raw_record    : contenido original como STRING JSON
    """
    # ── Transactions inválidas ────────────────────────────────
    tx = dlt.read_stream("raw_transactions")

    bad_tx = tx.filter(
        F.col("transaction_id").isNull() |
        F.col("user_id").isNull() |
        F.col("merchant_id").isNull() |
        F.col("amount").isNull() |
        (F.col("amount").cast(DoubleType()) <= 0)
    )
    quarantine_tx = build_quarantine_record(
        bad_tx,
        source_name="transactions",
        reject_reason="Campo crítico nulo o monto inválido"
    )

    # ── Users inválidos ───────────────────────────────────────
    users = dlt.read_stream("raw_users")

    bad_users = users.filter(
        F.col("user_id").isNull() |
        F.col("email").isNull()
    )
    quarantine_users = build_quarantine_record(
        bad_users,
        source_name="users",
        reject_reason="user_id o email nulo"
    )

    # ── Merchants inválidos ───────────────────────────────────
    merchants = dlt.read_stream("raw_merchants")

    bad_merchants = merchants.filter(
        F.col("merchant_id").isNull()
    )
    quarantine_merchants = build_quarantine_record(
        bad_merchants,
        source_name="merchants",
        reject_reason="merchant_id nulo"
    )

    # ── Unir todos los rechazos en una sola tabla ─────────────
    return quarantine_tx.union(quarantine_users).union(quarantine_merchants)
