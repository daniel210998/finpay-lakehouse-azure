# =============================================================
# gold.py — Capa Gold del pipeline FinPay
# KPIs de riesgo, tasas de reversa y detección de anomalías
# por comercio y canal para el área de riesgo.
# =============================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# =============================================================
# GOLD: RISK_KPIS
# KPIs principales de riesgo por comercio y canal.
# =============================================================

@dlt.table(
    name="risk_kpis",
    comment="Gold: KPIs de riesgo por comercio — tasa de reversa y score",
    table_properties={"quality": "gold"}
)
def gold_risk_kpis():
    """
    Agrega métricas de riesgo por merchant_id y canal.

    Métricas calculadas:
        total_transactions  : cantidad total de transacciones
        total_amount        : monto total procesado
        total_reversas      : cantidad de transacciones revertidas
        tasa_reversa        : reversas / total (indicador clave de fraude)
        avg_amount          : monto promedio por transacción
        score_riesgo        : score calculado en base a tasa de reversa
                              y monto promedio (0-100, mayor = más riesgo)
    """
    tx = dlt.read_stream("transactions")
    merchants = dlt.read("merchants")

    # ── Agregar métricas por comercio y canal ─────────────────
    kpis = (
        tx
        .groupBy(
            "merchant_id",
            "channel",
            "currency",
            F.col("transaction_date").alias("fecha")
        )
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.sum(
                F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)
            ).alias("total_reversas"),
            F.sum(
                F.when(F.col("status") == "aprobado", F.col("amount"))
                .otherwise(0)
            ).alias("monto_aprobado"),
        )
    )

    # ── Calcular tasa de reversa y score de riesgo ────────────
    kpis = (
        kpis
        .withColumn(
            "tasa_reversa",
            F.round(
                F.col("total_reversas") / F.col("total_transactions"),
                4
            )
        )
        # Score de riesgo: pondera tasa de reversa (70%) y monto promedio (30%)
        # Normalizado entre 0 y 100
        .withColumn(
            "score_riesgo",
            F.round(
                (F.col("tasa_reversa") * 70) +
                (F.when(F.col("avg_amount") > 5000, 30)
                 .when(F.col("avg_amount") > 1000, 15)
                 .otherwise(5)),
                2
            )
        )
    )

    # ── Enriquecer con datos del comercio ─────────────────────
    return (
        kpis
        .join(
            merchants.select(
                "merchant_id",
                "merchant_name",
                "category",
                "country",
                "risk_level"
            ),
            on="merchant_id",
            how="left"
        )
        .select(
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "channel",
            "currency",
            "fecha",
            "total_transactions",
            "total_amount",
            "monto_aprobado",
            "avg_amount",
            "total_reversas",
            "tasa_reversa",
            "score_riesgo",
            "risk_level",
        )
    )


# =============================================================
# GOLD: ANOMALIES
# Detección de comercios con tasas de reversa anómalas.
# =============================================================

@dlt.table(
    name="anomalies",
    comment="Gold: comercios con patrones anómalos de reversa — posible fraude",
    table_properties={"quality": "gold"}
)
def gold_anomalies():
    """
    Detecta anomalías basadas en umbrales de tasa de reversa.

    Criterios de anomalía:
        CRITICA  : tasa_reversa >= 0.30 (30% o más de reversas)
        ALTA     : tasa_reversa >= 0.15 (15% a 29%)
        MEDIA    : tasa_reversa >= 0.05 (5% a 14%)
        NORMAL   : tasa_reversa < 0.05

    Solo se reportan comercios con severidad MEDIA, ALTA o CRITICA.
    """
    kpis = dlt.read_stream("risk_kpis")

    return (
        kpis
        # ── Clasificar severidad de la anomalía ───────────────
        .withColumn(
            "severidad",
            F.when(F.col("tasa_reversa") >= 0.30, "CRITICA")
            .when(F.col("tasa_reversa") >= 0.15, "ALTA")
            .when(F.col("tasa_reversa") >= 0.05, "MEDIA")
            .otherwise("NORMAL")
        )
        # ── Filtrar solo anomalías reales ─────────────────────
        .filter(F.col("severidad") != "NORMAL")

        # ── Agregar timestamp de detección ────────────────────
        .withColumn("detected_at", F.current_timestamp())

        .select(
            "merchant_id",
            "merchant_name",
            "category",
            "country",
            "channel",
            "fecha",
            "total_transactions",
            "total_reversas",
            "tasa_reversa",
            "score_riesgo",
            "severidad",
            "detected_at",
        )
    )


# =============================================================
# GOLD: CHANNEL_SUMMARY
# Resumen de métricas por canal de origen.
# =============================================================

@dlt.table(
    name="channel_summary",
    comment="Gold: métricas agregadas por canal — web, app, POS",
    table_properties={"quality": "gold"}
)
def gold_channel_summary():
    """
    Agrega métricas por canal de origen para el área de riesgo.

    Canales: web · app · pos

    Métricas:
        total_transactions : volumen de operaciones por canal
        total_amount       : monto total procesado
        tasa_reversa       : indicador de riesgo por canal
        tasa_aprobacion    : porcentaje de transacciones aprobadas
        pct_del_total      : participación del canal en el total
    """
    tx = dlt.read_stream("transactions")

    # ── Total general para calcular porcentajes ───────────────
    total = tx.agg(F.count("*").alias("grand_total")).collect()[0][0]

    return (
        tx
        .groupBy("channel", F.col("transaction_date").alias("fecha"))
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.sum(
                F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)
            ).alias("total_reversas"),
            F.sum(
                F.when(F.col("status") == "aprobado", 1).otherwise(0)
            ).alias("total_aprobadas"),
            F.countDistinct("merchant_id").alias("merchants_activos"),
            F.countDistinct("user_id").alias("usuarios_activos"),
        )
        .withColumn(
            "tasa_reversa",
            F.round(
                F.col("total_reversas") / F.col("total_transactions"), 4
            )
        )
        .withColumn(
            "tasa_aprobacion",
            F.round(
                F.col("total_aprobadas") / F.col("total_transactions"), 4
            )
        )
        .withColumn(
            "pct_del_total",
            F.round(
                F.col("total_transactions") / F.lit(total) * 100, 2
            )
        )
        .select(
            "channel",
            "fecha",
            "total_transactions",
            "total_amount",
            "avg_amount",
            "total_reversas",
            "tasa_reversa",
            "total_aprobadas",
            "tasa_aprobacion",
            "merchants_activos",
            "usuarios_activos",
            "pct_del_total",
        )
    )
