-- ============================================================
-- 02_refresh_materialized_views.sql
-- Refresca las vistas materializadas del modelo dimensional
-- Ejecutado por finpay_semantic_job en cada ciclo tras el ETL
-- ============================================================

REFRESH MATERIALIZED VIEW fintech_finpay.gold.fact_transactions;
REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_merchant;
REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_user;
REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_channel;
REFRESH MATERIALIZED VIEW fintech_finpay.gold.dim_date;