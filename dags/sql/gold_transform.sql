-- Arquivo: dags/sql/gold_transform.sql
-- Objetivo: Mover a lógica de agregação Gold (Pandas) para o SQL (ELT).
-- Demonstra: Agregação eficiente e modelagem para consumo (Data Mart).

-- 1. DROP e RE-CRIAÇÃO da tabela Gold (DDL)
DROP TABLE IF EXISTS public.gold_volumes_by_destination CASCADE;

CREATE TABLE public.gold_volumes_by_destination (
    destination_hub VARCHAR PRIMARY KEY,
    total_packages NUMERIC,
    total_weight_kg NUMERIC,
    avg_process_time_sec NUMERIC,
    total_high_value_packages NUMERIC, 
    last_processed_at TIMESTAMP WITHOUT TIME ZONE
);

-- 2. INSERÇÃO e TRANSFORMAÇÃO (ELT)
INSERT INTO public.gold_volumes_by_destination
SELECT
    destination_hub,
    COUNT(event_id) AS total_packages,
    SUM(package_weight_kg) AS total_weight_kg,
    -- Utiliza a métrica de tempo criada no Silver
    AVG(process_time_sec) AS avg_process_time_sec,
    -- Agregação de booleanos (equivalente ao .sum() no Pandas)
    SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) AS total_high_value_packages,
    NOW() AS last_processed_at
FROM
    public.silver_enriched_awb_data
GROUP BY
    destination_hub
ON CONFLICT (destination_hub) DO NOTHING;