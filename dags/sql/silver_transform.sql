-- Arquivo: dags/sql/silver_transform.sql
-- Objetivo: Mover a lógica de enriquecimento Silver (Pandas) para o SQL (ELT).
-- Demonstra: Uso de Window Functions, CTEs e funções de tempo (EXTRACT(EPOCH)).

-- 1. DROP e RE-CRIAÇÃO da tabela Silver (DDL)
DROP TABLE IF EXISTS public.silver_enriched_awb_data CASCADE;

CREATE TABLE public.silver_enriched_awb_data (
    event_id VARCHAR PRIMARY KEY,
    scan_timestamp TIMESTAMP WITHOUT TIME ZONE,
    awb_number VARCHAR,
    carrier_name VARCHAR,
    product_category VARCHAR,
    package_weight_kg NUMERIC,
    declared_value NUMERIC,
    is_exception BOOLEAN,
    is_high_value BOOLEAN,
    destination_hub VARCHAR,
    process_time_sec NUMERIC NOT NULL
);

-- 2. INSERÇÃO e TRANSFORMAÇÃO (ELT)
WITH ProcessTime AS (
    -- CTE para calcular o tempo de processamento total por AWB (Window Function)
    SELECT
        awb_number,
        -- EXTRACT(EPOCH FROM ...) calcula a diferença de tempo em segundos.
        EXTRACT(EPOCH FROM (
            MAX(scan_timestamp) OVER (PARTITION BY awb_number) - 
            MIN(scan_timestamp) OVER (PARTITION BY awb_number)
        )) AS total_process_time_sec
    FROM
        public.bronze_awb_data
)
INSERT INTO public.silver_enriched_awb_data
SELECT
    b.event_id,
    b.scan_timestamp,
    b.awb_number,
    b.carrier_name,
    b.product_category,
    b.package_weight_kg,
    b.declared_value,
    b.is_exception,
    -- Enriquecimento 1: Cálculo de Alto Valor (> 1000)
    CASE 
        WHEN COALESCE(b.declared_value, 0) > 1000 THEN TRUE
        ELSE FALSE
    END AS is_high_value,
    b.destination_hub,
    -- Enriquecimento 2: Utiliza o tempo de processamento. COALESCE trata AWBs com apenas um scan.
    COALESCE(p.total_process_time_sec, 0) AS process_time_sec
FROM
    public.bronze_awb_data b
LEFT JOIN
    ProcessTime p ON b.awb_number = p.awb_number;