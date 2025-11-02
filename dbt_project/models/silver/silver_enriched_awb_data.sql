-- dbt_project/models/silver/silver_enriched_awb_data.sql
-- Objetivo: Limpar, tipar e enriquecer os dados brutos (Bronze).

{{
    config(
        materialized='table', 
        schema='silver'
    )
}}

WITH source_data AS (
    -- Referência aos dados brutos no schema 'raw_data'
    SELECT * FROM {{ source('raw_data', 'awb_raw') }} 
),

cleaned_data AS (
    SELECT
        -- 1. Chaves e Identificadores
        awb_id::TEXT AS awb_id,
        TRIM(UPPER(remetente_id)) AS remetente_id,
        
        -- 2. Correção de Tipos e Tratamento de Nulos/Erros
        CASE
            -- Evita valores negativos para peso
            WHEN peso_kg::NUMERIC < 0 THEN NULL 
            ELSE peso_kg::NUMERIC 
        END AS peso_kg,
        
        data_voo::DATE AS data_do_voo, -- Garantindo tipo DATE
        
        -- 3. Enriquecimento Simples: Categorização do Hub
        CASE 
            WHEN destino_codigo IN ('SAO', 'GRU') THEN 'HUB_SP'
            WHEN destino_codigo IN ('RIO', 'GIG') THEN 'HUB_RJ'
            ELSE 'OUTROS'
        END AS destination_hub,
        
        data_carga_bruta_ingestao::TIMESTAMP AS data_ingestao_original
        
    FROM source_data
    -- Filtro de qualidade inicial (removendo AWBs sem ID ou data)
    WHERE awb_id IS NOT NULL AND data_voo IS NOT NULL
)

SELECT * FROM cleaned_data