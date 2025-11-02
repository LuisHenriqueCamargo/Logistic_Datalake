-- dbt_project/models/gold/gold_volumes_by_destination.sql
-- Objetivo: Tabela agregada por dia/destino, otimizada para BI (Incremental)

{{
    config(
        materialized='incremental',
        unique_key=['daily_volume_key'], 
        incremental_strategy='merge',    
        schema='gold'
    )
}}

SELECT
    destination_hub,
    data_do_voo,
    COUNT(awb_id) AS total_volumes,
    SUM(peso_kg) AS total_volume_kg,
    
    -- Chave única para o MERGE incremental
    destination_hub || '_' || data_do_voo AS daily_volume_key,
    
    CURRENT_TIMESTAMP AS data_modificacao
    
FROM {{ ref('silver_enriched_awb_data') }} -- Referência ao modelo Silver

-- ** CLÁUSULA CRÍTICA PARA INCREMENTALIDADE **
{% if is_incremental() %}
  -- Só processa dados de voos posteriores ao que já está no destino (tabela GOLD)
  WHERE data_do_voo > (SELECT MAX(data_do_voo) FROM {{ this }})
{% endif %}

GROUP BY 1, 2
ORDER BY data_do_voo DESC