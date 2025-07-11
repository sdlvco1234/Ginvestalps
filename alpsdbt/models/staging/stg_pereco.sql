{{ config(materialized='table', schema='staging') }}
SELECT "NAF 732" AS NAF_code,
    "activite agrégée" AS act_agregee,
    "Naf_Reg" as NAF_reg,
    "NAF 88" as NAF_div,
    "aepi" as AEPI,
    "libellé naf 700" as NAF_libelle
FROM {{ source('raw_data', 'Perimetre_economique') }}



