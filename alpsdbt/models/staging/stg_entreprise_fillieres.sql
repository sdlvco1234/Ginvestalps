
{{ config(materialized='table', schema="staging") }}

SELECT
  REPLACE(siret_txt, '''', '') AS siret,
  MIN("Société") AS societe,
  MIN("NAF732") AS naf732,
  MIN("Activité NAF détaillée (INSEE)") AS activite_naf,
  -- Année de création : extraire l'année si possible, sinon NULL
  CAST(
    MIN(
      CASE
        WHEN "Année de création de l'établissement" ~ '^\d{4}$'
          THEN "Année de création de l'établissement"
        WHEN "Année de création de l'établissement" ~ '^\d{2}/\d{2}/\d{4}$'
          THEN RIGHT("Année de création de l'établissement", 4)
        ELSE NULL
      END
    ) AS INTEGER
  ) AS annee_creation,
  -- Code commune : convertir en texte avant regex, puis caster en integer
  CAST(
    MIN(
      CASE
        WHEN "Code commune INSEE" IS NOT NULL AND "Code commune INSEE"::text ~ '^\d+$'
          THEN "Code commune INSEE"::text
        ELSE NULL
      END
    ) AS INTEGER
  ) AS code_commune,
  MIN("EPCI") AS epci,
  MIN("Commune") AS commune,
  MIN("Effectifs 2024") AS effectifs_2024,
  -- Date de mise à jour des effectifs : ne caster que si format compatible
  CAST(
    MIN(
      CASE
        WHEN "Date de mise à jour des effectifs" ~ '^\d{4}-\d{2}-\d{2}'
          THEN "Date de mise à jour des effectifs"
        ELSE NULL
      END
    ) AS TIMESTAMP
  ) AS maj_effectifs
FROM {{ source('raw_data', 'Entreprises_Fillieres') }}
WHERE siret_txt IS NOT NULL AND TRIM(siret_txt) <> ''
GROUP BY siret_txt



