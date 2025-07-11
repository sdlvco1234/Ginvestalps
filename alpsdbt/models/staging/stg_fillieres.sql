{{ config(materialized='table', schema="staging") }}

SELECT REPLACE(siret_txt, '''', '') AS siretdup,

Cast("SPORT-MONTAGNE" as BOOLEAN) as sport_montagne,
"SPORTdomaine" as sport_domain,
Cast("CLEANTECH" as BOOLEAN) as cleantech,
Cast("SANTE" as BOOLEAN) as sante,
"SANTEdomaine" as sante_domain,
Cast("ENERGIE" as BOOLEAN) as energie,
"ENERGIEdomaine" as energie_domain,
Cast("NUMERIQUE" as BOOLEAN) as numerique,
"NUM_domaine" as num_domain,
Cast("CHIMIE" as BOOLEAN) as chimie,
"CHIMIEdomaine" as chimie_domaine

FROM {{ source('raw_data', 'Entreprises_Fillieres') }}