
{{ config(materialized='table', schema="core") }}

select siretdup, 'sport_montagne' as filiere, 1 as numero_filiere, sport_domain as categorie
from {{ ref('stg_fillieres') }}
where sport_montagne = true

union all

select siretdup, 'cleantech' as filiere, 2 as numero_filiere, sport_domain as categorie
from {{ ref('stg_fillieres') }}
where cleantech = true

union all

select siretdup, 'sante' as filiere, 3 as numero_filiere, sante_domain as categorie
from {{ ref('stg_fillieres') }}
where sante = true

union all

select siretdup, 'energie' as filiere, 4 as numero_filiere, energie_domain as categorie
from {{ ref('stg_fillieres') }}
where energie = true

union all

select siretdup, 'numerique' as filiere, 5 as numero_filiere, num_domain as categorie
from {{ ref('stg_fillieres') }}
where numerique = true

union all

select siretdup, 'chimie' as filiere, 6 as numero_filiere, chimie_domaine as categorie
from {{ ref('stg_fillieres') }}
where chimie = true
