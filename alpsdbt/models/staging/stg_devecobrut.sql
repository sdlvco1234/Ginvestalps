
{{ config(materialized='table', schema="staging") }}
select cast("SIRET" as VARCHAR(50)),
	cast("SIREN" as VARCHAR(50)),
	replace("Nom entreprise",'@','') as nom_entreprise,
	replace("Nom établissement", '@', '') as nom_etablissement,
	"Chiffre d'affaires" as CA,
	"Code NAF" as code_naf,
	cast("Date création" as DATE) as date_creation,
	"Taille d'entreprise" as "taille_d'entreprise",
	"Numéro" as numero,
	"Voie" as voie,
	"Code postal" as code_postal,
	"Commune" as commune 
FROM {{ source('raw_data', 'deveco') }}




