{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_JDE_PHONE_WHOISWHO',
		schema='DEMO_FL',
		unique_key = ['"LNK_PHONE_WHOISWHO_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['JDEDWARDS', 'LKS_JDE_PHONE_WHOISWHO_INCR', 'LKS_JDE_PHONE_WHOISWHO_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_DK"."PHONE_HKEY" AS "PHONE_HKEY"
			, "LKS_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "LKS_TEMP_SRC_DK"."DELETE_FLAG" || "LKS_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."PHONE_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "LKS_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("LKS_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."PHONE_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID") AS "CDC_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_LKS_JDE_PHONE_WHOISWHO_TMP') }} "LKS_TEMP_SRC_DK"
		WHERE  "LKS_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_US"."LNK_PHONE_WHOISWHO_HKEY" AS "LNK_PHONE_WHOISWHO_HKEY"
			, "LKS_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("LKS_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "LKS_TEMP_SRC_US"."PHONE_HKEY" ORDER BY "LKS_TEMP_SRC_US"."LOAD_DATE",
				"LKS_TEMP_SRC_US"."LOAD_CYCLE_ID"), TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('N' AS VARCHAR)
				THEN "END_DATING"."CDC_TIMESTAMP" ELSE "LKS_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "LKS_TEMP_SRC_US"."WPRCK7" AS "WPRCK7"
			, "LKS_TEMP_SRC_US"."WPCNLN" AS "WPCNLN"
			, "LKS_TEMP_SRC_US"."WPIDLN" AS "WPIDLN"
			, "LKS_TEMP_SRC_US"."WPAN8" AS "WPAN8"
		FROM {{ ref('JDEDWARDS_STG_LKS_JDE_PHONE_WHOISWHO_TMP') }} "LKS_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "LKS_TEMP_SRC_US"."PHONE_HKEY" = "END_DATING"."PHONE_HKEY" AND "LKS_TEMP_SRC_US"."LOAD_DATE" = "END_DATING"."LOAD_DATE"
		WHERE  "LKS_TEMP_SRC_US"."EQUAL" = 0 AND NOT("LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."LNK_PHONE_WHOISWHO_HKEY" AS "LNK_PHONE_WHOISWHO_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."WPRCK7" AS "WPRCK7"
			, "CALC_LOAD_END_DATE"."WPCNLN" AS "WPCNLN"
			, "CALC_LOAD_END_DATE"."WPIDLN" AS "WPIDLN"
			, "CALC_LOAD_END_DATE"."WPAN8" AS "WPAN8"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'LKS' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."LNK_PHONE_WHOISWHO_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."WPRCK7"
		, "FILTER_LOAD_END_DATE"."WPCNLN"
		, "FILTER_LOAD_END_DATE"."WPIDLN"
		, "FILTER_LOAD_END_DATE"."WPAN8"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."LNK_PHONE_WHOISWHO_HKEY" AS "LNK_PHONE_WHOISWHO_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."WPRCK7" AS "WPRCK7"
			, "STG_INR_SRC"."WPCNLN" AS "WPCNLN"
			, "STG_INR_SRC"."WPIDLN" AS "WPIDLN"
			, "STG_INR_SRC"."WPAN8" AS "WPAN8"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."PHONE_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('JDEDWARDS_STG_F0115') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."LNK_PHONE_WHOISWHO_HKEY" AS "LNK_PHONE_WHOISWHO_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."WPRCK7" AS "WPRCK7"
		, "STG_SRC"."WPCNLN" AS "WPCNLN"
		, "STG_SRC"."WPIDLN" AS "WPIDLN"
		, "STG_SRC"."WPAN8" AS "WPAN8"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'