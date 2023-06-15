{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_JDE_EMAIL_TMP',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'SAT_JDE_EMAIL_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT DISTINCT 
 			  "STG_DIS_SRC"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		FROM {{ ref('JDEDWARDS_STG_F01151') }} "STG_DIS_SRC"
		WHERE  "STG_DIS_SRC"."RECORD_TYPE" = 'S'
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAETP"),'~'),'\#','\\' || '\#'))
				|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAEMAL"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAUSER"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAPID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."EAUPMJ")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAJOBN"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."EAUPMT")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."EAEHIER")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAEFOR"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAECLASS"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."EACFNO1")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAGEN1"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EAFALGE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."EASYNCS")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."EACAAD")),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."EAAN8" AS "EAAN8"
			, "STG_TEMP_SRC"."EAIDLN" AS "EAIDLN"
			, "STG_TEMP_SRC"."EARCK7" AS "EARCK7"
			, "STG_TEMP_SRC"."EAETP" AS "EAETP"
			, "STG_TEMP_SRC"."EAEMAL" AS "EAEMAL"
			, "STG_TEMP_SRC"."EAUSER" AS "EAUSER"
			, "STG_TEMP_SRC"."EAPID" AS "EAPID"
			, "STG_TEMP_SRC"."EAUPMJ" AS "EAUPMJ"
			, "STG_TEMP_SRC"."EAJOBN" AS "EAJOBN"
			, "STG_TEMP_SRC"."EAUPMT" AS "EAUPMT"
			, "STG_TEMP_SRC"."EAEHIER" AS "EAEHIER"
			, "STG_TEMP_SRC"."EAEFOR" AS "EAEFOR"
			, "STG_TEMP_SRC"."EAECLASS" AS "EAECLASS"
			, "STG_TEMP_SRC"."EACFNO1" AS "EACFNO1"
			, "STG_TEMP_SRC"."EAGEN1" AS "EAGEN1"
			, "STG_TEMP_SRC"."EAFALGE" AS "EAFALGE"
			, "STG_TEMP_SRC"."EASYNCS" AS "EASYNCS"
			, "STG_TEMP_SRC"."EACAAD" AS "EACAAD"
		FROM {{ ref('JDEDWARDS_STG_F01151') }} "STG_TEMP_SRC"
		WHERE  "STG_TEMP_SRC"."RECORD_TYPE" = 'S'
		UNION ALL 
		SELECT 
			  "SAT_SRC"."EMAIL_HKEY" AS "EMAIL_HKEY"
			, "SAT_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "SAT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, 'SAT' AS "RECORD_TYPE"
			, 'SAT' AS "SOURCE"
			, 0 AS "ORIGIN_ID"
			, "SAT_SRC"."HASH_DIFF" AS "HASH_DIFF"
			, "SAT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "SAT_SRC"."EAAN8" AS "EAAN8"
			, "SAT_SRC"."EAIDLN" AS "EAIDLN"
			, "SAT_SRC"."EARCK7" AS "EARCK7"
			, "SAT_SRC"."EAETP" AS "EAETP"
			, "SAT_SRC"."EAEMAL" AS "EAEMAL"
			, "SAT_SRC"."EAUSER" AS "EAUSER"
			, "SAT_SRC"."EAPID" AS "EAPID"
			, "SAT_SRC"."EAUPMJ" AS "EAUPMJ"
			, "SAT_SRC"."EAJOBN" AS "EAJOBN"
			, "SAT_SRC"."EAUPMT" AS "EAUPMT"
			, "SAT_SRC"."EAEHIER" AS "EAEHIER"
			, "SAT_SRC"."EAEFOR" AS "EAEFOR"
			, "SAT_SRC"."EAECLASS" AS "EAECLASS"
			, "SAT_SRC"."EACFNO1" AS "EACFNO1"
			, "SAT_SRC"."EAGEN1" AS "EAGEN1"
			, "SAT_SRC"."EAFALGE" AS "EAFALGE"
			, "SAT_SRC"."EASYNCS" AS "EASYNCS"
			, "SAT_SRC"."EACAAD" AS "EACAAD"
		FROM {{ source('DEMO_FL', 'SAT_JDE_EMAIL') }} "SAT_SRC"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "SAT_SRC"."EMAIL_HKEY" = "DIST_STG"."EMAIL_HKEY"
		WHERE  "SAT_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."EMAIL_HKEY" AS "EMAIL_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."EMAIL_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."HASH_DIFF" AS "HASH_DIFF"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."EAAN8" AS "EAAN8"
		, "TEMP_TABLE_SET"."EAIDLN" AS "EAIDLN"
		, "TEMP_TABLE_SET"."EARCK7" AS "EARCK7"
		, "TEMP_TABLE_SET"."EAETP" AS "EAETP"
		, "TEMP_TABLE_SET"."EAEMAL" AS "EAEMAL"
		, "TEMP_TABLE_SET"."EAUSER" AS "EAUSER"
		, "TEMP_TABLE_SET"."EAPID" AS "EAPID"
		, "TEMP_TABLE_SET"."EAUPMJ" AS "EAUPMJ"
		, "TEMP_TABLE_SET"."EAJOBN" AS "EAJOBN"
		, "TEMP_TABLE_SET"."EAUPMT" AS "EAUPMT"
		, "TEMP_TABLE_SET"."EAEHIER" AS "EAEHIER"
		, "TEMP_TABLE_SET"."EAEFOR" AS "EAEFOR"
		, "TEMP_TABLE_SET"."EAECLASS" AS "EAECLASS"
		, "TEMP_TABLE_SET"."EACFNO1" AS "EACFNO1"
		, "TEMP_TABLE_SET"."EAGEN1" AS "EAGEN1"
		, "TEMP_TABLE_SET"."EAFALGE" AS "EAFALGE"
		, "TEMP_TABLE_SET"."EASYNCS" AS "EASYNCS"
		, "TEMP_TABLE_SET"."EACAAD" AS "EACAAD"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'