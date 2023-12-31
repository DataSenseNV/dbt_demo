{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_JDE_PHONE',
		schema='DEMO_FL',
		unique_key = ['"PHONE_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['JDEDWARDS', 'SAT_JDE_PHONE_INCR', 'SAT_JDE_PHONE_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_DK"."PHONE_HKEY" AS "PHONE_HKEY"
			, "SAT_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "SAT_TEMP_SRC_DK"."DELETE_FLAG" || "SAT_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."PHONE_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE",
				"SAT_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "SAT_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("SAT_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."PHONE_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE") AS "CDC_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_SAT_JDE_PHONE_TMP') }} "SAT_TEMP_SRC_DK"
		WHERE  "SAT_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_US"."PHONE_HKEY" AS "PHONE_HKEY"
			, "SAT_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("SAT_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "SAT_TEMP_SRC_US"."PHONE_HKEY" ORDER BY "SAT_TEMP_SRC_US"."LOAD_DATE")
				, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, "SAT_TEMP_SRC_US"."HASH_DIFF" AS "HASH_DIFF"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) THEN "END_DATING"."CDC_TIMESTAMP" ELSE "SAT_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "SAT_TEMP_SRC_US"."WPAN8" AS "WPAN8"
			, "SAT_TEMP_SRC_US"."WPIDLN" AS "WPIDLN"
			, "SAT_TEMP_SRC_US"."WPRCK7" AS "WPRCK7"
			, "SAT_TEMP_SRC_US"."WPCNLN" AS "WPCNLN"
			, "SAT_TEMP_SRC_US"."WPPHTP" AS "WPPHTP"
			, "SAT_TEMP_SRC_US"."WPAR1" AS "WPAR1"
			, "SAT_TEMP_SRC_US"."WPPH1" AS "WPPH1"
			, "SAT_TEMP_SRC_US"."WPUSER" AS "WPUSER"
			, "SAT_TEMP_SRC_US"."WPPID" AS "WPPID"
			, "SAT_TEMP_SRC_US"."WPUPMJ" AS "WPUPMJ"
			, "SAT_TEMP_SRC_US"."WPJOBN" AS "WPJOBN"
			, "SAT_TEMP_SRC_US"."WPUPMT" AS "WPUPMT"
			, "SAT_TEMP_SRC_US"."WPCFNO1" AS "WPCFNO1"
			, "SAT_TEMP_SRC_US"."WPGEN1" AS "WPGEN1"
			, "SAT_TEMP_SRC_US"."WPFALGE" AS "WPFALGE"
			, "SAT_TEMP_SRC_US"."WPSYNCS" AS "WPSYNCS"
			, "SAT_TEMP_SRC_US"."WPCAAD" AS "WPCAAD"
		FROM {{ ref('JDEDWARDS_STG_SAT_JDE_PHONE_TMP') }} "SAT_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "SAT_TEMP_SRC_US"."PHONE_HKEY" = "END_DATING"."PHONE_HKEY" AND "SAT_TEMP_SRC_US"."LOAD_DATE" = "END_DATING"."LOAD_DATE"
		WHERE  "SAT_TEMP_SRC_US"."EQUAL" = 0 AND NOT("SAT_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "SAT_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."PHONE_HKEY" AS "PHONE_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."HASH_DIFF" AS "HASH_DIFF"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."WPAN8" AS "WPAN8"
			, "CALC_LOAD_END_DATE"."WPIDLN" AS "WPIDLN"
			, "CALC_LOAD_END_DATE"."WPRCK7" AS "WPRCK7"
			, "CALC_LOAD_END_DATE"."WPCNLN" AS "WPCNLN"
			, "CALC_LOAD_END_DATE"."WPPHTP" AS "WPPHTP"
			, "CALC_LOAD_END_DATE"."WPAR1" AS "WPAR1"
			, "CALC_LOAD_END_DATE"."WPPH1" AS "WPPH1"
			, "CALC_LOAD_END_DATE"."WPUSER" AS "WPUSER"
			, "CALC_LOAD_END_DATE"."WPPID" AS "WPPID"
			, "CALC_LOAD_END_DATE"."WPUPMJ" AS "WPUPMJ"
			, "CALC_LOAD_END_DATE"."WPJOBN" AS "WPJOBN"
			, "CALC_LOAD_END_DATE"."WPUPMT" AS "WPUPMT"
			, "CALC_LOAD_END_DATE"."WPCFNO1" AS "WPCFNO1"
			, "CALC_LOAD_END_DATE"."WPGEN1" AS "WPGEN1"
			, "CALC_LOAD_END_DATE"."WPFALGE" AS "WPFALGE"
			, "CALC_LOAD_END_DATE"."WPSYNCS" AS "WPSYNCS"
			, "CALC_LOAD_END_DATE"."WPCAAD" AS "WPCAAD"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'SAT' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."PHONE_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."HASH_DIFF"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."WPAN8"
		, "FILTER_LOAD_END_DATE"."WPIDLN"
		, "FILTER_LOAD_END_DATE"."WPRCK7"
		, "FILTER_LOAD_END_DATE"."WPCNLN"
		, "FILTER_LOAD_END_DATE"."WPPHTP"
		, "FILTER_LOAD_END_DATE"."WPAR1"
		, "FILTER_LOAD_END_DATE"."WPPH1"
		, "FILTER_LOAD_END_DATE"."WPUSER"
		, "FILTER_LOAD_END_DATE"."WPPID"
		, "FILTER_LOAD_END_DATE"."WPUPMJ"
		, "FILTER_LOAD_END_DATE"."WPJOBN"
		, "FILTER_LOAD_END_DATE"."WPUPMT"
		, "FILTER_LOAD_END_DATE"."WPCFNO1"
		, "FILTER_LOAD_END_DATE"."WPGEN1"
		, "FILTER_LOAD_END_DATE"."WPFALGE"
		, "FILTER_LOAD_END_DATE"."WPSYNCS"
		, "FILTER_LOAD_END_DATE"."WPCAAD"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."PHONE_HKEY" AS "PHONE_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPPHTP"),'~'),'\#','\\' || '\#'))
				|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPAR1"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPPH1"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPUSER"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPPID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."WPUPMJ")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPJOBN"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."WPUPMT")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."WPCFNO1")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPGEN1"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."WPFALGE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."WPSYNCS")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."WPCAAD")),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."WPAN8" AS "WPAN8"
			, "STG_INR_SRC"."WPIDLN" AS "WPIDLN"
			, "STG_INR_SRC"."WPRCK7" AS "WPRCK7"
			, "STG_INR_SRC"."WPCNLN" AS "WPCNLN"
			, "STG_INR_SRC"."WPPHTP" AS "WPPHTP"
			, "STG_INR_SRC"."WPAR1" AS "WPAR1"
			, "STG_INR_SRC"."WPPH1" AS "WPPH1"
			, "STG_INR_SRC"."WPUSER" AS "WPUSER"
			, "STG_INR_SRC"."WPPID" AS "WPPID"
			, "STG_INR_SRC"."WPUPMJ" AS "WPUPMJ"
			, "STG_INR_SRC"."WPJOBN" AS "WPJOBN"
			, "STG_INR_SRC"."WPUPMT" AS "WPUPMT"
			, "STG_INR_SRC"."WPCFNO1" AS "WPCFNO1"
			, "STG_INR_SRC"."WPGEN1" AS "WPGEN1"
			, "STG_INR_SRC"."WPFALGE" AS "WPFALGE"
			, "STG_INR_SRC"."WPSYNCS" AS "WPSYNCS"
			, "STG_INR_SRC"."WPCAAD" AS "WPCAAD"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."PHONE_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('JDEDWARDS_STG_F0115') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."PHONE_HKEY" AS "PHONE_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."WPAN8" AS "WPAN8"
		, "STG_SRC"."WPIDLN" AS "WPIDLN"
		, "STG_SRC"."WPRCK7" AS "WPRCK7"
		, "STG_SRC"."WPCNLN" AS "WPCNLN"
		, "STG_SRC"."WPPHTP" AS "WPPHTP"
		, "STG_SRC"."WPAR1" AS "WPAR1"
		, "STG_SRC"."WPPH1" AS "WPPH1"
		, "STG_SRC"."WPUSER" AS "WPUSER"
		, "STG_SRC"."WPPID" AS "WPPID"
		, "STG_SRC"."WPUPMJ" AS "WPUPMJ"
		, "STG_SRC"."WPJOBN" AS "WPJOBN"
		, "STG_SRC"."WPUPMT" AS "WPUPMT"
		, "STG_SRC"."WPCFNO1" AS "WPCFNO1"
		, "STG_SRC"."WPGEN1" AS "WPGEN1"
		, "STG_SRC"."WPFALGE" AS "WPFALGE"
		, "STG_SRC"."WPSYNCS" AS "WPSYNCS"
		, "STG_SRC"."WPCAAD" AS "WPCAAD"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'