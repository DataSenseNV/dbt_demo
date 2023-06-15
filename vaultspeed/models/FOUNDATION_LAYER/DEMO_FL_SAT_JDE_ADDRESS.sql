{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_JDE_ADDRESS',
		schema='DEMO_FL',
		unique_key = ['"ADDRESS_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['JDEDWARDS', 'SAT_JDE_ADDRESS_INCR', 'SAT_JDE_ADDRESS_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_DK"."ADDRESS_HKEY" AS "ADDRESS_HKEY"
			, "SAT_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "SAT_TEMP_SRC_DK"."DELETE_FLAG" || "SAT_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ADDRESS_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE",
				"SAT_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "SAT_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("SAT_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ADDRESS_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE") AS "CDC_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_SAT_JDE_ADDRESS_TMP') }} "SAT_TEMP_SRC_DK"
		WHERE  "SAT_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_US"."ADDRESS_HKEY" AS "ADDRESS_HKEY"
			, "SAT_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("SAT_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "SAT_TEMP_SRC_US"."ADDRESS_HKEY" ORDER BY "SAT_TEMP_SRC_US"."LOAD_DATE")
				, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, "SAT_TEMP_SRC_US"."HASH_DIFF" AS "HASH_DIFF"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) THEN "END_DATING"."CDC_TIMESTAMP" ELSE "SAT_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "SAT_TEMP_SRC_US"."ALAN8" AS "ALAN8"
			, "SAT_TEMP_SRC_US"."ALEFTB" AS "ALEFTB"
			, "SAT_TEMP_SRC_US"."ALEFTF" AS "ALEFTF"
			, "SAT_TEMP_SRC_US"."ALADD1" AS "ALADD1"
			, "SAT_TEMP_SRC_US"."ALADD2" AS "ALADD2"
			, "SAT_TEMP_SRC_US"."ALADD3" AS "ALADD3"
			, "SAT_TEMP_SRC_US"."ALADD4" AS "ALADD4"
			, "SAT_TEMP_SRC_US"."ALADDZ" AS "ALADDZ"
			, "SAT_TEMP_SRC_US"."ALCTY1" AS "ALCTY1"
			, "SAT_TEMP_SRC_US"."ALCOUN" AS "ALCOUN"
			, "SAT_TEMP_SRC_US"."ALADDS" AS "ALADDS"
			, "SAT_TEMP_SRC_US"."ALCRTE" AS "ALCRTE"
			, "SAT_TEMP_SRC_US"."ALBKML" AS "ALBKML"
			, "SAT_TEMP_SRC_US"."ALCTR" AS "ALCTR"
			, "SAT_TEMP_SRC_US"."ALUSER" AS "ALUSER"
			, "SAT_TEMP_SRC_US"."ALPID" AS "ALPID"
			, "SAT_TEMP_SRC_US"."ALUPMJ" AS "ALUPMJ"
			, "SAT_TEMP_SRC_US"."ALJOBN" AS "ALJOBN"
			, "SAT_TEMP_SRC_US"."ALUPMT" AS "ALUPMT"
			, "SAT_TEMP_SRC_US"."ALSYNCS" AS "ALSYNCS"
			, "SAT_TEMP_SRC_US"."ALCAAD" AS "ALCAAD"
		FROM {{ ref('JDEDWARDS_STG_SAT_JDE_ADDRESS_TMP') }} "SAT_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "SAT_TEMP_SRC_US"."ADDRESS_HKEY" = "END_DATING"."ADDRESS_HKEY" AND "SAT_TEMP_SRC_US"."LOAD_DATE" = 
			"END_DATING"."LOAD_DATE"
		WHERE  "SAT_TEMP_SRC_US"."EQUAL" = 0 AND NOT("SAT_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "SAT_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."ADDRESS_HKEY" AS "ADDRESS_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."HASH_DIFF" AS "HASH_DIFF"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."ALAN8" AS "ALAN8"
			, "CALC_LOAD_END_DATE"."ALEFTB" AS "ALEFTB"
			, "CALC_LOAD_END_DATE"."ALEFTF" AS "ALEFTF"
			, "CALC_LOAD_END_DATE"."ALADD1" AS "ALADD1"
			, "CALC_LOAD_END_DATE"."ALADD2" AS "ALADD2"
			, "CALC_LOAD_END_DATE"."ALADD3" AS "ALADD3"
			, "CALC_LOAD_END_DATE"."ALADD4" AS "ALADD4"
			, "CALC_LOAD_END_DATE"."ALADDZ" AS "ALADDZ"
			, "CALC_LOAD_END_DATE"."ALCTY1" AS "ALCTY1"
			, "CALC_LOAD_END_DATE"."ALCOUN" AS "ALCOUN"
			, "CALC_LOAD_END_DATE"."ALADDS" AS "ALADDS"
			, "CALC_LOAD_END_DATE"."ALCRTE" AS "ALCRTE"
			, "CALC_LOAD_END_DATE"."ALBKML" AS "ALBKML"
			, "CALC_LOAD_END_DATE"."ALCTR" AS "ALCTR"
			, "CALC_LOAD_END_DATE"."ALUSER" AS "ALUSER"
			, "CALC_LOAD_END_DATE"."ALPID" AS "ALPID"
			, "CALC_LOAD_END_DATE"."ALUPMJ" AS "ALUPMJ"
			, "CALC_LOAD_END_DATE"."ALJOBN" AS "ALJOBN"
			, "CALC_LOAD_END_DATE"."ALUPMT" AS "ALUPMT"
			, "CALC_LOAD_END_DATE"."ALSYNCS" AS "ALSYNCS"
			, "CALC_LOAD_END_DATE"."ALCAAD" AS "ALCAAD"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'SAT' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."ADDRESS_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."HASH_DIFF"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."ALAN8"
		, "FILTER_LOAD_END_DATE"."ALEFTB"
		, "FILTER_LOAD_END_DATE"."ALEFTF"
		, "FILTER_LOAD_END_DATE"."ALADD1"
		, "FILTER_LOAD_END_DATE"."ALADD2"
		, "FILTER_LOAD_END_DATE"."ALADD3"
		, "FILTER_LOAD_END_DATE"."ALADD4"
		, "FILTER_LOAD_END_DATE"."ALADDZ"
		, "FILTER_LOAD_END_DATE"."ALCTY1"
		, "FILTER_LOAD_END_DATE"."ALCOUN"
		, "FILTER_LOAD_END_DATE"."ALADDS"
		, "FILTER_LOAD_END_DATE"."ALCRTE"
		, "FILTER_LOAD_END_DATE"."ALBKML"
		, "FILTER_LOAD_END_DATE"."ALCTR"
		, "FILTER_LOAD_END_DATE"."ALUSER"
		, "FILTER_LOAD_END_DATE"."ALPID"
		, "FILTER_LOAD_END_DATE"."ALUPMJ"
		, "FILTER_LOAD_END_DATE"."ALJOBN"
		, "FILTER_LOAD_END_DATE"."ALUPMT"
		, "FILTER_LOAD_END_DATE"."ALSYNCS"
		, "FILTER_LOAD_END_DATE"."ALCAAD"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."ADDRESS_HKEY" AS "ADDRESS_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALEFTF"),'~'),'\#','\\' || '\#'))
				|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALADD1"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALADD2"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALADD3"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALADD4"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALADDZ"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALCTY1"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALCOUN"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALADDS"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALCRTE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALBKML"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALCTR"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALUSER"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALPID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."ALUPMJ")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."ALJOBN"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."ALUPMT")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."ALSYNCS")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."ALCAAD")),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."ALAN8" AS "ALAN8"
			, "STG_INR_SRC"."ALEFTB" AS "ALEFTB"
			, "STG_INR_SRC"."ALEFTF" AS "ALEFTF"
			, "STG_INR_SRC"."ALADD1" AS "ALADD1"
			, "STG_INR_SRC"."ALADD2" AS "ALADD2"
			, "STG_INR_SRC"."ALADD3" AS "ALADD3"
			, "STG_INR_SRC"."ALADD4" AS "ALADD4"
			, "STG_INR_SRC"."ALADDZ" AS "ALADDZ"
			, "STG_INR_SRC"."ALCTY1" AS "ALCTY1"
			, "STG_INR_SRC"."ALCOUN" AS "ALCOUN"
			, "STG_INR_SRC"."ALADDS" AS "ALADDS"
			, "STG_INR_SRC"."ALCRTE" AS "ALCRTE"
			, "STG_INR_SRC"."ALBKML" AS "ALBKML"
			, "STG_INR_SRC"."ALCTR" AS "ALCTR"
			, "STG_INR_SRC"."ALUSER" AS "ALUSER"
			, "STG_INR_SRC"."ALPID" AS "ALPID"
			, "STG_INR_SRC"."ALUPMJ" AS "ALUPMJ"
			, "STG_INR_SRC"."ALJOBN" AS "ALJOBN"
			, "STG_INR_SRC"."ALUPMT" AS "ALUPMT"
			, "STG_INR_SRC"."ALSYNCS" AS "ALSYNCS"
			, "STG_INR_SRC"."ALCAAD" AS "ALCAAD"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."ADDRESS_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('JDEDWARDS_STG_F0116') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."ADDRESS_HKEY" AS "ADDRESS_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."ALAN8" AS "ALAN8"
		, "STG_SRC"."ALEFTB" AS "ALEFTB"
		, "STG_SRC"."ALEFTF" AS "ALEFTF"
		, "STG_SRC"."ALADD1" AS "ALADD1"
		, "STG_SRC"."ALADD2" AS "ALADD2"
		, "STG_SRC"."ALADD3" AS "ALADD3"
		, "STG_SRC"."ALADD4" AS "ALADD4"
		, "STG_SRC"."ALADDZ" AS "ALADDZ"
		, "STG_SRC"."ALCTY1" AS "ALCTY1"
		, "STG_SRC"."ALCOUN" AS "ALCOUN"
		, "STG_SRC"."ALADDS" AS "ALADDS"
		, "STG_SRC"."ALCRTE" AS "ALCRTE"
		, "STG_SRC"."ALBKML" AS "ALBKML"
		, "STG_SRC"."ALCTR" AS "ALCTR"
		, "STG_SRC"."ALUSER" AS "ALUSER"
		, "STG_SRC"."ALPID" AS "ALPID"
		, "STG_SRC"."ALUPMJ" AS "ALUPMJ"
		, "STG_SRC"."ALJOBN" AS "ALJOBN"
		, "STG_SRC"."ALUPMT" AS "ALUPMT"
		, "STG_SRC"."ALSYNCS" AS "ALSYNCS"
		, "STG_SRC"."ALCAAD" AS "ALCAAD"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'