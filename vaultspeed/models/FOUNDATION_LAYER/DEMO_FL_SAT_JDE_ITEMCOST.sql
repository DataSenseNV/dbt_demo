{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_JDE_ITEMCOST',
		schema='DEMO_FL',
		unique_key = ['"ITEMCOST_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['JDEDWARDS', 'SAT_JDE_ITEMCOST_INCR', 'SAT_JDE_ITEMCOST_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_DK"."ITEMCOST_HKEY" AS "ITEMCOST_HKEY"
			, "SAT_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "SAT_TEMP_SRC_DK"."DELETE_FLAG" || "SAT_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ITEMCOST_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE",
				"SAT_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "SAT_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("SAT_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "SAT_TEMP_SRC_DK"."ITEMCOST_HKEY" ORDER BY "SAT_TEMP_SRC_DK"."LOAD_DATE") AS "CDC_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_SAT_JDE_ITEMCOST_TMP') }} "SAT_TEMP_SRC_DK"
		WHERE  "SAT_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "SAT_TEMP_SRC_US"."ITEMCOST_HKEY" AS "ITEMCOST_HKEY"
			, "SAT_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("SAT_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "SAT_TEMP_SRC_US"."ITEMCOST_HKEY" ORDER BY "SAT_TEMP_SRC_US"."LOAD_DATE")
				, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, "SAT_TEMP_SRC_US"."HASH_DIFF" AS "HASH_DIFF"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) THEN "END_DATING"."CDC_TIMESTAMP" ELSE "SAT_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "SAT_TEMP_SRC_US"."COITM" AS "COITM"
			, "SAT_TEMP_SRC_US"."COMCU" AS "COMCU"
			, "SAT_TEMP_SRC_US"."COLOCN" AS "COLOCN"
			, "SAT_TEMP_SRC_US"."COLOTN" AS "COLOTN"
			, "SAT_TEMP_SRC_US"."COLEDG" AS "COLEDG"
			, "SAT_TEMP_SRC_US"."COLITM" AS "COLITM"
			, "SAT_TEMP_SRC_US"."COAITM" AS "COAITM"
			, "SAT_TEMP_SRC_US"."COLOTG" AS "COLOTG"
			, "SAT_TEMP_SRC_US"."COUNCS" AS "COUNCS"
			, "SAT_TEMP_SRC_US"."COCSPO" AS "COCSPO"
			, "SAT_TEMP_SRC_US"."COCSIN" AS "COCSIN"
			, "SAT_TEMP_SRC_US"."COURCD" AS "COURCD"
			, "SAT_TEMP_SRC_US"."COURDT" AS "COURDT"
			, "SAT_TEMP_SRC_US"."COURAT" AS "COURAT"
			, "SAT_TEMP_SRC_US"."COURAB" AS "COURAB"
			, "SAT_TEMP_SRC_US"."COURRF" AS "COURRF"
			, "SAT_TEMP_SRC_US"."COUSER" AS "COUSER"
			, "SAT_TEMP_SRC_US"."COPID" AS "COPID"
			, "SAT_TEMP_SRC_US"."COJOBN" AS "COJOBN"
			, "SAT_TEMP_SRC_US"."COUPMJ" AS "COUPMJ"
			, "SAT_TEMP_SRC_US"."COTDAY" AS "COTDAY"
			, "SAT_TEMP_SRC_US"."COCCFL" AS "COCCFL"
			, "SAT_TEMP_SRC_US"."COCRCS" AS "COCRCS"
			, "SAT_TEMP_SRC_US"."COOSTC" AS "COOSTC"
			, "SAT_TEMP_SRC_US"."COSTOC" AS "COSTOC"
		FROM {{ ref('JDEDWARDS_STG_SAT_JDE_ITEMCOST_TMP') }} "SAT_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "SAT_TEMP_SRC_US"."ITEMCOST_HKEY" = "END_DATING"."ITEMCOST_HKEY" AND "SAT_TEMP_SRC_US"."LOAD_DATE" = 
			"END_DATING"."LOAD_DATE"
		WHERE  "SAT_TEMP_SRC_US"."EQUAL" = 0 AND NOT("SAT_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "SAT_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."ITEMCOST_HKEY" AS "ITEMCOST_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."HASH_DIFF" AS "HASH_DIFF"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."COITM" AS "COITM"
			, "CALC_LOAD_END_DATE"."COMCU" AS "COMCU"
			, "CALC_LOAD_END_DATE"."COLOCN" AS "COLOCN"
			, "CALC_LOAD_END_DATE"."COLOTN" AS "COLOTN"
			, "CALC_LOAD_END_DATE"."COLEDG" AS "COLEDG"
			, "CALC_LOAD_END_DATE"."COLITM" AS "COLITM"
			, "CALC_LOAD_END_DATE"."COAITM" AS "COAITM"
			, "CALC_LOAD_END_DATE"."COLOTG" AS "COLOTG"
			, "CALC_LOAD_END_DATE"."COUNCS" AS "COUNCS"
			, "CALC_LOAD_END_DATE"."COCSPO" AS "COCSPO"
			, "CALC_LOAD_END_DATE"."COCSIN" AS "COCSIN"
			, "CALC_LOAD_END_DATE"."COURCD" AS "COURCD"
			, "CALC_LOAD_END_DATE"."COURDT" AS "COURDT"
			, "CALC_LOAD_END_DATE"."COURAT" AS "COURAT"
			, "CALC_LOAD_END_DATE"."COURAB" AS "COURAB"
			, "CALC_LOAD_END_DATE"."COURRF" AS "COURRF"
			, "CALC_LOAD_END_DATE"."COUSER" AS "COUSER"
			, "CALC_LOAD_END_DATE"."COPID" AS "COPID"
			, "CALC_LOAD_END_DATE"."COJOBN" AS "COJOBN"
			, "CALC_LOAD_END_DATE"."COUPMJ" AS "COUPMJ"
			, "CALC_LOAD_END_DATE"."COTDAY" AS "COTDAY"
			, "CALC_LOAD_END_DATE"."COCCFL" AS "COCCFL"
			, "CALC_LOAD_END_DATE"."COCRCS" AS "COCRCS"
			, "CALC_LOAD_END_DATE"."COOSTC" AS "COOSTC"
			, "CALC_LOAD_END_DATE"."COSTOC" AS "COSTOC"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'SAT' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."ITEMCOST_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."HASH_DIFF"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."COITM"
		, "FILTER_LOAD_END_DATE"."COMCU"
		, "FILTER_LOAD_END_DATE"."COLOCN"
		, "FILTER_LOAD_END_DATE"."COLOTN"
		, "FILTER_LOAD_END_DATE"."COLEDG"
		, "FILTER_LOAD_END_DATE"."COLITM"
		, "FILTER_LOAD_END_DATE"."COAITM"
		, "FILTER_LOAD_END_DATE"."COLOTG"
		, "FILTER_LOAD_END_DATE"."COUNCS"
		, "FILTER_LOAD_END_DATE"."COCSPO"
		, "FILTER_LOAD_END_DATE"."COCSIN"
		, "FILTER_LOAD_END_DATE"."COURCD"
		, "FILTER_LOAD_END_DATE"."COURDT"
		, "FILTER_LOAD_END_DATE"."COURAT"
		, "FILTER_LOAD_END_DATE"."COURAB"
		, "FILTER_LOAD_END_DATE"."COURRF"
		, "FILTER_LOAD_END_DATE"."COUSER"
		, "FILTER_LOAD_END_DATE"."COPID"
		, "FILTER_LOAD_END_DATE"."COJOBN"
		, "FILTER_LOAD_END_DATE"."COUPMJ"
		, "FILTER_LOAD_END_DATE"."COTDAY"
		, "FILTER_LOAD_END_DATE"."COCCFL"
		, "FILTER_LOAD_END_DATE"."COCRCS"
		, "FILTER_LOAD_END_DATE"."COOSTC"
		, "FILTER_LOAD_END_DATE"."COSTOC"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."ITEMCOST_HKEY" AS "ITEMCOST_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COLITM"),'~'),'\#','\\' || '\#'))
				|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COAITM"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COLOTG"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COUNCS")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COCSPO"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COCSIN"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COURCD"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COURDT")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COURAT")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COURAB")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COURRF"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COUSER"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COPID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COJOBN"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COUPMJ")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COTDAY")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_INR_SRC"."COCCFL"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COCRCS")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COOSTC")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_INR_SRC"."COSTOC")),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."COITM" AS "COITM"
			, "STG_INR_SRC"."COMCU" AS "COMCU"
			, "STG_INR_SRC"."COLOCN" AS "COLOCN"
			, "STG_INR_SRC"."COLOTN" AS "COLOTN"
			, "STG_INR_SRC"."COLEDG" AS "COLEDG"
			, "STG_INR_SRC"."COLITM" AS "COLITM"
			, "STG_INR_SRC"."COAITM" AS "COAITM"
			, "STG_INR_SRC"."COLOTG" AS "COLOTG"
			, "STG_INR_SRC"."COUNCS" AS "COUNCS"
			, "STG_INR_SRC"."COCSPO" AS "COCSPO"
			, "STG_INR_SRC"."COCSIN" AS "COCSIN"
			, "STG_INR_SRC"."COURCD" AS "COURCD"
			, "STG_INR_SRC"."COURDT" AS "COURDT"
			, "STG_INR_SRC"."COURAT" AS "COURAT"
			, "STG_INR_SRC"."COURAB" AS "COURAB"
			, "STG_INR_SRC"."COURRF" AS "COURRF"
			, "STG_INR_SRC"."COUSER" AS "COUSER"
			, "STG_INR_SRC"."COPID" AS "COPID"
			, "STG_INR_SRC"."COJOBN" AS "COJOBN"
			, "STG_INR_SRC"."COUPMJ" AS "COUPMJ"
			, "STG_INR_SRC"."COTDAY" AS "COTDAY"
			, "STG_INR_SRC"."COCCFL" AS "COCCFL"
			, "STG_INR_SRC"."COCRCS" AS "COCRCS"
			, "STG_INR_SRC"."COOSTC" AS "COOSTC"
			, "STG_INR_SRC"."COSTOC" AS "COSTOC"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."ITEMCOST_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('JDEDWARDS_STG_F4105') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."ITEMCOST_HKEY" AS "ITEMCOST_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."COITM" AS "COITM"
		, "STG_SRC"."COMCU" AS "COMCU"
		, "STG_SRC"."COLOCN" AS "COLOCN"
		, "STG_SRC"."COLOTN" AS "COLOTN"
		, "STG_SRC"."COLEDG" AS "COLEDG"
		, "STG_SRC"."COLITM" AS "COLITM"
		, "STG_SRC"."COAITM" AS "COAITM"
		, "STG_SRC"."COLOTG" AS "COLOTG"
		, "STG_SRC"."COUNCS" AS "COUNCS"
		, "STG_SRC"."COCSPO" AS "COCSPO"
		, "STG_SRC"."COCSIN" AS "COCSIN"
		, "STG_SRC"."COURCD" AS "COURCD"
		, "STG_SRC"."COURDT" AS "COURDT"
		, "STG_SRC"."COURAT" AS "COURAT"
		, "STG_SRC"."COURAB" AS "COURAB"
		, "STG_SRC"."COURRF" AS "COURRF"
		, "STG_SRC"."COUSER" AS "COUSER"
		, "STG_SRC"."COPID" AS "COPID"
		, "STG_SRC"."COJOBN" AS "COJOBN"
		, "STG_SRC"."COUPMJ" AS "COUPMJ"
		, "STG_SRC"."COTDAY" AS "COTDAY"
		, "STG_SRC"."COCCFL" AS "COCCFL"
		, "STG_SRC"."COCRCS" AS "COCRCS"
		, "STG_SRC"."COOSTC" AS "COOSTC"
		, "STG_SRC"."COSTOC" AS "COSTOC"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'