{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_JDE_ITEMCROSSREFERENCE_ITEMBRANCH',
		schema='DEMO_FL',
		unique_key = ['"LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['JDEDWARDS', 'LKS_JDE_ITEMCROSSREFERENCE_ITEMBRANCH_INCR', 'LKS_JDE_ITEMCROSSREFERENCE_ITEMBRANCH_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_DK"."ITEMCROSSREFERENCE_HKEY" AS "ITEMCROSSREFERENCE_HKEY"
			, "LKS_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "LKS_TEMP_SRC_DK"."DELETE_FLAG" || "LKS_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."ITEMCROSSREFERENCE_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "LKS_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("LKS_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."ITEMCROSSREFERENCE_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID") AS "CDC_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_LKS_JDE_ITEMCROSSREFERENCE_ITEMBRANCH_TMP') }} "LKS_TEMP_SRC_DK"
		WHERE  "LKS_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_US"."LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY" AS "LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY"
			, "LKS_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("LKS_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "LKS_TEMP_SRC_US"."ITEMCROSSREFERENCE_HKEY" ORDER BY "LKS_TEMP_SRC_US"."LOAD_DATE",
				"LKS_TEMP_SRC_US"."LOAD_CYCLE_ID"), TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('N' AS VARCHAR)
				THEN "END_DATING"."CDC_TIMESTAMP" ELSE "LKS_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "LKS_TEMP_SRC_US"."IVCIRV" AS "IVCIRV"
			, "LKS_TEMP_SRC_US"."IVEXDJ" AS "IVEXDJ"
			, "LKS_TEMP_SRC_US"."IVCITM" AS "IVCITM"
			, "LKS_TEMP_SRC_US"."IVAN8" AS "IVAN8"
			, "LKS_TEMP_SRC_US"."IVXRT" AS "IVXRT"
			, "LKS_TEMP_SRC_US"."IVITM" AS "IVITM"
			, "LKS_TEMP_SRC_US"."IVMCU" AS "IVMCU"
		FROM {{ ref('JDEDWARDS_STG_LKS_JDE_ITEMCROSSREFERENCE_ITEMBRANCH_TMP') }} "LKS_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "LKS_TEMP_SRC_US"."ITEMCROSSREFERENCE_HKEY" = "END_DATING"."ITEMCROSSREFERENCE_HKEY" AND "LKS_TEMP_SRC_US"."LOAD_DATE" =
			 "END_DATING"."LOAD_DATE"
		WHERE  "LKS_TEMP_SRC_US"."EQUAL" = 0 AND NOT("LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY" AS "LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."IVCIRV" AS "IVCIRV"
			, "CALC_LOAD_END_DATE"."IVEXDJ" AS "IVEXDJ"
			, "CALC_LOAD_END_DATE"."IVCITM" AS "IVCITM"
			, "CALC_LOAD_END_DATE"."IVAN8" AS "IVAN8"
			, "CALC_LOAD_END_DATE"."IVXRT" AS "IVXRT"
			, "CALC_LOAD_END_DATE"."IVITM" AS "IVITM"
			, "CALC_LOAD_END_DATE"."IVMCU" AS "IVMCU"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'LKS' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."IVCIRV"
		, "FILTER_LOAD_END_DATE"."IVEXDJ"
		, "FILTER_LOAD_END_DATE"."IVCITM"
		, "FILTER_LOAD_END_DATE"."IVAN8"
		, "FILTER_LOAD_END_DATE"."IVXRT"
		, "FILTER_LOAD_END_DATE"."IVITM"
		, "FILTER_LOAD_END_DATE"."IVMCU"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY" AS "LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."IVCIRV" AS "IVCIRV"
			, "STG_INR_SRC"."IVEXDJ" AS "IVEXDJ"
			, "STG_INR_SRC"."IVCITM" AS "IVCITM"
			, "STG_INR_SRC"."IVAN8" AS "IVAN8"
			, "STG_INR_SRC"."IVXRT" AS "IVXRT"
			, "STG_INR_SRC"."IVITM" AS "IVITM"
			, "STG_INR_SRC"."IVMCU" AS "IVMCU"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."ITEMCROSSREFERENCE_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('JDEDWARDS_STG_F4104') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY" AS "LNK_ITEMCROSSREFERENCE_ITEMBRANCH_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."IVCIRV" AS "IVCIRV"
		, "STG_SRC"."IVEXDJ" AS "IVEXDJ"
		, "STG_SRC"."IVCITM" AS "IVCITM"
		, "STG_SRC"."IVAN8" AS "IVAN8"
		, "STG_SRC"."IVXRT" AS "IVXRT"
		, "STG_SRC"."IVITM" AS "IVITM"
		, "STG_SRC"."IVMCU" AS "IVMCU"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'