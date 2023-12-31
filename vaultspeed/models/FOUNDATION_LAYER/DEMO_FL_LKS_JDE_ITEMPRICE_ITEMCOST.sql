{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_JDE_ITEMPRICE_ITEMCOST',
		schema='DEMO_FL',
		unique_key = ['"LNK_ITEMPRICE_ITEMCOST_HKEY"', '"LOAD_DATE"'],
		merge_update_columns = ['"LOAD_END_DATE"', '"DELETE_FLAG"', '"CDC_TIMESTAMP"'],
		tags=['JDEDWARDS', 'LKS_JDE_ITEMPRICE_ITEMCOST_INCR', 'LKS_JDE_ITEMPRICE_ITEMCOST_INIT']
	)
}}
select * from (
	WITH "END_DATING" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_DK"."ITEMPRICE_HKEY" AS "ITEMPRICE_HKEY"
			, "LKS_TEMP_SRC_DK"."LOAD_DATE" AS "LOAD_DATE"
			, CASE WHEN LEAD( "LKS_TEMP_SRC_DK"."DELETE_FLAG" || "LKS_TEMP_SRC_DK"."SOURCE",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."ITEMPRICE_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID")= 'Y' || 'STG' THEN CAST('Y' AS VARCHAR) ELSE "LKS_TEMP_SRC_DK"."DELETE_FLAG" END AS "DELETE_FLAG"
			, LEAD("LKS_TEMP_SRC_DK"."CDC_TIMESTAMP",1)OVER(PARTITION BY "LKS_TEMP_SRC_DK"."ITEMPRICE_HKEY" ORDER BY "LKS_TEMP_SRC_DK"."LOAD_DATE",
				"LKS_TEMP_SRC_DK"."LOAD_CYCLE_ID") AS "CDC_TIMESTAMP"
		FROM {{ ref('JDEDWARDS_STG_LKS_JDE_ITEMPRICE_ITEMCOST_TMP') }} "LKS_TEMP_SRC_DK"
		WHERE  "LKS_TEMP_SRC_DK"."EQUAL" = 0
	)
	, "CALC_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "LKS_TEMP_SRC_US"."LNK_ITEMPRICE_ITEMCOST_HKEY" AS "LNK_ITEMPRICE_ITEMCOST_HKEY"
			, "LKS_TEMP_SRC_US"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_TEMP_SRC_US"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, COALESCE(LEAD("LKS_TEMP_SRC_US"."LOAD_DATE",1)OVER(PARTITION BY "LKS_TEMP_SRC_US"."ITEMPRICE_HKEY" ORDER BY "LKS_TEMP_SRC_US"."LOAD_DATE",
				"LKS_TEMP_SRC_US"."LOAD_CYCLE_ID"), TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS')) AS "LOAD_END_DATE"
			, "END_DATING"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_TEMP_SRC_US"."SOURCE" AS "SOURCE"
			, CASE WHEN "END_DATING"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('N' AS VARCHAR)
				THEN "END_DATING"."CDC_TIMESTAMP" ELSE "LKS_TEMP_SRC_US"."CDC_TIMESTAMP" END AS "CDC_TIMESTAMP"
			, "LKS_TEMP_SRC_US"."BPTDAY" AS "BPTDAY"
			, "LKS_TEMP_SRC_US"."BPUPMJ" AS "BPUPMJ"
			, "LKS_TEMP_SRC_US"."BPEXDJ" AS "BPEXDJ"
			, "LKS_TEMP_SRC_US"."BPUOM" AS "BPUOM"
			, "LKS_TEMP_SRC_US"."BPCRCD" AS "BPCRCD"
			, "LKS_TEMP_SRC_US"."BPFRMP" AS "BPFRMP"
			, "LKS_TEMP_SRC_US"."BPLOTG" AS "BPLOTG"
			, "LKS_TEMP_SRC_US"."BPCGID" AS "BPCGID"
			, "LKS_TEMP_SRC_US"."BPIGID" AS "BPIGID"
			, "LKS_TEMP_SRC_US"."BPAN8" AS "BPAN8"
			, "LKS_TEMP_SRC_US"."BPLOTN" AS "BPLOTN"
			, "LKS_TEMP_SRC_US"."BPLOCN" AS "BPLOCN"
			, "LKS_TEMP_SRC_US"."BPMCU" AS "BPMCU"
			, "LKS_TEMP_SRC_US"."BPITM" AS "BPITM"
			, "LKS_TEMP_SRC_US"."BPLEDG" AS "BPLEDG"
		FROM {{ ref('JDEDWARDS_STG_LKS_JDE_ITEMPRICE_ITEMCOST_TMP') }} "LKS_TEMP_SRC_US"
		INNER JOIN "END_DATING" "END_DATING" ON  "LKS_TEMP_SRC_US"."ITEMPRICE_HKEY" = "END_DATING"."ITEMPRICE_HKEY" AND "LKS_TEMP_SRC_US"."LOAD_DATE" = 
			"END_DATING"."LOAD_DATE"
		WHERE  "LKS_TEMP_SRC_US"."EQUAL" = 0 AND NOT("LKS_TEMP_SRC_US"."DELETE_FLAG" = CAST('Y' AS VARCHAR) AND "LKS_TEMP_SRC_US"."SOURCE" = 'STG')
	)
	, "FILTER_LOAD_END_DATE" AS 
	( 
		SELECT 
			  "CALC_LOAD_END_DATE"."LNK_ITEMPRICE_ITEMCOST_HKEY" AS "LNK_ITEMPRICE_ITEMCOST_HKEY"
			, "CALC_LOAD_END_DATE"."LOAD_DATE" AS "LOAD_DATE"
			, "CALC_LOAD_END_DATE"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALC_LOAD_END_DATE"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, "CALC_LOAD_END_DATE"."DELETE_FLAG" AS "DELETE_FLAG"
			, "CALC_LOAD_END_DATE"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALC_LOAD_END_DATE"."BPTDAY" AS "BPTDAY"
			, "CALC_LOAD_END_DATE"."BPUPMJ" AS "BPUPMJ"
			, "CALC_LOAD_END_DATE"."BPEXDJ" AS "BPEXDJ"
			, "CALC_LOAD_END_DATE"."BPUOM" AS "BPUOM"
			, "CALC_LOAD_END_DATE"."BPCRCD" AS "BPCRCD"
			, "CALC_LOAD_END_DATE"."BPFRMP" AS "BPFRMP"
			, "CALC_LOAD_END_DATE"."BPLOTG" AS "BPLOTG"
			, "CALC_LOAD_END_DATE"."BPCGID" AS "BPCGID"
			, "CALC_LOAD_END_DATE"."BPIGID" AS "BPIGID"
			, "CALC_LOAD_END_DATE"."BPAN8" AS "BPAN8"
			, "CALC_LOAD_END_DATE"."BPLOTN" AS "BPLOTN"
			, "CALC_LOAD_END_DATE"."BPLOCN" AS "BPLOCN"
			, "CALC_LOAD_END_DATE"."BPMCU" AS "BPMCU"
			, "CALC_LOAD_END_DATE"."BPITM" AS "BPITM"
			, "CALC_LOAD_END_DATE"."BPLEDG" AS "BPLEDG"
		FROM "CALC_LOAD_END_DATE" "CALC_LOAD_END_DATE"
		WHERE  "CALC_LOAD_END_DATE"."DELETE_FLAG" = CAST('Y' AS VARCHAR) OR "CALC_LOAD_END_DATE"."SOURCE" = 'STG' OR("CALC_LOAD_END_DATE"."SOURCE" = 'LKS' AND "CALC_LOAD_END_DATE"."LOAD_END_DATE" != TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS'))
	)
	SELECT 
		  "FILTER_LOAD_END_DATE"."LNK_ITEMPRICE_ITEMCOST_HKEY"
		, "FILTER_LOAD_END_DATE"."LOAD_DATE"
		, "FILTER_LOAD_END_DATE"."LOAD_CYCLE_ID"
		, "FILTER_LOAD_END_DATE"."LOAD_END_DATE"
		, "FILTER_LOAD_END_DATE"."DELETE_FLAG"
		, "FILTER_LOAD_END_DATE"."CDC_TIMESTAMP"
		, "FILTER_LOAD_END_DATE"."BPTDAY"
		, "FILTER_LOAD_END_DATE"."BPUPMJ"
		, "FILTER_LOAD_END_DATE"."BPEXDJ"
		, "FILTER_LOAD_END_DATE"."BPUOM"
		, "FILTER_LOAD_END_DATE"."BPCRCD"
		, "FILTER_LOAD_END_DATE"."BPFRMP"
		, "FILTER_LOAD_END_DATE"."BPLOTG"
		, "FILTER_LOAD_END_DATE"."BPCGID"
		, "FILTER_LOAD_END_DATE"."BPIGID"
		, "FILTER_LOAD_END_DATE"."BPAN8"
		, "FILTER_LOAD_END_DATE"."BPLOTN"
		, "FILTER_LOAD_END_DATE"."BPLOCN"
		, "FILTER_LOAD_END_DATE"."BPMCU"
		, "FILTER_LOAD_END_DATE"."BPITM"
		, "FILTER_LOAD_END_DATE"."BPLEDG"
	FROM "FILTER_LOAD_END_DATE"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "STG_SRC" AS 
	( 
		SELECT 
			  "STG_INR_SRC"."LNK_ITEMPRICE_ITEMCOST_HKEY" AS "LNK_ITEMPRICE_ITEMCOST_HKEY"
			, "STG_INR_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_INR_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, CAST('N' AS VARCHAR) AS "DELETE_FLAG"
			, "STG_INR_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_INR_SRC"."BPTDAY" AS "BPTDAY"
			, "STG_INR_SRC"."BPUPMJ" AS "BPUPMJ"
			, "STG_INR_SRC"."BPEXDJ" AS "BPEXDJ"
			, "STG_INR_SRC"."BPUOM" AS "BPUOM"
			, "STG_INR_SRC"."BPCRCD" AS "BPCRCD"
			, "STG_INR_SRC"."BPFRMP" AS "BPFRMP"
			, "STG_INR_SRC"."BPLOTG" AS "BPLOTG"
			, "STG_INR_SRC"."BPCGID" AS "BPCGID"
			, "STG_INR_SRC"."BPIGID" AS "BPIGID"
			, "STG_INR_SRC"."BPAN8" AS "BPAN8"
			, "STG_INR_SRC"."BPLOTN" AS "BPLOTN"
			, "STG_INR_SRC"."BPLOCN" AS "BPLOCN"
			, "STG_INR_SRC"."BPMCU" AS "BPMCU"
			, "STG_INR_SRC"."BPITM" AS "BPITM"
			, "STG_INR_SRC"."BPLEDG" AS "BPLEDG"
			, ROW_NUMBER()OVER(PARTITION BY "STG_INR_SRC"."ITEMPRICE_HKEY" ORDER BY "STG_INR_SRC"."LOAD_DATE") AS "DUMMY"
		FROM {{ ref('JDEDWARDS_STG_F4106') }} "STG_INR_SRC"
	)
	SELECT 
		  "STG_SRC"."LNK_ITEMPRICE_ITEMCOST_HKEY" AS "LNK_ITEMPRICE_ITEMCOST_HKEY"
		, "STG_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "STG_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "STG_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "STG_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
		, "STG_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "STG_SRC"."BPTDAY" AS "BPTDAY"
		, "STG_SRC"."BPUPMJ" AS "BPUPMJ"
		, "STG_SRC"."BPEXDJ" AS "BPEXDJ"
		, "STG_SRC"."BPUOM" AS "BPUOM"
		, "STG_SRC"."BPCRCD" AS "BPCRCD"
		, "STG_SRC"."BPFRMP" AS "BPFRMP"
		, "STG_SRC"."BPLOTG" AS "BPLOTG"
		, "STG_SRC"."BPCGID" AS "BPCGID"
		, "STG_SRC"."BPIGID" AS "BPIGID"
		, "STG_SRC"."BPAN8" AS "BPAN8"
		, "STG_SRC"."BPLOTN" AS "BPLOTN"
		, "STG_SRC"."BPLOCN" AS "BPLOCN"
		, "STG_SRC"."BPMCU" AS "BPMCU"
		, "STG_SRC"."BPITM" AS "BPITM"
		, "STG_SRC"."BPLEDG" AS "BPLEDG"
	FROM "STG_SRC" "STG_SRC"
	WHERE  "STG_SRC"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'