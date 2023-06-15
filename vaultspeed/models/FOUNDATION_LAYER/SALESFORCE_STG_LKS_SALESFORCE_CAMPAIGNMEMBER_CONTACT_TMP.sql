{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LKS_SALESFORCE_CAMPAIGNMEMBER_CONTACT_TMP',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'LKS_SALESFORCE_CAMPAIGNMEMBER_CONTACT_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT 
			  "STG_DIS_SRC"."CAMPAIGNMEMBER_HKEY" AS "CAMPAIGNMEMBER_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, MIN("STG_DIS_SRC"."LOAD_DATE") AS "MIN_LOAD_TIMESTAMP"
		FROM {{ ref('SALESFORCE_STG_CAMPAIGNMEMBER') }} "STG_DIS_SRC"
		GROUP BY  "STG_DIS_SRC"."CAMPAIGNMEMBER_HKEY",  "STG_DIS_SRC"."LOAD_CYCLE_ID"
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."LNK_CAMPAIGNMEMBER_CONTACT_HKEY" AS "LNK_CAMPAIGNMEMBER_CONTACT_HKEY"
			, "STG_TEMP_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
			, "STG_TEMP_SRC"."CAMPAIGNMEMBER_HKEY" AS "CAMPAIGNMEMBER_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."ID" AS "ID"
			, "STG_TEMP_SRC"."CONTACTID" AS "CONTACTID"
		FROM {{ ref('SALESFORCE_STG_CAMPAIGNMEMBER') }} "STG_TEMP_SRC"
		UNION 
		SELECT 
			  "LKS_SRC"."LNK_CAMPAIGNMEMBER_CONTACT_HKEY" AS "LNK_CAMPAIGNMEMBER_CONTACT_HKEY"
			, "LNK_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
			, "LNK_SRC"."CAMPAIGNMEMBER_HKEY" AS "CAMPAIGNMEMBER_HKEY"
			, "LKS_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "LKS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "LKS_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, 'SAT' AS "RECORD_TYPE"
			, 'LKS' AS "SOURCE"
			, 0 AS "ORIGIN_ID"
			, "LKS_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
			, "LKS_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "LKS_SRC"."ID" AS "ID"
			, "LKS_SRC"."CONTACTID" AS "CONTACTID"
		FROM {{ source('DEMO_FL', 'LKS_SALESFORCE_CAMPAIGNMEMBER_CONTACT') }} "LKS_SRC"
		INNER JOIN {{ source('DEMO_FL', 'LNK_CAMPAIGNMEMBER_CONTACT') }} "LNK_SRC" ON  "LKS_SRC"."LNK_CAMPAIGNMEMBER_CONTACT_HKEY" = "LNK_SRC"."LNK_CAMPAIGNMEMBER_CONTACT_HKEY"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "LNK_SRC"."CAMPAIGNMEMBER_HKEY" = "DIST_STG"."CAMPAIGNMEMBER_HKEY"
		WHERE  "LKS_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."LNK_CAMPAIGNMEMBER_CONTACT_HKEY" AS "LNK_CAMPAIGNMEMBER_CONTACT_HKEY"
		, "TEMP_TABLE_SET"."CONTACT_HKEY" AS "CONTACT_HKEY"
		, "TEMP_TABLE_SET"."CAMPAIGNMEMBER_HKEY" AS "CAMPAIGNMEMBER_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."CONTACT_HKEY")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."CONTACT_HKEY"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."CAMPAIGNMEMBER_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."ID" AS "ID"
		, "TEMP_TABLE_SET"."CONTACTID" AS "CONTACTID"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'