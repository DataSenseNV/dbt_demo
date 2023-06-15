{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LNK_SOHEADER_ACCOUNT',
		schema='DEMO_FL',
		unique_key = ['"LNK_SOHEADER_ACCOUNT_HKEY"'],
		merge_update_columns = [],
		tags=['JDEDWARDS', 'LNK_JDE_SOHEADER_ACCOUNT_INCR', 'LNK_JDE_SOHEADER_ACCOUNT_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_SOHEADER_ACCOUNT_HKEY" AS "LNK_SOHEADER_ACCOUNT_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
			, "STG_SRC1"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
			, 0 AS "LOGPOSITION"
		FROM {{ ref('JDEDWARDS_STG_F4201') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_SOHEADER_ACCOUNT_HKEY" AS "LNK_SOHEADER_ACCOUNT_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
			, "CHANGE_SET"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_SOHEADER_ACCOUNT_HKEY" ORDER BY "CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."LNK_SOHEADER_ACCOUNT_HKEY" AS "LNK_SOHEADER_ACCOUNT_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
			, "MIN_LOAD_TIME"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."LNK_SOHEADER_ACCOUNT_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."ACCOUNT_HKEY"
		, "MIV"."SALESORDERHEADER_HKEY"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_SOHEADER_ACCOUNT_HKEY" AS "LNK_SOHEADER_ACCOUNT_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
			, "STG_SRC1"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
		FROM {{ ref('JDEDWARDS_STG_F4201') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_SOHEADER_ACCOUNT_HKEY" AS "LNK_SOHEADER_ACCOUNT_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
			, "CHANGE_SET"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_SOHEADER_ACCOUNT_HKEY" ORDER BY "CHANGE_SET"."LOAD_CYCLE_ID",
				"CHANGE_SET"."LOAD_DATE") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."LNK_SOHEADER_ACCOUNT_HKEY" AS "LNK_SOHEADER_ACCOUNT_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."ACCOUNT_HKEY" AS "ACCOUNT_HKEY"
		, "MIN_LOAD_TIME"."SALESORDERHEADER_HKEY" AS "SALESORDERHEADER_HKEY"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'