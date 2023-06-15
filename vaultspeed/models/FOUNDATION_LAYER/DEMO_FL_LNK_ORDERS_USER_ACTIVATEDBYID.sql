{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LNK_ORDERS_USER_ACTIVATEDBYID',
		schema='DEMO_FL',
		unique_key = ['"LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"'],
		merge_update_columns = [],
		tags=['SALESFORCE', 'LNK_SALESFORCE_ORDERS_USER_ACTIVATEDBYID_INCR', 'LNK_SALESFORCE_ORDERS_USER_ACTIVATEDBYID_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."USER_ACTIVATEDBYID_HKEY" AS "USER_ACTIVATEDBYID_HKEY"
			, "STG_SRC1"."ORDERS_HKEY" AS "ORDERS_HKEY"
			, 0 AS "LOGPOSITION"
		FROM {{ ref('SALESFORCE_STG_ORDERS') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."USER_ACTIVATEDBYID_HKEY" AS "USER_ACTIVATEDBYID_HKEY"
			, "CHANGE_SET"."ORDERS_HKEY" AS "ORDERS_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" ORDER BY "CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."USER_ACTIVATEDBYID_HKEY" AS "USER_ACTIVATEDBYID_HKEY"
			, "MIN_LOAD_TIME"."ORDERS_HKEY" AS "ORDERS_HKEY"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."USER_ACTIVATEDBYID_HKEY"
		, "MIV"."ORDERS_HKEY"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."USER_ACTIVATEDBYID_HKEY" AS "USER_ACTIVATEDBYID_HKEY"
			, "STG_SRC1"."ORDERS_HKEY" AS "ORDERS_HKEY"
		FROM {{ ref('SALESFORCE_STG_ORDERS') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."USER_ACTIVATEDBYID_HKEY" AS "USER_ACTIVATEDBYID_HKEY"
			, "CHANGE_SET"."ORDERS_HKEY" AS "ORDERS_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" ORDER BY "CHANGE_SET"."LOAD_CYCLE_ID",
				"CHANGE_SET"."LOAD_DATE") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."LNK_ORDERS_USER_ACTIVATEDBYID_HKEY" AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."USER_ACTIVATEDBYID_HKEY" AS "USER_ACTIVATEDBYID_HKEY"
		, "MIN_LOAD_TIME"."ORDERS_HKEY" AS "ORDERS_HKEY"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'