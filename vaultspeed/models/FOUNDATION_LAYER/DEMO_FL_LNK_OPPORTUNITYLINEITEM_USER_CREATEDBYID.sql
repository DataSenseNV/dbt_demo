{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID',
		schema='DEMO_FL',
		unique_key = ['"LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"'],
		merge_update_columns = [],
		tags=['SALESFORCE', 'LNK_SALESFORCE_OPPORTUNITYLINEITEM_USER_CREATEDBYID_INCR', 'LNK_SALESFORCE_OPPORTUNITYLINEITEM_USER_CREATEDBYID_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" AS "LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."USER_CREATEDBYID_HKEY" AS "USER_CREATEDBYID_HKEY"
			, "STG_SRC1"."OPPORTUNITYLINEITEM_HKEY" AS "OPPORTUNITYLINEITEM_HKEY"
			, 0 AS "LOGPOSITION"
		FROM {{ ref('SALESFORCE_STG_OPPORTUNITYLINEITEM') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" AS "LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."USER_CREATEDBYID_HKEY" AS "USER_CREATEDBYID_HKEY"
			, "CHANGE_SET"."OPPORTUNITYLINEITEM_HKEY" AS "OPPORTUNITYLINEITEM_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" ORDER BY "CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" AS "LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."USER_CREATEDBYID_HKEY" AS "USER_CREATEDBYID_HKEY"
			, "MIN_LOAD_TIME"."OPPORTUNITYLINEITEM_HKEY" AS "OPPORTUNITYLINEITEM_HKEY"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."USER_CREATEDBYID_HKEY"
		, "MIV"."OPPORTUNITYLINEITEM_HKEY"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" AS "LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."USER_CREATEDBYID_HKEY" AS "USER_CREATEDBYID_HKEY"
			, "STG_SRC1"."OPPORTUNITYLINEITEM_HKEY" AS "OPPORTUNITYLINEITEM_HKEY"
		FROM {{ ref('SALESFORCE_STG_OPPORTUNITYLINEITEM') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" AS "LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."USER_CREATEDBYID_HKEY" AS "USER_CREATEDBYID_HKEY"
			, "CHANGE_SET"."OPPORTUNITYLINEITEM_HKEY" AS "OPPORTUNITYLINEITEM_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" ORDER BY "CHANGE_SET"."LOAD_CYCLE_ID",
				"CHANGE_SET"."LOAD_DATE") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY" AS "LNK_OPPORTUNITYLINEITEM_USER_CREATEDBYID_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."USER_CREATEDBYID_HKEY" AS "USER_CREATEDBYID_HKEY"
		, "MIN_LOAD_TIME"."OPPORTUNITYLINEITEM_HKEY" AS "OPPORTUNITYLINEITEM_HKEY"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'