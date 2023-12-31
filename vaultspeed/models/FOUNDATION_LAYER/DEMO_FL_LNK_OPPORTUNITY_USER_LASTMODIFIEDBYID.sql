{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID',
		schema='DEMO_FL',
		unique_key = ['"LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"'],
		merge_update_columns = [],
		tags=['SALESFORCE', 'LNK_SALESFORCE_OPPORTUNITY_USER_LASTMODIFIEDBYID_INCR', 'LNK_SALESFORCE_OPPORTUNITY_USER_LASTMODIFIEDBYID_INIT']
	)
}}
select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" AS "LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."USER_LASTMODIFIEDBYID_HKEY" AS "USER_LASTMODIFIEDBYID_HKEY"
			, "STG_SRC1"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
			, 0 AS "LOGPOSITION"
		FROM {{ ref('SALESFORCE_STG_OPPORTUNITY') }} "STG_SRC1"
		WHERE  "STG_SRC1"."RECORD_TYPE" = 'S'
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" AS "LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."USER_LASTMODIFIEDBYID_HKEY" AS "USER_LASTMODIFIEDBYID_HKEY"
			, "CHANGE_SET"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" ORDER BY "CHANGE_SET"."LOAD_DATE",
				"CHANGE_SET"."LOGPOSITION") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	, "MIV" AS 
	( 
		SELECT 
			  "MIN_LOAD_TIME"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" AS "LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"
			, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
			, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "MIN_LOAD_TIME"."USER_LASTMODIFIEDBYID_HKEY" AS "USER_LASTMODIFIEDBYID_HKEY"
			, "MIN_LOAD_TIME"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
		FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
		WHERE  "MIN_LOAD_TIME"."DUMMY" = 1
	)
	SELECT 
		  "MIV"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"
		, "MIV"."LOAD_DATE"
		, "MIV"."LOAD_CYCLE_ID"
		, "MIV"."USER_LASTMODIFIEDBYID_HKEY"
		, "MIV"."OPPORTUNITY_HKEY"
	FROM "MIV"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "CHANGE_SET" AS 
	( 
		SELECT 
			  "STG_SRC1"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" AS "LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"
			, "STG_SRC1"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_SRC1"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "STG_SRC1"."USER_LASTMODIFIEDBYID_HKEY" AS "USER_LASTMODIFIEDBYID_HKEY"
			, "STG_SRC1"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
		FROM {{ ref('SALESFORCE_STG_OPPORTUNITY') }} "STG_SRC1"
	)
	, "MIN_LOAD_TIME" AS 
	( 
		SELECT 
			  "CHANGE_SET"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" AS "LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"
			, "CHANGE_SET"."LOAD_DATE" AS "LOAD_DATE"
			, "CHANGE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CHANGE_SET"."USER_LASTMODIFIEDBYID_HKEY" AS "USER_LASTMODIFIEDBYID_HKEY"
			, "CHANGE_SET"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
			, ROW_NUMBER()OVER(PARTITION BY "CHANGE_SET"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" ORDER BY "CHANGE_SET"."LOAD_CYCLE_ID",
				"CHANGE_SET"."LOAD_DATE") AS "DUMMY"
		FROM "CHANGE_SET" "CHANGE_SET"
	)
	SELECT 
		  "MIN_LOAD_TIME"."LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY" AS "LNK_OPPORTUNITY_USER_LASTMODIFIEDBYID_HKEY"
		, "MIN_LOAD_TIME"."LOAD_DATE" AS "LOAD_DATE"
		, "MIN_LOAD_TIME"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "MIN_LOAD_TIME"."USER_LASTMODIFIEDBYID_HKEY" AS "USER_LASTMODIFIEDBYID_HKEY"
		, "MIN_LOAD_TIME"."OPPORTUNITY_HKEY" AS "OPPORTUNITY_HKEY"
	FROM "MIN_LOAD_TIME" "MIN_LOAD_TIME"
	WHERE  "MIN_LOAD_TIME"."DUMMY" = 1

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'