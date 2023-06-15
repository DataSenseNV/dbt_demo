{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_PRICEBOOK2',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_PRICEBOOK2_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."PRICEBOOK2_HKEY" AS "PRICEBOOK2_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."NAME" AS "NAME"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."ISACTIVE" AS "ISACTIVE"
		, "DVT_SRC"."ISARCHIVED" AS "ISARCHIVED"
		, "DVT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "DVT_SRC"."ISSTANDARD" AS "ISSTANDARD"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_PRICEBOOK2') }} "DVT_SRC"
