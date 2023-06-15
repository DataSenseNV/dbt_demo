{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_OPPORTUNITYLINEITEM',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_OPPORTUNITYLINEITEM_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."OPPORTUNITYLINEITEM_HKEY" AS "OPPORTUNITYLINEITEM_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."PRICEBOOKENTRYID" AS "PRICEBOOKENTRYID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."SORTORDER" AS "SORTORDER"
		, "DVT_SRC"."QUANTITY" AS "QUANTITY"
		, "DVT_SRC"."DISCOUNT" AS "DISCOUNT"
		, "DVT_SRC"."TOTALPRICE" AS "TOTALPRICE"
		, "DVT_SRC"."UNITPRICE" AS "UNITPRICE"
		, "DVT_SRC"."SERVICEDATE" AS "SERVICEDATE"
		, "DVT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_OPPORTUNITYLINEITEM') }} "DVT_SRC"
