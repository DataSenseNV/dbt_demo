{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_ORDERITEM',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_ORDERITEM_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ORDERITEM_HKEY" AS "ORDERITEM_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."ORIGINALORDERITEMID" AS "ORIGINALORDERITEMID"
		, "DVT_SRC"."ORDERID" AS "ORDERID"
		, "DVT_SRC"."PRICEBOOKENTRYID" AS "PRICEBOOKENTRYID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."AVAILABLEQUANTITY" AS "AVAILABLEQUANTITY"
		, "DVT_SRC"."QUANTITY" AS "QUANTITY"
		, "DVT_SRC"."UNITPRICE" AS "UNITPRICE"
		, "DVT_SRC"."LISTPRICE" AS "LISTPRICE"
		, "DVT_SRC"."SERVICEDATE" AS "SERVICEDATE"
		, "DVT_SRC"."ENDDATE" AS "ENDDATE"
		, "DVT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."ORDERITEMNUMBER" AS "ORDERITEMNUMBER"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_ORDERITEM') }} "DVT_SRC"
