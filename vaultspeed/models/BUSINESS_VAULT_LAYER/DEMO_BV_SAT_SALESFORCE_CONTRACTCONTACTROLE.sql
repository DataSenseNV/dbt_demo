{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_CONTRACTCONTACTROLE',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_CONTRACTCONTACTROLE_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CONTRACTCONTACTROLE_HKEY" AS "CONTRACTCONTACTROLE_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."CONTRACTID" AS "CONTRACTID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."CONTACTID" AS "CONTACTID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."ROLE" AS "ROLE"
		, "DVT_SRC"."ISPRIMARY" AS "ISPRIMARY"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_CONTRACTCONTACTROLE') }} "DVT_SRC"
