{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_CUSTOMER__C',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_CUSTOMER__C_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CUSTOMER__C_HKEY" AS "CUSTOMER__C_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."OWNERID" AS "OWNERID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."NAME" AS "NAME"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
		, "DVT_SRC"."GENDER__C" AS "GENDER__C"
		, "DVT_SRC"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
		, "DVT_SRC"."PARTNER__C" AS "PARTNER__C"
		, "DVT_SRC"."DEPENDENTS__C" AS "DEPENDENTS__C"
		, "DVT_SRC"."TENURE__C" AS "TENURE__C"
		, "DVT_SRC"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
		, "DVT_SRC"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
		, "DVT_SRC"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
		, "DVT_SRC"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
		, "DVT_SRC"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
		, "DVT_SRC"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
		, "DVT_SRC"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
		, "DVT_SRC"."STREAMING_TV__C" AS "STREAMING_TV__C"
		, "DVT_SRC"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
		, "DVT_SRC"."CONTRACT__C" AS "CONTRACT__C"
		, "DVT_SRC"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
		, "DVT_SRC"."CHURN__C" AS "CHURN__C"
		, "DVT_SRC"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_CUSTOMER__C') }} "DVT_SRC"
