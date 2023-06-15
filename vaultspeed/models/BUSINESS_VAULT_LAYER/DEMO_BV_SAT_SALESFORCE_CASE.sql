{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_CASE',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_CASE_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CASE_HKEY" AS "CASE_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."ASSETID" AS "ASSETID"
		, "DVT_SRC"."CONTACTID" AS "CONTACTID"
		, "DVT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."PARENTID" AS "PARENTID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "DVT_SRC"."CASENUMBER" AS "CASENUMBER"
		, "DVT_SRC"."SOURCEID" AS "SOURCEID"
		, "DVT_SRC"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
		, "DVT_SRC"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
		, "DVT_SRC"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
		, "DVT_SRC"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
		, "DVT_SRC"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
		, "DVT_SRC"."TYPE" AS "TYPE"
		, "DVT_SRC"."STATUS" AS "STATUS"
		, "DVT_SRC"."REASON" AS "REASON"
		, "DVT_SRC"."ORIGIN" AS "ORIGIN"
		, "DVT_SRC"."SUBJECT" AS "SUBJECT"
		, "DVT_SRC"."PRIORITY" AS "PRIORITY"
		, "DVT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "DVT_SRC"."ISCLOSED" AS "ISCLOSED"
		, "DVT_SRC"."CLOSEDDATE" AS "CLOSEDDATE"
		, "DVT_SRC"."ISESCALATED" AS "ISESCALATED"
		, "DVT_SRC"."OWNERID" AS "OWNERID"
		, "DVT_SRC"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
		, "DVT_SRC"."CSAT__C" AS "CSAT__C"
		, "DVT_SRC"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
		, "DVT_SRC"."FCR__C" AS "FCR__C"
		, "DVT_SRC"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
		, "DVT_SRC"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
		, "DVT_SRC"."SLA_TYPE__C" AS "SLA_TYPE__C"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_CASE') }} "DVT_SRC"
