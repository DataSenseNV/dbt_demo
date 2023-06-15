{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='CASE',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'STG_SALESFORCE_CASE_INCR', 'STG_SALESFORCE_CASE_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "CASE_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_PARENTID_BK" || '\#' )) AS "CASE_PARENTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CONTACTID_BK" || '\#' )) AS "CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ASSETID_BK" || '\#' )) AS "ASSET_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_PARENTID_BK" || '\#' )) AS "LNK_CASE_CASE_PARENTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_CASE_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_CASE_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_CASE_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CONTACTID_BK" || '\#' )) AS "LNK_CASE_CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ASSETID_BK" || '\#' )) AS "LNK_CASE_ASSET_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."CONTACTID" AS "CONTACTID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."ASSETID" AS "ASSETID"
		, "EXT_SRC"."PARENTID" AS "PARENTID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "EXT_SRC"."ID_FK_ASSETID_BK" AS "ID_FK_ASSETID_BK"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_SRC"."CASENUMBER" AS "CASENUMBER"
		, "EXT_SRC"."SOURCEID" AS "SOURCEID"
		, "EXT_SRC"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
		, "EXT_SRC"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
		, "EXT_SRC"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
		, "EXT_SRC"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
		, "EXT_SRC"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
		, "EXT_SRC"."TYPE" AS "TYPE"
		, "EXT_SRC"."STATUS" AS "STATUS"
		, "EXT_SRC"."REASON" AS "REASON"
		, "EXT_SRC"."ORIGIN" AS "ORIGIN"
		, "EXT_SRC"."SUBJECT" AS "SUBJECT"
		, "EXT_SRC"."PRIORITY" AS "PRIORITY"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."ISCLOSED" AS "ISCLOSED"
		, "EXT_SRC"."CLOSEDDATE" AS "CLOSEDDATE"
		, "EXT_SRC"."ISESCALATED" AS "ISESCALATED"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
		, "EXT_SRC"."CSAT__C" AS "CSAT__C"
		, "EXT_SRC"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
		, "EXT_SRC"."FCR__C" AS "FCR__C"
		, "EXT_SRC"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
		, "EXT_SRC"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
		, "EXT_SRC"."SLA_TYPE__C" AS "SLA_TYPE__C"
	FROM {{ ref('SALESFORCE_EXT_CASE') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "CASE_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_PARENTID_BK" || '\#' )) AS "CASE_PARENTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CONTACTID_BK" || '\#' )) AS "CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ASSETID_BK" || '\#' )) AS "ASSET_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_PARENTID_BK" || '\#' )) AS "LNK_CASE_CASE_PARENTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_CASE_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_CASE_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_CASE_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CONTACTID_BK" || '\#' )) AS "LNK_CASE_CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ASSETID_BK" || '\#' )) AS "LNK_CASE_ASSET_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."CONTACTID" AS "CONTACTID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."ASSETID" AS "ASSETID"
		, "EXT_SRC"."PARENTID" AS "PARENTID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "EXT_SRC"."ID_FK_ASSETID_BK" AS "ID_FK_ASSETID_BK"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_SRC"."CASENUMBER" AS "CASENUMBER"
		, "EXT_SRC"."SOURCEID" AS "SOURCEID"
		, "EXT_SRC"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
		, "EXT_SRC"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
		, "EXT_SRC"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
		, "EXT_SRC"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
		, "EXT_SRC"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
		, "EXT_SRC"."TYPE" AS "TYPE"
		, "EXT_SRC"."STATUS" AS "STATUS"
		, "EXT_SRC"."REASON" AS "REASON"
		, "EXT_SRC"."ORIGIN" AS "ORIGIN"
		, "EXT_SRC"."SUBJECT" AS "SUBJECT"
		, "EXT_SRC"."PRIORITY" AS "PRIORITY"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."ISCLOSED" AS "ISCLOSED"
		, "EXT_SRC"."CLOSEDDATE" AS "CLOSEDDATE"
		, "EXT_SRC"."ISESCALATED" AS "ISESCALATED"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
		, "EXT_SRC"."CSAT__C" AS "CSAT__C"
		, "EXT_SRC"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
		, "EXT_SRC"."FCR__C" AS "FCR__C"
		, "EXT_SRC"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
		, "EXT_SRC"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
		, "EXT_SRC"."SLA_TYPE__C" AS "SLA_TYPE__C"
	FROM {{ ref('SALESFORCE_EXT_CASE') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'