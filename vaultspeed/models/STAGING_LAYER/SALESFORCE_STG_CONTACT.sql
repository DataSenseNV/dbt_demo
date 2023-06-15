{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='CONTACT',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'STG_SALESFORCE_CONTACT_INCR', 'STG_SALESFORCE_CONTACT_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_REPORTSTOID_BK" || '\#' )) AS "CONTACT_REPORTSTOID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || '\#' )) AS "CONTACT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "LNK_CONTACT_USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_CONTACT_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_REPORTSTOID_BK" || '\#' )) AS "LNK_CONTACT_CONTACT_REPORTSTOID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || '\#' )) AS "LNK_CONTACT_CONTACT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_CONTACT_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_CONTACT_USER_CREATEDBYID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."REPORTSTOID" AS "REPORTSTOID"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_REPORTSTOID_BK" AS "ID_FK_REPORTSTOID_BK"
		, "EXT_SRC"."ID_FK_MASTERRECORDID_BK" AS "ID_FK_MASTERRECORDID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."SALUTATION" AS "SALUTATION"
		, "EXT_SRC"."FIRSTNAME" AS "FIRSTNAME"
		, "EXT_SRC"."LASTNAME" AS "LASTNAME"
		, "EXT_SRC"."OTHERSTREET" AS "OTHERSTREET"
		, "EXT_SRC"."OTHERCITY" AS "OTHERCITY"
		, "EXT_SRC"."OTHERSTATE" AS "OTHERSTATE"
		, "EXT_SRC"."OTHERPOSTALCODE" AS "OTHERPOSTALCODE"
		, "EXT_SRC"."OTHERCOUNTRY" AS "OTHERCOUNTRY"
		, "EXT_SRC"."OTHERLATITUDE" AS "OTHERLATITUDE"
		, "EXT_SRC"."OTHERLONGITUDE" AS "OTHERLONGITUDE"
		, "EXT_SRC"."OTHERGEOCODEACCURACY" AS "OTHERGEOCODEACCURACY"
		, "EXT_SRC"."MAILINGSTREET" AS "MAILINGSTREET"
		, "EXT_SRC"."MAILINGCITY" AS "MAILINGCITY"
		, "EXT_SRC"."MAILINGSTATE" AS "MAILINGSTATE"
		, "EXT_SRC"."MAILINGPOSTALCODE" AS "MAILINGPOSTALCODE"
		, "EXT_SRC"."MAILINGCOUNTRY" AS "MAILINGCOUNTRY"
		, "EXT_SRC"."MAILINGLATITUDE" AS "MAILINGLATITUDE"
		, "EXT_SRC"."MAILINGLONGITUDE" AS "MAILINGLONGITUDE"
		, "EXT_SRC"."MAILINGGEOCODEACCURACY" AS "MAILINGGEOCODEACCURACY"
		, "EXT_SRC"."PHONE" AS "PHONE"
		, "EXT_SRC"."FAX" AS "FAX"
		, "EXT_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
		, "EXT_SRC"."HOMEPHONE" AS "HOMEPHONE"
		, "EXT_SRC"."OTHERPHONE" AS "OTHERPHONE"
		, "EXT_SRC"."ASSISTANTPHONE" AS "ASSISTANTPHONE"
		, "EXT_SRC"."EMAIL" AS "EMAIL"
		, "EXT_SRC"."TITLE" AS "TITLE"
		, "EXT_SRC"."DEPARTMENT" AS "DEPARTMENT"
		, "EXT_SRC"."ASSISTANTNAME" AS "ASSISTANTNAME"
		, "EXT_SRC"."LEADSOURCE" AS "LEADSOURCE"
		, "EXT_SRC"."BIRTHDATE" AS "BIRTHDATE"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."HASOPTEDOUTOFEMAIL" AS "HASOPTEDOUTOFEMAIL"
		, "EXT_SRC"."HASOPTEDOUTOFFAX" AS "HASOPTEDOUTOFFAX"
		, "EXT_SRC"."DONOTCALL" AS "DONOTCALL"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "EXT_SRC"."LASTCUREQUESTDATE" AS "LASTCUREQUESTDATE"
		, "EXT_SRC"."LASTCUUPDATEDATE" AS "LASTCUUPDATEDATE"
		, "EXT_SRC"."EMAILBOUNCEDREASON" AS "EMAILBOUNCEDREASON"
		, "EXT_SRC"."EMAILBOUNCEDDATE" AS "EMAILBOUNCEDDATE"
		, "EXT_SRC"."JIGSAW" AS "JIGSAW"
		, "EXT_SRC"."JIGSAWCONTACTID" AS "JIGSAWCONTACTID"
		, "EXT_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
	FROM {{ ref('SALESFORCE_EXT_CONTACT') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_REPORTSTOID_BK" || '\#' )) AS "CONTACT_REPORTSTOID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || '\#' )) AS "CONTACT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "LNK_CONTACT_USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_CONTACT_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_REPORTSTOID_BK" || '\#' )) AS "LNK_CONTACT_CONTACT_REPORTSTOID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || '\#' )) AS "LNK_CONTACT_CONTACT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_CONTACT_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_CONTACT_USER_CREATEDBYID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."REPORTSTOID" AS "REPORTSTOID"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_REPORTSTOID_BK" AS "ID_FK_REPORTSTOID_BK"
		, "EXT_SRC"."ID_FK_MASTERRECORDID_BK" AS "ID_FK_MASTERRECORDID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."SALUTATION" AS "SALUTATION"
		, "EXT_SRC"."FIRSTNAME" AS "FIRSTNAME"
		, "EXT_SRC"."LASTNAME" AS "LASTNAME"
		, "EXT_SRC"."OTHERSTREET" AS "OTHERSTREET"
		, "EXT_SRC"."OTHERCITY" AS "OTHERCITY"
		, "EXT_SRC"."OTHERSTATE" AS "OTHERSTATE"
		, "EXT_SRC"."OTHERPOSTALCODE" AS "OTHERPOSTALCODE"
		, "EXT_SRC"."OTHERCOUNTRY" AS "OTHERCOUNTRY"
		, "EXT_SRC"."OTHERLATITUDE" AS "OTHERLATITUDE"
		, "EXT_SRC"."OTHERLONGITUDE" AS "OTHERLONGITUDE"
		, "EXT_SRC"."OTHERGEOCODEACCURACY" AS "OTHERGEOCODEACCURACY"
		, "EXT_SRC"."MAILINGSTREET" AS "MAILINGSTREET"
		, "EXT_SRC"."MAILINGCITY" AS "MAILINGCITY"
		, "EXT_SRC"."MAILINGSTATE" AS "MAILINGSTATE"
		, "EXT_SRC"."MAILINGPOSTALCODE" AS "MAILINGPOSTALCODE"
		, "EXT_SRC"."MAILINGCOUNTRY" AS "MAILINGCOUNTRY"
		, "EXT_SRC"."MAILINGLATITUDE" AS "MAILINGLATITUDE"
		, "EXT_SRC"."MAILINGLONGITUDE" AS "MAILINGLONGITUDE"
		, "EXT_SRC"."MAILINGGEOCODEACCURACY" AS "MAILINGGEOCODEACCURACY"
		, "EXT_SRC"."PHONE" AS "PHONE"
		, "EXT_SRC"."FAX" AS "FAX"
		, "EXT_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
		, "EXT_SRC"."HOMEPHONE" AS "HOMEPHONE"
		, "EXT_SRC"."OTHERPHONE" AS "OTHERPHONE"
		, "EXT_SRC"."ASSISTANTPHONE" AS "ASSISTANTPHONE"
		, "EXT_SRC"."EMAIL" AS "EMAIL"
		, "EXT_SRC"."TITLE" AS "TITLE"
		, "EXT_SRC"."DEPARTMENT" AS "DEPARTMENT"
		, "EXT_SRC"."ASSISTANTNAME" AS "ASSISTANTNAME"
		, "EXT_SRC"."LEADSOURCE" AS "LEADSOURCE"
		, "EXT_SRC"."BIRTHDATE" AS "BIRTHDATE"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."HASOPTEDOUTOFEMAIL" AS "HASOPTEDOUTOFEMAIL"
		, "EXT_SRC"."HASOPTEDOUTOFFAX" AS "HASOPTEDOUTOFFAX"
		, "EXT_SRC"."DONOTCALL" AS "DONOTCALL"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "EXT_SRC"."LASTCUREQUESTDATE" AS "LASTCUREQUESTDATE"
		, "EXT_SRC"."LASTCUUPDATEDATE" AS "LASTCUUPDATEDATE"
		, "EXT_SRC"."EMAILBOUNCEDREASON" AS "EMAILBOUNCEDREASON"
		, "EXT_SRC"."EMAILBOUNCEDDATE" AS "EMAILBOUNCEDDATE"
		, "EXT_SRC"."JIGSAW" AS "JIGSAW"
		, "EXT_SRC"."JIGSAWCONTACTID" AS "JIGSAWCONTACTID"
		, "EXT_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
	FROM {{ ref('SALESFORCE_EXT_CONTACT') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'