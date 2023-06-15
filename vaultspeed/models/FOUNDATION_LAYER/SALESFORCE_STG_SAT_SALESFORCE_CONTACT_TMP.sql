{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_SALESFORCE_CONTACT_TMP',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'SAT_SALESFORCE_CONTACT_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT DISTINCT 
 			  "STG_DIS_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		FROM {{ ref('SALESFORCE_STG_CONTACT') }} "STG_DIS_SRC"
		WHERE  "STG_DIS_SRC"."RECORD_TYPE" = 'S'
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ISDELETED"),'~'),'\#','\\' || 
				'\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SALUTATION"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."FIRSTNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERSTREET"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERCITY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERSTATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERPOSTALCODE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERCOUNTRY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERLATITUDE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERLONGITUDE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERGEOCODEACCURACY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGSTREET"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGCITY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGSTATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGPOSTALCODE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGCOUNTRY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGLATITUDE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGLONGITUDE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MAILINGGEOCODEACCURACY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."PHONE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."FAX"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MOBILEPHONE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."HOMEPHONE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OTHERPHONE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ASSISTANTPHONE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EMAIL"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."TITLE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DEPARTMENT"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ASSISTANTNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LEADSOURCE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."BIRTHDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DESCRIPTION"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."HASOPTEDOUTOFEMAIL"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."HASOPTEDOUTOFFAX"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DONOTCALL"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CREATEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SYSTEMMODSTAMP"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTACTIVITYDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTCUREQUESTDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTCUUPDATEDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EMAILBOUNCEDREASON"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EMAILBOUNCEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."JIGSAW"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."JIGSAWCONTACTID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."INDIVIDUALID"),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."ID" AS "ID"
			, "STG_TEMP_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "STG_TEMP_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "STG_TEMP_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
			, "STG_TEMP_SRC"."REPORTSTOID" AS "REPORTSTOID"
			, "STG_TEMP_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "STG_TEMP_SRC"."OWNERID" AS "OWNERID"
			, "STG_TEMP_SRC"."ISDELETED" AS "ISDELETED"
			, "STG_TEMP_SRC"."SALUTATION" AS "SALUTATION"
			, "STG_TEMP_SRC"."FIRSTNAME" AS "FIRSTNAME"
			, "STG_TEMP_SRC"."LASTNAME" AS "LASTNAME"
			, "STG_TEMP_SRC"."OTHERSTREET" AS "OTHERSTREET"
			, "STG_TEMP_SRC"."OTHERCITY" AS "OTHERCITY"
			, "STG_TEMP_SRC"."OTHERSTATE" AS "OTHERSTATE"
			, "STG_TEMP_SRC"."OTHERPOSTALCODE" AS "OTHERPOSTALCODE"
			, "STG_TEMP_SRC"."OTHERCOUNTRY" AS "OTHERCOUNTRY"
			, "STG_TEMP_SRC"."OTHERLATITUDE" AS "OTHERLATITUDE"
			, "STG_TEMP_SRC"."OTHERLONGITUDE" AS "OTHERLONGITUDE"
			, "STG_TEMP_SRC"."OTHERGEOCODEACCURACY" AS "OTHERGEOCODEACCURACY"
			, "STG_TEMP_SRC"."MAILINGSTREET" AS "MAILINGSTREET"
			, "STG_TEMP_SRC"."MAILINGCITY" AS "MAILINGCITY"
			, "STG_TEMP_SRC"."MAILINGSTATE" AS "MAILINGSTATE"
			, "STG_TEMP_SRC"."MAILINGPOSTALCODE" AS "MAILINGPOSTALCODE"
			, "STG_TEMP_SRC"."MAILINGCOUNTRY" AS "MAILINGCOUNTRY"
			, "STG_TEMP_SRC"."MAILINGLATITUDE" AS "MAILINGLATITUDE"
			, "STG_TEMP_SRC"."MAILINGLONGITUDE" AS "MAILINGLONGITUDE"
			, "STG_TEMP_SRC"."MAILINGGEOCODEACCURACY" AS "MAILINGGEOCODEACCURACY"
			, "STG_TEMP_SRC"."PHONE" AS "PHONE"
			, "STG_TEMP_SRC"."FAX" AS "FAX"
			, "STG_TEMP_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
			, "STG_TEMP_SRC"."HOMEPHONE" AS "HOMEPHONE"
			, "STG_TEMP_SRC"."OTHERPHONE" AS "OTHERPHONE"
			, "STG_TEMP_SRC"."ASSISTANTPHONE" AS "ASSISTANTPHONE"
			, "STG_TEMP_SRC"."EMAIL" AS "EMAIL"
			, "STG_TEMP_SRC"."TITLE" AS "TITLE"
			, "STG_TEMP_SRC"."DEPARTMENT" AS "DEPARTMENT"
			, "STG_TEMP_SRC"."ASSISTANTNAME" AS "ASSISTANTNAME"
			, "STG_TEMP_SRC"."LEADSOURCE" AS "LEADSOURCE"
			, "STG_TEMP_SRC"."BIRTHDATE" AS "BIRTHDATE"
			, "STG_TEMP_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "STG_TEMP_SRC"."HASOPTEDOUTOFEMAIL" AS "HASOPTEDOUTOFEMAIL"
			, "STG_TEMP_SRC"."HASOPTEDOUTOFFAX" AS "HASOPTEDOUTOFFAX"
			, "STG_TEMP_SRC"."DONOTCALL" AS "DONOTCALL"
			, "STG_TEMP_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "STG_TEMP_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "STG_TEMP_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "STG_TEMP_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "STG_TEMP_SRC"."LASTCUREQUESTDATE" AS "LASTCUREQUESTDATE"
			, "STG_TEMP_SRC"."LASTCUUPDATEDATE" AS "LASTCUUPDATEDATE"
			, "STG_TEMP_SRC"."EMAILBOUNCEDREASON" AS "EMAILBOUNCEDREASON"
			, "STG_TEMP_SRC"."EMAILBOUNCEDDATE" AS "EMAILBOUNCEDDATE"
			, "STG_TEMP_SRC"."JIGSAW" AS "JIGSAW"
			, "STG_TEMP_SRC"."JIGSAWCONTACTID" AS "JIGSAWCONTACTID"
			, "STG_TEMP_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
		FROM {{ ref('SALESFORCE_STG_CONTACT') }} "STG_TEMP_SRC"
		WHERE  "STG_TEMP_SRC"."RECORD_TYPE" = 'S'
		UNION ALL 
		SELECT 
			  "SAT_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
			, "SAT_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "SAT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "SAT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
			, 'SAT' AS "RECORD_TYPE"
			, 'SAT' AS "SOURCE"
			, 0 AS "ORIGIN_ID"
			, "SAT_SRC"."HASH_DIFF" AS "HASH_DIFF"
			, "SAT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
			, "SAT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "SAT_SRC"."ID" AS "ID"
			, "SAT_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "SAT_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "SAT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
			, "SAT_SRC"."REPORTSTOID" AS "REPORTSTOID"
			, "SAT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "SAT_SRC"."OWNERID" AS "OWNERID"
			, "SAT_SRC"."ISDELETED" AS "ISDELETED"
			, "SAT_SRC"."SALUTATION" AS "SALUTATION"
			, "SAT_SRC"."FIRSTNAME" AS "FIRSTNAME"
			, "SAT_SRC"."LASTNAME" AS "LASTNAME"
			, "SAT_SRC"."OTHERSTREET" AS "OTHERSTREET"
			, "SAT_SRC"."OTHERCITY" AS "OTHERCITY"
			, "SAT_SRC"."OTHERSTATE" AS "OTHERSTATE"
			, "SAT_SRC"."OTHERPOSTALCODE" AS "OTHERPOSTALCODE"
			, "SAT_SRC"."OTHERCOUNTRY" AS "OTHERCOUNTRY"
			, "SAT_SRC"."OTHERLATITUDE" AS "OTHERLATITUDE"
			, "SAT_SRC"."OTHERLONGITUDE" AS "OTHERLONGITUDE"
			, "SAT_SRC"."OTHERGEOCODEACCURACY" AS "OTHERGEOCODEACCURACY"
			, "SAT_SRC"."MAILINGSTREET" AS "MAILINGSTREET"
			, "SAT_SRC"."MAILINGCITY" AS "MAILINGCITY"
			, "SAT_SRC"."MAILINGSTATE" AS "MAILINGSTATE"
			, "SAT_SRC"."MAILINGPOSTALCODE" AS "MAILINGPOSTALCODE"
			, "SAT_SRC"."MAILINGCOUNTRY" AS "MAILINGCOUNTRY"
			, "SAT_SRC"."MAILINGLATITUDE" AS "MAILINGLATITUDE"
			, "SAT_SRC"."MAILINGLONGITUDE" AS "MAILINGLONGITUDE"
			, "SAT_SRC"."MAILINGGEOCODEACCURACY" AS "MAILINGGEOCODEACCURACY"
			, "SAT_SRC"."PHONE" AS "PHONE"
			, "SAT_SRC"."FAX" AS "FAX"
			, "SAT_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
			, "SAT_SRC"."HOMEPHONE" AS "HOMEPHONE"
			, "SAT_SRC"."OTHERPHONE" AS "OTHERPHONE"
			, "SAT_SRC"."ASSISTANTPHONE" AS "ASSISTANTPHONE"
			, "SAT_SRC"."EMAIL" AS "EMAIL"
			, "SAT_SRC"."TITLE" AS "TITLE"
			, "SAT_SRC"."DEPARTMENT" AS "DEPARTMENT"
			, "SAT_SRC"."ASSISTANTNAME" AS "ASSISTANTNAME"
			, "SAT_SRC"."LEADSOURCE" AS "LEADSOURCE"
			, "SAT_SRC"."BIRTHDATE" AS "BIRTHDATE"
			, "SAT_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "SAT_SRC"."HASOPTEDOUTOFEMAIL" AS "HASOPTEDOUTOFEMAIL"
			, "SAT_SRC"."HASOPTEDOUTOFFAX" AS "HASOPTEDOUTOFFAX"
			, "SAT_SRC"."DONOTCALL" AS "DONOTCALL"
			, "SAT_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "SAT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "SAT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "SAT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "SAT_SRC"."LASTCUREQUESTDATE" AS "LASTCUREQUESTDATE"
			, "SAT_SRC"."LASTCUUPDATEDATE" AS "LASTCUUPDATEDATE"
			, "SAT_SRC"."EMAILBOUNCEDREASON" AS "EMAILBOUNCEDREASON"
			, "SAT_SRC"."EMAILBOUNCEDDATE" AS "EMAILBOUNCEDDATE"
			, "SAT_SRC"."JIGSAW" AS "JIGSAW"
			, "SAT_SRC"."JIGSAWCONTACTID" AS "JIGSAWCONTACTID"
			, "SAT_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
		FROM {{ source('DEMO_FL', 'SAT_SALESFORCE_CONTACT') }} "SAT_SRC"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "SAT_SRC"."CONTACT_HKEY" = "DIST_STG"."CONTACT_HKEY"
		WHERE  "SAT_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."CONTACT_HKEY" AS "CONTACT_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."CONTACT_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."HASH_DIFF" AS "HASH_DIFF"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."ID" AS "ID"
		, "TEMP_TABLE_SET"."CREATEDBYID" AS "CREATEDBYID"
		, "TEMP_TABLE_SET"."ACCOUNTID" AS "ACCOUNTID"
		, "TEMP_TABLE_SET"."MASTERRECORDID" AS "MASTERRECORDID"
		, "TEMP_TABLE_SET"."REPORTSTOID" AS "REPORTSTOID"
		, "TEMP_TABLE_SET"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "TEMP_TABLE_SET"."OWNERID" AS "OWNERID"
		, "TEMP_TABLE_SET"."ISDELETED" AS "ISDELETED"
		, "TEMP_TABLE_SET"."SALUTATION" AS "SALUTATION"
		, "TEMP_TABLE_SET"."FIRSTNAME" AS "FIRSTNAME"
		, "TEMP_TABLE_SET"."LASTNAME" AS "LASTNAME"
		, "TEMP_TABLE_SET"."OTHERSTREET" AS "OTHERSTREET"
		, "TEMP_TABLE_SET"."OTHERCITY" AS "OTHERCITY"
		, "TEMP_TABLE_SET"."OTHERSTATE" AS "OTHERSTATE"
		, "TEMP_TABLE_SET"."OTHERPOSTALCODE" AS "OTHERPOSTALCODE"
		, "TEMP_TABLE_SET"."OTHERCOUNTRY" AS "OTHERCOUNTRY"
		, "TEMP_TABLE_SET"."OTHERLATITUDE" AS "OTHERLATITUDE"
		, "TEMP_TABLE_SET"."OTHERLONGITUDE" AS "OTHERLONGITUDE"
		, "TEMP_TABLE_SET"."OTHERGEOCODEACCURACY" AS "OTHERGEOCODEACCURACY"
		, "TEMP_TABLE_SET"."MAILINGSTREET" AS "MAILINGSTREET"
		, "TEMP_TABLE_SET"."MAILINGCITY" AS "MAILINGCITY"
		, "TEMP_TABLE_SET"."MAILINGSTATE" AS "MAILINGSTATE"
		, "TEMP_TABLE_SET"."MAILINGPOSTALCODE" AS "MAILINGPOSTALCODE"
		, "TEMP_TABLE_SET"."MAILINGCOUNTRY" AS "MAILINGCOUNTRY"
		, "TEMP_TABLE_SET"."MAILINGLATITUDE" AS "MAILINGLATITUDE"
		, "TEMP_TABLE_SET"."MAILINGLONGITUDE" AS "MAILINGLONGITUDE"
		, "TEMP_TABLE_SET"."MAILINGGEOCODEACCURACY" AS "MAILINGGEOCODEACCURACY"
		, "TEMP_TABLE_SET"."PHONE" AS "PHONE"
		, "TEMP_TABLE_SET"."FAX" AS "FAX"
		, "TEMP_TABLE_SET"."MOBILEPHONE" AS "MOBILEPHONE"
		, "TEMP_TABLE_SET"."HOMEPHONE" AS "HOMEPHONE"
		, "TEMP_TABLE_SET"."OTHERPHONE" AS "OTHERPHONE"
		, "TEMP_TABLE_SET"."ASSISTANTPHONE" AS "ASSISTANTPHONE"
		, "TEMP_TABLE_SET"."EMAIL" AS "EMAIL"
		, "TEMP_TABLE_SET"."TITLE" AS "TITLE"
		, "TEMP_TABLE_SET"."DEPARTMENT" AS "DEPARTMENT"
		, "TEMP_TABLE_SET"."ASSISTANTNAME" AS "ASSISTANTNAME"
		, "TEMP_TABLE_SET"."LEADSOURCE" AS "LEADSOURCE"
		, "TEMP_TABLE_SET"."BIRTHDATE" AS "BIRTHDATE"
		, "TEMP_TABLE_SET"."DESCRIPTION" AS "DESCRIPTION"
		, "TEMP_TABLE_SET"."HASOPTEDOUTOFEMAIL" AS "HASOPTEDOUTOFEMAIL"
		, "TEMP_TABLE_SET"."HASOPTEDOUTOFFAX" AS "HASOPTEDOUTOFFAX"
		, "TEMP_TABLE_SET"."DONOTCALL" AS "DONOTCALL"
		, "TEMP_TABLE_SET"."CREATEDDATE" AS "CREATEDDATE"
		, "TEMP_TABLE_SET"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "TEMP_TABLE_SET"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "TEMP_TABLE_SET"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "TEMP_TABLE_SET"."LASTCUREQUESTDATE" AS "LASTCUREQUESTDATE"
		, "TEMP_TABLE_SET"."LASTCUUPDATEDATE" AS "LASTCUUPDATEDATE"
		, "TEMP_TABLE_SET"."EMAILBOUNCEDREASON" AS "EMAILBOUNCEDREASON"
		, "TEMP_TABLE_SET"."EMAILBOUNCEDDATE" AS "EMAILBOUNCEDDATE"
		, "TEMP_TABLE_SET"."JIGSAW" AS "JIGSAW"
		, "TEMP_TABLE_SET"."JIGSAWCONTACTID" AS "JIGSAWCONTACTID"
		, "TEMP_TABLE_SET"."INDIVIDUALID" AS "INDIVIDUALID"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'