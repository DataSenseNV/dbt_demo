{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_CONTACT',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_CONTACT_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."CONTACT_HKEY" AS "CONTACT_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "DVT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "DVT_SRC"."REPORTSTOID" AS "REPORTSTOID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."OWNERID" AS "OWNERID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."SALUTATION" AS "SALUTATION"
		, "DVT_SRC"."FIRSTNAME" AS "FIRSTNAME"
		, "DVT_SRC"."LASTNAME" AS "LASTNAME"
		, "DVT_SRC"."OTHERSTREET" AS "OTHERSTREET"
		, "DVT_SRC"."OTHERCITY" AS "OTHERCITY"
		, "DVT_SRC"."OTHERSTATE" AS "OTHERSTATE"
		, "DVT_SRC"."OTHERPOSTALCODE" AS "OTHERPOSTALCODE"
		, "DVT_SRC"."OTHERCOUNTRY" AS "OTHERCOUNTRY"
		, "DVT_SRC"."OTHERLATITUDE" AS "OTHERLATITUDE"
		, "DVT_SRC"."OTHERLONGITUDE" AS "OTHERLONGITUDE"
		, "DVT_SRC"."OTHERGEOCODEACCURACY" AS "OTHERGEOCODEACCURACY"
		, "DVT_SRC"."MAILINGSTREET" AS "MAILINGSTREET"
		, "DVT_SRC"."MAILINGCITY" AS "MAILINGCITY"
		, "DVT_SRC"."MAILINGSTATE" AS "MAILINGSTATE"
		, "DVT_SRC"."MAILINGPOSTALCODE" AS "MAILINGPOSTALCODE"
		, "DVT_SRC"."MAILINGCOUNTRY" AS "MAILINGCOUNTRY"
		, "DVT_SRC"."MAILINGLATITUDE" AS "MAILINGLATITUDE"
		, "DVT_SRC"."MAILINGLONGITUDE" AS "MAILINGLONGITUDE"
		, "DVT_SRC"."MAILINGGEOCODEACCURACY" AS "MAILINGGEOCODEACCURACY"
		, "DVT_SRC"."PHONE" AS "PHONE"
		, "DVT_SRC"."FAX" AS "FAX"
		, "DVT_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
		, "DVT_SRC"."HOMEPHONE" AS "HOMEPHONE"
		, "DVT_SRC"."OTHERPHONE" AS "OTHERPHONE"
		, "DVT_SRC"."ASSISTANTPHONE" AS "ASSISTANTPHONE"
		, "DVT_SRC"."EMAIL" AS "EMAIL"
		, "DVT_SRC"."TITLE" AS "TITLE"
		, "DVT_SRC"."DEPARTMENT" AS "DEPARTMENT"
		, "DVT_SRC"."ASSISTANTNAME" AS "ASSISTANTNAME"
		, "DVT_SRC"."LEADSOURCE" AS "LEADSOURCE"
		, "DVT_SRC"."BIRTHDATE" AS "BIRTHDATE"
		, "DVT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "DVT_SRC"."HASOPTEDOUTOFEMAIL" AS "HASOPTEDOUTOFEMAIL"
		, "DVT_SRC"."HASOPTEDOUTOFFAX" AS "HASOPTEDOUTOFFAX"
		, "DVT_SRC"."DONOTCALL" AS "DONOTCALL"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "DVT_SRC"."LASTCUREQUESTDATE" AS "LASTCUREQUESTDATE"
		, "DVT_SRC"."LASTCUUPDATEDATE" AS "LASTCUUPDATEDATE"
		, "DVT_SRC"."EMAILBOUNCEDREASON" AS "EMAILBOUNCEDREASON"
		, "DVT_SRC"."EMAILBOUNCEDDATE" AS "EMAILBOUNCEDDATE"
		, "DVT_SRC"."JIGSAW" AS "JIGSAW"
		, "DVT_SRC"."JIGSAWCONTACTID" AS "JIGSAWCONTACTID"
		, "DVT_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_CONTACT') }} "DVT_SRC"
