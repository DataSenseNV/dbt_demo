{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='SAT_SALESFORCE_USER_TMP',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'SAT_SALESFORCE_USER_INCR']
	)
}}
select * from (
	WITH "DIST_STG" AS 
	( 
		SELECT DISTINCT 
 			  "STG_DIS_SRC"."USER_HKEY" AS "USER_HKEY"
			, "STG_DIS_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		FROM {{ ref('SALESFORCE_STG_USER') }} "STG_DIS_SRC"
		WHERE  "STG_DIS_SRC"."RECORD_TYPE" = 'S'
	)
	, "TEMP_TABLE_SET" AS 
	( 
		SELECT 
			  "STG_TEMP_SRC"."USER_HKEY" AS "USER_HKEY"
			, "STG_TEMP_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "STG_TEMP_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, TO_TIMESTAMP('31/12/2999 23:59:59', 'DD/MM/YYYY HH24:MI:SS') AS "LOAD_END_DATE"
			, "STG_TEMP_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, 'STG' AS "SOURCE"
			, 1 AS "ORIGIN_ID"
			, UPPER(MD5_HEX(COALESCE(RTRIM( UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."USERNAME"),'~'),'\#','\\' || '\#')
				)|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."FIRSTNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."COMPANYNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DIVISION"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DEPARTMENT"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."TITLE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STREET"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CITY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."POSTALCODE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."COUNTRY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LATITUDE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LONGITUDE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."GEOCODEACCURACY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EMAIL"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SENDEREMAIL"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SENDERNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SIGNATURE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STAYINTOUCHSUBJECT"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STAYINTOUCHSIGNATURE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STAYINTOUCHNOTE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."PHONE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."FAX"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MOBILEPHONE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ALIAS"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."COMMUNITYNICKNAME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."ISACTIVE")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."ISSYSTEMCONTROLLED")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."TIMEZONESIDKEY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."USERROLEID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LOCALESIDKEY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."RECEIVESINFOEMAILS")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."RECEIVESADMININFOEMAILS")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EMAILENCODINGKEY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."PROFILEID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."USERTYPE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."USERSUBTYPE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."STARTDAY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ENDDAY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LANGUAGELOCALEKEY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EMPLOYEENUMBER"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DELEGATEDAPPROVERID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."MANAGERID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTLOGINDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTPASSWORDCHANGEDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CREATEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CREATEDBYID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LASTMODIFIEDBYID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SYSTEMMODSTAMP"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."NUMBEROFFAILEDLOGINS"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SUACCESSEXPIRATIONDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SUORGADMINEXPIRATIONDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OFFLINETRIALEXPIRATIONDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."WIRELESSTRIALEXPIRATIONDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."OFFLINEPDATRIALEXPIRATIONDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."FORECASTENABLED")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CONTACTID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ACCOUNTID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CALLCENTERID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."EXTENSION"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."FEDERATIONIDENTIFIER"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."ABOUTME"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."LOGINLIMIT"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."PROFILEPHOTOID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DIGESTFREQUENCY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."DEFAULTGROUPNOTIFICATIONFREQUENCY"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."WORKSPACEID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."SHARINGTYPE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CHATTERADOPTIONSTAGE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."CHATTERADOPTIONSTAGEMODIFIEDDATE"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."BANNERPHOTOID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( TO_CHAR("STG_TEMP_SRC"."ISPROFILEPHOTOACTIVE")),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."INDIVIDUALID"),'~'),'\#','\\' || '\#'))|| '\#' ||  UPPER(REPLACE(COALESCE(TRIM( "STG_TEMP_SRC"."GLOBALIDENTITY"),'~'),'\#','\\' || '\#'))|| '\#','\#' || '~'),'~') )) AS "HASH_DIFF"
			, CASE WHEN "STG_TEMP_SRC"."JRN_FLAG" = 'D' THEN CAST('Y' AS VARCHAR) ELSE CAST('N' AS VARCHAR) END AS "DELETE_FLAG"
			, "STG_TEMP_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "STG_TEMP_SRC"."ID" AS "ID"
			, "STG_TEMP_SRC"."USERNAME" AS "USERNAME"
			, "STG_TEMP_SRC"."FIRSTNAME" AS "FIRSTNAME"
			, "STG_TEMP_SRC"."LASTNAME" AS "LASTNAME"
			, "STG_TEMP_SRC"."COMPANYNAME" AS "COMPANYNAME"
			, "STG_TEMP_SRC"."DIVISION" AS "DIVISION"
			, "STG_TEMP_SRC"."DEPARTMENT" AS "DEPARTMENT"
			, "STG_TEMP_SRC"."TITLE" AS "TITLE"
			, "STG_TEMP_SRC"."STREET" AS "STREET"
			, "STG_TEMP_SRC"."CITY" AS "CITY"
			, "STG_TEMP_SRC"."STATE" AS "STATE"
			, "STG_TEMP_SRC"."POSTALCODE" AS "POSTALCODE"
			, "STG_TEMP_SRC"."COUNTRY" AS "COUNTRY"
			, "STG_TEMP_SRC"."LATITUDE" AS "LATITUDE"
			, "STG_TEMP_SRC"."LONGITUDE" AS "LONGITUDE"
			, "STG_TEMP_SRC"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
			, "STG_TEMP_SRC"."EMAIL" AS "EMAIL"
			, "STG_TEMP_SRC"."SENDEREMAIL" AS "SENDEREMAIL"
			, "STG_TEMP_SRC"."SENDERNAME" AS "SENDERNAME"
			, "STG_TEMP_SRC"."SIGNATURE" AS "SIGNATURE"
			, "STG_TEMP_SRC"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
			, "STG_TEMP_SRC"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
			, "STG_TEMP_SRC"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
			, "STG_TEMP_SRC"."PHONE" AS "PHONE"
			, "STG_TEMP_SRC"."FAX" AS "FAX"
			, "STG_TEMP_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
			, "STG_TEMP_SRC"."ALIAS" AS "ALIAS"
			, "STG_TEMP_SRC"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
			, "STG_TEMP_SRC"."ISACTIVE" AS "ISACTIVE"
			, "STG_TEMP_SRC"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
			, "STG_TEMP_SRC"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
			, "STG_TEMP_SRC"."USERROLEID" AS "USERROLEID"
			, "STG_TEMP_SRC"."LOCALESIDKEY" AS "LOCALESIDKEY"
			, "STG_TEMP_SRC"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
			, "STG_TEMP_SRC"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
			, "STG_TEMP_SRC"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
			, "STG_TEMP_SRC"."PROFILEID" AS "PROFILEID"
			, "STG_TEMP_SRC"."USERTYPE" AS "USERTYPE"
			, "STG_TEMP_SRC"."USERSUBTYPE" AS "USERSUBTYPE"
			, "STG_TEMP_SRC"."STARTDAY" AS "STARTDAY"
			, "STG_TEMP_SRC"."ENDDAY" AS "ENDDAY"
			, "STG_TEMP_SRC"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
			, "STG_TEMP_SRC"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
			, "STG_TEMP_SRC"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
			, "STG_TEMP_SRC"."MANAGERID" AS "MANAGERID"
			, "STG_TEMP_SRC"."LASTLOGINDATE" AS "LASTLOGINDATE"
			, "STG_TEMP_SRC"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
			, "STG_TEMP_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "STG_TEMP_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "STG_TEMP_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "STG_TEMP_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "STG_TEMP_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "STG_TEMP_SRC"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
			, "STG_TEMP_SRC"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
			, "STG_TEMP_SRC"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
			, "STG_TEMP_SRC"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
			, "STG_TEMP_SRC"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
			, "STG_TEMP_SRC"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, "STG_TEMP_SRC"."FORECASTENABLED" AS "FORECASTENABLED"
			, "STG_TEMP_SRC"."CONTACTID" AS "CONTACTID"
			, "STG_TEMP_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "STG_TEMP_SRC"."CALLCENTERID" AS "CALLCENTERID"
			, "STG_TEMP_SRC"."EXTENSION" AS "EXTENSION"
			, "STG_TEMP_SRC"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
			, "STG_TEMP_SRC"."ABOUTME" AS "ABOUTME"
			, "STG_TEMP_SRC"."LOGINLIMIT" AS "LOGINLIMIT"
			, "STG_TEMP_SRC"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
			, "STG_TEMP_SRC"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
			, "STG_TEMP_SRC"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, "STG_TEMP_SRC"."WORKSPACEID" AS "WORKSPACEID"
			, "STG_TEMP_SRC"."SHARINGTYPE" AS "SHARINGTYPE"
			, "STG_TEMP_SRC"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
			, "STG_TEMP_SRC"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, "STG_TEMP_SRC"."BANNERPHOTOID" AS "BANNERPHOTOID"
			, "STG_TEMP_SRC"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
			, "STG_TEMP_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
			, "STG_TEMP_SRC"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		FROM {{ ref('SALESFORCE_STG_USER') }} "STG_TEMP_SRC"
		WHERE  "STG_TEMP_SRC"."RECORD_TYPE" = 'S'
		UNION ALL 
		SELECT 
			  "SAT_SRC"."USER_HKEY" AS "USER_HKEY"
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
			, "SAT_SRC"."USERNAME" AS "USERNAME"
			, "SAT_SRC"."FIRSTNAME" AS "FIRSTNAME"
			, "SAT_SRC"."LASTNAME" AS "LASTNAME"
			, "SAT_SRC"."COMPANYNAME" AS "COMPANYNAME"
			, "SAT_SRC"."DIVISION" AS "DIVISION"
			, "SAT_SRC"."DEPARTMENT" AS "DEPARTMENT"
			, "SAT_SRC"."TITLE" AS "TITLE"
			, "SAT_SRC"."STREET" AS "STREET"
			, "SAT_SRC"."CITY" AS "CITY"
			, "SAT_SRC"."STATE" AS "STATE"
			, "SAT_SRC"."POSTALCODE" AS "POSTALCODE"
			, "SAT_SRC"."COUNTRY" AS "COUNTRY"
			, "SAT_SRC"."LATITUDE" AS "LATITUDE"
			, "SAT_SRC"."LONGITUDE" AS "LONGITUDE"
			, "SAT_SRC"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
			, "SAT_SRC"."EMAIL" AS "EMAIL"
			, "SAT_SRC"."SENDEREMAIL" AS "SENDEREMAIL"
			, "SAT_SRC"."SENDERNAME" AS "SENDERNAME"
			, "SAT_SRC"."SIGNATURE" AS "SIGNATURE"
			, "SAT_SRC"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
			, "SAT_SRC"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
			, "SAT_SRC"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
			, "SAT_SRC"."PHONE" AS "PHONE"
			, "SAT_SRC"."FAX" AS "FAX"
			, "SAT_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
			, "SAT_SRC"."ALIAS" AS "ALIAS"
			, "SAT_SRC"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
			, "SAT_SRC"."ISACTIVE" AS "ISACTIVE"
			, "SAT_SRC"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
			, "SAT_SRC"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
			, "SAT_SRC"."USERROLEID" AS "USERROLEID"
			, "SAT_SRC"."LOCALESIDKEY" AS "LOCALESIDKEY"
			, "SAT_SRC"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
			, "SAT_SRC"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
			, "SAT_SRC"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
			, "SAT_SRC"."PROFILEID" AS "PROFILEID"
			, "SAT_SRC"."USERTYPE" AS "USERTYPE"
			, "SAT_SRC"."USERSUBTYPE" AS "USERSUBTYPE"
			, "SAT_SRC"."STARTDAY" AS "STARTDAY"
			, "SAT_SRC"."ENDDAY" AS "ENDDAY"
			, "SAT_SRC"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
			, "SAT_SRC"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
			, "SAT_SRC"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
			, "SAT_SRC"."MANAGERID" AS "MANAGERID"
			, "SAT_SRC"."LASTLOGINDATE" AS "LASTLOGINDATE"
			, "SAT_SRC"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
			, "SAT_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "SAT_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "SAT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "SAT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "SAT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "SAT_SRC"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
			, "SAT_SRC"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
			, "SAT_SRC"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
			, "SAT_SRC"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
			, "SAT_SRC"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
			, "SAT_SRC"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, "SAT_SRC"."FORECASTENABLED" AS "FORECASTENABLED"
			, "SAT_SRC"."CONTACTID" AS "CONTACTID"
			, "SAT_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "SAT_SRC"."CALLCENTERID" AS "CALLCENTERID"
			, "SAT_SRC"."EXTENSION" AS "EXTENSION"
			, "SAT_SRC"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
			, "SAT_SRC"."ABOUTME" AS "ABOUTME"
			, "SAT_SRC"."LOGINLIMIT" AS "LOGINLIMIT"
			, "SAT_SRC"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
			, "SAT_SRC"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
			, "SAT_SRC"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, "SAT_SRC"."WORKSPACEID" AS "WORKSPACEID"
			, "SAT_SRC"."SHARINGTYPE" AS "SHARINGTYPE"
			, "SAT_SRC"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
			, "SAT_SRC"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, "SAT_SRC"."BANNERPHOTOID" AS "BANNERPHOTOID"
			, "SAT_SRC"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
			, "SAT_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
			, "SAT_SRC"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		FROM {{ source('DEMO_FL', 'SAT_SALESFORCE_USER') }} "SAT_SRC"
		INNER JOIN "DIST_STG" "DIST_STG" ON  "SAT_SRC"."USER_HKEY" = "DIST_STG"."USER_HKEY"
		WHERE  "SAT_SRC"."LOAD_END_DATE" = TO_TIMESTAMP('31/12/2999 23:59:59' , 'DD/MM/YYYY HH24:MI:SS')
	)
	SELECT 
		  "TEMP_TABLE_SET"."USER_HKEY" AS "USER_HKEY"
		, "TEMP_TABLE_SET"."LOAD_DATE" AS "LOAD_DATE"
		, "TEMP_TABLE_SET"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "TEMP_TABLE_SET"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "TEMP_TABLE_SET"."RECORD_TYPE" AS "RECORD_TYPE"
		, "TEMP_TABLE_SET"."SOURCE" AS "SOURCE"
		, CASE WHEN "TEMP_TABLE_SET"."SOURCE" = 'STG' AND TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF")
			= LAG( TO_CHAR("TEMP_TABLE_SET"."DELETE_FLAG") || TO_CHAR("TEMP_TABLE_SET"."HASH_DIFF"),1)OVER(PARTITION BY "TEMP_TABLE_SET"."USER_HKEY" ORDER BY "TEMP_TABLE_SET"."LOAD_DATE","TEMP_TABLE_SET"."ORIGIN_ID")THEN 1 ELSE 0 END AS "EQUAL"
		, "TEMP_TABLE_SET"."HASH_DIFF" AS "HASH_DIFF"
		, "TEMP_TABLE_SET"."DELETE_FLAG" AS "DELETE_FLAG"
		, "TEMP_TABLE_SET"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "TEMP_TABLE_SET"."ID" AS "ID"
		, "TEMP_TABLE_SET"."USERNAME" AS "USERNAME"
		, "TEMP_TABLE_SET"."FIRSTNAME" AS "FIRSTNAME"
		, "TEMP_TABLE_SET"."LASTNAME" AS "LASTNAME"
		, "TEMP_TABLE_SET"."COMPANYNAME" AS "COMPANYNAME"
		, "TEMP_TABLE_SET"."DIVISION" AS "DIVISION"
		, "TEMP_TABLE_SET"."DEPARTMENT" AS "DEPARTMENT"
		, "TEMP_TABLE_SET"."TITLE" AS "TITLE"
		, "TEMP_TABLE_SET"."STREET" AS "STREET"
		, "TEMP_TABLE_SET"."CITY" AS "CITY"
		, "TEMP_TABLE_SET"."STATE" AS "STATE"
		, "TEMP_TABLE_SET"."POSTALCODE" AS "POSTALCODE"
		, "TEMP_TABLE_SET"."COUNTRY" AS "COUNTRY"
		, "TEMP_TABLE_SET"."LATITUDE" AS "LATITUDE"
		, "TEMP_TABLE_SET"."LONGITUDE" AS "LONGITUDE"
		, "TEMP_TABLE_SET"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
		, "TEMP_TABLE_SET"."EMAIL" AS "EMAIL"
		, "TEMP_TABLE_SET"."SENDEREMAIL" AS "SENDEREMAIL"
		, "TEMP_TABLE_SET"."SENDERNAME" AS "SENDERNAME"
		, "TEMP_TABLE_SET"."SIGNATURE" AS "SIGNATURE"
		, "TEMP_TABLE_SET"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
		, "TEMP_TABLE_SET"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
		, "TEMP_TABLE_SET"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
		, "TEMP_TABLE_SET"."PHONE" AS "PHONE"
		, "TEMP_TABLE_SET"."FAX" AS "FAX"
		, "TEMP_TABLE_SET"."MOBILEPHONE" AS "MOBILEPHONE"
		, "TEMP_TABLE_SET"."ALIAS" AS "ALIAS"
		, "TEMP_TABLE_SET"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
		, "TEMP_TABLE_SET"."ISACTIVE" AS "ISACTIVE"
		, "TEMP_TABLE_SET"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
		, "TEMP_TABLE_SET"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
		, "TEMP_TABLE_SET"."USERROLEID" AS "USERROLEID"
		, "TEMP_TABLE_SET"."LOCALESIDKEY" AS "LOCALESIDKEY"
		, "TEMP_TABLE_SET"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
		, "TEMP_TABLE_SET"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
		, "TEMP_TABLE_SET"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
		, "TEMP_TABLE_SET"."PROFILEID" AS "PROFILEID"
		, "TEMP_TABLE_SET"."USERTYPE" AS "USERTYPE"
		, "TEMP_TABLE_SET"."USERSUBTYPE" AS "USERSUBTYPE"
		, "TEMP_TABLE_SET"."STARTDAY" AS "STARTDAY"
		, "TEMP_TABLE_SET"."ENDDAY" AS "ENDDAY"
		, "TEMP_TABLE_SET"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
		, "TEMP_TABLE_SET"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
		, "TEMP_TABLE_SET"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
		, "TEMP_TABLE_SET"."MANAGERID" AS "MANAGERID"
		, "TEMP_TABLE_SET"."LASTLOGINDATE" AS "LASTLOGINDATE"
		, "TEMP_TABLE_SET"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
		, "TEMP_TABLE_SET"."CREATEDDATE" AS "CREATEDDATE"
		, "TEMP_TABLE_SET"."CREATEDBYID" AS "CREATEDBYID"
		, "TEMP_TABLE_SET"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "TEMP_TABLE_SET"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "TEMP_TABLE_SET"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "TEMP_TABLE_SET"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
		, "TEMP_TABLE_SET"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
		, "TEMP_TABLE_SET"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
		, "TEMP_TABLE_SET"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
		, "TEMP_TABLE_SET"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
		, "TEMP_TABLE_SET"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
		, "TEMP_TABLE_SET"."FORECASTENABLED" AS "FORECASTENABLED"
		, "TEMP_TABLE_SET"."CONTACTID" AS "CONTACTID"
		, "TEMP_TABLE_SET"."ACCOUNTID" AS "ACCOUNTID"
		, "TEMP_TABLE_SET"."CALLCENTERID" AS "CALLCENTERID"
		, "TEMP_TABLE_SET"."EXTENSION" AS "EXTENSION"
		, "TEMP_TABLE_SET"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
		, "TEMP_TABLE_SET"."ABOUTME" AS "ABOUTME"
		, "TEMP_TABLE_SET"."LOGINLIMIT" AS "LOGINLIMIT"
		, "TEMP_TABLE_SET"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
		, "TEMP_TABLE_SET"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
		, "TEMP_TABLE_SET"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
		, "TEMP_TABLE_SET"."WORKSPACEID" AS "WORKSPACEID"
		, "TEMP_TABLE_SET"."SHARINGTYPE" AS "SHARINGTYPE"
		, "TEMP_TABLE_SET"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
		, "TEMP_TABLE_SET"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
		, "TEMP_TABLE_SET"."BANNERPHOTOID" AS "BANNERPHOTOID"
		, "TEMP_TABLE_SET"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
		, "TEMP_TABLE_SET"."INDIVIDUALID" AS "INDIVIDUALID"
		, "TEMP_TABLE_SET"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
	FROM "TEMP_TABLE_SET" "TEMP_TABLE_SET"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'