{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_USER',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_USER_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."USER_HKEY" AS "USER_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."USERNAME" AS "USERNAME"
		, "DVT_SRC"."FIRSTNAME" AS "FIRSTNAME"
		, "DVT_SRC"."LASTNAME" AS "LASTNAME"
		, "DVT_SRC"."COMPANYNAME" AS "COMPANYNAME"
		, "DVT_SRC"."DIVISION" AS "DIVISION"
		, "DVT_SRC"."DEPARTMENT" AS "DEPARTMENT"
		, "DVT_SRC"."TITLE" AS "TITLE"
		, "DVT_SRC"."STREET" AS "STREET"
		, "DVT_SRC"."CITY" AS "CITY"
		, "DVT_SRC"."STATE" AS "STATE"
		, "DVT_SRC"."POSTALCODE" AS "POSTALCODE"
		, "DVT_SRC"."COUNTRY" AS "COUNTRY"
		, "DVT_SRC"."LATITUDE" AS "LATITUDE"
		, "DVT_SRC"."LONGITUDE" AS "LONGITUDE"
		, "DVT_SRC"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
		, "DVT_SRC"."EMAIL" AS "EMAIL"
		, "DVT_SRC"."SENDEREMAIL" AS "SENDEREMAIL"
		, "DVT_SRC"."SENDERNAME" AS "SENDERNAME"
		, "DVT_SRC"."SIGNATURE" AS "SIGNATURE"
		, "DVT_SRC"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
		, "DVT_SRC"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
		, "DVT_SRC"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
		, "DVT_SRC"."PHONE" AS "PHONE"
		, "DVT_SRC"."FAX" AS "FAX"
		, "DVT_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
		, "DVT_SRC"."ALIAS" AS "ALIAS"
		, "DVT_SRC"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
		, "DVT_SRC"."ISACTIVE" AS "ISACTIVE"
		, "DVT_SRC"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
		, "DVT_SRC"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
		, "DVT_SRC"."USERROLEID" AS "USERROLEID"
		, "DVT_SRC"."LOCALESIDKEY" AS "LOCALESIDKEY"
		, "DVT_SRC"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
		, "DVT_SRC"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
		, "DVT_SRC"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
		, "DVT_SRC"."PROFILEID" AS "PROFILEID"
		, "DVT_SRC"."USERTYPE" AS "USERTYPE"
		, "DVT_SRC"."USERSUBTYPE" AS "USERSUBTYPE"
		, "DVT_SRC"."STARTDAY" AS "STARTDAY"
		, "DVT_SRC"."ENDDAY" AS "ENDDAY"
		, "DVT_SRC"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
		, "DVT_SRC"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
		, "DVT_SRC"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
		, "DVT_SRC"."MANAGERID" AS "MANAGERID"
		, "DVT_SRC"."LASTLOGINDATE" AS "LASTLOGINDATE"
		, "DVT_SRC"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
		, "DVT_SRC"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
		, "DVT_SRC"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
		, "DVT_SRC"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
		, "DVT_SRC"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
		, "DVT_SRC"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
		, "DVT_SRC"."FORECASTENABLED" AS "FORECASTENABLED"
		, "DVT_SRC"."CONTACTID" AS "CONTACTID"
		, "DVT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "DVT_SRC"."CALLCENTERID" AS "CALLCENTERID"
		, "DVT_SRC"."EXTENSION" AS "EXTENSION"
		, "DVT_SRC"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
		, "DVT_SRC"."ABOUTME" AS "ABOUTME"
		, "DVT_SRC"."LOGINLIMIT" AS "LOGINLIMIT"
		, "DVT_SRC"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
		, "DVT_SRC"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
		, "DVT_SRC"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
		, "DVT_SRC"."WORKSPACEID" AS "WORKSPACEID"
		, "DVT_SRC"."SHARINGTYPE" AS "SHARINGTYPE"
		, "DVT_SRC"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
		, "DVT_SRC"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
		, "DVT_SRC"."BANNERPHOTOID" AS "BANNERPHOTOID"
		, "DVT_SRC"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
		, "DVT_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
		, "DVT_SRC"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_USER') }} "DVT_SRC"