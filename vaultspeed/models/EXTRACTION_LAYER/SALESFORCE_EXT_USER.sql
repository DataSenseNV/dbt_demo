{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='USER',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_USER_INCR', 'EXT_SALESFORCE_USER_INIT']
	)
}}
select * from (
	WITH "CALCULATE_BK" AS 
	( 
		SELECT 
			  "LCI_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, DATEADD(microsecond, 2*row_number() over (order by "TDFV_SRC"."CDC_TIMESTAMP"),
				TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()))   AS "LOAD_DATE"
			, "TDFV_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, COALESCE("TDFV_SRC"."JRN_FLAG","MEX_SRC"."ATTRIBUTE_VARCHAR") AS "JRN_FLAG"
			, "TDFV_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "TDFV_SRC"."ID" AS "ID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, "TDFV_SRC"."USERNAME" AS "USERNAME"
			, "TDFV_SRC"."FIRSTNAME" AS "FIRSTNAME"
			, "TDFV_SRC"."LASTNAME" AS "LASTNAME"
			, "TDFV_SRC"."COMPANYNAME" AS "COMPANYNAME"
			, "TDFV_SRC"."DIVISION" AS "DIVISION"
			, "TDFV_SRC"."DEPARTMENT" AS "DEPARTMENT"
			, "TDFV_SRC"."TITLE" AS "TITLE"
			, "TDFV_SRC"."STREET" AS "STREET"
			, "TDFV_SRC"."CITY" AS "CITY"
			, "TDFV_SRC"."STATE" AS "STATE"
			, "TDFV_SRC"."POSTALCODE" AS "POSTALCODE"
			, "TDFV_SRC"."COUNTRY" AS "COUNTRY"
			, "TDFV_SRC"."LATITUDE" AS "LATITUDE"
			, "TDFV_SRC"."LONGITUDE" AS "LONGITUDE"
			, "TDFV_SRC"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
			, "TDFV_SRC"."EMAIL" AS "EMAIL"
			, "TDFV_SRC"."SENDEREMAIL" AS "SENDEREMAIL"
			, "TDFV_SRC"."SENDERNAME" AS "SENDERNAME"
			, "TDFV_SRC"."SIGNATURE" AS "SIGNATURE"
			, "TDFV_SRC"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
			, "TDFV_SRC"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
			, "TDFV_SRC"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
			, "TDFV_SRC"."PHONE" AS "PHONE"
			, "TDFV_SRC"."FAX" AS "FAX"
			, "TDFV_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
			, "TDFV_SRC"."ALIAS" AS "ALIAS"
			, "TDFV_SRC"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
			, "TDFV_SRC"."ISACTIVE" AS "ISACTIVE"
			, "TDFV_SRC"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
			, "TDFV_SRC"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
			, "TDFV_SRC"."USERROLEID" AS "USERROLEID"
			, "TDFV_SRC"."LOCALESIDKEY" AS "LOCALESIDKEY"
			, "TDFV_SRC"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
			, "TDFV_SRC"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
			, "TDFV_SRC"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
			, "TDFV_SRC"."PROFILEID" AS "PROFILEID"
			, "TDFV_SRC"."USERTYPE" AS "USERTYPE"
			, "TDFV_SRC"."USERSUBTYPE" AS "USERSUBTYPE"
			, "TDFV_SRC"."STARTDAY" AS "STARTDAY"
			, "TDFV_SRC"."ENDDAY" AS "ENDDAY"
			, "TDFV_SRC"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
			, "TDFV_SRC"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
			, "TDFV_SRC"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
			, "TDFV_SRC"."MANAGERID" AS "MANAGERID"
			, "TDFV_SRC"."LASTLOGINDATE" AS "LASTLOGINDATE"
			, "TDFV_SRC"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "TDFV_SRC"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
			, "TDFV_SRC"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
			, "TDFV_SRC"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
			, "TDFV_SRC"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
			, "TDFV_SRC"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
			, "TDFV_SRC"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, "TDFV_SRC"."FORECASTENABLED" AS "FORECASTENABLED"
			, "TDFV_SRC"."CONTACTID" AS "CONTACTID"
			, "TDFV_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "TDFV_SRC"."CALLCENTERID" AS "CALLCENTERID"
			, "TDFV_SRC"."EXTENSION" AS "EXTENSION"
			, "TDFV_SRC"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
			, "TDFV_SRC"."ABOUTME" AS "ABOUTME"
			, "TDFV_SRC"."LOGINLIMIT" AS "LOGINLIMIT"
			, "TDFV_SRC"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
			, "TDFV_SRC"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
			, "TDFV_SRC"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, "TDFV_SRC"."WORKSPACEID" AS "WORKSPACEID"
			, "TDFV_SRC"."SHARINGTYPE" AS "SHARINGTYPE"
			, "TDFV_SRC"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
			, "TDFV_SRC"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, "TDFV_SRC"."BANNERPHOTOID" AS "BANNERPHOTOID"
			, "TDFV_SRC"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
			, "TDFV_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
			, "TDFV_SRC"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		FROM {{ ref('SALESFORCE_DFV_VW_USER') }} "TDFV_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'LOAD_CYCLE_INFO') }} "LCI_SRC" ON  1 = 1
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  1 = 1
		WHERE  "MEX_SRC"."RECORD_TYPE" = 'N'
	)
	, "EXT_UNION" AS 
	( 
		SELECT 
			  "CALCULATE_BK"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
			, "CALCULATE_BK"."LOAD_DATE" AS "LOAD_DATE"
			, "CALCULATE_BK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CALCULATE_BK"."JRN_FLAG" AS "JRN_FLAG"
			, "CALCULATE_BK"."RECORD_TYPE" AS "RECORD_TYPE"
			, "CALCULATE_BK"."ID" AS "ID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."USERNAME" AS "USERNAME"
			, "CALCULATE_BK"."FIRSTNAME" AS "FIRSTNAME"
			, "CALCULATE_BK"."LASTNAME" AS "LASTNAME"
			, "CALCULATE_BK"."COMPANYNAME" AS "COMPANYNAME"
			, "CALCULATE_BK"."DIVISION" AS "DIVISION"
			, "CALCULATE_BK"."DEPARTMENT" AS "DEPARTMENT"
			, "CALCULATE_BK"."TITLE" AS "TITLE"
			, "CALCULATE_BK"."STREET" AS "STREET"
			, "CALCULATE_BK"."CITY" AS "CITY"
			, "CALCULATE_BK"."STATE" AS "STATE"
			, "CALCULATE_BK"."POSTALCODE" AS "POSTALCODE"
			, "CALCULATE_BK"."COUNTRY" AS "COUNTRY"
			, "CALCULATE_BK"."LATITUDE" AS "LATITUDE"
			, "CALCULATE_BK"."LONGITUDE" AS "LONGITUDE"
			, "CALCULATE_BK"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
			, "CALCULATE_BK"."EMAIL" AS "EMAIL"
			, "CALCULATE_BK"."SENDEREMAIL" AS "SENDEREMAIL"
			, "CALCULATE_BK"."SENDERNAME" AS "SENDERNAME"
			, "CALCULATE_BK"."SIGNATURE" AS "SIGNATURE"
			, "CALCULATE_BK"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
			, "CALCULATE_BK"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
			, "CALCULATE_BK"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
			, "CALCULATE_BK"."PHONE" AS "PHONE"
			, "CALCULATE_BK"."FAX" AS "FAX"
			, "CALCULATE_BK"."MOBILEPHONE" AS "MOBILEPHONE"
			, "CALCULATE_BK"."ALIAS" AS "ALIAS"
			, "CALCULATE_BK"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
			, "CALCULATE_BK"."ISACTIVE" AS "ISACTIVE"
			, "CALCULATE_BK"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
			, "CALCULATE_BK"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
			, "CALCULATE_BK"."USERROLEID" AS "USERROLEID"
			, "CALCULATE_BK"."LOCALESIDKEY" AS "LOCALESIDKEY"
			, "CALCULATE_BK"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
			, "CALCULATE_BK"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
			, "CALCULATE_BK"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
			, "CALCULATE_BK"."PROFILEID" AS "PROFILEID"
			, "CALCULATE_BK"."USERTYPE" AS "USERTYPE"
			, "CALCULATE_BK"."USERSUBTYPE" AS "USERSUBTYPE"
			, "CALCULATE_BK"."STARTDAY" AS "STARTDAY"
			, "CALCULATE_BK"."ENDDAY" AS "ENDDAY"
			, "CALCULATE_BK"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
			, "CALCULATE_BK"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
			, "CALCULATE_BK"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
			, "CALCULATE_BK"."MANAGERID" AS "MANAGERID"
			, "CALCULATE_BK"."LASTLOGINDATE" AS "LASTLOGINDATE"
			, "CALCULATE_BK"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALCULATE_BK"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
			, "CALCULATE_BK"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
			, "CALCULATE_BK"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
			, "CALCULATE_BK"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
			, "CALCULATE_BK"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
			, "CALCULATE_BK"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, "CALCULATE_BK"."FORECASTENABLED" AS "FORECASTENABLED"
			, "CALCULATE_BK"."CONTACTID" AS "CONTACTID"
			, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
			, "CALCULATE_BK"."CALLCENTERID" AS "CALLCENTERID"
			, "CALCULATE_BK"."EXTENSION" AS "EXTENSION"
			, "CALCULATE_BK"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
			, "CALCULATE_BK"."ABOUTME" AS "ABOUTME"
			, "CALCULATE_BK"."LOGINLIMIT" AS "LOGINLIMIT"
			, "CALCULATE_BK"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
			, "CALCULATE_BK"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
			, "CALCULATE_BK"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, "CALCULATE_BK"."WORKSPACEID" AS "WORKSPACEID"
			, "CALCULATE_BK"."SHARINGTYPE" AS "SHARINGTYPE"
			, "CALCULATE_BK"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
			, "CALCULATE_BK"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, "CALCULATE_BK"."BANNERPHOTOID" AS "BANNERPHOTOID"
			, "CALCULATE_BK"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
			, "CALCULATE_BK"."INDIVIDUALID" AS "INDIVIDUALID"
			, "CALCULATE_BK"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."ID" AS "ID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."USERNAME" AS "USERNAME"
		, "EXT_UNION"."FIRSTNAME" AS "FIRSTNAME"
		, "EXT_UNION"."LASTNAME" AS "LASTNAME"
		, "EXT_UNION"."COMPANYNAME" AS "COMPANYNAME"
		, "EXT_UNION"."DIVISION" AS "DIVISION"
		, "EXT_UNION"."DEPARTMENT" AS "DEPARTMENT"
		, "EXT_UNION"."TITLE" AS "TITLE"
		, "EXT_UNION"."STREET" AS "STREET"
		, "EXT_UNION"."CITY" AS "CITY"
		, "EXT_UNION"."STATE" AS "STATE"
		, "EXT_UNION"."POSTALCODE" AS "POSTALCODE"
		, "EXT_UNION"."COUNTRY" AS "COUNTRY"
		, "EXT_UNION"."LATITUDE" AS "LATITUDE"
		, "EXT_UNION"."LONGITUDE" AS "LONGITUDE"
		, "EXT_UNION"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
		, "EXT_UNION"."EMAIL" AS "EMAIL"
		, "EXT_UNION"."SENDEREMAIL" AS "SENDEREMAIL"
		, "EXT_UNION"."SENDERNAME" AS "SENDERNAME"
		, "EXT_UNION"."SIGNATURE" AS "SIGNATURE"
		, "EXT_UNION"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
		, "EXT_UNION"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
		, "EXT_UNION"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
		, "EXT_UNION"."PHONE" AS "PHONE"
		, "EXT_UNION"."FAX" AS "FAX"
		, "EXT_UNION"."MOBILEPHONE" AS "MOBILEPHONE"
		, "EXT_UNION"."ALIAS" AS "ALIAS"
		, "EXT_UNION"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
		, "EXT_UNION"."ISACTIVE" AS "ISACTIVE"
		, "EXT_UNION"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
		, "EXT_UNION"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
		, "EXT_UNION"."USERROLEID" AS "USERROLEID"
		, "EXT_UNION"."LOCALESIDKEY" AS "LOCALESIDKEY"
		, "EXT_UNION"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
		, "EXT_UNION"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
		, "EXT_UNION"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
		, "EXT_UNION"."PROFILEID" AS "PROFILEID"
		, "EXT_UNION"."USERTYPE" AS "USERTYPE"
		, "EXT_UNION"."USERSUBTYPE" AS "USERSUBTYPE"
		, "EXT_UNION"."STARTDAY" AS "STARTDAY"
		, "EXT_UNION"."ENDDAY" AS "ENDDAY"
		, "EXT_UNION"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
		, "EXT_UNION"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
		, "EXT_UNION"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
		, "EXT_UNION"."MANAGERID" AS "MANAGERID"
		, "EXT_UNION"."LASTLOGINDATE" AS "LASTLOGINDATE"
		, "EXT_UNION"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_UNION"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
		, "EXT_UNION"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
		, "EXT_UNION"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
		, "EXT_UNION"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
		, "EXT_UNION"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
		, "EXT_UNION"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
		, "EXT_UNION"."FORECASTENABLED" AS "FORECASTENABLED"
		, "EXT_UNION"."CONTACTID" AS "CONTACTID"
		, "EXT_UNION"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_UNION"."CALLCENTERID" AS "CALLCENTERID"
		, "EXT_UNION"."EXTENSION" AS "EXTENSION"
		, "EXT_UNION"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
		, "EXT_UNION"."ABOUTME" AS "ABOUTME"
		, "EXT_UNION"."LOGINLIMIT" AS "LOGINLIMIT"
		, "EXT_UNION"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
		, "EXT_UNION"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
		, "EXT_UNION"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
		, "EXT_UNION"."WORKSPACEID" AS "WORKSPACEID"
		, "EXT_UNION"."SHARINGTYPE" AS "SHARINGTYPE"
		, "EXT_UNION"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
		, "EXT_UNION"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
		, "EXT_UNION"."BANNERPHOTOID" AS "BANNERPHOTOID"
		, "EXT_UNION"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
		, "EXT_UNION"."INDIVIDUALID" AS "INDIVIDUALID"
		, "EXT_UNION"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
	FROM "EXT_UNION" "EXT_UNION"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	WITH "LOAD_INIT_DATA" AS 
	( 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, TO_CHAR('S') AS "RECORD_TYPE"
			, COALESCE("INI_SRC"."ID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ID"
			, "INI_SRC"."USERNAME" AS "USERNAME"
			, "INI_SRC"."FIRSTNAME" AS "FIRSTNAME"
			, "INI_SRC"."LASTNAME" AS "LASTNAME"
			, "INI_SRC"."COMPANYNAME" AS "COMPANYNAME"
			, "INI_SRC"."DIVISION" AS "DIVISION"
			, "INI_SRC"."DEPARTMENT" AS "DEPARTMENT"
			, "INI_SRC"."TITLE" AS "TITLE"
			, "INI_SRC"."STREET" AS "STREET"
			, "INI_SRC"."CITY" AS "CITY"
			, "INI_SRC"."STATE" AS "STATE"
			, "INI_SRC"."POSTALCODE" AS "POSTALCODE"
			, "INI_SRC"."COUNTRY" AS "COUNTRY"
			, "INI_SRC"."LATITUDE" AS "LATITUDE"
			, "INI_SRC"."LONGITUDE" AS "LONGITUDE"
			, "INI_SRC"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
			, "INI_SRC"."EMAIL" AS "EMAIL"
			, "INI_SRC"."SENDEREMAIL" AS "SENDEREMAIL"
			, "INI_SRC"."SENDERNAME" AS "SENDERNAME"
			, "INI_SRC"."SIGNATURE" AS "SIGNATURE"
			, "INI_SRC"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
			, "INI_SRC"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
			, "INI_SRC"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
			, "INI_SRC"."PHONE" AS "PHONE"
			, "INI_SRC"."FAX" AS "FAX"
			, "INI_SRC"."MOBILEPHONE" AS "MOBILEPHONE"
			, "INI_SRC"."ALIAS" AS "ALIAS"
			, "INI_SRC"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
			, "INI_SRC"."ISACTIVE" AS "ISACTIVE"
			, "INI_SRC"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
			, "INI_SRC"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
			, "INI_SRC"."USERROLEID" AS "USERROLEID"
			, "INI_SRC"."LOCALESIDKEY" AS "LOCALESIDKEY"
			, "INI_SRC"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
			, "INI_SRC"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
			, "INI_SRC"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
			, "INI_SRC"."PROFILEID" AS "PROFILEID"
			, "INI_SRC"."USERTYPE" AS "USERTYPE"
			, "INI_SRC"."USERSUBTYPE" AS "USERSUBTYPE"
			, "INI_SRC"."STARTDAY" AS "STARTDAY"
			, "INI_SRC"."ENDDAY" AS "ENDDAY"
			, "INI_SRC"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
			, "INI_SRC"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
			, "INI_SRC"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
			, "INI_SRC"."MANAGERID" AS "MANAGERID"
			, "INI_SRC"."LASTLOGINDATE" AS "LASTLOGINDATE"
			, "INI_SRC"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "INI_SRC"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
			, "INI_SRC"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
			, "INI_SRC"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
			, "INI_SRC"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
			, "INI_SRC"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
			, "INI_SRC"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, "INI_SRC"."FORECASTENABLED" AS "FORECASTENABLED"
			, "INI_SRC"."CONTACTID" AS "CONTACTID"
			, "INI_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "INI_SRC"."CALLCENTERID" AS "CALLCENTERID"
			, "INI_SRC"."EXTENSION" AS "EXTENSION"
			, "INI_SRC"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
			, "INI_SRC"."ABOUTME" AS "ABOUTME"
			, "INI_SRC"."LOGINLIMIT" AS "LOGINLIMIT"
			, "INI_SRC"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
			, "INI_SRC"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
			, "INI_SRC"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, "INI_SRC"."WORKSPACEID" AS "WORKSPACEID"
			, "INI_SRC"."SHARINGTYPE" AS "SHARINGTYPE"
			, "INI_SRC"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
			, "INI_SRC"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, "INI_SRC"."BANNERPHOTOID" AS "BANNERPHOTOID"
			, "INI_SRC"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
			, "INI_SRC"."INDIVIDUALID" AS "INDIVIDUALID"
			, "INI_SRC"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		FROM {{ source('SALESFORCE_INI', 'USER') }} "INI_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."ID" AS "ID"
			, "LOAD_INIT_DATA"."USERNAME" AS "USERNAME"
			, "LOAD_INIT_DATA"."FIRSTNAME" AS "FIRSTNAME"
			, "LOAD_INIT_DATA"."LASTNAME" AS "LASTNAME"
			, "LOAD_INIT_DATA"."COMPANYNAME" AS "COMPANYNAME"
			, "LOAD_INIT_DATA"."DIVISION" AS "DIVISION"
			, "LOAD_INIT_DATA"."DEPARTMENT" AS "DEPARTMENT"
			, "LOAD_INIT_DATA"."TITLE" AS "TITLE"
			, "LOAD_INIT_DATA"."STREET" AS "STREET"
			, "LOAD_INIT_DATA"."CITY" AS "CITY"
			, "LOAD_INIT_DATA"."STATE" AS "STATE"
			, "LOAD_INIT_DATA"."POSTALCODE" AS "POSTALCODE"
			, "LOAD_INIT_DATA"."COUNTRY" AS "COUNTRY"
			, "LOAD_INIT_DATA"."LATITUDE" AS "LATITUDE"
			, "LOAD_INIT_DATA"."LONGITUDE" AS "LONGITUDE"
			, "LOAD_INIT_DATA"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
			, "LOAD_INIT_DATA"."EMAIL" AS "EMAIL"
			, "LOAD_INIT_DATA"."SENDEREMAIL" AS "SENDEREMAIL"
			, "LOAD_INIT_DATA"."SENDERNAME" AS "SENDERNAME"
			, "LOAD_INIT_DATA"."SIGNATURE" AS "SIGNATURE"
			, "LOAD_INIT_DATA"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
			, "LOAD_INIT_DATA"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
			, "LOAD_INIT_DATA"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
			, "LOAD_INIT_DATA"."PHONE" AS "PHONE"
			, "LOAD_INIT_DATA"."FAX" AS "FAX"
			, "LOAD_INIT_DATA"."MOBILEPHONE" AS "MOBILEPHONE"
			, "LOAD_INIT_DATA"."ALIAS" AS "ALIAS"
			, "LOAD_INIT_DATA"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
			, "LOAD_INIT_DATA"."ISACTIVE" AS "ISACTIVE"
			, "LOAD_INIT_DATA"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
			, "LOAD_INIT_DATA"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
			, "LOAD_INIT_DATA"."USERROLEID" AS "USERROLEID"
			, "LOAD_INIT_DATA"."LOCALESIDKEY" AS "LOCALESIDKEY"
			, "LOAD_INIT_DATA"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
			, "LOAD_INIT_DATA"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
			, "LOAD_INIT_DATA"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
			, "LOAD_INIT_DATA"."PROFILEID" AS "PROFILEID"
			, "LOAD_INIT_DATA"."USERTYPE" AS "USERTYPE"
			, "LOAD_INIT_DATA"."USERSUBTYPE" AS "USERSUBTYPE"
			, "LOAD_INIT_DATA"."STARTDAY" AS "STARTDAY"
			, "LOAD_INIT_DATA"."ENDDAY" AS "ENDDAY"
			, "LOAD_INIT_DATA"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
			, "LOAD_INIT_DATA"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
			, "LOAD_INIT_DATA"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
			, "LOAD_INIT_DATA"."MANAGERID" AS "MANAGERID"
			, "LOAD_INIT_DATA"."LASTLOGINDATE" AS "LASTLOGINDATE"
			, "LOAD_INIT_DATA"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "LOAD_INIT_DATA"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
			, "LOAD_INIT_DATA"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
			, "LOAD_INIT_DATA"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
			, "LOAD_INIT_DATA"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
			, "LOAD_INIT_DATA"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
			, "LOAD_INIT_DATA"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, "LOAD_INIT_DATA"."FORECASTENABLED" AS "FORECASTENABLED"
			, "LOAD_INIT_DATA"."CONTACTID" AS "CONTACTID"
			, "LOAD_INIT_DATA"."ACCOUNTID" AS "ACCOUNTID"
			, "LOAD_INIT_DATA"."CALLCENTERID" AS "CALLCENTERID"
			, "LOAD_INIT_DATA"."EXTENSION" AS "EXTENSION"
			, "LOAD_INIT_DATA"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
			, "LOAD_INIT_DATA"."ABOUTME" AS "ABOUTME"
			, "LOAD_INIT_DATA"."LOGINLIMIT" AS "LOGINLIMIT"
			, "LOAD_INIT_DATA"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
			, "LOAD_INIT_DATA"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
			, "LOAD_INIT_DATA"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, "LOAD_INIT_DATA"."WORKSPACEID" AS "WORKSPACEID"
			, "LOAD_INIT_DATA"."SHARINGTYPE" AS "SHARINGTYPE"
			, "LOAD_INIT_DATA"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
			, "LOAD_INIT_DATA"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, "LOAD_INIT_DATA"."BANNERPHOTOID" AS "BANNERPHOTOID"
			, "LOAD_INIT_DATA"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
			, "LOAD_INIT_DATA"."INDIVIDUALID" AS "INDIVIDUALID"
			, "LOAD_INIT_DATA"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "USERNAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "FIRSTNAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTNAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "COMPANYNAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DIVISION"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DEPARTMENT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "TITLE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STREET"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CITY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "POSTALCODE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "COUNTRY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LATITUDE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LONGITUDE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "GEOCODEACCURACY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "EMAIL"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SENDEREMAIL"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SENDERNAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SIGNATURE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STAYINTOUCHSUBJECT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STAYINTOUCHSIGNATURE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STAYINTOUCHNOTE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PHONE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "FAX"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "MOBILEPHONE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ALIAS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "COMMUNITYNICKNAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISACTIVE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISSYSTEMCONTROLLED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "TIMEZONESIDKEY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "USERROLEID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LOCALESIDKEY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "RECEIVESINFOEMAILS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "RECEIVESADMININFOEMAILS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "EMAILENCODINGKEY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PROFILEID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "USERTYPE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "USERSUBTYPE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STARTDAY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ENDDAY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LANGUAGELOCALEKEY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "EMPLOYEENUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DELEGATEDAPPROVERID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "MANAGERID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTLOGINDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTPASSWORDCHANGEDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "NUMBEROFFAILEDLOGINS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SUACCESSEXPIRATIONDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SUORGADMINEXPIRATIONDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OFFLINETRIALEXPIRATIONDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WIRELESSTRIALEXPIRATIONDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "FORECASTENABLED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTACTID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CALLCENTERID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "EXTENSION"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "FEDERATIONIDENTIFIER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ABOUTME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LOGINLIMIT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PROFILEPHOTOID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DIGESTFREQUENCY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WORKSPACEID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHARINGTYPE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CHATTERADOPTIONSTAGE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BANNERPHOTOID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISPROFILEPHOTOACTIVE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "INDIVIDUALID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "GLOBALIDENTITY"
		FROM {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_EXT_SRC"
	)
	, "CALCULATE_BK" AS 
	( 
		SELECT 
			  COALESCE("PREP_EXCEP"."LOAD_CYCLE_ID","LCI_SRC"."LOAD_CYCLE_ID") AS "LOAD_CYCLE_ID"
			, "LCI_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "LCI_SRC"."LOAD_DATE" AS "CDC_TIMESTAMP"
			, "PREP_EXCEP"."JRN_FLAG" AS "JRN_FLAG"
			, "PREP_EXCEP"."RECORD_TYPE" AS "RECORD_TYPE"
			, "PREP_EXCEP"."ID" AS "ID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, "PREP_EXCEP"."USERNAME" AS "USERNAME"
			, "PREP_EXCEP"."FIRSTNAME" AS "FIRSTNAME"
			, "PREP_EXCEP"."LASTNAME" AS "LASTNAME"
			, "PREP_EXCEP"."COMPANYNAME" AS "COMPANYNAME"
			, "PREP_EXCEP"."DIVISION" AS "DIVISION"
			, "PREP_EXCEP"."DEPARTMENT" AS "DEPARTMENT"
			, "PREP_EXCEP"."TITLE" AS "TITLE"
			, "PREP_EXCEP"."STREET" AS "STREET"
			, "PREP_EXCEP"."CITY" AS "CITY"
			, "PREP_EXCEP"."STATE" AS "STATE"
			, "PREP_EXCEP"."POSTALCODE" AS "POSTALCODE"
			, "PREP_EXCEP"."COUNTRY" AS "COUNTRY"
			, "PREP_EXCEP"."LATITUDE" AS "LATITUDE"
			, "PREP_EXCEP"."LONGITUDE" AS "LONGITUDE"
			, "PREP_EXCEP"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
			, "PREP_EXCEP"."EMAIL" AS "EMAIL"
			, "PREP_EXCEP"."SENDEREMAIL" AS "SENDEREMAIL"
			, "PREP_EXCEP"."SENDERNAME" AS "SENDERNAME"
			, "PREP_EXCEP"."SIGNATURE" AS "SIGNATURE"
			, "PREP_EXCEP"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
			, "PREP_EXCEP"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
			, "PREP_EXCEP"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
			, "PREP_EXCEP"."PHONE" AS "PHONE"
			, "PREP_EXCEP"."FAX" AS "FAX"
			, "PREP_EXCEP"."MOBILEPHONE" AS "MOBILEPHONE"
			, "PREP_EXCEP"."ALIAS" AS "ALIAS"
			, "PREP_EXCEP"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
			, "PREP_EXCEP"."ISACTIVE" AS "ISACTIVE"
			, "PREP_EXCEP"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
			, "PREP_EXCEP"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
			, "PREP_EXCEP"."USERROLEID" AS "USERROLEID"
			, "PREP_EXCEP"."LOCALESIDKEY" AS "LOCALESIDKEY"
			, "PREP_EXCEP"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
			, "PREP_EXCEP"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
			, "PREP_EXCEP"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
			, "PREP_EXCEP"."PROFILEID" AS "PROFILEID"
			, "PREP_EXCEP"."USERTYPE" AS "USERTYPE"
			, "PREP_EXCEP"."USERSUBTYPE" AS "USERSUBTYPE"
			, "PREP_EXCEP"."STARTDAY" AS "STARTDAY"
			, "PREP_EXCEP"."ENDDAY" AS "ENDDAY"
			, "PREP_EXCEP"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
			, "PREP_EXCEP"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
			, "PREP_EXCEP"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
			, "PREP_EXCEP"."MANAGERID" AS "MANAGERID"
			, "PREP_EXCEP"."LASTLOGINDATE" AS "LASTLOGINDATE"
			, "PREP_EXCEP"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "PREP_EXCEP"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
			, "PREP_EXCEP"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
			, "PREP_EXCEP"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
			, "PREP_EXCEP"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
			, "PREP_EXCEP"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
			, "PREP_EXCEP"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
			, "PREP_EXCEP"."FORECASTENABLED" AS "FORECASTENABLED"
			, "PREP_EXCEP"."CONTACTID" AS "CONTACTID"
			, "PREP_EXCEP"."ACCOUNTID" AS "ACCOUNTID"
			, "PREP_EXCEP"."CALLCENTERID" AS "CALLCENTERID"
			, "PREP_EXCEP"."EXTENSION" AS "EXTENSION"
			, "PREP_EXCEP"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
			, "PREP_EXCEP"."ABOUTME" AS "ABOUTME"
			, "PREP_EXCEP"."LOGINLIMIT" AS "LOGINLIMIT"
			, "PREP_EXCEP"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
			, "PREP_EXCEP"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
			, "PREP_EXCEP"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
			, "PREP_EXCEP"."WORKSPACEID" AS "WORKSPACEID"
			, "PREP_EXCEP"."SHARINGTYPE" AS "SHARINGTYPE"
			, "PREP_EXCEP"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
			, "PREP_EXCEP"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
			, "PREP_EXCEP"."BANNERPHOTOID" AS "BANNERPHOTOID"
			, "PREP_EXCEP"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
			, "PREP_EXCEP"."INDIVIDUALID" AS "INDIVIDUALID"
			, "PREP_EXCEP"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
		FROM "PREP_EXCEP" "PREP_EXCEP"
		INNER JOIN {{ source('SALESFORCE_MTD', 'LOAD_CYCLE_INFO') }} "LCI_SRC" ON  1 = 1
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  1 = 1
		WHERE  "MEX_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "CALCULATE_BK"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "CALCULATE_BK"."LOAD_DATE" AS "LOAD_DATE"
		, "CALCULATE_BK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "CALCULATE_BK"."JRN_FLAG" AS "JRN_FLAG"
		, "CALCULATE_BK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "CALCULATE_BK"."ID" AS "ID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."USERNAME" AS "USERNAME"
		, "CALCULATE_BK"."FIRSTNAME" AS "FIRSTNAME"
		, "CALCULATE_BK"."LASTNAME" AS "LASTNAME"
		, "CALCULATE_BK"."COMPANYNAME" AS "COMPANYNAME"
		, "CALCULATE_BK"."DIVISION" AS "DIVISION"
		, "CALCULATE_BK"."DEPARTMENT" AS "DEPARTMENT"
		, "CALCULATE_BK"."TITLE" AS "TITLE"
		, "CALCULATE_BK"."STREET" AS "STREET"
		, "CALCULATE_BK"."CITY" AS "CITY"
		, "CALCULATE_BK"."STATE" AS "STATE"
		, "CALCULATE_BK"."POSTALCODE" AS "POSTALCODE"
		, "CALCULATE_BK"."COUNTRY" AS "COUNTRY"
		, "CALCULATE_BK"."LATITUDE" AS "LATITUDE"
		, "CALCULATE_BK"."LONGITUDE" AS "LONGITUDE"
		, "CALCULATE_BK"."GEOCODEACCURACY" AS "GEOCODEACCURACY"
		, "CALCULATE_BK"."EMAIL" AS "EMAIL"
		, "CALCULATE_BK"."SENDEREMAIL" AS "SENDEREMAIL"
		, "CALCULATE_BK"."SENDERNAME" AS "SENDERNAME"
		, "CALCULATE_BK"."SIGNATURE" AS "SIGNATURE"
		, "CALCULATE_BK"."STAYINTOUCHSUBJECT" AS "STAYINTOUCHSUBJECT"
		, "CALCULATE_BK"."STAYINTOUCHSIGNATURE" AS "STAYINTOUCHSIGNATURE"
		, "CALCULATE_BK"."STAYINTOUCHNOTE" AS "STAYINTOUCHNOTE"
		, "CALCULATE_BK"."PHONE" AS "PHONE"
		, "CALCULATE_BK"."FAX" AS "FAX"
		, "CALCULATE_BK"."MOBILEPHONE" AS "MOBILEPHONE"
		, "CALCULATE_BK"."ALIAS" AS "ALIAS"
		, "CALCULATE_BK"."COMMUNITYNICKNAME" AS "COMMUNITYNICKNAME"
		, "CALCULATE_BK"."ISACTIVE" AS "ISACTIVE"
		, "CALCULATE_BK"."ISSYSTEMCONTROLLED" AS "ISSYSTEMCONTROLLED"
		, "CALCULATE_BK"."TIMEZONESIDKEY" AS "TIMEZONESIDKEY"
		, "CALCULATE_BK"."USERROLEID" AS "USERROLEID"
		, "CALCULATE_BK"."LOCALESIDKEY" AS "LOCALESIDKEY"
		, "CALCULATE_BK"."RECEIVESINFOEMAILS" AS "RECEIVESINFOEMAILS"
		, "CALCULATE_BK"."RECEIVESADMININFOEMAILS" AS "RECEIVESADMININFOEMAILS"
		, "CALCULATE_BK"."EMAILENCODINGKEY" AS "EMAILENCODINGKEY"
		, "CALCULATE_BK"."PROFILEID" AS "PROFILEID"
		, "CALCULATE_BK"."USERTYPE" AS "USERTYPE"
		, "CALCULATE_BK"."USERSUBTYPE" AS "USERSUBTYPE"
		, "CALCULATE_BK"."STARTDAY" AS "STARTDAY"
		, "CALCULATE_BK"."ENDDAY" AS "ENDDAY"
		, "CALCULATE_BK"."LANGUAGELOCALEKEY" AS "LANGUAGELOCALEKEY"
		, "CALCULATE_BK"."EMPLOYEENUMBER" AS "EMPLOYEENUMBER"
		, "CALCULATE_BK"."DELEGATEDAPPROVERID" AS "DELEGATEDAPPROVERID"
		, "CALCULATE_BK"."MANAGERID" AS "MANAGERID"
		, "CALCULATE_BK"."LASTLOGINDATE" AS "LASTLOGINDATE"
		, "CALCULATE_BK"."LASTPASSWORDCHANGEDATE" AS "LASTPASSWORDCHANGEDATE"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "CALCULATE_BK"."NUMBEROFFAILEDLOGINS" AS "NUMBEROFFAILEDLOGINS"
		, "CALCULATE_BK"."SUACCESSEXPIRATIONDATE" AS "SUACCESSEXPIRATIONDATE"
		, "CALCULATE_BK"."SUORGADMINEXPIRATIONDATE" AS "SUORGADMINEXPIRATIONDATE"
		, "CALCULATE_BK"."OFFLINETRIALEXPIRATIONDATE" AS "OFFLINETRIALEXPIRATIONDATE"
		, "CALCULATE_BK"."WIRELESSTRIALEXPIRATIONDATE" AS "WIRELESSTRIALEXPIRATIONDATE"
		, "CALCULATE_BK"."OFFLINEPDATRIALEXPIRATIONDATE" AS "OFFLINEPDATRIALEXPIRATIONDATE"
		, "CALCULATE_BK"."FORECASTENABLED" AS "FORECASTENABLED"
		, "CALCULATE_BK"."CONTACTID" AS "CONTACTID"
		, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
		, "CALCULATE_BK"."CALLCENTERID" AS "CALLCENTERID"
		, "CALCULATE_BK"."EXTENSION" AS "EXTENSION"
		, "CALCULATE_BK"."FEDERATIONIDENTIFIER" AS "FEDERATIONIDENTIFIER"
		, "CALCULATE_BK"."ABOUTME" AS "ABOUTME"
		, "CALCULATE_BK"."LOGINLIMIT" AS "LOGINLIMIT"
		, "CALCULATE_BK"."PROFILEPHOTOID" AS "PROFILEPHOTOID"
		, "CALCULATE_BK"."DIGESTFREQUENCY" AS "DIGESTFREQUENCY"
		, "CALCULATE_BK"."DEFAULTGROUPNOTIFICATIONFREQUENCY" AS "DEFAULTGROUPNOTIFICATIONFREQUENCY"
		, "CALCULATE_BK"."WORKSPACEID" AS "WORKSPACEID"
		, "CALCULATE_BK"."SHARINGTYPE" AS "SHARINGTYPE"
		, "CALCULATE_BK"."CHATTERADOPTIONSTAGE" AS "CHATTERADOPTIONSTAGE"
		, "CALCULATE_BK"."CHATTERADOPTIONSTAGEMODIFIEDDATE" AS "CHATTERADOPTIONSTAGEMODIFIEDDATE"
		, "CALCULATE_BK"."BANNERPHOTOID" AS "BANNERPHOTOID"
		, "CALCULATE_BK"."ISPROFILEPHOTOACTIVE" AS "ISPROFILEPHOTOACTIVE"
		, "CALCULATE_BK"."INDIVIDUALID" AS "INDIVIDUALID"
		, "CALCULATE_BK"."GLOBALIDENTITY" AS "GLOBALIDENTITY"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'