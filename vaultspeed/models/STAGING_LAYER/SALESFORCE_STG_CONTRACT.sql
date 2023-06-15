{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='CONTRACT',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'STG_SALESFORCE_CONTRACT_INCR', 'STG_SALESFORCE_CONTRACT_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "CONTRACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CUSTOMERSIGNEDID_BK" || '\#' )) AS "CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_COMPANYSIGNEDID_BK" || '\#' )) AS "USER_COMPANYSIGNEDID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CUSTOMERSIGNEDID_BK" || '\#' )) AS "LNK_CONTRACT_CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_CONTRACT_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_CONTRACT_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "LNK_CONTRACT_USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "LNK_CONTRACT_PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_CONTRACT_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "LNK_CONTRACT_USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_COMPANYSIGNEDID_BK" || '\#' )) AS "LNK_CONTRACT_USER_COMPANYSIGNEDID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
		, "EXT_SRC"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
		, "EXT_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_CUSTOMERSIGNEDID_BK" AS "ID_FK_CUSTOMERSIGNEDID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "EXT_SRC"."ID_FK_COMPANYSIGNEDID_BK" AS "ID_FK_COMPANYSIGNEDID_BK"
		, "EXT_SRC"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
		, "EXT_SRC"."STARTDATE" AS "STARTDATE"
		, "EXT_SRC"."BILLINGSTREET" AS "BILLINGSTREET"
		, "EXT_SRC"."BILLINGCITY" AS "BILLINGCITY"
		, "EXT_SRC"."BILLINGSTATE" AS "BILLINGSTATE"
		, "EXT_SRC"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
		, "EXT_SRC"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
		, "EXT_SRC"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
		, "EXT_SRC"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
		, "EXT_SRC"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
		, "EXT_SRC"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
		, "EXT_SRC"."SHIPPINGCITY" AS "SHIPPINGCITY"
		, "EXT_SRC"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
		, "EXT_SRC"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
		, "EXT_SRC"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
		, "EXT_SRC"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
		, "EXT_SRC"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
		, "EXT_SRC"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
		, "EXT_SRC"."CONTRACTTERM" AS "CONTRACTTERM"
		, "EXT_SRC"."STATUS" AS "STATUS"
		, "EXT_SRC"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
		, "EXT_SRC"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
		, "EXT_SRC"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
		, "EXT_SRC"."SPECIALTERMS" AS "SPECIALTERMS"
		, "EXT_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "EXT_SRC"."STATUSCODE" AS "STATUSCODE"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
		, "EXT_SRC"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
	FROM {{ ref('SALESFORCE_EXT_CONTRACT') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "CONTRACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CUSTOMERSIGNEDID_BK" || '\#' )) AS "CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_COMPANYSIGNEDID_BK" || '\#' )) AS "USER_COMPANYSIGNEDID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CUSTOMERSIGNEDID_BK" || '\#' )) AS "LNK_CONTRACT_CONTACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_CONTRACT_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_CONTRACT_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "LNK_CONTRACT_USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "LNK_CONTRACT_PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_CONTRACT_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "LNK_CONTRACT_USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_COMPANYSIGNEDID_BK" || '\#' )) AS "LNK_CONTRACT_USER_COMPANYSIGNEDID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
		, "EXT_SRC"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
		, "EXT_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_CUSTOMERSIGNEDID_BK" AS "ID_FK_CUSTOMERSIGNEDID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "EXT_SRC"."ID_FK_COMPANYSIGNEDID_BK" AS "ID_FK_COMPANYSIGNEDID_BK"
		, "EXT_SRC"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
		, "EXT_SRC"."STARTDATE" AS "STARTDATE"
		, "EXT_SRC"."BILLINGSTREET" AS "BILLINGSTREET"
		, "EXT_SRC"."BILLINGCITY" AS "BILLINGCITY"
		, "EXT_SRC"."BILLINGSTATE" AS "BILLINGSTATE"
		, "EXT_SRC"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
		, "EXT_SRC"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
		, "EXT_SRC"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
		, "EXT_SRC"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
		, "EXT_SRC"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
		, "EXT_SRC"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
		, "EXT_SRC"."SHIPPINGCITY" AS "SHIPPINGCITY"
		, "EXT_SRC"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
		, "EXT_SRC"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
		, "EXT_SRC"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
		, "EXT_SRC"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
		, "EXT_SRC"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
		, "EXT_SRC"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
		, "EXT_SRC"."CONTRACTTERM" AS "CONTRACTTERM"
		, "EXT_SRC"."STATUS" AS "STATUS"
		, "EXT_SRC"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
		, "EXT_SRC"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
		, "EXT_SRC"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
		, "EXT_SRC"."SPECIALTERMS" AS "SPECIALTERMS"
		, "EXT_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "EXT_SRC"."STATUSCODE" AS "STATUSCODE"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
		, "EXT_SRC"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
	FROM {{ ref('SALESFORCE_EXT_CONTRACT') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'