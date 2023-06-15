{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='ACCOUNT',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'STG_SALESFORCE_ACCOUNT_INCR', 'STG_SALESFORCE_ACCOUNT_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_PARENTID_BK" || '\#' )) AS "ACCOUNT_PARENTID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || '\#' )) AS "ACCOUNT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || 
			'\#' )) AS "LNK_ACCOUNT_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_PARENTID_BK" || 
			'\#' )) AS "LNK_ACCOUNT_ACCOUNT_PARENTID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || 
			'\#' )) AS "LNK_ACCOUNT_ACCOUNT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )
			) AS "LNK_ACCOUNT_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "LNK_ACCOUNT_USER_OWNERID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, 'SALESFORCE' AS "SRC_BK"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_SRC"."PARENTID" AS "PARENTID"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ACCOUNT_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "EXT_SRC"."ID_FK_MASTERRECORDID_BK" AS "ID_FK_MASTERRECORDID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."TYPE" AS "TYPE"
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
		, "EXT_SRC"."PHONE" AS "PHONE"
		, "EXT_SRC"."FAX" AS "FAX"
		, "EXT_SRC"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
		, "EXT_SRC"."WEBSITE" AS "WEBSITE"
		, "EXT_SRC"."SIC" AS "SIC"
		, "EXT_SRC"."INDUSTRY" AS "INDUSTRY"
		, "EXT_SRC"."ANNUALREVENUE" AS "ANNUALREVENUE"
		, "EXT_SRC"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
		, "EXT_SRC"."OWNERSHIP" AS "OWNERSHIP"
		, "EXT_SRC"."TICKERSYMBOL" AS "TICKERSYMBOL"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."RATING" AS "RATING"
		, "EXT_SRC"."SITE" AS "SITE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "EXT_SRC"."JIGSAW" AS "JIGSAW"
		, "EXT_SRC"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
		, "EXT_SRC"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
		, "EXT_SRC"."SICDESC" AS "SICDESC"
		, "EXT_SRC"."SEGMENT__C" AS "SEGMENT__C"
		, "EXT_SRC"."DEMO_COLUMN" AS "DEMO_COLUMN"
	FROM {{ ref('SALESFORCE_EXT_ACCOUNT') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_PARENTID_BK" || '\#' )) AS "ACCOUNT_PARENTID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || '\#' )) AS "ACCOUNT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "USER_OWNERID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || 
			'\#' )) AS "LNK_ACCOUNT_USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_PARENTID_BK" || 
			'\#' )) AS "LNK_ACCOUNT_ACCOUNT_PARENTID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_MASTERRECORDID_BK" || 
			'\#' )) AS "LNK_ACCOUNT_ACCOUNT_MASTERRECORDID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )
			) AS "LNK_ACCOUNT_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OWNERID_BK" || '\#' )) AS "LNK_ACCOUNT_USER_OWNERID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, 'SALESFORCE' AS "SRC_BK"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_SRC"."PARENTID" AS "PARENTID"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ACCOUNT_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "EXT_SRC"."ID_FK_MASTERRECORDID_BK" AS "ID_FK_MASTERRECORDID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."TYPE" AS "TYPE"
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
		, "EXT_SRC"."PHONE" AS "PHONE"
		, "EXT_SRC"."FAX" AS "FAX"
		, "EXT_SRC"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
		, "EXT_SRC"."WEBSITE" AS "WEBSITE"
		, "EXT_SRC"."SIC" AS "SIC"
		, "EXT_SRC"."INDUSTRY" AS "INDUSTRY"
		, "EXT_SRC"."ANNUALREVENUE" AS "ANNUALREVENUE"
		, "EXT_SRC"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
		, "EXT_SRC"."OWNERSHIP" AS "OWNERSHIP"
		, "EXT_SRC"."TICKERSYMBOL" AS "TICKERSYMBOL"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."RATING" AS "RATING"
		, "EXT_SRC"."SITE" AS "SITE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "EXT_SRC"."JIGSAW" AS "JIGSAW"
		, "EXT_SRC"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
		, "EXT_SRC"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
		, "EXT_SRC"."SICDESC" AS "SICDESC"
		, "EXT_SRC"."SEGMENT__C" AS "SEGMENT__C"
		, "EXT_SRC"."DEMO_COLUMN" AS "DEMO_COLUMN"
	FROM {{ ref('SALESFORCE_EXT_ACCOUNT') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'