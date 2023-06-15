{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='ORDERS',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'STG_SALESFORCE_ORDERS_INCR', 'STG_SALESFORCE_ORDERS_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "ORDERS_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ORIGINALORDERID_BK" || '\#' )) AS "ORDERS_ORIGINALORDERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_SHIPTOCONTACTID_BK" || '\#' )) AS "CONTACT_SHIPTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" || '\#' )) AS "CONTACT_CUSTOMERAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_BILLTOCONTACTID_BK" || '\#' )) AS "CONTACT_BILLTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CONTRACTID_BK" || '\#' )) AS "CONTRACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OPPORTUNITYID_BK" || '\#' )) AS "OPPORTUNITY_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_COMPANYAUTHORIZEDBYID_BK" || '\#' )) AS "USER_COMPANYAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ORIGINALORDERID_BK" || '\#' )) AS "LNK_ORDERS_ORDERS_ORIGINALORDERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_SHIPTOCONTACTID_BK" || '\#' )) AS "LNK_ORDERS_CONTACT_SHIPTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" || '\#' )) AS "LNK_ORDERS_CONTACT_CUSTOMERAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_BILLTOCONTACTID_BK" || '\#' )) AS "LNK_ORDERS_CONTACT_BILLTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CONTRACTID_BK" || '\#' )) AS "LNK_ORDERS_CONTRACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "LNK_ORDERS_PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_ORDERS_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OPPORTUNITYID_BK" || '\#' )) AS "LNK_ORDERS_OPPORTUNITY_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_COMPANYAUTHORIZEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_COMPANYAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_LASTMODIFIEDBYID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."CONTRACTID" AS "CONTRACTID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "EXT_SRC"."ORIGINALORDERID" AS "ORIGINALORDERID"
		, "EXT_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "EXT_SRC"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
		, "EXT_SRC"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
		, "EXT_SRC"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
		, "EXT_SRC"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
		, "EXT_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_ORIGINALORDERID_BK" AS "ID_FK_ORIGINALORDERID_BK"
		, "EXT_SRC"."ID_FK_SHIPTOCONTACTID_BK" AS "ID_FK_SHIPTOCONTACTID_BK"
		, "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "EXT_SRC"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" AS "ID_FK_CUSTOMERAUTHORIZEDBYID_BK"
		, "EXT_SRC"."ID_FK_BILLTOCONTACTID_BK" AS "ID_FK_BILLTOCONTACTID_BK"
		, "EXT_SRC"."ID_FK_CONTRACTID_BK" AS "ID_FK_CONTRACTID_BK"
		, "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_OPPORTUNITYID_BK" AS "ID_FK_OPPORTUNITYID_BK"
		, "EXT_SRC"."ID_FK_COMPANYAUTHORIZEDBYID_BK" AS "ID_FK_COMPANYAUTHORIZEDBYID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
		, "EXT_SRC"."ENDDATE" AS "ENDDATE"
		, "EXT_SRC"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
		, "EXT_SRC"."STATUS" AS "STATUS"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
		, "EXT_SRC"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
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
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."PODATE" AS "PODATE"
		, "EXT_SRC"."PONUMBER" AS "PONUMBER"
		, "EXT_SRC"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
		, "EXT_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "EXT_SRC"."STATUSCODE" AS "STATUSCODE"
		, "EXT_SRC"."ORDERNUMBER" AS "ORDERNUMBER"
		, "EXT_SRC"."TOTALAMOUNT" AS "TOTALAMOUNT"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
	FROM {{ ref('SALESFORCE_EXT_ORDERS') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' )) AS "ORDERS_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ORIGINALORDERID_BK" || '\#' )) AS "ORDERS_ORIGINALORDERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_SHIPTOCONTACTID_BK" || '\#' )) AS "CONTACT_SHIPTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" || '\#' )) AS "CONTACT_CUSTOMERAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_BILLTOCONTACTID_BK" || '\#' )) AS "CONTACT_BILLTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CONTRACTID_BK" || '\#' )) AS "CONTRACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_OPPORTUNITYID_BK" || '\#' )) AS "OPPORTUNITY_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_COMPANYAUTHORIZEDBYID_BK" || '\#' )) AS "USER_COMPANYAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "USER_LASTMODIFIEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_CREATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ORIGINALORDERID_BK" || '\#' )) AS "LNK_ORDERS_ORDERS_ORIGINALORDERID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_SHIPTOCONTACTID_BK" || '\#' )) AS "LNK_ORDERS_CONTACT_SHIPTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_ACTIVATEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" || '\#' )) AS "LNK_ORDERS_CONTACT_CUSTOMERAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_BILLTOCONTACTID_BK" || '\#' )) AS "LNK_ORDERS_CONTACT_BILLTOCONTACTID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CONTRACTID_BK" || '\#' )) AS "LNK_ORDERS_CONTRACT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" || '\#' )) AS "LNK_ORDERS_PRICEBOOK2_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || 'SALESFORCE' || '\#' || "EXT_SRC"."ID_FK_ACCOUNTID_BK" || '\#' )) AS "LNK_ORDERS_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_OPPORTUNITYID_BK" || '\#' )) AS "LNK_ORDERS_OPPORTUNITY_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_COMPANYAUTHORIZEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_COMPANYAUTHORIZEDBYID_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" || '\#' )) AS "LNK_ORDERS_USER_LASTMODIFIEDBYID_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."CONTRACTID" AS "CONTRACTID"
		, "EXT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "EXT_SRC"."ORIGINALORDERID" AS "ORIGINALORDERID"
		, "EXT_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "EXT_SRC"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
		, "EXT_SRC"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
		, "EXT_SRC"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
		, "EXT_SRC"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
		, "EXT_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."ID_BK" AS "ID_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."ID_FK_ORIGINALORDERID_BK" AS "ID_FK_ORIGINALORDERID_BK"
		, "EXT_SRC"."ID_FK_SHIPTOCONTACTID_BK" AS "ID_FK_SHIPTOCONTACTID_BK"
		, "EXT_SRC"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "EXT_SRC"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" AS "ID_FK_CUSTOMERAUTHORIZEDBYID_BK"
		, "EXT_SRC"."ID_FK_BILLTOCONTACTID_BK" AS "ID_FK_BILLTOCONTACTID_BK"
		, "EXT_SRC"."ID_FK_CONTRACTID_BK" AS "ID_FK_CONTRACTID_BK"
		, "EXT_SRC"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "EXT_SRC"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_SRC"."ID_FK_OPPORTUNITYID_BK" AS "ID_FK_OPPORTUNITYID_BK"
		, "EXT_SRC"."ID_FK_COMPANYAUTHORIZEDBYID_BK" AS "ID_FK_COMPANYAUTHORIZEDBYID_BK"
		, "EXT_SRC"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_SRC"."OWNERID" AS "OWNERID"
		, "EXT_SRC"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
		, "EXT_SRC"."ENDDATE" AS "ENDDATE"
		, "EXT_SRC"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
		, "EXT_SRC"."STATUS" AS "STATUS"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
		, "EXT_SRC"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
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
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."PODATE" AS "PODATE"
		, "EXT_SRC"."PONUMBER" AS "PONUMBER"
		, "EXT_SRC"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
		, "EXT_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "EXT_SRC"."STATUSCODE" AS "STATUSCODE"
		, "EXT_SRC"."ORDERNUMBER" AS "ORDERNUMBER"
		, "EXT_SRC"."TOTALAMOUNT" AS "TOTALAMOUNT"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
	FROM {{ ref('SALESFORCE_EXT_ORDERS') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'