{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_ORDERS',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_ORDERS_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ORDERS_HKEY" AS "ORDERS_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
		, "DVT_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "DVT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "DVT_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "DVT_SRC"."CONTRACTID" AS "CONTRACTID"
		, "DVT_SRC"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
		, "DVT_SRC"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
		, "DVT_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "DVT_SRC"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
		, "DVT_SRC"."ORIGINALORDERID" AS "ORIGINALORDERID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."OWNERID" AS "OWNERID"
		, "DVT_SRC"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
		, "DVT_SRC"."ENDDATE" AS "ENDDATE"
		, "DVT_SRC"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
		, "DVT_SRC"."STATUS" AS "STATUS"
		, "DVT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "DVT_SRC"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
		, "DVT_SRC"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
		, "DVT_SRC"."TYPE" AS "TYPE"
		, "DVT_SRC"."BILLINGSTREET" AS "BILLINGSTREET"
		, "DVT_SRC"."BILLINGCITY" AS "BILLINGCITY"
		, "DVT_SRC"."BILLINGSTATE" AS "BILLINGSTATE"
		, "DVT_SRC"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
		, "DVT_SRC"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
		, "DVT_SRC"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
		, "DVT_SRC"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
		, "DVT_SRC"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
		, "DVT_SRC"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
		, "DVT_SRC"."SHIPPINGCITY" AS "SHIPPINGCITY"
		, "DVT_SRC"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
		, "DVT_SRC"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
		, "DVT_SRC"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
		, "DVT_SRC"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
		, "DVT_SRC"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
		, "DVT_SRC"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
		, "DVT_SRC"."NAME" AS "NAME"
		, "DVT_SRC"."PODATE" AS "PODATE"
		, "DVT_SRC"."PONUMBER" AS "PONUMBER"
		, "DVT_SRC"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
		, "DVT_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "DVT_SRC"."STATUSCODE" AS "STATUSCODE"
		, "DVT_SRC"."ORDERNUMBER" AS "ORDERNUMBER"
		, "DVT_SRC"."TOTALAMOUNT" AS "TOTALAMOUNT"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_ORDERS') }} "DVT_SRC"
