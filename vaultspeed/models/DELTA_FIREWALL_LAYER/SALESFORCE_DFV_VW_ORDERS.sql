{{
	config(
		materialized='view',
		alias='VW_ORDERS',
		schema='SALESFORCE_DFV',
		tags=['view', 'SALESFORCE', 'SRC_SALESFORCE_ORDERS_TDFV_INCR']
	)
}}
	WITH "DELTA_WINDOW" AS 
	( 
		SELECT 
			  "LWT_SRC"."FMC_BEGIN_LW_TIMESTAMP" AS "FMC_BEGIN_LW_TIMESTAMP"
			, "LWT_SRC"."FMC_END_LW_TIMESTAMP" AS "FMC_END_LW_TIMESTAMP"
		FROM {{ source('SALESFORCE_MTD', 'FMC_LOADING_WINDOW_TABLE') }} "LWT_SRC"
	)
	, "DELTA_VIEW_FILTER" AS 
	( 
		SELECT 
			  "CDC_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CDC_SRC"."JRN_FLAG" AS "JRN_FLAG"
			, TO_CHAR('S' ) AS "RECORD_TYPE"
			, "CDC_SRC"."ID" AS "ID"
			, "CDC_SRC"."CONTRACTID" AS "CONTRACTID"
			, "CDC_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "CDC_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "CDC_SRC"."ORIGINALORDERID" AS "ORIGINALORDERID"
			, "CDC_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "CDC_SRC"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
			, "CDC_SRC"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
			, "CDC_SRC"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
			, "CDC_SRC"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
			, "CDC_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "CDC_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "CDC_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CDC_SRC"."OWNERID" AS "OWNERID"
			, "CDC_SRC"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "CDC_SRC"."ENDDATE" AS "ENDDATE"
			, "CDC_SRC"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "CDC_SRC"."STATUS" AS "STATUS"
			, "CDC_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "CDC_SRC"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "CDC_SRC"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "CDC_SRC"."TYPE" AS "TYPE"
			, "CDC_SRC"."BILLINGSTREET" AS "BILLINGSTREET"
			, "CDC_SRC"."BILLINGCITY" AS "BILLINGCITY"
			, "CDC_SRC"."BILLINGSTATE" AS "BILLINGSTATE"
			, "CDC_SRC"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "CDC_SRC"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "CDC_SRC"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "CDC_SRC"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "CDC_SRC"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "CDC_SRC"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "CDC_SRC"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "CDC_SRC"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "CDC_SRC"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "CDC_SRC"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "CDC_SRC"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "CDC_SRC"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "CDC_SRC"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "CDC_SRC"."NAME" AS "NAME"
			, "CDC_SRC"."PODATE" AS "PODATE"
			, "CDC_SRC"."PONUMBER" AS "PONUMBER"
			, "CDC_SRC"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "CDC_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "CDC_SRC"."STATUSCODE" AS "STATUSCODE"
			, "CDC_SRC"."ORDERNUMBER" AS "ORDERNUMBER"
			, "CDC_SRC"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "CDC_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "CDC_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CDC_SRC"."ISDELETED" AS "ISDELETED"
			, "CDC_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		FROM {{ source('SALESFORCE_CDC', 'CDC_ORDERS') }} "CDC_SRC"
		INNER JOIN "DELTA_WINDOW" "DELTA_WINDOW" ON  1 = 1
		WHERE  "CDC_SRC"."CDC_TIMESTAMP" > "DELTA_WINDOW"."FMC_BEGIN_LW_TIMESTAMP" AND "CDC_SRC"."CDC_TIMESTAMP" <= "DELTA_WINDOW"."FMC_END_LW_TIMESTAMP"
	)
	, "DELTA_VIEW" AS 
	( 
		SELECT 
			  "DELTA_VIEW_FILTER"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW_FILTER"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW_FILTER"."RECORD_TYPE" AS "RECORD_TYPE"
			, "DELTA_VIEW_FILTER"."ID" AS "ID"
			, "DELTA_VIEW_FILTER"."CONTRACTID" AS "CONTRACTID"
			, "DELTA_VIEW_FILTER"."ACCOUNTID" AS "ACCOUNTID"
			, "DELTA_VIEW_FILTER"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "DELTA_VIEW_FILTER"."ORIGINALORDERID" AS "ORIGINALORDERID"
			, "DELTA_VIEW_FILTER"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "DELTA_VIEW_FILTER"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
			, "DELTA_VIEW_FILTER"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
			, "DELTA_VIEW_FILTER"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
			, "DELTA_VIEW_FILTER"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
			, "DELTA_VIEW_FILTER"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "DELTA_VIEW_FILTER"."CREATEDBYID" AS "CREATEDBYID"
			, "DELTA_VIEW_FILTER"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "DELTA_VIEW_FILTER"."OWNERID" AS "OWNERID"
			, "DELTA_VIEW_FILTER"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "DELTA_VIEW_FILTER"."ENDDATE" AS "ENDDATE"
			, "DELTA_VIEW_FILTER"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "DELTA_VIEW_FILTER"."STATUS" AS "STATUS"
			, "DELTA_VIEW_FILTER"."DESCRIPTION" AS "DESCRIPTION"
			, "DELTA_VIEW_FILTER"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "DELTA_VIEW_FILTER"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "DELTA_VIEW_FILTER"."TYPE" AS "TYPE"
			, "DELTA_VIEW_FILTER"."BILLINGSTREET" AS "BILLINGSTREET"
			, "DELTA_VIEW_FILTER"."BILLINGCITY" AS "BILLINGCITY"
			, "DELTA_VIEW_FILTER"."BILLINGSTATE" AS "BILLINGSTATE"
			, "DELTA_VIEW_FILTER"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "DELTA_VIEW_FILTER"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "DELTA_VIEW_FILTER"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "DELTA_VIEW_FILTER"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "DELTA_VIEW_FILTER"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "DELTA_VIEW_FILTER"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "DELTA_VIEW_FILTER"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "DELTA_VIEW_FILTER"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "DELTA_VIEW_FILTER"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "DELTA_VIEW_FILTER"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "DELTA_VIEW_FILTER"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "DELTA_VIEW_FILTER"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "DELTA_VIEW_FILTER"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "DELTA_VIEW_FILTER"."NAME" AS "NAME"
			, "DELTA_VIEW_FILTER"."PODATE" AS "PODATE"
			, "DELTA_VIEW_FILTER"."PONUMBER" AS "PONUMBER"
			, "DELTA_VIEW_FILTER"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "DELTA_VIEW_FILTER"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "DELTA_VIEW_FILTER"."STATUSCODE" AS "STATUSCODE"
			, "DELTA_VIEW_FILTER"."ORDERNUMBER" AS "ORDERNUMBER"
			, "DELTA_VIEW_FILTER"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "DELTA_VIEW_FILTER"."CREATEDDATE" AS "CREATEDDATE"
			, "DELTA_VIEW_FILTER"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "DELTA_VIEW_FILTER"."ISDELETED" AS "ISDELETED"
			, "DELTA_VIEW_FILTER"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		FROM "DELTA_VIEW_FILTER" "DELTA_VIEW_FILTER"
	)
	, "PREPJOINBK" AS 
	( 
		SELECT 
			  "DELTA_VIEW"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW"."RECORD_TYPE" AS "RECORD_TYPE"
			, COALESCE("DELTA_VIEW"."ID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID"
			, COALESCE("DELTA_VIEW"."CREATEDBYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "CREATEDBYID"
			, COALESCE("DELTA_VIEW"."ORIGINALORDERID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ORIGINALORDERID"
			, COALESCE("DELTA_VIEW"."SHIPTOCONTACTID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "SHIPTOCONTACTID"
			, COALESCE("DELTA_VIEW"."ACTIVATEDBYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ACTIVATEDBYID"
			, COALESCE("DELTA_VIEW"."CUSTOMERAUTHORIZEDBYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "CUSTOMERAUTHORIZEDBYID"
			, COALESCE("DELTA_VIEW"."BILLTOCONTACTID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BILLTOCONTACTID"
			, COALESCE("DELTA_VIEW"."CONTRACTID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "CONTRACTID"
			, COALESCE("DELTA_VIEW"."PRICEBOOK2ID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "PRICEBOOK2ID"
			, COALESCE("DELTA_VIEW"."ACCOUNTID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ACCOUNTID"
			, COALESCE("DELTA_VIEW"."OPPORTUNITYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "OPPORTUNITYID"
			, COALESCE("DELTA_VIEW"."COMPANYAUTHORIZEDBYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "COMPANYAUTHORIZEDBYID"
			, COALESCE("DELTA_VIEW"."LASTMODIFIEDBYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "LASTMODIFIEDBYID"
			, "DELTA_VIEW"."OWNERID" AS "OWNERID"
			, "DELTA_VIEW"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "DELTA_VIEW"."ENDDATE" AS "ENDDATE"
			, "DELTA_VIEW"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "DELTA_VIEW"."STATUS" AS "STATUS"
			, "DELTA_VIEW"."DESCRIPTION" AS "DESCRIPTION"
			, "DELTA_VIEW"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "DELTA_VIEW"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "DELTA_VIEW"."TYPE" AS "TYPE"
			, "DELTA_VIEW"."BILLINGSTREET" AS "BILLINGSTREET"
			, "DELTA_VIEW"."BILLINGCITY" AS "BILLINGCITY"
			, "DELTA_VIEW"."BILLINGSTATE" AS "BILLINGSTATE"
			, "DELTA_VIEW"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "DELTA_VIEW"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "DELTA_VIEW"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "DELTA_VIEW"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "DELTA_VIEW"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "DELTA_VIEW"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "DELTA_VIEW"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "DELTA_VIEW"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "DELTA_VIEW"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "DELTA_VIEW"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "DELTA_VIEW"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "DELTA_VIEW"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "DELTA_VIEW"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "DELTA_VIEW"."NAME" AS "NAME"
			, "DELTA_VIEW"."PODATE" AS "PODATE"
			, "DELTA_VIEW"."PONUMBER" AS "PONUMBER"
			, "DELTA_VIEW"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "DELTA_VIEW"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "DELTA_VIEW"."STATUSCODE" AS "STATUSCODE"
			, "DELTA_VIEW"."ORDERNUMBER" AS "ORDERNUMBER"
			, "DELTA_VIEW"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "DELTA_VIEW"."CREATEDDATE" AS "CREATEDDATE"
			, "DELTA_VIEW"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "DELTA_VIEW"."ISDELETED" AS "ISDELETED"
			, "DELTA_VIEW"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		FROM "DELTA_VIEW" "DELTA_VIEW"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_BK_SRC" ON  1 = 1
		WHERE  "MEX_BK_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "PREPJOINBK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "PREPJOINBK"."JRN_FLAG" AS "JRN_FLAG"
		, "PREPJOINBK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "PREPJOINBK"."ID" AS "ID"
		, "PREPJOINBK"."CONTRACTID" AS "CONTRACTID"
		, "PREPJOINBK"."ACCOUNTID" AS "ACCOUNTID"
		, "PREPJOINBK"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "PREPJOINBK"."ORIGINALORDERID" AS "ORIGINALORDERID"
		, "PREPJOINBK"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "PREPJOINBK"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
		, "PREPJOINBK"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
		, "PREPJOINBK"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
		, "PREPJOINBK"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
		, "PREPJOINBK"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "PREPJOINBK"."CREATEDBYID" AS "CREATEDBYID"
		, "PREPJOINBK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "PREPJOINBK"."OWNERID" AS "OWNERID"
		, "PREPJOINBK"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
		, "PREPJOINBK"."ENDDATE" AS "ENDDATE"
		, "PREPJOINBK"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
		, "PREPJOINBK"."STATUS" AS "STATUS"
		, "PREPJOINBK"."DESCRIPTION" AS "DESCRIPTION"
		, "PREPJOINBK"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
		, "PREPJOINBK"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
		, "PREPJOINBK"."TYPE" AS "TYPE"
		, "PREPJOINBK"."BILLINGSTREET" AS "BILLINGSTREET"
		, "PREPJOINBK"."BILLINGCITY" AS "BILLINGCITY"
		, "PREPJOINBK"."BILLINGSTATE" AS "BILLINGSTATE"
		, "PREPJOINBK"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
		, "PREPJOINBK"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
		, "PREPJOINBK"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
		, "PREPJOINBK"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
		, "PREPJOINBK"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
		, "PREPJOINBK"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
		, "PREPJOINBK"."SHIPPINGCITY" AS "SHIPPINGCITY"
		, "PREPJOINBK"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
		, "PREPJOINBK"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
		, "PREPJOINBK"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
		, "PREPJOINBK"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
		, "PREPJOINBK"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
		, "PREPJOINBK"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
		, "PREPJOINBK"."NAME" AS "NAME"
		, "PREPJOINBK"."PODATE" AS "PODATE"
		, "PREPJOINBK"."PONUMBER" AS "PONUMBER"
		, "PREPJOINBK"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
		, "PREPJOINBK"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "PREPJOINBK"."STATUSCODE" AS "STATUSCODE"
		, "PREPJOINBK"."ORDERNUMBER" AS "ORDERNUMBER"
		, "PREPJOINBK"."TOTALAMOUNT" AS "TOTALAMOUNT"
		, "PREPJOINBK"."CREATEDDATE" AS "CREATEDDATE"
		, "PREPJOINBK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "PREPJOINBK"."ISDELETED" AS "ISDELETED"
		, "PREPJOINBK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
	FROM "PREPJOINBK" "PREPJOINBK"
