{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='ORDERS',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_ORDERS_INCR', 'EXT_SALESFORCE_ORDERS_INIT']
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
			, "TDFV_SRC"."CONTRACTID" AS "CONTRACTID"
			, "TDFV_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "TDFV_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "TDFV_SRC"."ORIGINALORDERID" AS "ORIGINALORDERID"
			, "TDFV_SRC"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "TDFV_SRC"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
			, "TDFV_SRC"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
			, "TDFV_SRC"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
			, "TDFV_SRC"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
			, "TDFV_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ORIGINALORDERID"),'\#','\\' || '\#') AS "ID_FK_ORIGINALORDERID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."SHIPTOCONTACTID"),'\#','\\' || '\#') AS "ID_FK_SHIPTOCONTACTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ACTIVATEDBYID"),'\#','\\' || '\#') AS "ID_FK_ACTIVATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CUSTOMERAUTHORIZEDBYID"),'\#','\\' || '\#') AS "ID_FK_CUSTOMERAUTHORIZEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."BILLTOCONTACTID"),'\#','\\' || '\#') AS "ID_FK_BILLTOCONTACTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CONTRACTID"),'\#','\\' || '\#') AS "ID_FK_CONTRACTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."PRICEBOOK2ID"),'\#','\\' || '\#') AS "ID_FK_PRICEBOOK2ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."OPPORTUNITYID"),'\#','\\' || '\#') AS "ID_FK_OPPORTUNITYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."COMPANYAUTHORIZEDBYID"),'\#','\\' || '\#') AS "ID_FK_COMPANYAUTHORIZEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "TDFV_SRC"."OWNERID" AS "OWNERID"
			, "TDFV_SRC"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "TDFV_SRC"."ENDDATE" AS "ENDDATE"
			, "TDFV_SRC"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "TDFV_SRC"."STATUS" AS "STATUS"
			, "TDFV_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "TDFV_SRC"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "TDFV_SRC"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "TDFV_SRC"."TYPE" AS "TYPE"
			, "TDFV_SRC"."BILLINGSTREET" AS "BILLINGSTREET"
			, "TDFV_SRC"."BILLINGCITY" AS "BILLINGCITY"
			, "TDFV_SRC"."BILLINGSTATE" AS "BILLINGSTATE"
			, "TDFV_SRC"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "TDFV_SRC"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "TDFV_SRC"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "TDFV_SRC"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "TDFV_SRC"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "TDFV_SRC"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "TDFV_SRC"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "TDFV_SRC"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "TDFV_SRC"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "TDFV_SRC"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "TDFV_SRC"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "TDFV_SRC"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "TDFV_SRC"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "TDFV_SRC"."NAME" AS "NAME"
			, "TDFV_SRC"."PODATE" AS "PODATE"
			, "TDFV_SRC"."PONUMBER" AS "PONUMBER"
			, "TDFV_SRC"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "TDFV_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "TDFV_SRC"."STATUSCODE" AS "STATUSCODE"
			, "TDFV_SRC"."ORDERNUMBER" AS "ORDERNUMBER"
			, "TDFV_SRC"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."ISDELETED" AS "ISDELETED"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		FROM {{ ref('SALESFORCE_DFV_VW_ORDERS') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."CONTRACTID" AS "CONTRACTID"
			, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
			, "CALCULATE_BK"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "CALCULATE_BK"."ORIGINALORDERID" AS "ORIGINALORDERID"
			, "CALCULATE_BK"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "CALCULATE_BK"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
			, "CALCULATE_BK"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
			, "CALCULATE_BK"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
			, "CALCULATE_BK"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
			, "CALCULATE_BK"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_ORIGINALORDERID_BK" AS "ID_FK_ORIGINALORDERID_BK"
			, "CALCULATE_BK"."ID_FK_SHIPTOCONTACTID_BK" AS "ID_FK_SHIPTOCONTACTID_BK"
			, "CALCULATE_BK"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" AS "ID_FK_CUSTOMERAUTHORIZEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_BILLTOCONTACTID_BK" AS "ID_FK_BILLTOCONTACTID_BK"
			, "CALCULATE_BK"."ID_FK_CONTRACTID_BK" AS "ID_FK_CONTRACTID_BK"
			, "CALCULATE_BK"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
			, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
			, "CALCULATE_BK"."ID_FK_OPPORTUNITYID_BK" AS "ID_FK_OPPORTUNITYID_BK"
			, "CALCULATE_BK"."ID_FK_COMPANYAUTHORIZEDBYID_BK" AS "ID_FK_COMPANYAUTHORIZEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "CALCULATE_BK"."OWNERID" AS "OWNERID"
			, "CALCULATE_BK"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "CALCULATE_BK"."ENDDATE" AS "ENDDATE"
			, "CALCULATE_BK"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "CALCULATE_BK"."STATUS" AS "STATUS"
			, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
			, "CALCULATE_BK"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "CALCULATE_BK"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "CALCULATE_BK"."TYPE" AS "TYPE"
			, "CALCULATE_BK"."BILLINGSTREET" AS "BILLINGSTREET"
			, "CALCULATE_BK"."BILLINGCITY" AS "BILLINGCITY"
			, "CALCULATE_BK"."BILLINGSTATE" AS "BILLINGSTATE"
			, "CALCULATE_BK"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "CALCULATE_BK"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "CALCULATE_BK"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "CALCULATE_BK"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "CALCULATE_BK"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "CALCULATE_BK"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "CALCULATE_BK"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "CALCULATE_BK"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "CALCULATE_BK"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "CALCULATE_BK"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "CALCULATE_BK"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "CALCULATE_BK"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "CALCULATE_BK"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "CALCULATE_BK"."NAME" AS "NAME"
			, "CALCULATE_BK"."PODATE" AS "PODATE"
			, "CALCULATE_BK"."PONUMBER" AS "PONUMBER"
			, "CALCULATE_BK"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "CALCULATE_BK"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "CALCULATE_BK"."STATUSCODE" AS "STATUSCODE"
			, "CALCULATE_BK"."ORDERNUMBER" AS "ORDERNUMBER"
			, "CALCULATE_BK"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."ID" AS "ID"
		, "EXT_UNION"."CONTRACTID" AS "CONTRACTID"
		, "EXT_UNION"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_UNION"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "EXT_UNION"."ORIGINALORDERID" AS "ORIGINALORDERID"
		, "EXT_UNION"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "EXT_UNION"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
		, "EXT_UNION"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
		, "EXT_UNION"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
		, "EXT_UNION"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
		, "EXT_UNION"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_UNION"."ID_FK_ORIGINALORDERID_BK" AS "ID_FK_ORIGINALORDERID_BK"
		, "EXT_UNION"."ID_FK_SHIPTOCONTACTID_BK" AS "ID_FK_SHIPTOCONTACTID_BK"
		, "EXT_UNION"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "EXT_UNION"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" AS "ID_FK_CUSTOMERAUTHORIZEDBYID_BK"
		, "EXT_UNION"."ID_FK_BILLTOCONTACTID_BK" AS "ID_FK_BILLTOCONTACTID_BK"
		, "EXT_UNION"."ID_FK_CONTRACTID_BK" AS "ID_FK_CONTRACTID_BK"
		, "EXT_UNION"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "EXT_UNION"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_UNION"."ID_FK_OPPORTUNITYID_BK" AS "ID_FK_OPPORTUNITYID_BK"
		, "EXT_UNION"."ID_FK_COMPANYAUTHORIZEDBYID_BK" AS "ID_FK_COMPANYAUTHORIZEDBYID_BK"
		, "EXT_UNION"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_UNION"."OWNERID" AS "OWNERID"
		, "EXT_UNION"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
		, "EXT_UNION"."ENDDATE" AS "ENDDATE"
		, "EXT_UNION"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
		, "EXT_UNION"."STATUS" AS "STATUS"
		, "EXT_UNION"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_UNION"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
		, "EXT_UNION"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
		, "EXT_UNION"."TYPE" AS "TYPE"
		, "EXT_UNION"."BILLINGSTREET" AS "BILLINGSTREET"
		, "EXT_UNION"."BILLINGCITY" AS "BILLINGCITY"
		, "EXT_UNION"."BILLINGSTATE" AS "BILLINGSTATE"
		, "EXT_UNION"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
		, "EXT_UNION"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
		, "EXT_UNION"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
		, "EXT_UNION"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
		, "EXT_UNION"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
		, "EXT_UNION"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
		, "EXT_UNION"."SHIPPINGCITY" AS "SHIPPINGCITY"
		, "EXT_UNION"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
		, "EXT_UNION"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
		, "EXT_UNION"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
		, "EXT_UNION"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
		, "EXT_UNION"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
		, "EXT_UNION"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
		, "EXT_UNION"."NAME" AS "NAME"
		, "EXT_UNION"."PODATE" AS "PODATE"
		, "EXT_UNION"."PONUMBER" AS "PONUMBER"
		, "EXT_UNION"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
		, "EXT_UNION"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "EXT_UNION"."STATUSCODE" AS "STATUSCODE"
		, "EXT_UNION"."ORDERNUMBER" AS "ORDERNUMBER"
		, "EXT_UNION"."TOTALAMOUNT" AS "TOTALAMOUNT"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."ISDELETED" AS "ISDELETED"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
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
			, COALESCE("INI_SRC"."CREATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CREATEDBYID"
			, COALESCE("INI_SRC"."ORIGINALORDERID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ORIGINALORDERID"
			, COALESCE("INI_SRC"."SHIPTOCONTACTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "SHIPTOCONTACTID"
			, COALESCE("INI_SRC"."ACTIVATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ACTIVATEDBYID"
			, COALESCE("INI_SRC"."CUSTOMERAUTHORIZEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CUSTOMERAUTHORIZEDBYID"
			, COALESCE("INI_SRC"."BILLTOCONTACTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BILLTOCONTACTID"
			, COALESCE("INI_SRC"."CONTRACTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CONTRACTID"
			, COALESCE("INI_SRC"."PRICEBOOK2ID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "PRICEBOOK2ID"
			, COALESCE("INI_SRC"."ACCOUNTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ACCOUNTID"
			, COALESCE("INI_SRC"."OPPORTUNITYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "OPPORTUNITYID"
			, COALESCE("INI_SRC"."COMPANYAUTHORIZEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "COMPANYAUTHORIZEDBYID"
			, COALESCE("INI_SRC"."LASTMODIFIEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LASTMODIFIEDBYID"
			, "INI_SRC"."OWNERID" AS "OWNERID"
			, "INI_SRC"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "INI_SRC"."ENDDATE" AS "ENDDATE"
			, "INI_SRC"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "INI_SRC"."STATUS" AS "STATUS"
			, "INI_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "INI_SRC"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "INI_SRC"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "INI_SRC"."TYPE" AS "TYPE"
			, "INI_SRC"."BILLINGSTREET" AS "BILLINGSTREET"
			, "INI_SRC"."BILLINGCITY" AS "BILLINGCITY"
			, "INI_SRC"."BILLINGSTATE" AS "BILLINGSTATE"
			, "INI_SRC"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "INI_SRC"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "INI_SRC"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "INI_SRC"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "INI_SRC"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "INI_SRC"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "INI_SRC"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "INI_SRC"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "INI_SRC"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "INI_SRC"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "INI_SRC"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "INI_SRC"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "INI_SRC"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "INI_SRC"."NAME" AS "NAME"
			, "INI_SRC"."PODATE" AS "PODATE"
			, "INI_SRC"."PONUMBER" AS "PONUMBER"
			, "INI_SRC"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "INI_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "INI_SRC"."STATUSCODE" AS "STATUSCODE"
			, "INI_SRC"."ORDERNUMBER" AS "ORDERNUMBER"
			, "INI_SRC"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."ISDELETED" AS "ISDELETED"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		FROM {{ source('SALESFORCE_INI', 'ORDERS') }} "INI_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."ID" AS "ID"
			, "LOAD_INIT_DATA"."CONTRACTID" AS "CONTRACTID"
			, "LOAD_INIT_DATA"."ACCOUNTID" AS "ACCOUNTID"
			, "LOAD_INIT_DATA"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "LOAD_INIT_DATA"."ORIGINALORDERID" AS "ORIGINALORDERID"
			, "LOAD_INIT_DATA"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "LOAD_INIT_DATA"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
			, "LOAD_INIT_DATA"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
			, "LOAD_INIT_DATA"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
			, "LOAD_INIT_DATA"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
			, "LOAD_INIT_DATA"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."OWNERID" AS "OWNERID"
			, "LOAD_INIT_DATA"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "LOAD_INIT_DATA"."ENDDATE" AS "ENDDATE"
			, "LOAD_INIT_DATA"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "LOAD_INIT_DATA"."STATUS" AS "STATUS"
			, "LOAD_INIT_DATA"."DESCRIPTION" AS "DESCRIPTION"
			, "LOAD_INIT_DATA"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "LOAD_INIT_DATA"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "LOAD_INIT_DATA"."TYPE" AS "TYPE"
			, "LOAD_INIT_DATA"."BILLINGSTREET" AS "BILLINGSTREET"
			, "LOAD_INIT_DATA"."BILLINGCITY" AS "BILLINGCITY"
			, "LOAD_INIT_DATA"."BILLINGSTATE" AS "BILLINGSTATE"
			, "LOAD_INIT_DATA"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "LOAD_INIT_DATA"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "LOAD_INIT_DATA"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "LOAD_INIT_DATA"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "LOAD_INIT_DATA"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "LOAD_INIT_DATA"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "LOAD_INIT_DATA"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "LOAD_INIT_DATA"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "LOAD_INIT_DATA"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "LOAD_INIT_DATA"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "LOAD_INIT_DATA"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "LOAD_INIT_DATA"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "LOAD_INIT_DATA"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "LOAD_INIT_DATA"."NAME" AS "NAME"
			, "LOAD_INIT_DATA"."PODATE" AS "PODATE"
			, "LOAD_INIT_DATA"."PONUMBER" AS "PONUMBER"
			, "LOAD_INIT_DATA"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "LOAD_INIT_DATA"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "LOAD_INIT_DATA"."STATUSCODE" AS "STATUSCODE"
			, "LOAD_INIT_DATA"."ORDERNUMBER" AS "ORDERNUMBER"
			, "LOAD_INIT_DATA"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."ISDELETED" AS "ISDELETED"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTRACTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PRICEBOOK2ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ORIGINALORDERID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OPPORTUNITYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CUSTOMERAUTHORIZEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "COMPANYAUTHORIZEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLTOCONTACTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPTOCONTACTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACTIVATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNERID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "EFFECTIVEDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ENDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISREDUCTIONORDER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATUS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DESCRIPTION"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CUSTOMERAUTHORIZEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "COMPANYAUTHORIZEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "TYPE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGSTREET"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGCITY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGSTATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGPOSTALCODE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGCOUNTRY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGLATITUDE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGLONGITUDE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BILLINGGEOCODEACCURACY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGSTREET"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGCITY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGSTATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGPOSTALCODE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGCOUNTRY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGLATITUDE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGLONGITUDE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SHIPPINGGEOCODEACCURACY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "NAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PODATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PONUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ORDERREFERENCENUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACTIVATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATUSCODE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ORDERNUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "TOTALAMOUNT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISDELETED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
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
			, "PREP_EXCEP"."CONTRACTID" AS "CONTRACTID"
			, "PREP_EXCEP"."ACCOUNTID" AS "ACCOUNTID"
			, "PREP_EXCEP"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "PREP_EXCEP"."ORIGINALORDERID" AS "ORIGINALORDERID"
			, "PREP_EXCEP"."OPPORTUNITYID" AS "OPPORTUNITYID"
			, "PREP_EXCEP"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
			, "PREP_EXCEP"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
			, "PREP_EXCEP"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
			, "PREP_EXCEP"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
			, "PREP_EXCEP"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ORIGINALORDERID"),'\#','\\' || '\#') AS "ID_FK_ORIGINALORDERID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."SHIPTOCONTACTID"),'\#','\\' || '\#') AS "ID_FK_SHIPTOCONTACTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ACTIVATEDBYID"),'\#','\\' || '\#') AS "ID_FK_ACTIVATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CUSTOMERAUTHORIZEDBYID"),'\#','\\' || '\#') AS "ID_FK_CUSTOMERAUTHORIZEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."BILLTOCONTACTID"),'\#','\\' || '\#') AS "ID_FK_BILLTOCONTACTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CONTRACTID"),'\#','\\' || '\#') AS "ID_FK_CONTRACTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."PRICEBOOK2ID"),'\#','\\' || '\#') AS "ID_FK_PRICEBOOK2ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."OPPORTUNITYID"),'\#','\\' || '\#') AS "ID_FK_OPPORTUNITYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."COMPANYAUTHORIZEDBYID"),'\#','\\' || '\#') AS "ID_FK_COMPANYAUTHORIZEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "PREP_EXCEP"."OWNERID" AS "OWNERID"
			, "PREP_EXCEP"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
			, "PREP_EXCEP"."ENDDATE" AS "ENDDATE"
			, "PREP_EXCEP"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
			, "PREP_EXCEP"."STATUS" AS "STATUS"
			, "PREP_EXCEP"."DESCRIPTION" AS "DESCRIPTION"
			, "PREP_EXCEP"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
			, "PREP_EXCEP"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
			, "PREP_EXCEP"."TYPE" AS "TYPE"
			, "PREP_EXCEP"."BILLINGSTREET" AS "BILLINGSTREET"
			, "PREP_EXCEP"."BILLINGCITY" AS "BILLINGCITY"
			, "PREP_EXCEP"."BILLINGSTATE" AS "BILLINGSTATE"
			, "PREP_EXCEP"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
			, "PREP_EXCEP"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
			, "PREP_EXCEP"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
			, "PREP_EXCEP"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
			, "PREP_EXCEP"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
			, "PREP_EXCEP"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
			, "PREP_EXCEP"."SHIPPINGCITY" AS "SHIPPINGCITY"
			, "PREP_EXCEP"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
			, "PREP_EXCEP"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
			, "PREP_EXCEP"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
			, "PREP_EXCEP"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
			, "PREP_EXCEP"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
			, "PREP_EXCEP"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
			, "PREP_EXCEP"."NAME" AS "NAME"
			, "PREP_EXCEP"."PODATE" AS "PODATE"
			, "PREP_EXCEP"."PONUMBER" AS "PONUMBER"
			, "PREP_EXCEP"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
			, "PREP_EXCEP"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "PREP_EXCEP"."STATUSCODE" AS "STATUSCODE"
			, "PREP_EXCEP"."ORDERNUMBER" AS "ORDERNUMBER"
			, "PREP_EXCEP"."TOTALAMOUNT" AS "TOTALAMOUNT"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."ISDELETED" AS "ISDELETED"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
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
		, "CALCULATE_BK"."CONTRACTID" AS "CONTRACTID"
		, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
		, "CALCULATE_BK"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "CALCULATE_BK"."ORIGINALORDERID" AS "ORIGINALORDERID"
		, "CALCULATE_BK"."OPPORTUNITYID" AS "OPPORTUNITYID"
		, "CALCULATE_BK"."CUSTOMERAUTHORIZEDBYID" AS "CUSTOMERAUTHORIZEDBYID"
		, "CALCULATE_BK"."COMPANYAUTHORIZEDBYID" AS "COMPANYAUTHORIZEDBYID"
		, "CALCULATE_BK"."BILLTOCONTACTID" AS "BILLTOCONTACTID"
		, "CALCULATE_BK"."SHIPTOCONTACTID" AS "SHIPTOCONTACTID"
		, "CALCULATE_BK"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_ORIGINALORDERID_BK" AS "ID_FK_ORIGINALORDERID_BK"
		, "CALCULATE_BK"."ID_FK_SHIPTOCONTACTID_BK" AS "ID_FK_SHIPTOCONTACTID_BK"
		, "CALCULATE_BK"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_CUSTOMERAUTHORIZEDBYID_BK" AS "ID_FK_CUSTOMERAUTHORIZEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_BILLTOCONTACTID_BK" AS "ID_FK_BILLTOCONTACTID_BK"
		, "CALCULATE_BK"."ID_FK_CONTRACTID_BK" AS "ID_FK_CONTRACTID_BK"
		, "CALCULATE_BK"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "CALCULATE_BK"."ID_FK_OPPORTUNITYID_BK" AS "ID_FK_OPPORTUNITYID_BK"
		, "CALCULATE_BK"."ID_FK_COMPANYAUTHORIZEDBYID_BK" AS "ID_FK_COMPANYAUTHORIZEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "CALCULATE_BK"."OWNERID" AS "OWNERID"
		, "CALCULATE_BK"."EFFECTIVEDATE" AS "EFFECTIVEDATE"
		, "CALCULATE_BK"."ENDDATE" AS "ENDDATE"
		, "CALCULATE_BK"."ISREDUCTIONORDER" AS "ISREDUCTIONORDER"
		, "CALCULATE_BK"."STATUS" AS "STATUS"
		, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
		, "CALCULATE_BK"."CUSTOMERAUTHORIZEDDATE" AS "CUSTOMERAUTHORIZEDDATE"
		, "CALCULATE_BK"."COMPANYAUTHORIZEDDATE" AS "COMPANYAUTHORIZEDDATE"
		, "CALCULATE_BK"."TYPE" AS "TYPE"
		, "CALCULATE_BK"."BILLINGSTREET" AS "BILLINGSTREET"
		, "CALCULATE_BK"."BILLINGCITY" AS "BILLINGCITY"
		, "CALCULATE_BK"."BILLINGSTATE" AS "BILLINGSTATE"
		, "CALCULATE_BK"."BILLINGPOSTALCODE" AS "BILLINGPOSTALCODE"
		, "CALCULATE_BK"."BILLINGCOUNTRY" AS "BILLINGCOUNTRY"
		, "CALCULATE_BK"."BILLINGLATITUDE" AS "BILLINGLATITUDE"
		, "CALCULATE_BK"."BILLINGLONGITUDE" AS "BILLINGLONGITUDE"
		, "CALCULATE_BK"."BILLINGGEOCODEACCURACY" AS "BILLINGGEOCODEACCURACY"
		, "CALCULATE_BK"."SHIPPINGSTREET" AS "SHIPPINGSTREET"
		, "CALCULATE_BK"."SHIPPINGCITY" AS "SHIPPINGCITY"
		, "CALCULATE_BK"."SHIPPINGSTATE" AS "SHIPPINGSTATE"
		, "CALCULATE_BK"."SHIPPINGPOSTALCODE" AS "SHIPPINGPOSTALCODE"
		, "CALCULATE_BK"."SHIPPINGCOUNTRY" AS "SHIPPINGCOUNTRY"
		, "CALCULATE_BK"."SHIPPINGLATITUDE" AS "SHIPPINGLATITUDE"
		, "CALCULATE_BK"."SHIPPINGLONGITUDE" AS "SHIPPINGLONGITUDE"
		, "CALCULATE_BK"."SHIPPINGGEOCODEACCURACY" AS "SHIPPINGGEOCODEACCURACY"
		, "CALCULATE_BK"."NAME" AS "NAME"
		, "CALCULATE_BK"."PODATE" AS "PODATE"
		, "CALCULATE_BK"."PONUMBER" AS "PONUMBER"
		, "CALCULATE_BK"."ORDERREFERENCENUMBER" AS "ORDERREFERENCENUMBER"
		, "CALCULATE_BK"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "CALCULATE_BK"."STATUSCODE" AS "STATUSCODE"
		, "CALCULATE_BK"."ORDERNUMBER" AS "ORDERNUMBER"
		, "CALCULATE_BK"."TOTALAMOUNT" AS "TOTALAMOUNT"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'