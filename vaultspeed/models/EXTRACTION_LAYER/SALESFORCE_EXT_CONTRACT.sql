{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='CONTRACT',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_CONTRACT_INCR', 'EXT_SALESFORCE_CONTRACT_INIT']
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
			, "TDFV_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "TDFV_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "TDFV_SRC"."OWNERID" AS "OWNERID"
			, "TDFV_SRC"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
			, "TDFV_SRC"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
			, "TDFV_SRC"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CUSTOMERSIGNEDID"),'\#','\\' || '\#') AS "ID_FK_CUSTOMERSIGNEDID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."PRICEBOOK2ID"),'\#','\\' || '\#') AS "ID_FK_PRICEBOOK2ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ACTIVATEDBYID"),'\#','\\' || '\#') AS "ID_FK_ACTIVATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."COMPANYSIGNEDID"),'\#','\\' || '\#') AS "ID_FK_COMPANYSIGNEDID_BK"
			, "TDFV_SRC"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
			, "TDFV_SRC"."STARTDATE" AS "STARTDATE"
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
			, "TDFV_SRC"."CONTRACTTERM" AS "CONTRACTTERM"
			, "TDFV_SRC"."STATUS" AS "STATUS"
			, "TDFV_SRC"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
			, "TDFV_SRC"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
			, "TDFV_SRC"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
			, "TDFV_SRC"."SPECIALTERMS" AS "SPECIALTERMS"
			, "TDFV_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "TDFV_SRC"."STATUSCODE" AS "STATUSCODE"
			, "TDFV_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "TDFV_SRC"."NAME" AS "NAME"
			, "TDFV_SRC"."ISDELETED" AS "ISDELETED"
			, "TDFV_SRC"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
			, "TDFV_SRC"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "TDFV_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		FROM {{ ref('SALESFORCE_DFV_VW_CONTRACT') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
			, "CALCULATE_BK"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "CALCULATE_BK"."OWNERID" AS "OWNERID"
			, "CALCULATE_BK"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
			, "CALCULATE_BK"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
			, "CALCULATE_BK"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."ID_FK_CUSTOMERSIGNEDID_BK" AS "ID_FK_CUSTOMERSIGNEDID_BK"
			, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
			, "CALCULATE_BK"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
			, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
			, "CALCULATE_BK"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_COMPANYSIGNEDID_BK" AS "ID_FK_COMPANYSIGNEDID_BK"
			, "CALCULATE_BK"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
			, "CALCULATE_BK"."STARTDATE" AS "STARTDATE"
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
			, "CALCULATE_BK"."CONTRACTTERM" AS "CONTRACTTERM"
			, "CALCULATE_BK"."STATUS" AS "STATUS"
			, "CALCULATE_BK"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
			, "CALCULATE_BK"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
			, "CALCULATE_BK"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
			, "CALCULATE_BK"."SPECIALTERMS" AS "SPECIALTERMS"
			, "CALCULATE_BK"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "CALCULATE_BK"."STATUSCODE" AS "STATUSCODE"
			, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
			, "CALCULATE_BK"."NAME" AS "NAME"
			, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
			, "CALCULATE_BK"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
			, "CALCULATE_BK"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALCULATE_BK"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."ID" AS "ID"
		, "EXT_UNION"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_UNION"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "EXT_UNION"."OWNERID" AS "OWNERID"
		, "EXT_UNION"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
		, "EXT_UNION"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
		, "EXT_UNION"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."ID_FK_CUSTOMERSIGNEDID_BK" AS "ID_FK_CUSTOMERSIGNEDID_BK"
		, "EXT_UNION"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_UNION"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_UNION"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_UNION"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "EXT_UNION"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_UNION"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "EXT_UNION"."ID_FK_COMPANYSIGNEDID_BK" AS "ID_FK_COMPANYSIGNEDID_BK"
		, "EXT_UNION"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
		, "EXT_UNION"."STARTDATE" AS "STARTDATE"
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
		, "EXT_UNION"."CONTRACTTERM" AS "CONTRACTTERM"
		, "EXT_UNION"."STATUS" AS "STATUS"
		, "EXT_UNION"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
		, "EXT_UNION"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
		, "EXT_UNION"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
		, "EXT_UNION"."SPECIALTERMS" AS "SPECIALTERMS"
		, "EXT_UNION"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "EXT_UNION"."STATUSCODE" AS "STATUSCODE"
		, "EXT_UNION"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_UNION"."NAME" AS "NAME"
		, "EXT_UNION"."ISDELETED" AS "ISDELETED"
		, "EXT_UNION"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
		, "EXT_UNION"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_UNION"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
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
			, COALESCE("INI_SRC"."CUSTOMERSIGNEDID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CUSTOMERSIGNEDID"
			, COALESCE("INI_SRC"."CREATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CREATEDBYID"
			, COALESCE("INI_SRC"."LASTMODIFIEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LASTMODIFIEDBYID"
			, COALESCE("INI_SRC"."OWNERID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "OWNERID"
			, COALESCE("INI_SRC"."PRICEBOOK2ID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "PRICEBOOK2ID"
			, COALESCE("INI_SRC"."ACCOUNTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ACCOUNTID"
			, COALESCE("INI_SRC"."ACTIVATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ACTIVATEDBYID"
			, COALESCE("INI_SRC"."COMPANYSIGNEDID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "COMPANYSIGNEDID"
			, "INI_SRC"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
			, "INI_SRC"."STARTDATE" AS "STARTDATE"
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
			, "INI_SRC"."CONTRACTTERM" AS "CONTRACTTERM"
			, "INI_SRC"."STATUS" AS "STATUS"
			, "INI_SRC"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
			, "INI_SRC"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
			, "INI_SRC"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
			, "INI_SRC"."SPECIALTERMS" AS "SPECIALTERMS"
			, "INI_SRC"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "INI_SRC"."STATUSCODE" AS "STATUSCODE"
			, "INI_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "INI_SRC"."NAME" AS "NAME"
			, "INI_SRC"."ISDELETED" AS "ISDELETED"
			, "INI_SRC"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
			, "INI_SRC"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "INI_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		FROM {{ source('SALESFORCE_INI', 'CONTRACT') }} "INI_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."ID" AS "ID"
			, "LOAD_INIT_DATA"."ACCOUNTID" AS "ACCOUNTID"
			, "LOAD_INIT_DATA"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "LOAD_INIT_DATA"."OWNERID" AS "OWNERID"
			, "LOAD_INIT_DATA"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
			, "LOAD_INIT_DATA"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
			, "LOAD_INIT_DATA"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
			, "LOAD_INIT_DATA"."STARTDATE" AS "STARTDATE"
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
			, "LOAD_INIT_DATA"."CONTRACTTERM" AS "CONTRACTTERM"
			, "LOAD_INIT_DATA"."STATUS" AS "STATUS"
			, "LOAD_INIT_DATA"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
			, "LOAD_INIT_DATA"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
			, "LOAD_INIT_DATA"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
			, "LOAD_INIT_DATA"."SPECIALTERMS" AS "SPECIALTERMS"
			, "LOAD_INIT_DATA"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "LOAD_INIT_DATA"."STATUSCODE" AS "STATUSCODE"
			, "LOAD_INIT_DATA"."DESCRIPTION" AS "DESCRIPTION"
			, "LOAD_INIT_DATA"."NAME" AS "NAME"
			, "LOAD_INIT_DATA"."ISDELETED" AS "ISDELETED"
			, "LOAD_INIT_DATA"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
			, "LOAD_INIT_DATA"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "LOAD_INIT_DATA"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PRICEBOOK2ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNERID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "COMPANYSIGNEDID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CUSTOMERSIGNEDID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACTIVATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNEREXPIRATIONNOTICE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STARTDATE"
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
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTRACTTERM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATUS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "COMPANYSIGNEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CUSTOMERSIGNEDTITLE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CUSTOMERSIGNEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SPECIALTERMS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACTIVATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATUSCODE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DESCRIPTION"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "NAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISDELETED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTRACTNUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTAPPROVEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTACTIVITYDATE"
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
			, "PREP_EXCEP"."ACCOUNTID" AS "ACCOUNTID"
			, "PREP_EXCEP"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "PREP_EXCEP"."OWNERID" AS "OWNERID"
			, "PREP_EXCEP"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
			, "PREP_EXCEP"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
			, "PREP_EXCEP"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CUSTOMERSIGNEDID"),'\#','\\' || '\#') AS "ID_FK_CUSTOMERSIGNEDID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."PRICEBOOK2ID"),'\#','\\' || '\#') AS "ID_FK_PRICEBOOK2ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ACTIVATEDBYID"),'\#','\\' || '\#') AS "ID_FK_ACTIVATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."COMPANYSIGNEDID"),'\#','\\' || '\#') AS "ID_FK_COMPANYSIGNEDID_BK"
			, "PREP_EXCEP"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
			, "PREP_EXCEP"."STARTDATE" AS "STARTDATE"
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
			, "PREP_EXCEP"."CONTRACTTERM" AS "CONTRACTTERM"
			, "PREP_EXCEP"."STATUS" AS "STATUS"
			, "PREP_EXCEP"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
			, "PREP_EXCEP"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
			, "PREP_EXCEP"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
			, "PREP_EXCEP"."SPECIALTERMS" AS "SPECIALTERMS"
			, "PREP_EXCEP"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
			, "PREP_EXCEP"."STATUSCODE" AS "STATUSCODE"
			, "PREP_EXCEP"."DESCRIPTION" AS "DESCRIPTION"
			, "PREP_EXCEP"."NAME" AS "NAME"
			, "PREP_EXCEP"."ISDELETED" AS "ISDELETED"
			, "PREP_EXCEP"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
			, "PREP_EXCEP"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "PREP_EXCEP"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
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
		, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
		, "CALCULATE_BK"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "CALCULATE_BK"."OWNERID" AS "OWNERID"
		, "CALCULATE_BK"."COMPANYSIGNEDID" AS "COMPANYSIGNEDID"
		, "CALCULATE_BK"."CUSTOMERSIGNEDID" AS "CUSTOMERSIGNEDID"
		, "CALCULATE_BK"."ACTIVATEDBYID" AS "ACTIVATEDBYID"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."ID_FK_CUSTOMERSIGNEDID_BK" AS "ID_FK_CUSTOMERSIGNEDID_BK"
		, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "CALCULATE_BK"."ID_FK_PRICEBOOK2ID_BK" AS "ID_FK_PRICEBOOK2ID_BK"
		, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "CALCULATE_BK"."ID_FK_ACTIVATEDBYID_BK" AS "ID_FK_ACTIVATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_COMPANYSIGNEDID_BK" AS "ID_FK_COMPANYSIGNEDID_BK"
		, "CALCULATE_BK"."OWNEREXPIRATIONNOTICE" AS "OWNEREXPIRATIONNOTICE"
		, "CALCULATE_BK"."STARTDATE" AS "STARTDATE"
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
		, "CALCULATE_BK"."CONTRACTTERM" AS "CONTRACTTERM"
		, "CALCULATE_BK"."STATUS" AS "STATUS"
		, "CALCULATE_BK"."COMPANYSIGNEDDATE" AS "COMPANYSIGNEDDATE"
		, "CALCULATE_BK"."CUSTOMERSIGNEDTITLE" AS "CUSTOMERSIGNEDTITLE"
		, "CALCULATE_BK"."CUSTOMERSIGNEDDATE" AS "CUSTOMERSIGNEDDATE"
		, "CALCULATE_BK"."SPECIALTERMS" AS "SPECIALTERMS"
		, "CALCULATE_BK"."ACTIVATEDDATE" AS "ACTIVATEDDATE"
		, "CALCULATE_BK"."STATUSCODE" AS "STATUSCODE"
		, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
		, "CALCULATE_BK"."NAME" AS "NAME"
		, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
		, "CALCULATE_BK"."CONTRACTNUMBER" AS "CONTRACTNUMBER"
		, "CALCULATE_BK"."LASTAPPROVEDDATE" AS "LASTAPPROVEDDATE"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "CALCULATE_BK"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'