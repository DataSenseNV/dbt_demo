{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='ACCOUNT',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_ACCOUNT_INCR', 'EXT_SALESFORCE_ACCOUNT_INIT']
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
			, "TDFV_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
			, "TDFV_SRC"."PARENTID" AS "PARENTID"
			, "TDFV_SRC"."OWNERID" AS "OWNERID"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."PARENTID"),'\#','\\' || '\#') AS "ID_FK_PARENTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."MASTERRECORDID"),'\#','\\' || '\#') AS "ID_FK_MASTERRECORDID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, "TDFV_SRC"."ISDELETED" AS "ISDELETED"
			, "TDFV_SRC"."NAME" AS "NAME"
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
			, "TDFV_SRC"."PHONE" AS "PHONE"
			, "TDFV_SRC"."FAX" AS "FAX"
			, "TDFV_SRC"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
			, "TDFV_SRC"."WEBSITE" AS "WEBSITE"
			, "TDFV_SRC"."SIC" AS "SIC"
			, "TDFV_SRC"."INDUSTRY" AS "INDUSTRY"
			, "TDFV_SRC"."ANNUALREVENUE" AS "ANNUALREVENUE"
			, "TDFV_SRC"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
			, "TDFV_SRC"."OWNERSHIP" AS "OWNERSHIP"
			, "TDFV_SRC"."TICKERSYMBOL" AS "TICKERSYMBOL"
			, "TDFV_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "TDFV_SRC"."RATING" AS "RATING"
			, "TDFV_SRC"."SITE" AS "SITE"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "TDFV_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "TDFV_SRC"."JIGSAW" AS "JIGSAW"
			, "TDFV_SRC"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
			, "TDFV_SRC"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
			, "TDFV_SRC"."SICDESC" AS "SICDESC"
			, "TDFV_SRC"."SEGMENT__C" AS "SEGMENT__C"
			, "TDFV_SRC"."DEMO_COLUMN" AS "DEMO_COLUMN"
		FROM {{ ref('SALESFORCE_DFV_VW_ACCOUNT') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."MASTERRECORDID" AS "MASTERRECORDID"
			, "CALCULATE_BK"."PARENTID" AS "PARENTID"
			, "CALCULATE_BK"."OWNERID" AS "OWNERID"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
			, "CALCULATE_BK"."ID_FK_MASTERRECORDID_BK" AS "ID_FK_MASTERRECORDID_BK"
			, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
			, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
			, "CALCULATE_BK"."NAME" AS "NAME"
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
			, "CALCULATE_BK"."PHONE" AS "PHONE"
			, "CALCULATE_BK"."FAX" AS "FAX"
			, "CALCULATE_BK"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
			, "CALCULATE_BK"."WEBSITE" AS "WEBSITE"
			, "CALCULATE_BK"."SIC" AS "SIC"
			, "CALCULATE_BK"."INDUSTRY" AS "INDUSTRY"
			, "CALCULATE_BK"."ANNUALREVENUE" AS "ANNUALREVENUE"
			, "CALCULATE_BK"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
			, "CALCULATE_BK"."OWNERSHIP" AS "OWNERSHIP"
			, "CALCULATE_BK"."TICKERSYMBOL" AS "TICKERSYMBOL"
			, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
			, "CALCULATE_BK"."RATING" AS "RATING"
			, "CALCULATE_BK"."SITE" AS "SITE"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALCULATE_BK"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "CALCULATE_BK"."JIGSAW" AS "JIGSAW"
			, "CALCULATE_BK"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
			, "CALCULATE_BK"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
			, "CALCULATE_BK"."SICDESC" AS "SICDESC"
			, "CALCULATE_BK"."SEGMENT__C" AS "SEGMENT__C"
			, "CALCULATE_BK"."DEMO_COLUMN" AS "DEMO_COLUMN"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."ID" AS "ID"
		, "EXT_UNION"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_UNION"."PARENTID" AS "PARENTID"
		, "EXT_UNION"."OWNERID" AS "OWNERID"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_UNION"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "EXT_UNION"."ID_FK_MASTERRECORDID_BK" AS "ID_FK_MASTERRECORDID_BK"
		, "EXT_UNION"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_UNION"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_UNION"."ISDELETED" AS "ISDELETED"
		, "EXT_UNION"."NAME" AS "NAME"
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
		, "EXT_UNION"."PHONE" AS "PHONE"
		, "EXT_UNION"."FAX" AS "FAX"
		, "EXT_UNION"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
		, "EXT_UNION"."WEBSITE" AS "WEBSITE"
		, "EXT_UNION"."SIC" AS "SIC"
		, "EXT_UNION"."INDUSTRY" AS "INDUSTRY"
		, "EXT_UNION"."ANNUALREVENUE" AS "ANNUALREVENUE"
		, "EXT_UNION"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
		, "EXT_UNION"."OWNERSHIP" AS "OWNERSHIP"
		, "EXT_UNION"."TICKERSYMBOL" AS "TICKERSYMBOL"
		, "EXT_UNION"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_UNION"."RATING" AS "RATING"
		, "EXT_UNION"."SITE" AS "SITE"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_UNION"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "EXT_UNION"."JIGSAW" AS "JIGSAW"
		, "EXT_UNION"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
		, "EXT_UNION"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
		, "EXT_UNION"."SICDESC" AS "SICDESC"
		, "EXT_UNION"."SEGMENT__C" AS "SEGMENT__C"
		, "EXT_UNION"."DEMO_COLUMN" AS "DEMO_COLUMN"
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
			, COALESCE("INI_SRC"."LASTMODIFIEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LASTMODIFIEDBYID"
			, COALESCE("INI_SRC"."PARENTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "PARENTID"
			, COALESCE("INI_SRC"."MASTERRECORDID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "MASTERRECORDID"
			, COALESCE("INI_SRC"."CREATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CREATEDBYID"
			, COALESCE("INI_SRC"."OWNERID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "OWNERID"
			, "INI_SRC"."ISDELETED" AS "ISDELETED"
			, "INI_SRC"."NAME" AS "NAME"
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
			, "INI_SRC"."PHONE" AS "PHONE"
			, "INI_SRC"."FAX" AS "FAX"
			, "INI_SRC"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
			, "INI_SRC"."WEBSITE" AS "WEBSITE"
			, "INI_SRC"."SIC" AS "SIC"
			, "INI_SRC"."INDUSTRY" AS "INDUSTRY"
			, "INI_SRC"."ANNUALREVENUE" AS "ANNUALREVENUE"
			, "INI_SRC"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
			, "INI_SRC"."OWNERSHIP" AS "OWNERSHIP"
			, "INI_SRC"."TICKERSYMBOL" AS "TICKERSYMBOL"
			, "INI_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "INI_SRC"."RATING" AS "RATING"
			, "INI_SRC"."SITE" AS "SITE"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "INI_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "INI_SRC"."JIGSAW" AS "JIGSAW"
			, "INI_SRC"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
			, "INI_SRC"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
			, "INI_SRC"."SICDESC" AS "SICDESC"
			, "INI_SRC"."SEGMENT__C" AS "SEGMENT__C"
			, "INI_SRC"."DEMO_COLUMN" AS "DEMO_COLUMN"
		FROM {{ source('SALESFORCE_INI', 'ACCOUNT') }} "INI_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."ID" AS "ID"
			, "LOAD_INIT_DATA"."MASTERRECORDID" AS "MASTERRECORDID"
			, "LOAD_INIT_DATA"."PARENTID" AS "PARENTID"
			, "LOAD_INIT_DATA"."OWNERID" AS "OWNERID"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."ISDELETED" AS "ISDELETED"
			, "LOAD_INIT_DATA"."NAME" AS "NAME"
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
			, "LOAD_INIT_DATA"."PHONE" AS "PHONE"
			, "LOAD_INIT_DATA"."FAX" AS "FAX"
			, "LOAD_INIT_DATA"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
			, "LOAD_INIT_DATA"."WEBSITE" AS "WEBSITE"
			, "LOAD_INIT_DATA"."SIC" AS "SIC"
			, "LOAD_INIT_DATA"."INDUSTRY" AS "INDUSTRY"
			, "LOAD_INIT_DATA"."ANNUALREVENUE" AS "ANNUALREVENUE"
			, "LOAD_INIT_DATA"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
			, "LOAD_INIT_DATA"."OWNERSHIP" AS "OWNERSHIP"
			, "LOAD_INIT_DATA"."TICKERSYMBOL" AS "TICKERSYMBOL"
			, "LOAD_INIT_DATA"."DESCRIPTION" AS "DESCRIPTION"
			, "LOAD_INIT_DATA"."RATING" AS "RATING"
			, "LOAD_INIT_DATA"."SITE" AS "SITE"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "LOAD_INIT_DATA"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "LOAD_INIT_DATA"."JIGSAW" AS "JIGSAW"
			, "LOAD_INIT_DATA"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
			, "LOAD_INIT_DATA"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
			, "LOAD_INIT_DATA"."SICDESC" AS "SICDESC"
			, "LOAD_INIT_DATA"."SEGMENT__C" AS "SEGMENT__C"
			, "LOAD_INIT_DATA"."DEMO_COLUMN" AS "DEMO_COLUMN"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "MASTERRECORDID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PARENTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNERID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISDELETED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "NAME"
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
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PHONE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "FAX"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTNUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WEBSITE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "SIC"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "INDUSTRY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ANNUALREVENUE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "NUMBEROFEMPLOYEES"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNERSHIP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "TICKERSYMBOL"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DESCRIPTION"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "RATING"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SITE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTACTIVITYDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "JIGSAW"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "JIGSAWCOMPANYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTSOURCE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SICDESC"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SEGMENT__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DEMO_COLUMN"
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
			, "PREP_EXCEP"."MASTERRECORDID" AS "MASTERRECORDID"
			, "PREP_EXCEP"."PARENTID" AS "PARENTID"
			, "PREP_EXCEP"."OWNERID" AS "OWNERID"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."PARENTID"),'\#','\\' || '\#') AS "ID_FK_PARENTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."MASTERRECORDID"),'\#','\\' || '\#') AS "ID_FK_MASTERRECORDID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, "PREP_EXCEP"."ISDELETED" AS "ISDELETED"
			, "PREP_EXCEP"."NAME" AS "NAME"
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
			, "PREP_EXCEP"."PHONE" AS "PHONE"
			, "PREP_EXCEP"."FAX" AS "FAX"
			, "PREP_EXCEP"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
			, "PREP_EXCEP"."WEBSITE" AS "WEBSITE"
			, "PREP_EXCEP"."SIC" AS "SIC"
			, "PREP_EXCEP"."INDUSTRY" AS "INDUSTRY"
			, "PREP_EXCEP"."ANNUALREVENUE" AS "ANNUALREVENUE"
			, "PREP_EXCEP"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
			, "PREP_EXCEP"."OWNERSHIP" AS "OWNERSHIP"
			, "PREP_EXCEP"."TICKERSYMBOL" AS "TICKERSYMBOL"
			, "PREP_EXCEP"."DESCRIPTION" AS "DESCRIPTION"
			, "PREP_EXCEP"."RATING" AS "RATING"
			, "PREP_EXCEP"."SITE" AS "SITE"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "PREP_EXCEP"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "PREP_EXCEP"."JIGSAW" AS "JIGSAW"
			, "PREP_EXCEP"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
			, "PREP_EXCEP"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
			, "PREP_EXCEP"."SICDESC" AS "SICDESC"
			, "PREP_EXCEP"."SEGMENT__C" AS "SEGMENT__C"
			, "PREP_EXCEP"."DEMO_COLUMN" AS "DEMO_COLUMN"
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
		, "CALCULATE_BK"."MASTERRECORDID" AS "MASTERRECORDID"
		, "CALCULATE_BK"."PARENTID" AS "PARENTID"
		, "CALCULATE_BK"."OWNERID" AS "OWNERID"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "CALCULATE_BK"."ID_FK_MASTERRECORDID_BK" AS "ID_FK_MASTERRECORDID_BK"
		, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
		, "CALCULATE_BK"."NAME" AS "NAME"
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
		, "CALCULATE_BK"."PHONE" AS "PHONE"
		, "CALCULATE_BK"."FAX" AS "FAX"
		, "CALCULATE_BK"."ACCOUNTNUMBER" AS "ACCOUNTNUMBER"
		, "CALCULATE_BK"."WEBSITE" AS "WEBSITE"
		, "CALCULATE_BK"."SIC" AS "SIC"
		, "CALCULATE_BK"."INDUSTRY" AS "INDUSTRY"
		, "CALCULATE_BK"."ANNUALREVENUE" AS "ANNUALREVENUE"
		, "CALCULATE_BK"."NUMBEROFEMPLOYEES" AS "NUMBEROFEMPLOYEES"
		, "CALCULATE_BK"."OWNERSHIP" AS "OWNERSHIP"
		, "CALCULATE_BK"."TICKERSYMBOL" AS "TICKERSYMBOL"
		, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
		, "CALCULATE_BK"."RATING" AS "RATING"
		, "CALCULATE_BK"."SITE" AS "SITE"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "CALCULATE_BK"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "CALCULATE_BK"."JIGSAW" AS "JIGSAW"
		, "CALCULATE_BK"."JIGSAWCOMPANYID" AS "JIGSAWCOMPANYID"
		, "CALCULATE_BK"."ACCOUNTSOURCE" AS "ACCOUNTSOURCE"
		, "CALCULATE_BK"."SICDESC" AS "SICDESC"
		, "CALCULATE_BK"."SEGMENT__C" AS "SEGMENT__C"
		, "CALCULATE_BK"."DEMO_COLUMN" AS "DEMO_COLUMN"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'