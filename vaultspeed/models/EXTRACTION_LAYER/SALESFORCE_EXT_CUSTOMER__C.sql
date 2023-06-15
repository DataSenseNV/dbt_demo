{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='CUSTOMER__C',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_CUSTOMERC_INCR', 'EXT_SALESFORCE_CUSTOMERC_INIT']
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
			, "TDFV_SRC"."OWNERID" AS "OWNERID"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "TDFV_SRC"."ISDELETED" AS "ISDELETED"
			, "TDFV_SRC"."NAME" AS "NAME"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "TDFV_SRC"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
			, "TDFV_SRC"."GENDER__C" AS "GENDER__C"
			, "TDFV_SRC"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
			, "TDFV_SRC"."PARTNER__C" AS "PARTNER__C"
			, "TDFV_SRC"."DEPENDENTS__C" AS "DEPENDENTS__C"
			, "TDFV_SRC"."TENURE__C" AS "TENURE__C"
			, "TDFV_SRC"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
			, "TDFV_SRC"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
			, "TDFV_SRC"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
			, "TDFV_SRC"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
			, "TDFV_SRC"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
			, "TDFV_SRC"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
			, "TDFV_SRC"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
			, "TDFV_SRC"."STREAMING_TV__C" AS "STREAMING_TV__C"
			, "TDFV_SRC"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
			, "TDFV_SRC"."CONTRACT__C" AS "CONTRACT__C"
			, "TDFV_SRC"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
			, "TDFV_SRC"."CHURN__C" AS "CHURN__C"
			, "TDFV_SRC"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
		FROM {{ ref('SALESFORCE_DFV_VW_CUSTOMER__C') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."OWNERID" AS "OWNERID"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
			, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
			, "CALCULATE_BK"."NAME" AS "NAME"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALCULATE_BK"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
			, "CALCULATE_BK"."GENDER__C" AS "GENDER__C"
			, "CALCULATE_BK"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
			, "CALCULATE_BK"."PARTNER__C" AS "PARTNER__C"
			, "CALCULATE_BK"."DEPENDENTS__C" AS "DEPENDENTS__C"
			, "CALCULATE_BK"."TENURE__C" AS "TENURE__C"
			, "CALCULATE_BK"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
			, "CALCULATE_BK"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
			, "CALCULATE_BK"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
			, "CALCULATE_BK"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
			, "CALCULATE_BK"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
			, "CALCULATE_BK"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
			, "CALCULATE_BK"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
			, "CALCULATE_BK"."STREAMING_TV__C" AS "STREAMING_TV__C"
			, "CALCULATE_BK"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
			, "CALCULATE_BK"."CONTRACT__C" AS "CONTRACT__C"
			, "CALCULATE_BK"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
			, "CALCULATE_BK"."CHURN__C" AS "CHURN__C"
			, "CALCULATE_BK"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."ID" AS "ID"
		, "EXT_UNION"."OWNERID" AS "OWNERID"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_UNION"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_UNION"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_UNION"."ISDELETED" AS "ISDELETED"
		, "EXT_UNION"."NAME" AS "NAME"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_UNION"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
		, "EXT_UNION"."GENDER__C" AS "GENDER__C"
		, "EXT_UNION"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
		, "EXT_UNION"."PARTNER__C" AS "PARTNER__C"
		, "EXT_UNION"."DEPENDENTS__C" AS "DEPENDENTS__C"
		, "EXT_UNION"."TENURE__C" AS "TENURE__C"
		, "EXT_UNION"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
		, "EXT_UNION"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
		, "EXT_UNION"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
		, "EXT_UNION"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
		, "EXT_UNION"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
		, "EXT_UNION"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
		, "EXT_UNION"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
		, "EXT_UNION"."STREAMING_TV__C" AS "STREAMING_TV__C"
		, "EXT_UNION"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
		, "EXT_UNION"."CONTRACT__C" AS "CONTRACT__C"
		, "EXT_UNION"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
		, "EXT_UNION"."CHURN__C" AS "CHURN__C"
		, "EXT_UNION"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
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
			, COALESCE("INI_SRC"."OWNERID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "OWNERID"
			, COALESCE("INI_SRC"."LASTMODIFIEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LASTMODIFIEDBYID"
			, "INI_SRC"."ISDELETED" AS "ISDELETED"
			, "INI_SRC"."NAME" AS "NAME"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "INI_SRC"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
			, "INI_SRC"."GENDER__C" AS "GENDER__C"
			, "INI_SRC"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
			, "INI_SRC"."PARTNER__C" AS "PARTNER__C"
			, "INI_SRC"."DEPENDENTS__C" AS "DEPENDENTS__C"
			, "INI_SRC"."TENURE__C" AS "TENURE__C"
			, "INI_SRC"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
			, "INI_SRC"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
			, "INI_SRC"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
			, "INI_SRC"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
			, "INI_SRC"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
			, "INI_SRC"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
			, "INI_SRC"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
			, "INI_SRC"."STREAMING_TV__C" AS "STREAMING_TV__C"
			, "INI_SRC"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
			, "INI_SRC"."CONTRACT__C" AS "CONTRACT__C"
			, "INI_SRC"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
			, "INI_SRC"."CHURN__C" AS "CHURN__C"
			, "INI_SRC"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
		FROM {{ source('SALESFORCE_INI', 'CUSTOMER__C') }} "INI_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."ID" AS "ID"
			, "LOAD_INIT_DATA"."OWNERID" AS "OWNERID"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."ISDELETED" AS "ISDELETED"
			, "LOAD_INIT_DATA"."NAME" AS "NAME"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "LOAD_INIT_DATA"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
			, "LOAD_INIT_DATA"."GENDER__C" AS "GENDER__C"
			, "LOAD_INIT_DATA"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
			, "LOAD_INIT_DATA"."PARTNER__C" AS "PARTNER__C"
			, "LOAD_INIT_DATA"."DEPENDENTS__C" AS "DEPENDENTS__C"
			, "LOAD_INIT_DATA"."TENURE__C" AS "TENURE__C"
			, "LOAD_INIT_DATA"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
			, "LOAD_INIT_DATA"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
			, "LOAD_INIT_DATA"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
			, "LOAD_INIT_DATA"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
			, "LOAD_INIT_DATA"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
			, "LOAD_INIT_DATA"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
			, "LOAD_INIT_DATA"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
			, "LOAD_INIT_DATA"."STREAMING_TV__C" AS "STREAMING_TV__C"
			, "LOAD_INIT_DATA"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
			, "LOAD_INIT_DATA"."CONTRACT__C" AS "CONTRACT__C"
			, "LOAD_INIT_DATA"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
			, "LOAD_INIT_DATA"."CHURN__C" AS "CHURN__C"
			, "LOAD_INIT_DATA"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNERID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISDELETED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "NAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CUSTOMER_ID__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "GENDER__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "SENIOR_CITIZEN__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "PARTNER__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "DEPENDENTS__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "TENURE__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "PHONE_SERVICE__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "MULTIPLE_LINES__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "INTERNET_SERVICE__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ONLINE_SECURITY__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ONLINE_BACKUP__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DEVICE_PROTECTION__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "TECH_SUPPORT__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STREAMING_TV__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STREAMING_MOVIES__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTRACT__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "PAPERLESS_BILLING__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "CHURN__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PAYMENT_METHOD__C"
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
			, "PREP_EXCEP"."OWNERID" AS "OWNERID"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "PREP_EXCEP"."ISDELETED" AS "ISDELETED"
			, "PREP_EXCEP"."NAME" AS "NAME"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "PREP_EXCEP"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
			, "PREP_EXCEP"."GENDER__C" AS "GENDER__C"
			, "PREP_EXCEP"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
			, "PREP_EXCEP"."PARTNER__C" AS "PARTNER__C"
			, "PREP_EXCEP"."DEPENDENTS__C" AS "DEPENDENTS__C"
			, "PREP_EXCEP"."TENURE__C" AS "TENURE__C"
			, "PREP_EXCEP"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
			, "PREP_EXCEP"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
			, "PREP_EXCEP"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
			, "PREP_EXCEP"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
			, "PREP_EXCEP"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
			, "PREP_EXCEP"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
			, "PREP_EXCEP"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
			, "PREP_EXCEP"."STREAMING_TV__C" AS "STREAMING_TV__C"
			, "PREP_EXCEP"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
			, "PREP_EXCEP"."CONTRACT__C" AS "CONTRACT__C"
			, "PREP_EXCEP"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
			, "PREP_EXCEP"."CHURN__C" AS "CHURN__C"
			, "PREP_EXCEP"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
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
		, "CALCULATE_BK"."OWNERID" AS "OWNERID"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
		, "CALCULATE_BK"."NAME" AS "NAME"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "CALCULATE_BK"."CUSTOMER_ID__C" AS "CUSTOMER_ID__C"
		, "CALCULATE_BK"."GENDER__C" AS "GENDER__C"
		, "CALCULATE_BK"."SENIOR_CITIZEN__C" AS "SENIOR_CITIZEN__C"
		, "CALCULATE_BK"."PARTNER__C" AS "PARTNER__C"
		, "CALCULATE_BK"."DEPENDENTS__C" AS "DEPENDENTS__C"
		, "CALCULATE_BK"."TENURE__C" AS "TENURE__C"
		, "CALCULATE_BK"."PHONE_SERVICE__C" AS "PHONE_SERVICE__C"
		, "CALCULATE_BK"."MULTIPLE_LINES__C" AS "MULTIPLE_LINES__C"
		, "CALCULATE_BK"."INTERNET_SERVICE__C" AS "INTERNET_SERVICE__C"
		, "CALCULATE_BK"."ONLINE_SECURITY__C" AS "ONLINE_SECURITY__C"
		, "CALCULATE_BK"."ONLINE_BACKUP__C" AS "ONLINE_BACKUP__C"
		, "CALCULATE_BK"."DEVICE_PROTECTION__C" AS "DEVICE_PROTECTION__C"
		, "CALCULATE_BK"."TECH_SUPPORT__C" AS "TECH_SUPPORT__C"
		, "CALCULATE_BK"."STREAMING_TV__C" AS "STREAMING_TV__C"
		, "CALCULATE_BK"."STREAMING_MOVIES__C" AS "STREAMING_MOVIES__C"
		, "CALCULATE_BK"."CONTRACT__C" AS "CONTRACT__C"
		, "CALCULATE_BK"."PAPERLESS_BILLING__C" AS "PAPERLESS_BILLING__C"
		, "CALCULATE_BK"."CHURN__C" AS "CHURN__C"
		, "CALCULATE_BK"."PAYMENT_METHOD__C" AS "PAYMENT_METHOD__C"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'