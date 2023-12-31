{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='CASE',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_CASE_INCR', 'EXT_SALESFORCE_CASE_INIT']
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
			, "TDFV_SRC"."CONTACTID" AS "CONTACTID"
			, "TDFV_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "TDFV_SRC"."ASSETID" AS "ASSETID"
			, "TDFV_SRC"."PARENTID" AS "PARENTID"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."PARENTID"),'\#','\\' || '\#') AS "ID_FK_PARENTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CONTACTID"),'\#','\\' || '\#') AS "ID_FK_CONTACTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ASSETID"),'\#','\\' || '\#') AS "ID_FK_ASSETID_BK"
			, "TDFV_SRC"."ISDELETED" AS "ISDELETED"
			, "TDFV_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
			, "TDFV_SRC"."CASENUMBER" AS "CASENUMBER"
			, "TDFV_SRC"."SOURCEID" AS "SOURCEID"
			, "TDFV_SRC"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
			, "TDFV_SRC"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
			, "TDFV_SRC"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
			, "TDFV_SRC"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
			, "TDFV_SRC"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
			, "TDFV_SRC"."TYPE" AS "TYPE"
			, "TDFV_SRC"."STATUS" AS "STATUS"
			, "TDFV_SRC"."REASON" AS "REASON"
			, "TDFV_SRC"."ORIGIN" AS "ORIGIN"
			, "TDFV_SRC"."SUBJECT" AS "SUBJECT"
			, "TDFV_SRC"."PRIORITY" AS "PRIORITY"
			, "TDFV_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "TDFV_SRC"."ISCLOSED" AS "ISCLOSED"
			, "TDFV_SRC"."CLOSEDDATE" AS "CLOSEDDATE"
			, "TDFV_SRC"."ISESCALATED" AS "ISESCALATED"
			, "TDFV_SRC"."OWNERID" AS "OWNERID"
			, "TDFV_SRC"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "TDFV_SRC"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
			, "TDFV_SRC"."CSAT__C" AS "CSAT__C"
			, "TDFV_SRC"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
			, "TDFV_SRC"."FCR__C" AS "FCR__C"
			, "TDFV_SRC"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
			, "TDFV_SRC"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
			, "TDFV_SRC"."SLA_TYPE__C" AS "SLA_TYPE__C"
		FROM {{ ref('SALESFORCE_DFV_VW_CASE') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."CONTACTID" AS "CONTACTID"
			, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
			, "CALCULATE_BK"."ASSETID" AS "ASSETID"
			, "CALCULATE_BK"."PARENTID" AS "PARENTID"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
			, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
			, "CALCULATE_BK"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
			, "CALCULATE_BK"."ID_FK_ASSETID_BK" AS "ID_FK_ASSETID_BK"
			, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
			, "CALCULATE_BK"."MASTERRECORDID" AS "MASTERRECORDID"
			, "CALCULATE_BK"."CASENUMBER" AS "CASENUMBER"
			, "CALCULATE_BK"."SOURCEID" AS "SOURCEID"
			, "CALCULATE_BK"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
			, "CALCULATE_BK"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
			, "CALCULATE_BK"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
			, "CALCULATE_BK"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
			, "CALCULATE_BK"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
			, "CALCULATE_BK"."TYPE" AS "TYPE"
			, "CALCULATE_BK"."STATUS" AS "STATUS"
			, "CALCULATE_BK"."REASON" AS "REASON"
			, "CALCULATE_BK"."ORIGIN" AS "ORIGIN"
			, "CALCULATE_BK"."SUBJECT" AS "SUBJECT"
			, "CALCULATE_BK"."PRIORITY" AS "PRIORITY"
			, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
			, "CALCULATE_BK"."ISCLOSED" AS "ISCLOSED"
			, "CALCULATE_BK"."CLOSEDDATE" AS "CLOSEDDATE"
			, "CALCULATE_BK"."ISESCALATED" AS "ISESCALATED"
			, "CALCULATE_BK"."OWNERID" AS "OWNERID"
			, "CALCULATE_BK"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALCULATE_BK"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
			, "CALCULATE_BK"."CSAT__C" AS "CSAT__C"
			, "CALCULATE_BK"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
			, "CALCULATE_BK"."FCR__C" AS "FCR__C"
			, "CALCULATE_BK"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
			, "CALCULATE_BK"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
			, "CALCULATE_BK"."SLA_TYPE__C" AS "SLA_TYPE__C"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."ID" AS "ID"
		, "EXT_UNION"."CONTACTID" AS "CONTACTID"
		, "EXT_UNION"."ACCOUNTID" AS "ACCOUNTID"
		, "EXT_UNION"."ASSETID" AS "ASSETID"
		, "EXT_UNION"."PARENTID" AS "PARENTID"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "EXT_UNION"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_UNION"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_UNION"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_UNION"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "EXT_UNION"."ID_FK_ASSETID_BK" AS "ID_FK_ASSETID_BK"
		, "EXT_UNION"."ISDELETED" AS "ISDELETED"
		, "EXT_UNION"."MASTERRECORDID" AS "MASTERRECORDID"
		, "EXT_UNION"."CASENUMBER" AS "CASENUMBER"
		, "EXT_UNION"."SOURCEID" AS "SOURCEID"
		, "EXT_UNION"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
		, "EXT_UNION"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
		, "EXT_UNION"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
		, "EXT_UNION"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
		, "EXT_UNION"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
		, "EXT_UNION"."TYPE" AS "TYPE"
		, "EXT_UNION"."STATUS" AS "STATUS"
		, "EXT_UNION"."REASON" AS "REASON"
		, "EXT_UNION"."ORIGIN" AS "ORIGIN"
		, "EXT_UNION"."SUBJECT" AS "SUBJECT"
		, "EXT_UNION"."PRIORITY" AS "PRIORITY"
		, "EXT_UNION"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_UNION"."ISCLOSED" AS "ISCLOSED"
		, "EXT_UNION"."CLOSEDDATE" AS "CLOSEDDATE"
		, "EXT_UNION"."ISESCALATED" AS "ISESCALATED"
		, "EXT_UNION"."OWNERID" AS "OWNERID"
		, "EXT_UNION"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_UNION"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
		, "EXT_UNION"."CSAT__C" AS "CSAT__C"
		, "EXT_UNION"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
		, "EXT_UNION"."FCR__C" AS "FCR__C"
		, "EXT_UNION"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
		, "EXT_UNION"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
		, "EXT_UNION"."SLA_TYPE__C" AS "SLA_TYPE__C"
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
			, COALESCE("INI_SRC"."PARENTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "PARENTID"
			, COALESCE("INI_SRC"."CREATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CREATEDBYID"
			, COALESCE("INI_SRC"."LASTMODIFIEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LASTMODIFIEDBYID"
			, COALESCE("INI_SRC"."ACCOUNTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ACCOUNTID"
			, COALESCE("INI_SRC"."CONTACTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CONTACTID"
			, COALESCE("INI_SRC"."ASSETID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ASSETID"
			, "INI_SRC"."ISDELETED" AS "ISDELETED"
			, "INI_SRC"."MASTERRECORDID" AS "MASTERRECORDID"
			, "INI_SRC"."CASENUMBER" AS "CASENUMBER"
			, "INI_SRC"."SOURCEID" AS "SOURCEID"
			, "INI_SRC"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
			, "INI_SRC"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
			, "INI_SRC"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
			, "INI_SRC"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
			, "INI_SRC"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
			, "INI_SRC"."TYPE" AS "TYPE"
			, "INI_SRC"."STATUS" AS "STATUS"
			, "INI_SRC"."REASON" AS "REASON"
			, "INI_SRC"."ORIGIN" AS "ORIGIN"
			, "INI_SRC"."SUBJECT" AS "SUBJECT"
			, "INI_SRC"."PRIORITY" AS "PRIORITY"
			, "INI_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "INI_SRC"."ISCLOSED" AS "ISCLOSED"
			, "INI_SRC"."CLOSEDDATE" AS "CLOSEDDATE"
			, "INI_SRC"."ISESCALATED" AS "ISESCALATED"
			, "INI_SRC"."OWNERID" AS "OWNERID"
			, "INI_SRC"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "INI_SRC"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
			, "INI_SRC"."CSAT__C" AS "CSAT__C"
			, "INI_SRC"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
			, "INI_SRC"."FCR__C" AS "FCR__C"
			, "INI_SRC"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
			, "INI_SRC"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
			, "INI_SRC"."SLA_TYPE__C" AS "SLA_TYPE__C"
		FROM {{ source('SALESFORCE_INI', 'CASE') }} "INI_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."ID" AS "ID"
			, "LOAD_INIT_DATA"."CONTACTID" AS "CONTACTID"
			, "LOAD_INIT_DATA"."ACCOUNTID" AS "ACCOUNTID"
			, "LOAD_INIT_DATA"."ASSETID" AS "ASSETID"
			, "LOAD_INIT_DATA"."PARENTID" AS "PARENTID"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."ISDELETED" AS "ISDELETED"
			, "LOAD_INIT_DATA"."MASTERRECORDID" AS "MASTERRECORDID"
			, "LOAD_INIT_DATA"."CASENUMBER" AS "CASENUMBER"
			, "LOAD_INIT_DATA"."SOURCEID" AS "SOURCEID"
			, "LOAD_INIT_DATA"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
			, "LOAD_INIT_DATA"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
			, "LOAD_INIT_DATA"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
			, "LOAD_INIT_DATA"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
			, "LOAD_INIT_DATA"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
			, "LOAD_INIT_DATA"."TYPE" AS "TYPE"
			, "LOAD_INIT_DATA"."STATUS" AS "STATUS"
			, "LOAD_INIT_DATA"."REASON" AS "REASON"
			, "LOAD_INIT_DATA"."ORIGIN" AS "ORIGIN"
			, "LOAD_INIT_DATA"."SUBJECT" AS "SUBJECT"
			, "LOAD_INIT_DATA"."PRIORITY" AS "PRIORITY"
			, "LOAD_INIT_DATA"."DESCRIPTION" AS "DESCRIPTION"
			, "LOAD_INIT_DATA"."ISCLOSED" AS "ISCLOSED"
			, "LOAD_INIT_DATA"."CLOSEDDATE" AS "CLOSEDDATE"
			, "LOAD_INIT_DATA"."ISESCALATED" AS "ISESCALATED"
			, "LOAD_INIT_DATA"."OWNERID" AS "OWNERID"
			, "LOAD_INIT_DATA"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "LOAD_INIT_DATA"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
			, "LOAD_INIT_DATA"."CSAT__C" AS "CSAT__C"
			, "LOAD_INIT_DATA"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
			, "LOAD_INIT_DATA"."FCR__C" AS "FCR__C"
			, "LOAD_INIT_DATA"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
			, "LOAD_INIT_DATA"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
			, "LOAD_INIT_DATA"."SLA_TYPE__C" AS "SLA_TYPE__C"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTACTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ASSETID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PARENTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISDELETED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "MASTERRECORDID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "CASENUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SOURCEID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BUSINESSHOURSID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SUPPLIEDNAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SUPPLIEDEMAIL"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SUPPLIEDPHONE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SUPPLIEDCOMPANY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "TYPE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATUS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "REASON"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ORIGIN"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SUBJECT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PRIORITY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DESCRIPTION"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISCLOSED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CLOSEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISESCALATED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNERID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "ISCLOSEDONCREATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "EVENTSPROCESSEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "CSAT__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "CASE_EXTERNALID__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "FCR__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PRODUCT_FAMILY_KB__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SLAVIOLATION__C"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SLA_TYPE__C"
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
			, "PREP_EXCEP"."CONTACTID" AS "CONTACTID"
			, "PREP_EXCEP"."ACCOUNTID" AS "ACCOUNTID"
			, "PREP_EXCEP"."ASSETID" AS "ASSETID"
			, "PREP_EXCEP"."PARENTID" AS "PARENTID"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."PARENTID"),'\#','\\' || '\#') AS "ID_FK_PARENTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CONTACTID"),'\#','\\' || '\#') AS "ID_FK_CONTACTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ASSETID"),'\#','\\' || '\#') AS "ID_FK_ASSETID_BK"
			, "PREP_EXCEP"."ISDELETED" AS "ISDELETED"
			, "PREP_EXCEP"."MASTERRECORDID" AS "MASTERRECORDID"
			, "PREP_EXCEP"."CASENUMBER" AS "CASENUMBER"
			, "PREP_EXCEP"."SOURCEID" AS "SOURCEID"
			, "PREP_EXCEP"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
			, "PREP_EXCEP"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
			, "PREP_EXCEP"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
			, "PREP_EXCEP"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
			, "PREP_EXCEP"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
			, "PREP_EXCEP"."TYPE" AS "TYPE"
			, "PREP_EXCEP"."STATUS" AS "STATUS"
			, "PREP_EXCEP"."REASON" AS "REASON"
			, "PREP_EXCEP"."ORIGIN" AS "ORIGIN"
			, "PREP_EXCEP"."SUBJECT" AS "SUBJECT"
			, "PREP_EXCEP"."PRIORITY" AS "PRIORITY"
			, "PREP_EXCEP"."DESCRIPTION" AS "DESCRIPTION"
			, "PREP_EXCEP"."ISCLOSED" AS "ISCLOSED"
			, "PREP_EXCEP"."CLOSEDDATE" AS "CLOSEDDATE"
			, "PREP_EXCEP"."ISESCALATED" AS "ISESCALATED"
			, "PREP_EXCEP"."OWNERID" AS "OWNERID"
			, "PREP_EXCEP"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "PREP_EXCEP"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
			, "PREP_EXCEP"."CSAT__C" AS "CSAT__C"
			, "PREP_EXCEP"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
			, "PREP_EXCEP"."FCR__C" AS "FCR__C"
			, "PREP_EXCEP"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
			, "PREP_EXCEP"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
			, "PREP_EXCEP"."SLA_TYPE__C" AS "SLA_TYPE__C"
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
		, "CALCULATE_BK"."CONTACTID" AS "CONTACTID"
		, "CALCULATE_BK"."ACCOUNTID" AS "ACCOUNTID"
		, "CALCULATE_BK"."ASSETID" AS "ASSETID"
		, "CALCULATE_BK"."PARENTID" AS "PARENTID"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "CALCULATE_BK"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "CALCULATE_BK"."ID_FK_ASSETID_BK" AS "ID_FK_ASSETID_BK"
		, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
		, "CALCULATE_BK"."MASTERRECORDID" AS "MASTERRECORDID"
		, "CALCULATE_BK"."CASENUMBER" AS "CASENUMBER"
		, "CALCULATE_BK"."SOURCEID" AS "SOURCEID"
		, "CALCULATE_BK"."BUSINESSHOURSID" AS "BUSINESSHOURSID"
		, "CALCULATE_BK"."SUPPLIEDNAME" AS "SUPPLIEDNAME"
		, "CALCULATE_BK"."SUPPLIEDEMAIL" AS "SUPPLIEDEMAIL"
		, "CALCULATE_BK"."SUPPLIEDPHONE" AS "SUPPLIEDPHONE"
		, "CALCULATE_BK"."SUPPLIEDCOMPANY" AS "SUPPLIEDCOMPANY"
		, "CALCULATE_BK"."TYPE" AS "TYPE"
		, "CALCULATE_BK"."STATUS" AS "STATUS"
		, "CALCULATE_BK"."REASON" AS "REASON"
		, "CALCULATE_BK"."ORIGIN" AS "ORIGIN"
		, "CALCULATE_BK"."SUBJECT" AS "SUBJECT"
		, "CALCULATE_BK"."PRIORITY" AS "PRIORITY"
		, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
		, "CALCULATE_BK"."ISCLOSED" AS "ISCLOSED"
		, "CALCULATE_BK"."CLOSEDDATE" AS "CLOSEDDATE"
		, "CALCULATE_BK"."ISESCALATED" AS "ISESCALATED"
		, "CALCULATE_BK"."OWNERID" AS "OWNERID"
		, "CALCULATE_BK"."ISCLOSEDONCREATE" AS "ISCLOSEDONCREATE"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "CALCULATE_BK"."EVENTSPROCESSEDDATE" AS "EVENTSPROCESSEDDATE"
		, "CALCULATE_BK"."CSAT__C" AS "CSAT__C"
		, "CALCULATE_BK"."CASE_EXTERNALID__C" AS "CASE_EXTERNALID__C"
		, "CALCULATE_BK"."FCR__C" AS "FCR__C"
		, "CALCULATE_BK"."PRODUCT_FAMILY_KB__C" AS "PRODUCT_FAMILY_KB__C"
		, "CALCULATE_BK"."SLAVIOLATION__C" AS "SLAVIOLATION__C"
		, "CALCULATE_BK"."SLA_TYPE__C" AS "SLA_TYPE__C"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'