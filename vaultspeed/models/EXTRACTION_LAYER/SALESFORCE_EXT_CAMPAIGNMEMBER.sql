{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='CAMPAIGNMEMBER',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_CAMPAIGNMEMBER_INCR', 'EXT_SALESFORCE_CAMPAIGNMEMBER_INIT']
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
			, "TDFV_SRC"."CAMPAIGNID" AS "CAMPAIGNID"
			, "TDFV_SRC"."LEADID" AS "LEADID"
			, "TDFV_SRC"."CONTACTID" AS "CONTACTID"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CAMPAIGNID"),'\#','\\' || '\#') AS "ID_FK_CAMPAIGNID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LEADID"),'\#','\\' || '\#') AS "ID_FK_LEADID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CONTACTID"),'\#','\\' || '\#') AS "ID_FK_CONTACTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, "TDFV_SRC"."ISDELETED" AS "ISDELETED"
			, "TDFV_SRC"."STATUS" AS "STATUS"
			, "TDFV_SRC"."HASRESPONDED" AS "HASRESPONDED"
			, "TDFV_SRC"."ISPRIMARY" AS "ISPRIMARY"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "TDFV_SRC"."FIRSTRESPONDEDDATE" AS "FIRSTRESPONDEDDATE"
		FROM {{ ref('SALESFORCE_DFV_VW_CAMPAIGNMEMBER') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."CAMPAIGNID" AS "CAMPAIGNID"
			, "CALCULATE_BK"."LEADID" AS "LEADID"
			, "CALCULATE_BK"."CONTACTID" AS "CONTACTID"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."ID_FK_CAMPAIGNID_BK" AS "ID_FK_CAMPAIGNID_BK"
			, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_LEADID_BK" AS "ID_FK_LEADID_BK"
			, "CALCULATE_BK"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
			, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
			, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
			, "CALCULATE_BK"."STATUS" AS "STATUS"
			, "CALCULATE_BK"."HASRESPONDED" AS "HASRESPONDED"
			, "CALCULATE_BK"."ISPRIMARY" AS "ISPRIMARY"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALCULATE_BK"."FIRSTRESPONDEDDATE" AS "FIRSTRESPONDEDDATE"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."ID" AS "ID"
		, "EXT_UNION"."CAMPAIGNID" AS "CAMPAIGNID"
		, "EXT_UNION"."LEADID" AS "LEADID"
		, "EXT_UNION"."CONTACTID" AS "CONTACTID"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."ID_FK_CAMPAIGNID_BK" AS "ID_FK_CAMPAIGNID_BK"
		, "EXT_UNION"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_UNION"."ID_FK_LEADID_BK" AS "ID_FK_LEADID_BK"
		, "EXT_UNION"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "EXT_UNION"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_UNION"."ISDELETED" AS "ISDELETED"
		, "EXT_UNION"."STATUS" AS "STATUS"
		, "EXT_UNION"."HASRESPONDED" AS "HASRESPONDED"
		, "EXT_UNION"."ISPRIMARY" AS "ISPRIMARY"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_UNION"."FIRSTRESPONDEDDATE" AS "FIRSTRESPONDEDDATE"
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
			, COALESCE("INI_SRC"."CAMPAIGNID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CAMPAIGNID"
			, COALESCE("INI_SRC"."LASTMODIFIEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LASTMODIFIEDBYID"
			, COALESCE("INI_SRC"."LEADID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LEADID"
			, COALESCE("INI_SRC"."CONTACTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CONTACTID"
			, COALESCE("INI_SRC"."CREATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CREATEDBYID"
			, "INI_SRC"."ISDELETED" AS "ISDELETED"
			, "INI_SRC"."STATUS" AS "STATUS"
			, "INI_SRC"."HASRESPONDED" AS "HASRESPONDED"
			, "INI_SRC"."ISPRIMARY" AS "ISPRIMARY"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "INI_SRC"."FIRSTRESPONDEDDATE" AS "FIRSTRESPONDEDDATE"
		FROM {{ source('SALESFORCE_INI', 'CAMPAIGNMEMBER') }} "INI_SRC"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."ID" AS "ID"
			, "LOAD_INIT_DATA"."CAMPAIGNID" AS "CAMPAIGNID"
			, "LOAD_INIT_DATA"."LEADID" AS "LEADID"
			, "LOAD_INIT_DATA"."CONTACTID" AS "CONTACTID"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."ISDELETED" AS "ISDELETED"
			, "LOAD_INIT_DATA"."STATUS" AS "STATUS"
			, "LOAD_INIT_DATA"."HASRESPONDED" AS "HASRESPONDED"
			, "LOAD_INIT_DATA"."ISPRIMARY" AS "ISPRIMARY"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "LOAD_INIT_DATA"."FIRSTRESPONDEDDATE" AS "FIRSTRESPONDEDDATE"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CAMPAIGNID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LEADID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTACTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISDELETED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATUS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "HASRESPONDED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISPRIMARY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "FIRSTRESPONDEDDATE"
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
			, "PREP_EXCEP"."CAMPAIGNID" AS "CAMPAIGNID"
			, "PREP_EXCEP"."LEADID" AS "LEADID"
			, "PREP_EXCEP"."CONTACTID" AS "CONTACTID"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CAMPAIGNID"),'\#','\\' || '\#') AS "ID_FK_CAMPAIGNID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LEADID"),'\#','\\' || '\#') AS "ID_FK_LEADID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CONTACTID"),'\#','\\' || '\#') AS "ID_FK_CONTACTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, "PREP_EXCEP"."ISDELETED" AS "ISDELETED"
			, "PREP_EXCEP"."STATUS" AS "STATUS"
			, "PREP_EXCEP"."HASRESPONDED" AS "HASRESPONDED"
			, "PREP_EXCEP"."ISPRIMARY" AS "ISPRIMARY"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "PREP_EXCEP"."FIRSTRESPONDEDDATE" AS "FIRSTRESPONDEDDATE"
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
		, "CALCULATE_BK"."CAMPAIGNID" AS "CAMPAIGNID"
		, "CALCULATE_BK"."LEADID" AS "LEADID"
		, "CALCULATE_BK"."CONTACTID" AS "CONTACTID"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."ID_FK_CAMPAIGNID_BK" AS "ID_FK_CAMPAIGNID_BK"
		, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_LEADID_BK" AS "ID_FK_LEADID_BK"
		, "CALCULATE_BK"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
		, "CALCULATE_BK"."STATUS" AS "STATUS"
		, "CALCULATE_BK"."HASRESPONDED" AS "HASRESPONDED"
		, "CALCULATE_BK"."ISPRIMARY" AS "ISPRIMARY"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "CALCULATE_BK"."FIRSTRESPONDEDDATE" AS "FIRSTRESPONDEDDATE"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'