{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='ASSET',
		schema='SALESFORCE_EXT',
		tags=['SALESFORCE', 'EXT_SALESFORCE_ASSET_INCR', 'EXT_SALESFORCE_ASSET_INIT']
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
			, "TDFV_SRC"."PARENTID" AS "PARENTID"
			, "TDFV_SRC"."ROOTASSETID" AS "ROOTASSETID"
			, "TDFV_SRC"."PRODUCT2ID" AS "PRODUCT2ID"
			, "TDFV_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "TDFV_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "TDFV_SRC"."OWNERID" AS "OWNERID"
			, "TDFV_SRC"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
			, "TDFV_SRC"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."PARENTID"),'\#','\\' || '\#') AS "ID_FK_PARENTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."PRODUCT2ID"),'\#','\\' || '\#') AS "ID_FK_PRODUCT2ID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ASSETPROVIDEDBYID"),'\#','\\' || '\#') AS "ID_FK_ASSETPROVIDEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ROOTASSETID"),'\#','\\' || '\#') AS "ID_FK_ROOTASSETID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CONTACTID"),'\#','\\' || '\#') AS "ID_FK_CONTACTID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM( "TDFV_SRC"."ASSETSERVICEDBYID"),'\#','\\' || '\#') AS "ID_FK_ASSETSERVICEDBYID_BK"
			, "TDFV_SRC"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
			, "TDFV_SRC"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
			, "TDFV_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "TDFV_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "TDFV_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "TDFV_SRC"."ISDELETED" AS "ISDELETED"
			, "TDFV_SRC"."NAME" AS "NAME"
			, "TDFV_SRC"."SERIALNUMBER" AS "SERIALNUMBER"
			, "TDFV_SRC"."INSTALLDATE" AS "INSTALLDATE"
			, "TDFV_SRC"."PURCHASEDATE" AS "PURCHASEDATE"
			, "TDFV_SRC"."USAGEENDDATE" AS "USAGEENDDATE"
			, "TDFV_SRC"."STATUS" AS "STATUS"
			, "TDFV_SRC"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
			, "TDFV_SRC"."PRICE" AS "PRICE"
			, "TDFV_SRC"."QUANTITY" AS "QUANTITY"
			, "TDFV_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "TDFV_SRC"."ISINTERNAL" AS "ISINTERNAL"
		FROM {{ ref('SALESFORCE_DFV_VW_ASSET') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."PARENTID" AS "PARENTID"
			, "CALCULATE_BK"."ROOTASSETID" AS "ROOTASSETID"
			, "CALCULATE_BK"."PRODUCT2ID" AS "PRODUCT2ID"
			, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
			, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CALCULATE_BK"."OWNERID" AS "OWNERID"
			, "CALCULATE_BK"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
			, "CALCULATE_BK"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
			, "CALCULATE_BK"."ID_BK" AS "ID_BK"
			, "CALCULATE_BK"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
			, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
			, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
			, "CALCULATE_BK"."ID_FK_PRODUCT2ID_BK" AS "ID_FK_PRODUCT2ID_BK"
			, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_ASSETPROVIDEDBYID_BK" AS "ID_FK_ASSETPROVIDEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_ROOTASSETID_BK" AS "ID_FK_ROOTASSETID_BK"
			, "CALCULATE_BK"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
			, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
			, "CALCULATE_BK"."ID_FK_ASSETSERVICEDBYID_BK" AS "ID_FK_ASSETSERVICEDBYID_BK"
			, "CALCULATE_BK"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
			, "CALCULATE_BK"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
			, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
			, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
			, "CALCULATE_BK"."NAME" AS "NAME"
			, "CALCULATE_BK"."SERIALNUMBER" AS "SERIALNUMBER"
			, "CALCULATE_BK"."INSTALLDATE" AS "INSTALLDATE"
			, "CALCULATE_BK"."PURCHASEDATE" AS "PURCHASEDATE"
			, "CALCULATE_BK"."USAGEENDDATE" AS "USAGEENDDATE"
			, "CALCULATE_BK"."STATUS" AS "STATUS"
			, "CALCULATE_BK"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
			, "CALCULATE_BK"."PRICE" AS "PRICE"
			, "CALCULATE_BK"."QUANTITY" AS "QUANTITY"
			, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
			, "CALCULATE_BK"."ISINTERNAL" AS "ISINTERNAL"
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
		, "EXT_UNION"."PARENTID" AS "PARENTID"
		, "EXT_UNION"."ROOTASSETID" AS "ROOTASSETID"
		, "EXT_UNION"."PRODUCT2ID" AS "PRODUCT2ID"
		, "EXT_UNION"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_UNION"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_UNION"."OWNERID" AS "OWNERID"
		, "EXT_UNION"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
		, "EXT_UNION"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
		, "EXT_UNION"."ID_BK" AS "ID_BK"
		, "EXT_UNION"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "EXT_UNION"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "EXT_UNION"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "EXT_UNION"."ID_FK_PRODUCT2ID_BK" AS "ID_FK_PRODUCT2ID_BK"
		, "EXT_UNION"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "EXT_UNION"."ID_FK_ASSETPROVIDEDBYID_BK" AS "ID_FK_ASSETPROVIDEDBYID_BK"
		, "EXT_UNION"."ID_FK_ROOTASSETID_BK" AS "ID_FK_ROOTASSETID_BK"
		, "EXT_UNION"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "EXT_UNION"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_UNION"."ID_FK_ASSETSERVICEDBYID_BK" AS "ID_FK_ASSETSERVICEDBYID_BK"
		, "EXT_UNION"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
		, "EXT_UNION"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
		, "EXT_UNION"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_UNION"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_UNION"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_UNION"."ISDELETED" AS "ISDELETED"
		, "EXT_UNION"."NAME" AS "NAME"
		, "EXT_UNION"."SERIALNUMBER" AS "SERIALNUMBER"
		, "EXT_UNION"."INSTALLDATE" AS "INSTALLDATE"
		, "EXT_UNION"."PURCHASEDATE" AS "PURCHASEDATE"
		, "EXT_UNION"."USAGEENDDATE" AS "USAGEENDDATE"
		, "EXT_UNION"."STATUS" AS "STATUS"
		, "EXT_UNION"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
		, "EXT_UNION"."PRICE" AS "PRICE"
		, "EXT_UNION"."QUANTITY" AS "QUANTITY"
		, "EXT_UNION"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_UNION"."ISINTERNAL" AS "ISINTERNAL"
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
			, COALESCE("INI_SRC"."OWNERID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "OWNERID"
			, COALESCE("INI_SRC"."ACCOUNTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ACCOUNTID"
			, COALESCE("INI_SRC"."PRODUCT2ID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "PRODUCT2ID"
			, COALESCE("INI_SRC"."LASTMODIFIEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "LASTMODIFIEDBYID"
			, COALESCE("INI_SRC"."ASSETPROVIDEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ASSETPROVIDEDBYID"
			, COALESCE("INI_SRC"."ROOTASSETID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ROOTASSETID"
			, COALESCE("INI_SRC"."CONTACTID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CONTACTID"
			, COALESCE("INI_SRC"."CREATEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "CREATEDBYID"
			, COALESCE("INI_SRC"."ASSETSERVICEDBYID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "ASSETSERVICEDBYID"
			, "INI_SRC"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
			, "INI_SRC"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
			, "INI_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "INI_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "INI_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "INI_SRC"."ISDELETED" AS "ISDELETED"
			, "INI_SRC"."NAME" AS "NAME"
			, "INI_SRC"."SERIALNUMBER" AS "SERIALNUMBER"
			, "INI_SRC"."INSTALLDATE" AS "INSTALLDATE"
			, "INI_SRC"."PURCHASEDATE" AS "PURCHASEDATE"
			, "INI_SRC"."USAGEENDDATE" AS "USAGEENDDATE"
			, "INI_SRC"."STATUS" AS "STATUS"
			, "INI_SRC"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
			, "INI_SRC"."PRICE" AS "PRICE"
			, "INI_SRC"."QUANTITY" AS "QUANTITY"
			, "INI_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "INI_SRC"."ISINTERNAL" AS "ISINTERNAL"
		FROM {{ source('SALESFORCE_INI', 'ASSET') }} "INI_SRC"
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
			, "LOAD_INIT_DATA"."PARENTID" AS "PARENTID"
			, "LOAD_INIT_DATA"."ROOTASSETID" AS "ROOTASSETID"
			, "LOAD_INIT_DATA"."PRODUCT2ID" AS "PRODUCT2ID"
			, "LOAD_INIT_DATA"."CREATEDBYID" AS "CREATEDBYID"
			, "LOAD_INIT_DATA"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "LOAD_INIT_DATA"."OWNERID" AS "OWNERID"
			, "LOAD_INIT_DATA"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
			, "LOAD_INIT_DATA"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
			, "LOAD_INIT_DATA"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
			, "LOAD_INIT_DATA"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
			, "LOAD_INIT_DATA"."CREATEDDATE" AS "CREATEDDATE"
			, "LOAD_INIT_DATA"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "LOAD_INIT_DATA"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "LOAD_INIT_DATA"."ISDELETED" AS "ISDELETED"
			, "LOAD_INIT_DATA"."NAME" AS "NAME"
			, "LOAD_INIT_DATA"."SERIALNUMBER" AS "SERIALNUMBER"
			, "LOAD_INIT_DATA"."INSTALLDATE" AS "INSTALLDATE"
			, "LOAD_INIT_DATA"."PURCHASEDATE" AS "PURCHASEDATE"
			, "LOAD_INIT_DATA"."USAGEENDDATE" AS "USAGEENDDATE"
			, "LOAD_INIT_DATA"."STATUS" AS "STATUS"
			, "LOAD_INIT_DATA"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
			, "LOAD_INIT_DATA"."PRICE" AS "PRICE"
			, "LOAD_INIT_DATA"."QUANTITY" AS "QUANTITY"
			, "LOAD_INIT_DATA"."DESCRIPTION" AS "DESCRIPTION"
			, "LOAD_INIT_DATA"."ISINTERNAL" AS "ISINTERNAL"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CONTACTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PARENTID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ROOTASSETID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PRODUCT2ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "OWNERID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ASSETPROVIDEDBYID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ASSETSERVICEDBYID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ACCOUNTROLLUPID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISCOMPETITORPRODUCT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "CREATEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "LASTMODIFIEDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SYSTEMMODSTAMP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISDELETED"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "NAME"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "SERIALNUMBER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "INSTALLDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PURCHASEDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "USAGEENDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "STATUS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DIGITALASSETSTATUS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "PRICE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "QUANTITY"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "DESCRIPTION"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "ISINTERNAL"
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
			, "PREP_EXCEP"."PARENTID" AS "PARENTID"
			, "PREP_EXCEP"."ROOTASSETID" AS "ROOTASSETID"
			, "PREP_EXCEP"."PRODUCT2ID" AS "PRODUCT2ID"
			, "PREP_EXCEP"."CREATEDBYID" AS "CREATEDBYID"
			, "PREP_EXCEP"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "PREP_EXCEP"."OWNERID" AS "OWNERID"
			, "PREP_EXCEP"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
			, "PREP_EXCEP"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."ID"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."PARENTID"),'\#','\\' || '\#') AS "ID_FK_PARENTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."OWNERID"),'\#','\\' || '\#') AS "ID_FK_OWNERID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ACCOUNTID"),'\#','\\' || '\#') AS "ID_FK_ACCOUNTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."PRODUCT2ID"),'\#','\\' || '\#') AS "ID_FK_PRODUCT2ID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."LASTMODIFIEDBYID"),'\#','\\' || '\#') AS "ID_FK_LASTMODIFIEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ASSETPROVIDEDBYID"),'\#','\\' || '\#') AS "ID_FK_ASSETPROVIDEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ROOTASSETID"),'\#','\\' || '\#') AS "ID_FK_ROOTASSETID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CONTACTID"),'\#','\\' || '\#') AS "ID_FK_CONTACTID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."CREATEDBYID"),'\#','\\' || '\#') AS "ID_FK_CREATEDBYID_BK"
			, REPLACE(TRIM("PREP_EXCEP"."ASSETSERVICEDBYID"),'\#','\\' || '\#') AS "ID_FK_ASSETSERVICEDBYID_BK"
			, "PREP_EXCEP"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
			, "PREP_EXCEP"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
			, "PREP_EXCEP"."CREATEDDATE" AS "CREATEDDATE"
			, "PREP_EXCEP"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "PREP_EXCEP"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "PREP_EXCEP"."ISDELETED" AS "ISDELETED"
			, "PREP_EXCEP"."NAME" AS "NAME"
			, "PREP_EXCEP"."SERIALNUMBER" AS "SERIALNUMBER"
			, "PREP_EXCEP"."INSTALLDATE" AS "INSTALLDATE"
			, "PREP_EXCEP"."PURCHASEDATE" AS "PURCHASEDATE"
			, "PREP_EXCEP"."USAGEENDDATE" AS "USAGEENDDATE"
			, "PREP_EXCEP"."STATUS" AS "STATUS"
			, "PREP_EXCEP"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
			, "PREP_EXCEP"."PRICE" AS "PRICE"
			, "PREP_EXCEP"."QUANTITY" AS "QUANTITY"
			, "PREP_EXCEP"."DESCRIPTION" AS "DESCRIPTION"
			, "PREP_EXCEP"."ISINTERNAL" AS "ISINTERNAL"
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
		, "CALCULATE_BK"."PARENTID" AS "PARENTID"
		, "CALCULATE_BK"."ROOTASSETID" AS "ROOTASSETID"
		, "CALCULATE_BK"."PRODUCT2ID" AS "PRODUCT2ID"
		, "CALCULATE_BK"."CREATEDBYID" AS "CREATEDBYID"
		, "CALCULATE_BK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "CALCULATE_BK"."OWNERID" AS "OWNERID"
		, "CALCULATE_BK"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
		, "CALCULATE_BK"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
		, "CALCULATE_BK"."ID_BK" AS "ID_BK"
		, "CALCULATE_BK"."ID_FK_PARENTID_BK" AS "ID_FK_PARENTID_BK"
		, "CALCULATE_BK"."ID_FK_OWNERID_BK" AS "ID_FK_OWNERID_BK"
		, "CALCULATE_BK"."ID_FK_ACCOUNTID_BK" AS "ID_FK_ACCOUNTID_BK"
		, "CALCULATE_BK"."ID_FK_PRODUCT2ID_BK" AS "ID_FK_PRODUCT2ID_BK"
		, "CALCULATE_BK"."ID_FK_LASTMODIFIEDBYID_BK" AS "ID_FK_LASTMODIFIEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_ASSETPROVIDEDBYID_BK" AS "ID_FK_ASSETPROVIDEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_ROOTASSETID_BK" AS "ID_FK_ROOTASSETID_BK"
		, "CALCULATE_BK"."ID_FK_CONTACTID_BK" AS "ID_FK_CONTACTID_BK"
		, "CALCULATE_BK"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "CALCULATE_BK"."ID_FK_ASSETSERVICEDBYID_BK" AS "ID_FK_ASSETSERVICEDBYID_BK"
		, "CALCULATE_BK"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
		, "CALCULATE_BK"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
		, "CALCULATE_BK"."CREATEDDATE" AS "CREATEDDATE"
		, "CALCULATE_BK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "CALCULATE_BK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "CALCULATE_BK"."ISDELETED" AS "ISDELETED"
		, "CALCULATE_BK"."NAME" AS "NAME"
		, "CALCULATE_BK"."SERIALNUMBER" AS "SERIALNUMBER"
		, "CALCULATE_BK"."INSTALLDATE" AS "INSTALLDATE"
		, "CALCULATE_BK"."PURCHASEDATE" AS "PURCHASEDATE"
		, "CALCULATE_BK"."USAGEENDDATE" AS "USAGEENDDATE"
		, "CALCULATE_BK"."STATUS" AS "STATUS"
		, "CALCULATE_BK"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
		, "CALCULATE_BK"."PRICE" AS "PRICE"
		, "CALCULATE_BK"."QUANTITY" AS "QUANTITY"
		, "CALCULATE_BK"."DESCRIPTION" AS "DESCRIPTION"
		, "CALCULATE_BK"."ISINTERNAL" AS "ISINTERNAL"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'