{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "SALESFORCE" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='PRODUCT2',
		schema='SALESFORCE_STG',
		tags=['SALESFORCE', 'STG_SALESFORCE_PRODUCT2_INCR', 'STG_SALESFORCE_PRODUCT2_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' )) AS "PRODUCT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )
			) AS "LNK_PRODUCT_USER_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, 'SALESFORCE' AS "SRC_BK"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."ID_BK" AS "PRODUCT_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."PRODUCTCODE" AS "PRODUCTCODE"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."ISACTIVE" AS "ISACTIVE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."FAMILY" AS "FAMILY"
		, "EXT_SRC"."EXTERNALDATASOURCEID" AS "EXTERNALDATASOURCEID"
		, "EXT_SRC"."EXTERNALID" AS "EXTERNALID"
		, "EXT_SRC"."DISPLAYURL" AS "DISPLAYURL"
		, "EXT_SRC"."QUANTITYUNITOFMEASURE" AS "QUANTITYUNITOFMEASURE"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."ISARCHIVED" AS "ISARCHIVED"
		, "EXT_SRC"."STOCKKEEPINGUNIT" AS "STOCKKEEPINGUNIT"
	FROM {{ ref('SALESFORCE_EXT_PRODUCT2') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'SALESFORCE'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' )) AS "PRODUCT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )) AS "USER_HKEY"
		, UPPER(MD5_HEX( 'SALESFORCE' || '\#' || "EXT_SRC"."ID_BK" || '\#' || "EXT_SRC"."ID_FK_CREATEDBYID_BK" || '\#' )
			) AS "LNK_PRODUCT_USER_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, 'SALESFORCE' AS "SRC_BK"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ID" AS "ID"
		, "EXT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "EXT_SRC"."ID_BK" AS "PRODUCT_BK"
		, "EXT_SRC"."ID_FK_CREATEDBYID_BK" AS "ID_FK_CREATEDBYID_BK"
		, "EXT_SRC"."NAME" AS "NAME"
		, "EXT_SRC"."PRODUCTCODE" AS "PRODUCTCODE"
		, "EXT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "EXT_SRC"."ISACTIVE" AS "ISACTIVE"
		, "EXT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "EXT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "EXT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "EXT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "EXT_SRC"."FAMILY" AS "FAMILY"
		, "EXT_SRC"."EXTERNALDATASOURCEID" AS "EXTERNALDATASOURCEID"
		, "EXT_SRC"."EXTERNALID" AS "EXTERNALID"
		, "EXT_SRC"."DISPLAYURL" AS "DISPLAYURL"
		, "EXT_SRC"."QUANTITYUNITOFMEASURE" AS "QUANTITYUNITOFMEASURE"
		, "EXT_SRC"."ISDELETED" AS "ISDELETED"
		, "EXT_SRC"."ISARCHIVED" AS "ISARCHIVED"
		, "EXT_SRC"."STOCKKEEPINGUNIT" AS "STOCKKEEPINGUNIT"
	FROM {{ ref('SALESFORCE_EXT_PRODUCT2') }} "EXT_SRC"
	INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'SALESFORCE'