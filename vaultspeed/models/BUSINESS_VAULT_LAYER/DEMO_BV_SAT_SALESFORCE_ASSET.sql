{{
	config(
		materialized='view',
		alias='SAT_SALESFORCE_ASSET',
		schema='DEMO_BV',
		tags=['view', 'BV', 'BV_SAT_SALESFORCE_ASSET_BUSINESS_VIEW']
	)
}}
	SELECT 
		  "DVT_SRC"."ASSET_HKEY" AS "ASSET_HKEY"
		, "DVT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "DVT_SRC"."LOAD_END_DATE" AS "LOAD_END_DATE"
		, "DVT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "DVT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "DVT_SRC"."ID" AS "ID"
		, "DVT_SRC"."ASSETSERVICEDBYID" AS "ASSETSERVICEDBYID"
		, "DVT_SRC"."CREATEDBYID" AS "CREATEDBYID"
		, "DVT_SRC"."CONTACTID" AS "CONTACTID"
		, "DVT_SRC"."ROOTASSETID" AS "ROOTASSETID"
		, "DVT_SRC"."ASSETPROVIDEDBYID" AS "ASSETPROVIDEDBYID"
		, "DVT_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "DVT_SRC"."PRODUCT2ID" AS "PRODUCT2ID"
		, "DVT_SRC"."ACCOUNTID" AS "ACCOUNTID"
		, "DVT_SRC"."OWNERID" AS "OWNERID"
		, "DVT_SRC"."PARENTID" AS "PARENTID"
		, "DVT_SRC"."HASH_DIFF" AS "HASH_DIFF"
		, "DVT_SRC"."ACCOUNTROLLUPID" AS "ACCOUNTROLLUPID"
		, "DVT_SRC"."ISCOMPETITORPRODUCT" AS "ISCOMPETITORPRODUCT"
		, "DVT_SRC"."CREATEDDATE" AS "CREATEDDATE"
		, "DVT_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "DVT_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "DVT_SRC"."ISDELETED" AS "ISDELETED"
		, "DVT_SRC"."NAME" AS "NAME"
		, "DVT_SRC"."SERIALNUMBER" AS "SERIALNUMBER"
		, "DVT_SRC"."INSTALLDATE" AS "INSTALLDATE"
		, "DVT_SRC"."PURCHASEDATE" AS "PURCHASEDATE"
		, "DVT_SRC"."USAGEENDDATE" AS "USAGEENDDATE"
		, "DVT_SRC"."STATUS" AS "STATUS"
		, "DVT_SRC"."DIGITALASSETSTATUS" AS "DIGITALASSETSTATUS"
		, "DVT_SRC"."PRICE" AS "PRICE"
		, "DVT_SRC"."QUANTITY" AS "QUANTITY"
		, "DVT_SRC"."DESCRIPTION" AS "DESCRIPTION"
		, "DVT_SRC"."ISINTERNAL" AS "ISINTERNAL"
		, "DVT_SRC"."DELETE_FLAG" AS "DELETE_FLAG"
	FROM {{ ref('DEMO_FL_SAT_SALESFORCE_ASSET') }} "DVT_SRC"
