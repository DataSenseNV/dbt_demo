{{
	config(
		materialized='view',
		alias='VW_OPPORTUNITY',
		schema='SALESFORCE_DFV',
		tags=['view', 'SALESFORCE', 'SRC_SALESFORCE_OPPORTUNITY_TDFV_INCR']
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
			, "CDC_SRC"."ACCOUNTID" AS "ACCOUNTID"
			, "CDC_SRC"."CAMPAIGNID" AS "CAMPAIGNID"
			, "CDC_SRC"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "CDC_SRC"."OWNERID" AS "OWNERID"
			, "CDC_SRC"."CREATEDBYID" AS "CREATEDBYID"
			, "CDC_SRC"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "CDC_SRC"."CONTRACTID" AS "CONTRACTID"
			, "CDC_SRC"."ISDELETED" AS "ISDELETED"
			, "CDC_SRC"."RECORDTYPEID" AS "RECORDTYPEID"
			, "CDC_SRC"."ISPRIVATE" AS "ISPRIVATE"
			, "CDC_SRC"."NAME" AS "NAME"
			, "CDC_SRC"."DESCRIPTION" AS "DESCRIPTION"
			, "CDC_SRC"."STAGENAME" AS "STAGENAME"
			, "CDC_SRC"."STAGESORTORDER" AS "STAGESORTORDER"
			, "CDC_SRC"."AMOUNT" AS "AMOUNT"
			, "CDC_SRC"."PROBABILITY" AS "PROBABILITY"
			, "CDC_SRC"."EXPECTEDREVENUE" AS "EXPECTEDREVENUE"
			, "CDC_SRC"."TOTALOPPORTUNITYQUANTITY" AS "TOTALOPPORTUNITYQUANTITY"
			, "CDC_SRC"."CLOSEDATE" AS "CLOSEDATE"
			, "CDC_SRC"."TYPE" AS "TYPE"
			, "CDC_SRC"."NEXTSTEP" AS "NEXTSTEP"
			, "CDC_SRC"."LEADSOURCE" AS "LEADSOURCE"
			, "CDC_SRC"."ISCLOSED" AS "ISCLOSED"
			, "CDC_SRC"."ISWON" AS "ISWON"
			, "CDC_SRC"."FORECASTCATEGORY" AS "FORECASTCATEGORY"
			, "CDC_SRC"."FORECASTCATEGORYNAME" AS "FORECASTCATEGORYNAME"
			, "CDC_SRC"."HASOPPORTUNITYLINEITEM" AS "HASOPPORTUNITYLINEITEM"
			, "CDC_SRC"."CREATEDDATE" AS "CREATEDDATE"
			, "CDC_SRC"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "CDC_SRC"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "CDC_SRC"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "CDC_SRC"."LASTSTAGECHANGEDATE" AS "LASTSTAGECHANGEDATE"
			, "CDC_SRC"."FISCALYEAR" AS "FISCALYEAR"
			, "CDC_SRC"."FISCALQUARTER" AS "FISCALQUARTER"
			, "CDC_SRC"."CONTACTID" AS "CONTACTID"
			, "CDC_SRC"."PRIMARYPARTNERACCOUNTID" AS "PRIMARYPARTNERACCOUNTID"
			, "CDC_SRC"."OPPORTUNITY_SOURCE__C" AS "OPPORTUNITY_SOURCE__C"
		FROM {{ source('SALESFORCE_CDC', 'CDC_OPPORTUNITY') }} "CDC_SRC"
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
			, "DELTA_VIEW_FILTER"."ACCOUNTID" AS "ACCOUNTID"
			, "DELTA_VIEW_FILTER"."CAMPAIGNID" AS "CAMPAIGNID"
			, "DELTA_VIEW_FILTER"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
			, "DELTA_VIEW_FILTER"."OWNERID" AS "OWNERID"
			, "DELTA_VIEW_FILTER"."CREATEDBYID" AS "CREATEDBYID"
			, "DELTA_VIEW_FILTER"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
			, "DELTA_VIEW_FILTER"."CONTRACTID" AS "CONTRACTID"
			, "DELTA_VIEW_FILTER"."ISDELETED" AS "ISDELETED"
			, "DELTA_VIEW_FILTER"."RECORDTYPEID" AS "RECORDTYPEID"
			, "DELTA_VIEW_FILTER"."ISPRIVATE" AS "ISPRIVATE"
			, "DELTA_VIEW_FILTER"."NAME" AS "NAME"
			, "DELTA_VIEW_FILTER"."DESCRIPTION" AS "DESCRIPTION"
			, "DELTA_VIEW_FILTER"."STAGENAME" AS "STAGENAME"
			, "DELTA_VIEW_FILTER"."STAGESORTORDER" AS "STAGESORTORDER"
			, "DELTA_VIEW_FILTER"."AMOUNT" AS "AMOUNT"
			, "DELTA_VIEW_FILTER"."PROBABILITY" AS "PROBABILITY"
			, "DELTA_VIEW_FILTER"."EXPECTEDREVENUE" AS "EXPECTEDREVENUE"
			, "DELTA_VIEW_FILTER"."TOTALOPPORTUNITYQUANTITY" AS "TOTALOPPORTUNITYQUANTITY"
			, "DELTA_VIEW_FILTER"."CLOSEDATE" AS "CLOSEDATE"
			, "DELTA_VIEW_FILTER"."TYPE" AS "TYPE"
			, "DELTA_VIEW_FILTER"."NEXTSTEP" AS "NEXTSTEP"
			, "DELTA_VIEW_FILTER"."LEADSOURCE" AS "LEADSOURCE"
			, "DELTA_VIEW_FILTER"."ISCLOSED" AS "ISCLOSED"
			, "DELTA_VIEW_FILTER"."ISWON" AS "ISWON"
			, "DELTA_VIEW_FILTER"."FORECASTCATEGORY" AS "FORECASTCATEGORY"
			, "DELTA_VIEW_FILTER"."FORECASTCATEGORYNAME" AS "FORECASTCATEGORYNAME"
			, "DELTA_VIEW_FILTER"."HASOPPORTUNITYLINEITEM" AS "HASOPPORTUNITYLINEITEM"
			, "DELTA_VIEW_FILTER"."CREATEDDATE" AS "CREATEDDATE"
			, "DELTA_VIEW_FILTER"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "DELTA_VIEW_FILTER"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "DELTA_VIEW_FILTER"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "DELTA_VIEW_FILTER"."LASTSTAGECHANGEDATE" AS "LASTSTAGECHANGEDATE"
			, "DELTA_VIEW_FILTER"."FISCALYEAR" AS "FISCALYEAR"
			, "DELTA_VIEW_FILTER"."FISCALQUARTER" AS "FISCALQUARTER"
			, "DELTA_VIEW_FILTER"."CONTACTID" AS "CONTACTID"
			, "DELTA_VIEW_FILTER"."PRIMARYPARTNERACCOUNTID" AS "PRIMARYPARTNERACCOUNTID"
			, "DELTA_VIEW_FILTER"."OPPORTUNITY_SOURCE__C" AS "OPPORTUNITY_SOURCE__C"
		FROM "DELTA_VIEW_FILTER" "DELTA_VIEW_FILTER"
	)
	, "PREPJOINBK" AS 
	( 
		SELECT 
			  "DELTA_VIEW"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW"."RECORD_TYPE" AS "RECORD_TYPE"
			, COALESCE("DELTA_VIEW"."ID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ID"
			, COALESCE("DELTA_VIEW"."LASTMODIFIEDBYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "LASTMODIFIEDBYID"
			, COALESCE("DELTA_VIEW"."ACCOUNTID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "ACCOUNTID"
			, COALESCE("DELTA_VIEW"."PRICEBOOK2ID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "PRICEBOOK2ID"
			, COALESCE("DELTA_VIEW"."OWNERID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "OWNERID"
			, COALESCE("DELTA_VIEW"."CREATEDBYID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "CREATEDBYID"
			, COALESCE("DELTA_VIEW"."CAMPAIGNID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "CAMPAIGNID"
			, COALESCE("DELTA_VIEW"."CONTRACTID","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "CONTRACTID"
			, "DELTA_VIEW"."ISDELETED" AS "ISDELETED"
			, "DELTA_VIEW"."RECORDTYPEID" AS "RECORDTYPEID"
			, "DELTA_VIEW"."ISPRIVATE" AS "ISPRIVATE"
			, "DELTA_VIEW"."NAME" AS "NAME"
			, "DELTA_VIEW"."DESCRIPTION" AS "DESCRIPTION"
			, "DELTA_VIEW"."STAGENAME" AS "STAGENAME"
			, "DELTA_VIEW"."STAGESORTORDER" AS "STAGESORTORDER"
			, "DELTA_VIEW"."AMOUNT" AS "AMOUNT"
			, "DELTA_VIEW"."PROBABILITY" AS "PROBABILITY"
			, "DELTA_VIEW"."EXPECTEDREVENUE" AS "EXPECTEDREVENUE"
			, "DELTA_VIEW"."TOTALOPPORTUNITYQUANTITY" AS "TOTALOPPORTUNITYQUANTITY"
			, "DELTA_VIEW"."CLOSEDATE" AS "CLOSEDATE"
			, "DELTA_VIEW"."TYPE" AS "TYPE"
			, "DELTA_VIEW"."NEXTSTEP" AS "NEXTSTEP"
			, "DELTA_VIEW"."LEADSOURCE" AS "LEADSOURCE"
			, "DELTA_VIEW"."ISCLOSED" AS "ISCLOSED"
			, "DELTA_VIEW"."ISWON" AS "ISWON"
			, "DELTA_VIEW"."FORECASTCATEGORY" AS "FORECASTCATEGORY"
			, "DELTA_VIEW"."FORECASTCATEGORYNAME" AS "FORECASTCATEGORYNAME"
			, "DELTA_VIEW"."HASOPPORTUNITYLINEITEM" AS "HASOPPORTUNITYLINEITEM"
			, "DELTA_VIEW"."CREATEDDATE" AS "CREATEDDATE"
			, "DELTA_VIEW"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
			, "DELTA_VIEW"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
			, "DELTA_VIEW"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
			, "DELTA_VIEW"."LASTSTAGECHANGEDATE" AS "LASTSTAGECHANGEDATE"
			, "DELTA_VIEW"."FISCALYEAR" AS "FISCALYEAR"
			, "DELTA_VIEW"."FISCALQUARTER" AS "FISCALQUARTER"
			, "DELTA_VIEW"."CONTACTID" AS "CONTACTID"
			, "DELTA_VIEW"."PRIMARYPARTNERACCOUNTID" AS "PRIMARYPARTNERACCOUNTID"
			, "DELTA_VIEW"."OPPORTUNITY_SOURCE__C" AS "OPPORTUNITY_SOURCE__C"
		FROM "DELTA_VIEW" "DELTA_VIEW"
		INNER JOIN {{ source('SALESFORCE_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_BK_SRC" ON  1 = 1
		WHERE  "MEX_BK_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "PREPJOINBK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "PREPJOINBK"."JRN_FLAG" AS "JRN_FLAG"
		, "PREPJOINBK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "PREPJOINBK"."ID" AS "ID"
		, "PREPJOINBK"."ACCOUNTID" AS "ACCOUNTID"
		, "PREPJOINBK"."CAMPAIGNID" AS "CAMPAIGNID"
		, "PREPJOINBK"."PRICEBOOK2ID" AS "PRICEBOOK2ID"
		, "PREPJOINBK"."OWNERID" AS "OWNERID"
		, "PREPJOINBK"."CREATEDBYID" AS "CREATEDBYID"
		, "PREPJOINBK"."LASTMODIFIEDBYID" AS "LASTMODIFIEDBYID"
		, "PREPJOINBK"."CONTRACTID" AS "CONTRACTID"
		, "PREPJOINBK"."ISDELETED" AS "ISDELETED"
		, "PREPJOINBK"."RECORDTYPEID" AS "RECORDTYPEID"
		, "PREPJOINBK"."ISPRIVATE" AS "ISPRIVATE"
		, "PREPJOINBK"."NAME" AS "NAME"
		, "PREPJOINBK"."DESCRIPTION" AS "DESCRIPTION"
		, "PREPJOINBK"."STAGENAME" AS "STAGENAME"
		, "PREPJOINBK"."STAGESORTORDER" AS "STAGESORTORDER"
		, "PREPJOINBK"."AMOUNT" AS "AMOUNT"
		, "PREPJOINBK"."PROBABILITY" AS "PROBABILITY"
		, "PREPJOINBK"."EXPECTEDREVENUE" AS "EXPECTEDREVENUE"
		, "PREPJOINBK"."TOTALOPPORTUNITYQUANTITY" AS "TOTALOPPORTUNITYQUANTITY"
		, "PREPJOINBK"."CLOSEDATE" AS "CLOSEDATE"
		, "PREPJOINBK"."TYPE" AS "TYPE"
		, "PREPJOINBK"."NEXTSTEP" AS "NEXTSTEP"
		, "PREPJOINBK"."LEADSOURCE" AS "LEADSOURCE"
		, "PREPJOINBK"."ISCLOSED" AS "ISCLOSED"
		, "PREPJOINBK"."ISWON" AS "ISWON"
		, "PREPJOINBK"."FORECASTCATEGORY" AS "FORECASTCATEGORY"
		, "PREPJOINBK"."FORECASTCATEGORYNAME" AS "FORECASTCATEGORYNAME"
		, "PREPJOINBK"."HASOPPORTUNITYLINEITEM" AS "HASOPPORTUNITYLINEITEM"
		, "PREPJOINBK"."CREATEDDATE" AS "CREATEDDATE"
		, "PREPJOINBK"."LASTMODIFIEDDATE" AS "LASTMODIFIEDDATE"
		, "PREPJOINBK"."SYSTEMMODSTAMP" AS "SYSTEMMODSTAMP"
		, "PREPJOINBK"."LASTACTIVITYDATE" AS "LASTACTIVITYDATE"
		, "PREPJOINBK"."LASTSTAGECHANGEDATE" AS "LASTSTAGECHANGEDATE"
		, "PREPJOINBK"."FISCALYEAR" AS "FISCALYEAR"
		, "PREPJOINBK"."FISCALQUARTER" AS "FISCALQUARTER"
		, "PREPJOINBK"."CONTACTID" AS "CONTACTID"
		, "PREPJOINBK"."PRIMARYPARTNERACCOUNTID" AS "PRIMARYPARTNERACCOUNTID"
		, "PREPJOINBK"."OPPORTUNITY_SOURCE__C" AS "OPPORTUNITY_SOURCE__C"
	FROM "PREPJOINBK" "PREPJOINBK"
