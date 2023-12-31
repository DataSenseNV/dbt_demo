{{
	config(
		materialized='view',
		alias='VW_F4105',
		schema='JDEDWARDS_DFV',
		tags=['view', 'JDEDWARDS', 'SRC_JDE_ITEMCOST_TDFV_INCR']
	)
}}
	WITH "DELTA_WINDOW" AS 
	( 
		SELECT 
			  "LWT_SRC"."FMC_BEGIN_LW_TIMESTAMP" AS "FMC_BEGIN_LW_TIMESTAMP"
			, "LWT_SRC"."FMC_END_LW_TIMESTAMP" AS "FMC_END_LW_TIMESTAMP"
		FROM {{ source('JDEDWARDS_MTD', 'FMC_LOADING_WINDOW_TABLE') }} "LWT_SRC"
	)
	, "DELTA_VIEW_FILTER" AS 
	( 
		SELECT 
			  "CDC_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "CDC_SRC"."JRN_FLAG" AS "JRN_FLAG"
			, TO_CHAR('S' ) AS "RECORD_TYPE"
			, "CDC_SRC"."COITM" AS "COITM"
			, "CDC_SRC"."COMCU" AS "COMCU"
			, "CDC_SRC"."COLOCN" AS "COLOCN"
			, "CDC_SRC"."COLOTN" AS "COLOTN"
			, "CDC_SRC"."COLEDG" AS "COLEDG"
			, "CDC_SRC"."COLITM" AS "COLITM"
			, "CDC_SRC"."COAITM" AS "COAITM"
			, "CDC_SRC"."COLOTG" AS "COLOTG"
			, "CDC_SRC"."COUNCS" AS "COUNCS"
			, "CDC_SRC"."COCSPO" AS "COCSPO"
			, "CDC_SRC"."COCSIN" AS "COCSIN"
			, "CDC_SRC"."COURCD" AS "COURCD"
			, "CDC_SRC"."COURDT" AS "COURDT"
			, "CDC_SRC"."COURAT" AS "COURAT"
			, "CDC_SRC"."COURAB" AS "COURAB"
			, "CDC_SRC"."COURRF" AS "COURRF"
			, "CDC_SRC"."COUSER" AS "COUSER"
			, "CDC_SRC"."COPID" AS "COPID"
			, "CDC_SRC"."COJOBN" AS "COJOBN"
			, "CDC_SRC"."COUPMJ" AS "COUPMJ"
			, "CDC_SRC"."COTDAY" AS "COTDAY"
			, "CDC_SRC"."COCCFL" AS "COCCFL"
			, "CDC_SRC"."COCRCS" AS "COCRCS"
			, "CDC_SRC"."COOSTC" AS "COOSTC"
			, "CDC_SRC"."COSTOC" AS "COSTOC"
		FROM {{ source('JDEDWARDS_CDC', 'CDC_F4105') }} "CDC_SRC"
		INNER JOIN "DELTA_WINDOW" "DELTA_WINDOW" ON  1 = 1
		WHERE  "CDC_SRC"."CDC_TIMESTAMP" > "DELTA_WINDOW"."FMC_BEGIN_LW_TIMESTAMP" AND "CDC_SRC"."CDC_TIMESTAMP" <= "DELTA_WINDOW"."FMC_END_LW_TIMESTAMP"
	)
	, "DELTA_VIEW" AS 
	( 
		SELECT 
			  "DELTA_VIEW_FILTER"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW_FILTER"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW_FILTER"."RECORD_TYPE" AS "RECORD_TYPE"
			, "DELTA_VIEW_FILTER"."COITM" AS "COITM"
			, "DELTA_VIEW_FILTER"."COMCU" AS "COMCU"
			, "DELTA_VIEW_FILTER"."COLOCN" AS "COLOCN"
			, "DELTA_VIEW_FILTER"."COLOTN" AS "COLOTN"
			, "DELTA_VIEW_FILTER"."COLEDG" AS "COLEDG"
			, "DELTA_VIEW_FILTER"."COLITM" AS "COLITM"
			, "DELTA_VIEW_FILTER"."COAITM" AS "COAITM"
			, "DELTA_VIEW_FILTER"."COLOTG" AS "COLOTG"
			, "DELTA_VIEW_FILTER"."COUNCS" AS "COUNCS"
			, "DELTA_VIEW_FILTER"."COCSPO" AS "COCSPO"
			, "DELTA_VIEW_FILTER"."COCSIN" AS "COCSIN"
			, "DELTA_VIEW_FILTER"."COURCD" AS "COURCD"
			, "DELTA_VIEW_FILTER"."COURDT" AS "COURDT"
			, "DELTA_VIEW_FILTER"."COURAT" AS "COURAT"
			, "DELTA_VIEW_FILTER"."COURAB" AS "COURAB"
			, "DELTA_VIEW_FILTER"."COURRF" AS "COURRF"
			, "DELTA_VIEW_FILTER"."COUSER" AS "COUSER"
			, "DELTA_VIEW_FILTER"."COPID" AS "COPID"
			, "DELTA_VIEW_FILTER"."COJOBN" AS "COJOBN"
			, "DELTA_VIEW_FILTER"."COUPMJ" AS "COUPMJ"
			, "DELTA_VIEW_FILTER"."COTDAY" AS "COTDAY"
			, "DELTA_VIEW_FILTER"."COCCFL" AS "COCCFL"
			, "DELTA_VIEW_FILTER"."COCRCS" AS "COCRCS"
			, "DELTA_VIEW_FILTER"."COOSTC" AS "COOSTC"
			, "DELTA_VIEW_FILTER"."COSTOC" AS "COSTOC"
		FROM "DELTA_VIEW_FILTER" "DELTA_VIEW_FILTER"
	)
	, "PREPJOINBK" AS 
	( 
		SELECT 
			  "DELTA_VIEW"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW"."RECORD_TYPE" AS "RECORD_TYPE"
			, COALESCE("DELTA_VIEW"."COITM", CAST("MEX_BK_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "COITM"
			, COALESCE("DELTA_VIEW"."COMCU","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "COMCU"
			, COALESCE("DELTA_VIEW"."COLOCN","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "COLOCN"
			, COALESCE("DELTA_VIEW"."COLOTN","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "COLOTN"
			, COALESCE("DELTA_VIEW"."COLEDG","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "COLEDG"
			, "DELTA_VIEW"."COLITM" AS "COLITM"
			, "DELTA_VIEW"."COAITM" AS "COAITM"
			, "DELTA_VIEW"."COLOTG" AS "COLOTG"
			, "DELTA_VIEW"."COUNCS" AS "COUNCS"
			, "DELTA_VIEW"."COCSPO" AS "COCSPO"
			, "DELTA_VIEW"."COCSIN" AS "COCSIN"
			, "DELTA_VIEW"."COURCD" AS "COURCD"
			, "DELTA_VIEW"."COURDT" AS "COURDT"
			, "DELTA_VIEW"."COURAT" AS "COURAT"
			, "DELTA_VIEW"."COURAB" AS "COURAB"
			, "DELTA_VIEW"."COURRF" AS "COURRF"
			, "DELTA_VIEW"."COUSER" AS "COUSER"
			, "DELTA_VIEW"."COPID" AS "COPID"
			, "DELTA_VIEW"."COJOBN" AS "COJOBN"
			, "DELTA_VIEW"."COUPMJ" AS "COUPMJ"
			, "DELTA_VIEW"."COTDAY" AS "COTDAY"
			, "DELTA_VIEW"."COCCFL" AS "COCCFL"
			, "DELTA_VIEW"."COCRCS" AS "COCRCS"
			, "DELTA_VIEW"."COOSTC" AS "COOSTC"
			, "DELTA_VIEW"."COSTOC" AS "COSTOC"
		FROM "DELTA_VIEW" "DELTA_VIEW"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_BK_SRC" ON  1 = 1
		WHERE  "MEX_BK_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "PREPJOINBK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "PREPJOINBK"."JRN_FLAG" AS "JRN_FLAG"
		, "PREPJOINBK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "PREPJOINBK"."COITM" AS "COITM"
		, "PREPJOINBK"."COMCU" AS "COMCU"
		, "PREPJOINBK"."COLOCN" AS "COLOCN"
		, "PREPJOINBK"."COLOTN" AS "COLOTN"
		, "PREPJOINBK"."COLEDG" AS "COLEDG"
		, "PREPJOINBK"."COLITM" AS "COLITM"
		, "PREPJOINBK"."COAITM" AS "COAITM"
		, "PREPJOINBK"."COLOTG" AS "COLOTG"
		, "PREPJOINBK"."COUNCS" AS "COUNCS"
		, "PREPJOINBK"."COCSPO" AS "COCSPO"
		, "PREPJOINBK"."COCSIN" AS "COCSIN"
		, "PREPJOINBK"."COURCD" AS "COURCD"
		, "PREPJOINBK"."COURDT" AS "COURDT"
		, "PREPJOINBK"."COURAT" AS "COURAT"
		, "PREPJOINBK"."COURAB" AS "COURAB"
		, "PREPJOINBK"."COURRF" AS "COURRF"
		, "PREPJOINBK"."COUSER" AS "COUSER"
		, "PREPJOINBK"."COPID" AS "COPID"
		, "PREPJOINBK"."COJOBN" AS "COJOBN"
		, "PREPJOINBK"."COUPMJ" AS "COUPMJ"
		, "PREPJOINBK"."COTDAY" AS "COTDAY"
		, "PREPJOINBK"."COCCFL" AS "COCCFL"
		, "PREPJOINBK"."COCRCS" AS "COCRCS"
		, "PREPJOINBK"."COOSTC" AS "COOSTC"
		, "PREPJOINBK"."COSTOC" AS "COSTOC"
	FROM "PREPJOINBK" "PREPJOINBK"
