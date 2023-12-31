{{
	config(
		materialized='view',
		alias='VW_F4104',
		schema='JDEDWARDS_DFV',
		tags=['view', 'JDEDWARDS', 'SRC_JDE_ITEMCROSSREFERENCE_TDFV_INCR']
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
			, "CDC_SRC"."IVAN8" AS "IVAN8"
			, "CDC_SRC"."IVXRT" AS "IVXRT"
			, "CDC_SRC"."IVITM" AS "IVITM"
			, "CDC_SRC"."IVEXDJ" AS "IVEXDJ"
			, "CDC_SRC"."IVCITM" AS "IVCITM"
			, "CDC_SRC"."IVCIRV" AS "IVCIRV"
			, "CDC_SRC"."IVMCU" AS "IVMCU"
			, "CDC_SRC"."IVEFTJ" AS "IVEFTJ"
			, "CDC_SRC"."IVDSC1" AS "IVDSC1"
			, "CDC_SRC"."IVDSC2" AS "IVDSC2"
			, "CDC_SRC"."IVALN" AS "IVALN"
			, "CDC_SRC"."IVLITM" AS "IVLITM"
			, "CDC_SRC"."IVAITM" AS "IVAITM"
			, "CDC_SRC"."IVURCD" AS "IVURCD"
			, "CDC_SRC"."IVURDT" AS "IVURDT"
			, "CDC_SRC"."IVURAT" AS "IVURAT"
			, "CDC_SRC"."IVURAB" AS "IVURAB"
			, "CDC_SRC"."IVURRF" AS "IVURRF"
			, "CDC_SRC"."IVUSER" AS "IVUSER"
			, "CDC_SRC"."IVPID" AS "IVPID"
			, "CDC_SRC"."IVJOBN" AS "IVJOBN"
			, "CDC_SRC"."IVUPMJ" AS "IVUPMJ"
			, "CDC_SRC"."IVTDAY" AS "IVTDAY"
			, "CDC_SRC"."IVRATO" AS "IVRATO"
			, "CDC_SRC"."IVAPSP" AS "IVAPSP"
			, "CDC_SRC"."IVIDEM" AS "IVIDEM"
			, "CDC_SRC"."IVAPSS" AS "IVAPSS"
			, "CDC_SRC"."IVADIND" AS "IVADIND"
			, "CDC_SRC"."IVBPIND" AS "IVBPIND"
			, "CDC_SRC"."IVCARDNO" AS "IVCARDNO"
		FROM {{ source('JDEDWARDS_CDC', 'CDC_F4104') }} "CDC_SRC"
		INNER JOIN "DELTA_WINDOW" "DELTA_WINDOW" ON  1 = 1
		WHERE  "CDC_SRC"."CDC_TIMESTAMP" > "DELTA_WINDOW"."FMC_BEGIN_LW_TIMESTAMP" AND "CDC_SRC"."CDC_TIMESTAMP" <= "DELTA_WINDOW"."FMC_END_LW_TIMESTAMP"
	)
	, "DELTA_VIEW" AS 
	( 
		SELECT 
			  "DELTA_VIEW_FILTER"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW_FILTER"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW_FILTER"."RECORD_TYPE" AS "RECORD_TYPE"
			, "DELTA_VIEW_FILTER"."IVAN8" AS "IVAN8"
			, "DELTA_VIEW_FILTER"."IVXRT" AS "IVXRT"
			, "DELTA_VIEW_FILTER"."IVITM" AS "IVITM"
			, "DELTA_VIEW_FILTER"."IVEXDJ" AS "IVEXDJ"
			, "DELTA_VIEW_FILTER"."IVCITM" AS "IVCITM"
			, "DELTA_VIEW_FILTER"."IVCIRV" AS "IVCIRV"
			, "DELTA_VIEW_FILTER"."IVMCU" AS "IVMCU"
			, "DELTA_VIEW_FILTER"."IVEFTJ" AS "IVEFTJ"
			, "DELTA_VIEW_FILTER"."IVDSC1" AS "IVDSC1"
			, "DELTA_VIEW_FILTER"."IVDSC2" AS "IVDSC2"
			, "DELTA_VIEW_FILTER"."IVALN" AS "IVALN"
			, "DELTA_VIEW_FILTER"."IVLITM" AS "IVLITM"
			, "DELTA_VIEW_FILTER"."IVAITM" AS "IVAITM"
			, "DELTA_VIEW_FILTER"."IVURCD" AS "IVURCD"
			, "DELTA_VIEW_FILTER"."IVURDT" AS "IVURDT"
			, "DELTA_VIEW_FILTER"."IVURAT" AS "IVURAT"
			, "DELTA_VIEW_FILTER"."IVURAB" AS "IVURAB"
			, "DELTA_VIEW_FILTER"."IVURRF" AS "IVURRF"
			, "DELTA_VIEW_FILTER"."IVUSER" AS "IVUSER"
			, "DELTA_VIEW_FILTER"."IVPID" AS "IVPID"
			, "DELTA_VIEW_FILTER"."IVJOBN" AS "IVJOBN"
			, "DELTA_VIEW_FILTER"."IVUPMJ" AS "IVUPMJ"
			, "DELTA_VIEW_FILTER"."IVTDAY" AS "IVTDAY"
			, "DELTA_VIEW_FILTER"."IVRATO" AS "IVRATO"
			, "DELTA_VIEW_FILTER"."IVAPSP" AS "IVAPSP"
			, "DELTA_VIEW_FILTER"."IVIDEM" AS "IVIDEM"
			, "DELTA_VIEW_FILTER"."IVAPSS" AS "IVAPSS"
			, "DELTA_VIEW_FILTER"."IVADIND" AS "IVADIND"
			, "DELTA_VIEW_FILTER"."IVBPIND" AS "IVBPIND"
			, "DELTA_VIEW_FILTER"."IVCARDNO" AS "IVCARDNO"
		FROM "DELTA_VIEW_FILTER" "DELTA_VIEW_FILTER"
	)
	, "PREPJOINBK" AS 
	( 
		SELECT 
			  "DELTA_VIEW"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW"."RECORD_TYPE" AS "RECORD_TYPE"
			, COALESCE("DELTA_VIEW"."IVAN8", CAST("MEX_BK_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "IVAN8"
			, COALESCE("DELTA_VIEW"."IVXRT","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "IVXRT"
			, COALESCE("DELTA_VIEW"."IVITM", CAST("MEX_BK_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "IVITM"
			, COALESCE("DELTA_VIEW"."IVEXDJ", CAST("MEX_BK_SRC"."KEY_ATTRIBUTE_NUMBER" AS NUMBER)) AS "IVEXDJ"
			, COALESCE("DELTA_VIEW"."IVCITM","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "IVCITM"
			, COALESCE("DELTA_VIEW"."IVCIRV","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "IVCIRV"
			, COALESCE("DELTA_VIEW"."IVMCU","MEX_BK_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "IVMCU"
			, "DELTA_VIEW"."IVEFTJ" AS "IVEFTJ"
			, "DELTA_VIEW"."IVDSC1" AS "IVDSC1"
			, "DELTA_VIEW"."IVDSC2" AS "IVDSC2"
			, "DELTA_VIEW"."IVALN" AS "IVALN"
			, "DELTA_VIEW"."IVLITM" AS "IVLITM"
			, "DELTA_VIEW"."IVAITM" AS "IVAITM"
			, "DELTA_VIEW"."IVURCD" AS "IVURCD"
			, "DELTA_VIEW"."IVURDT" AS "IVURDT"
			, "DELTA_VIEW"."IVURAT" AS "IVURAT"
			, "DELTA_VIEW"."IVURAB" AS "IVURAB"
			, "DELTA_VIEW"."IVURRF" AS "IVURRF"
			, "DELTA_VIEW"."IVUSER" AS "IVUSER"
			, "DELTA_VIEW"."IVPID" AS "IVPID"
			, "DELTA_VIEW"."IVJOBN" AS "IVJOBN"
			, "DELTA_VIEW"."IVUPMJ" AS "IVUPMJ"
			, "DELTA_VIEW"."IVTDAY" AS "IVTDAY"
			, "DELTA_VIEW"."IVRATO" AS "IVRATO"
			, "DELTA_VIEW"."IVAPSP" AS "IVAPSP"
			, "DELTA_VIEW"."IVIDEM" AS "IVIDEM"
			, "DELTA_VIEW"."IVAPSS" AS "IVAPSS"
			, "DELTA_VIEW"."IVADIND" AS "IVADIND"
			, "DELTA_VIEW"."IVBPIND" AS "IVBPIND"
			, "DELTA_VIEW"."IVCARDNO" AS "IVCARDNO"
		FROM "DELTA_VIEW" "DELTA_VIEW"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_BK_SRC" ON  1 = 1
		WHERE  "MEX_BK_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "PREPJOINBK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "PREPJOINBK"."JRN_FLAG" AS "JRN_FLAG"
		, "PREPJOINBK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "PREPJOINBK"."IVAN8" AS "IVAN8"
		, "PREPJOINBK"."IVXRT" AS "IVXRT"
		, "PREPJOINBK"."IVITM" AS "IVITM"
		, "PREPJOINBK"."IVEXDJ" AS "IVEXDJ"
		, "PREPJOINBK"."IVCITM" AS "IVCITM"
		, "PREPJOINBK"."IVCIRV" AS "IVCIRV"
		, "PREPJOINBK"."IVMCU" AS "IVMCU"
		, "PREPJOINBK"."IVEFTJ" AS "IVEFTJ"
		, "PREPJOINBK"."IVDSC1" AS "IVDSC1"
		, "PREPJOINBK"."IVDSC2" AS "IVDSC2"
		, "PREPJOINBK"."IVALN" AS "IVALN"
		, "PREPJOINBK"."IVLITM" AS "IVLITM"
		, "PREPJOINBK"."IVAITM" AS "IVAITM"
		, "PREPJOINBK"."IVURCD" AS "IVURCD"
		, "PREPJOINBK"."IVURDT" AS "IVURDT"
		, "PREPJOINBK"."IVURAT" AS "IVURAT"
		, "PREPJOINBK"."IVURAB" AS "IVURAB"
		, "PREPJOINBK"."IVURRF" AS "IVURRF"
		, "PREPJOINBK"."IVUSER" AS "IVUSER"
		, "PREPJOINBK"."IVPID" AS "IVPID"
		, "PREPJOINBK"."IVJOBN" AS "IVJOBN"
		, "PREPJOINBK"."IVUPMJ" AS "IVUPMJ"
		, "PREPJOINBK"."IVTDAY" AS "IVTDAY"
		, "PREPJOINBK"."IVRATO" AS "IVRATO"
		, "PREPJOINBK"."IVAPSP" AS "IVAPSP"
		, "PREPJOINBK"."IVIDEM" AS "IVIDEM"
		, "PREPJOINBK"."IVAPSS" AS "IVAPSS"
		, "PREPJOINBK"."IVADIND" AS "IVADIND"
		, "PREPJOINBK"."IVBPIND" AS "IVBPIND"
		, "PREPJOINBK"."IVCARDNO" AS "IVCARDNO"
	FROM "PREPJOINBK" "PREPJOINBK"
