{{
	config(
		materialized='view',
		alias='VW_F0111',
		schema='JDEDWARDS_DFV',
		tags=['view', 'JDEDWARDS', 'SRC_JDE_WHOISWHO_TDFV_INCR']
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
			, "CDC_SRC"."WWAN8" AS "WWAN8"
			, "CDC_SRC"."WWIDLN" AS "WWIDLN"
			, "CDC_SRC"."WWDSS5" AS "WWDSS5"
			, "CDC_SRC"."WWMLNM" AS "WWMLNM"
			, "CDC_SRC"."WWATTL" AS "WWATTL"
			, "CDC_SRC"."WWREM1" AS "WWREM1"
			, "CDC_SRC"."WWSLNM" AS "WWSLNM"
			, "CDC_SRC"."WWALPH" AS "WWALPH"
			, "CDC_SRC"."WWDC" AS "WWDC"
			, "CDC_SRC"."WWGNNM" AS "WWGNNM"
			, "CDC_SRC"."WWMDNM" AS "WWMDNM"
			, "CDC_SRC"."WWSRNM" AS "WWSRNM"
			, "CDC_SRC"."WWTYC" AS "WWTYC"
			, "CDC_SRC"."WWW001" AS "WWW001"
			, "CDC_SRC"."WWW002" AS "WWW002"
			, "CDC_SRC"."WWW003" AS "WWW003"
			, "CDC_SRC"."WWW004" AS "WWW004"
			, "CDC_SRC"."WWW005" AS "WWW005"
			, "CDC_SRC"."WWW006" AS "WWW006"
			, "CDC_SRC"."WWW007" AS "WWW007"
			, "CDC_SRC"."WWW008" AS "WWW008"
			, "CDC_SRC"."WWW009" AS "WWW009"
			, "CDC_SRC"."WWW010" AS "WWW010"
			, "CDC_SRC"."WWMLN1" AS "WWMLN1"
			, "CDC_SRC"."WWALP1" AS "WWALP1"
			, "CDC_SRC"."WWUSER" AS "WWUSER"
			, "CDC_SRC"."WWPID" AS "WWPID"
			, "CDC_SRC"."WWUPMJ" AS "WWUPMJ"
			, "CDC_SRC"."WWJOBN" AS "WWJOBN"
			, "CDC_SRC"."WWUPMT" AS "WWUPMT"
			, "CDC_SRC"."WWNTYP" AS "WWNTYP"
			, "CDC_SRC"."WWNICK" AS "WWNICK"
			, "CDC_SRC"."WWGEND" AS "WWGEND"
			, "CDC_SRC"."WWDDATE" AS "WWDDATE"
			, "CDC_SRC"."WWDMON" AS "WWDMON"
			, "CDC_SRC"."WWDYR" AS "WWDYR"
			, "CDC_SRC"."WWWN001" AS "WWWN001"
			, "CDC_SRC"."WWWN002" AS "WWWN002"
			, "CDC_SRC"."WWWN003" AS "WWWN003"
			, "CDC_SRC"."WWWN004" AS "WWWN004"
			, "CDC_SRC"."WWWN005" AS "WWWN005"
			, "CDC_SRC"."WWWN006" AS "WWWN006"
			, "CDC_SRC"."WWWN007" AS "WWWN007"
			, "CDC_SRC"."WWWN008" AS "WWWN008"
			, "CDC_SRC"."WWWN009" AS "WWWN009"
			, "CDC_SRC"."WWWN010" AS "WWWN010"
			, "CDC_SRC"."WWFUCO" AS "WWFUCO"
			, "CDC_SRC"."WWPCM" AS "WWPCM"
			, "CDC_SRC"."WWPCF" AS "WWPCF"
			, "CDC_SRC"."WWACTIN" AS "WWACTIN"
			, "CDC_SRC"."WWCFRGUID" AS "WWCFRGUID"
			, "CDC_SRC"."WWSYNCS" AS "WWSYNCS"
			, "CDC_SRC"."WWCAAD" AS "WWCAAD"
		FROM {{ source('JDEDWARDS_CDC', 'CDC_F0111') }} "CDC_SRC"
		INNER JOIN "DELTA_WINDOW" "DELTA_WINDOW" ON  1 = 1
		WHERE  "CDC_SRC"."CDC_TIMESTAMP" > "DELTA_WINDOW"."FMC_BEGIN_LW_TIMESTAMP" AND "CDC_SRC"."CDC_TIMESTAMP" <= "DELTA_WINDOW"."FMC_END_LW_TIMESTAMP"
	)
	, "DELTA_VIEW" AS 
	( 
		SELECT 
			  "DELTA_VIEW_FILTER"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW_FILTER"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW_FILTER"."RECORD_TYPE" AS "RECORD_TYPE"
			, "DELTA_VIEW_FILTER"."WWAN8" AS "WWAN8"
			, "DELTA_VIEW_FILTER"."WWIDLN" AS "WWIDLN"
			, "DELTA_VIEW_FILTER"."WWDSS5" AS "WWDSS5"
			, "DELTA_VIEW_FILTER"."WWMLNM" AS "WWMLNM"
			, "DELTA_VIEW_FILTER"."WWATTL" AS "WWATTL"
			, "DELTA_VIEW_FILTER"."WWREM1" AS "WWREM1"
			, "DELTA_VIEW_FILTER"."WWSLNM" AS "WWSLNM"
			, "DELTA_VIEW_FILTER"."WWALPH" AS "WWALPH"
			, "DELTA_VIEW_FILTER"."WWDC" AS "WWDC"
			, "DELTA_VIEW_FILTER"."WWGNNM" AS "WWGNNM"
			, "DELTA_VIEW_FILTER"."WWMDNM" AS "WWMDNM"
			, "DELTA_VIEW_FILTER"."WWSRNM" AS "WWSRNM"
			, "DELTA_VIEW_FILTER"."WWTYC" AS "WWTYC"
			, "DELTA_VIEW_FILTER"."WWW001" AS "WWW001"
			, "DELTA_VIEW_FILTER"."WWW002" AS "WWW002"
			, "DELTA_VIEW_FILTER"."WWW003" AS "WWW003"
			, "DELTA_VIEW_FILTER"."WWW004" AS "WWW004"
			, "DELTA_VIEW_FILTER"."WWW005" AS "WWW005"
			, "DELTA_VIEW_FILTER"."WWW006" AS "WWW006"
			, "DELTA_VIEW_FILTER"."WWW007" AS "WWW007"
			, "DELTA_VIEW_FILTER"."WWW008" AS "WWW008"
			, "DELTA_VIEW_FILTER"."WWW009" AS "WWW009"
			, "DELTA_VIEW_FILTER"."WWW010" AS "WWW010"
			, "DELTA_VIEW_FILTER"."WWMLN1" AS "WWMLN1"
			, "DELTA_VIEW_FILTER"."WWALP1" AS "WWALP1"
			, "DELTA_VIEW_FILTER"."WWUSER" AS "WWUSER"
			, "DELTA_VIEW_FILTER"."WWPID" AS "WWPID"
			, "DELTA_VIEW_FILTER"."WWUPMJ" AS "WWUPMJ"
			, "DELTA_VIEW_FILTER"."WWJOBN" AS "WWJOBN"
			, "DELTA_VIEW_FILTER"."WWUPMT" AS "WWUPMT"
			, "DELTA_VIEW_FILTER"."WWNTYP" AS "WWNTYP"
			, "DELTA_VIEW_FILTER"."WWNICK" AS "WWNICK"
			, "DELTA_VIEW_FILTER"."WWGEND" AS "WWGEND"
			, "DELTA_VIEW_FILTER"."WWDDATE" AS "WWDDATE"
			, "DELTA_VIEW_FILTER"."WWDMON" AS "WWDMON"
			, "DELTA_VIEW_FILTER"."WWDYR" AS "WWDYR"
			, "DELTA_VIEW_FILTER"."WWWN001" AS "WWWN001"
			, "DELTA_VIEW_FILTER"."WWWN002" AS "WWWN002"
			, "DELTA_VIEW_FILTER"."WWWN003" AS "WWWN003"
			, "DELTA_VIEW_FILTER"."WWWN004" AS "WWWN004"
			, "DELTA_VIEW_FILTER"."WWWN005" AS "WWWN005"
			, "DELTA_VIEW_FILTER"."WWWN006" AS "WWWN006"
			, "DELTA_VIEW_FILTER"."WWWN007" AS "WWWN007"
			, "DELTA_VIEW_FILTER"."WWWN008" AS "WWWN008"
			, "DELTA_VIEW_FILTER"."WWWN009" AS "WWWN009"
			, "DELTA_VIEW_FILTER"."WWWN010" AS "WWWN010"
			, "DELTA_VIEW_FILTER"."WWFUCO" AS "WWFUCO"
			, "DELTA_VIEW_FILTER"."WWPCM" AS "WWPCM"
			, "DELTA_VIEW_FILTER"."WWPCF" AS "WWPCF"
			, "DELTA_VIEW_FILTER"."WWACTIN" AS "WWACTIN"
			, "DELTA_VIEW_FILTER"."WWCFRGUID" AS "WWCFRGUID"
			, "DELTA_VIEW_FILTER"."WWSYNCS" AS "WWSYNCS"
			, "DELTA_VIEW_FILTER"."WWCAAD" AS "WWCAAD"
		FROM "DELTA_VIEW_FILTER" "DELTA_VIEW_FILTER"
	)
	, "PREPJOINBK" AS 
	( 
		SELECT 
			  "DELTA_VIEW"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
			, "DELTA_VIEW"."JRN_FLAG" AS "JRN_FLAG"
			, "DELTA_VIEW"."RECORD_TYPE" AS "RECORD_TYPE"
			, COALESCE("DELTA_VIEW"."WWAN8", CAST("MEX_BK_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WWAN8"
			, COALESCE("DELTA_VIEW"."WWIDLN", CAST("MEX_BK_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WWIDLN"
			, "DELTA_VIEW"."WWDSS5" AS "WWDSS5"
			, "DELTA_VIEW"."WWMLNM" AS "WWMLNM"
			, "DELTA_VIEW"."WWATTL" AS "WWATTL"
			, "DELTA_VIEW"."WWREM1" AS "WWREM1"
			, "DELTA_VIEW"."WWSLNM" AS "WWSLNM"
			, "DELTA_VIEW"."WWALPH" AS "WWALPH"
			, "DELTA_VIEW"."WWDC" AS "WWDC"
			, "DELTA_VIEW"."WWGNNM" AS "WWGNNM"
			, "DELTA_VIEW"."WWMDNM" AS "WWMDNM"
			, "DELTA_VIEW"."WWSRNM" AS "WWSRNM"
			, "DELTA_VIEW"."WWTYC" AS "WWTYC"
			, "DELTA_VIEW"."WWW001" AS "WWW001"
			, "DELTA_VIEW"."WWW002" AS "WWW002"
			, "DELTA_VIEW"."WWW003" AS "WWW003"
			, "DELTA_VIEW"."WWW004" AS "WWW004"
			, "DELTA_VIEW"."WWW005" AS "WWW005"
			, "DELTA_VIEW"."WWW006" AS "WWW006"
			, "DELTA_VIEW"."WWW007" AS "WWW007"
			, "DELTA_VIEW"."WWW008" AS "WWW008"
			, "DELTA_VIEW"."WWW009" AS "WWW009"
			, "DELTA_VIEW"."WWW010" AS "WWW010"
			, "DELTA_VIEW"."WWMLN1" AS "WWMLN1"
			, "DELTA_VIEW"."WWALP1" AS "WWALP1"
			, "DELTA_VIEW"."WWUSER" AS "WWUSER"
			, "DELTA_VIEW"."WWPID" AS "WWPID"
			, "DELTA_VIEW"."WWUPMJ" AS "WWUPMJ"
			, "DELTA_VIEW"."WWJOBN" AS "WWJOBN"
			, "DELTA_VIEW"."WWUPMT" AS "WWUPMT"
			, "DELTA_VIEW"."WWNTYP" AS "WWNTYP"
			, "DELTA_VIEW"."WWNICK" AS "WWNICK"
			, "DELTA_VIEW"."WWGEND" AS "WWGEND"
			, "DELTA_VIEW"."WWDDATE" AS "WWDDATE"
			, "DELTA_VIEW"."WWDMON" AS "WWDMON"
			, "DELTA_VIEW"."WWDYR" AS "WWDYR"
			, "DELTA_VIEW"."WWWN001" AS "WWWN001"
			, "DELTA_VIEW"."WWWN002" AS "WWWN002"
			, "DELTA_VIEW"."WWWN003" AS "WWWN003"
			, "DELTA_VIEW"."WWWN004" AS "WWWN004"
			, "DELTA_VIEW"."WWWN005" AS "WWWN005"
			, "DELTA_VIEW"."WWWN006" AS "WWWN006"
			, "DELTA_VIEW"."WWWN007" AS "WWWN007"
			, "DELTA_VIEW"."WWWN008" AS "WWWN008"
			, "DELTA_VIEW"."WWWN009" AS "WWWN009"
			, "DELTA_VIEW"."WWWN010" AS "WWWN010"
			, "DELTA_VIEW"."WWFUCO" AS "WWFUCO"
			, "DELTA_VIEW"."WWPCM" AS "WWPCM"
			, "DELTA_VIEW"."WWPCF" AS "WWPCF"
			, "DELTA_VIEW"."WWACTIN" AS "WWACTIN"
			, "DELTA_VIEW"."WWCFRGUID" AS "WWCFRGUID"
			, "DELTA_VIEW"."WWSYNCS" AS "WWSYNCS"
			, "DELTA_VIEW"."WWCAAD" AS "WWCAAD"
		FROM "DELTA_VIEW" "DELTA_VIEW"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_BK_SRC" ON  1 = 1
		WHERE  "MEX_BK_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "PREPJOINBK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "PREPJOINBK"."JRN_FLAG" AS "JRN_FLAG"
		, "PREPJOINBK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "PREPJOINBK"."WWAN8" AS "WWAN8"
		, "PREPJOINBK"."WWIDLN" AS "WWIDLN"
		, "PREPJOINBK"."WWDSS5" AS "WWDSS5"
		, "PREPJOINBK"."WWMLNM" AS "WWMLNM"
		, "PREPJOINBK"."WWATTL" AS "WWATTL"
		, "PREPJOINBK"."WWREM1" AS "WWREM1"
		, "PREPJOINBK"."WWSLNM" AS "WWSLNM"
		, "PREPJOINBK"."WWALPH" AS "WWALPH"
		, "PREPJOINBK"."WWDC" AS "WWDC"
		, "PREPJOINBK"."WWGNNM" AS "WWGNNM"
		, "PREPJOINBK"."WWMDNM" AS "WWMDNM"
		, "PREPJOINBK"."WWSRNM" AS "WWSRNM"
		, "PREPJOINBK"."WWTYC" AS "WWTYC"
		, "PREPJOINBK"."WWW001" AS "WWW001"
		, "PREPJOINBK"."WWW002" AS "WWW002"
		, "PREPJOINBK"."WWW003" AS "WWW003"
		, "PREPJOINBK"."WWW004" AS "WWW004"
		, "PREPJOINBK"."WWW005" AS "WWW005"
		, "PREPJOINBK"."WWW006" AS "WWW006"
		, "PREPJOINBK"."WWW007" AS "WWW007"
		, "PREPJOINBK"."WWW008" AS "WWW008"
		, "PREPJOINBK"."WWW009" AS "WWW009"
		, "PREPJOINBK"."WWW010" AS "WWW010"
		, "PREPJOINBK"."WWMLN1" AS "WWMLN1"
		, "PREPJOINBK"."WWALP1" AS "WWALP1"
		, "PREPJOINBK"."WWUSER" AS "WWUSER"
		, "PREPJOINBK"."WWPID" AS "WWPID"
		, "PREPJOINBK"."WWUPMJ" AS "WWUPMJ"
		, "PREPJOINBK"."WWJOBN" AS "WWJOBN"
		, "PREPJOINBK"."WWUPMT" AS "WWUPMT"
		, "PREPJOINBK"."WWNTYP" AS "WWNTYP"
		, "PREPJOINBK"."WWNICK" AS "WWNICK"
		, "PREPJOINBK"."WWGEND" AS "WWGEND"
		, "PREPJOINBK"."WWDDATE" AS "WWDDATE"
		, "PREPJOINBK"."WWDMON" AS "WWDMON"
		, "PREPJOINBK"."WWDYR" AS "WWDYR"
		, "PREPJOINBK"."WWWN001" AS "WWWN001"
		, "PREPJOINBK"."WWWN002" AS "WWWN002"
		, "PREPJOINBK"."WWWN003" AS "WWWN003"
		, "PREPJOINBK"."WWWN004" AS "WWWN004"
		, "PREPJOINBK"."WWWN005" AS "WWWN005"
		, "PREPJOINBK"."WWWN006" AS "WWWN006"
		, "PREPJOINBK"."WWWN007" AS "WWWN007"
		, "PREPJOINBK"."WWWN008" AS "WWWN008"
		, "PREPJOINBK"."WWWN009" AS "WWWN009"
		, "PREPJOINBK"."WWWN010" AS "WWWN010"
		, "PREPJOINBK"."WWFUCO" AS "WWFUCO"
		, "PREPJOINBK"."WWPCM" AS "WWPCM"
		, "PREPJOINBK"."WWPCF" AS "WWPCF"
		, "PREPJOINBK"."WWACTIN" AS "WWACTIN"
		, "PREPJOINBK"."WWCFRGUID" AS "WWCFRGUID"
		, "PREPJOINBK"."WWSYNCS" AS "WWSYNCS"
		, "PREPJOINBK"."WWCAAD" AS "WWCAAD"
	FROM "PREPJOINBK" "PREPJOINBK"
