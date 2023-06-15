{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='F0111',
		schema='JDEDWARDS_EXT',
		tags=['JDEDWARDS', 'EXT_JDE_WHOISWHO_INCR', 'EXT_JDE_WHOISWHO_INIT']
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
			, "TDFV_SRC"."WWAN8" AS "WWAN8"
			, "TDFV_SRC"."WWIDLN" AS "WWIDLN"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."WWAN8")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WWAN8_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."WWIDLN")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WWIDLN_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."WWAN8")) AS "ABAN8_FK_WWAN8_BK"
			, "TDFV_SRC"."WWDSS5" AS "WWDSS5"
			, "TDFV_SRC"."WWMLNM" AS "WWMLNM"
			, "TDFV_SRC"."WWATTL" AS "WWATTL"
			, "TDFV_SRC"."WWREM1" AS "WWREM1"
			, "TDFV_SRC"."WWSLNM" AS "WWSLNM"
			, "TDFV_SRC"."WWALPH" AS "WWALPH"
			, "TDFV_SRC"."WWDC" AS "WWDC"
			, "TDFV_SRC"."WWGNNM" AS "WWGNNM"
			, "TDFV_SRC"."WWMDNM" AS "WWMDNM"
			, "TDFV_SRC"."WWSRNM" AS "WWSRNM"
			, "TDFV_SRC"."WWTYC" AS "WWTYC"
			, "TDFV_SRC"."WWW001" AS "WWW001"
			, "TDFV_SRC"."WWW002" AS "WWW002"
			, "TDFV_SRC"."WWW003" AS "WWW003"
			, "TDFV_SRC"."WWW004" AS "WWW004"
			, "TDFV_SRC"."WWW005" AS "WWW005"
			, "TDFV_SRC"."WWW006" AS "WWW006"
			, "TDFV_SRC"."WWW007" AS "WWW007"
			, "TDFV_SRC"."WWW008" AS "WWW008"
			, "TDFV_SRC"."WWW009" AS "WWW009"
			, "TDFV_SRC"."WWW010" AS "WWW010"
			, "TDFV_SRC"."WWMLN1" AS "WWMLN1"
			, "TDFV_SRC"."WWALP1" AS "WWALP1"
			, "TDFV_SRC"."WWUSER" AS "WWUSER"
			, "TDFV_SRC"."WWPID" AS "WWPID"
			, "TDFV_SRC"."WWUPMJ" AS "WWUPMJ"
			, "TDFV_SRC"."WWJOBN" AS "WWJOBN"
			, "TDFV_SRC"."WWUPMT" AS "WWUPMT"
			, "TDFV_SRC"."WWNTYP" AS "WWNTYP"
			, "TDFV_SRC"."WWNICK" AS "WWNICK"
			, "TDFV_SRC"."WWGEND" AS "WWGEND"
			, "TDFV_SRC"."WWDDATE" AS "WWDDATE"
			, "TDFV_SRC"."WWDMON" AS "WWDMON"
			, "TDFV_SRC"."WWDYR" AS "WWDYR"
			, "TDFV_SRC"."WWWN001" AS "WWWN001"
			, "TDFV_SRC"."WWWN002" AS "WWWN002"
			, "TDFV_SRC"."WWWN003" AS "WWWN003"
			, "TDFV_SRC"."WWWN004" AS "WWWN004"
			, "TDFV_SRC"."WWWN005" AS "WWWN005"
			, "TDFV_SRC"."WWWN006" AS "WWWN006"
			, "TDFV_SRC"."WWWN007" AS "WWWN007"
			, "TDFV_SRC"."WWWN008" AS "WWWN008"
			, "TDFV_SRC"."WWWN009" AS "WWWN009"
			, "TDFV_SRC"."WWWN010" AS "WWWN010"
			, "TDFV_SRC"."WWFUCO" AS "WWFUCO"
			, "TDFV_SRC"."WWPCM" AS "WWPCM"
			, "TDFV_SRC"."WWPCF" AS "WWPCF"
			, "TDFV_SRC"."WWACTIN" AS "WWACTIN"
			, "TDFV_SRC"."WWCFRGUID" AS "WWCFRGUID"
			, "TDFV_SRC"."WWSYNCS" AS "WWSYNCS"
			, "TDFV_SRC"."WWCAAD" AS "WWCAAD"
		FROM {{ ref('JDEDWARDS_DFV_VW_F0111') }} "TDFV_SRC"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'LOAD_CYCLE_INFO') }} "LCI_SRC" ON  1 = 1
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  1 = 1
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
			, "CALCULATE_BK"."WWAN8" AS "WWAN8"
			, "CALCULATE_BK"."WWIDLN" AS "WWIDLN"
			, "CALCULATE_BK"."WWAN8_BK" AS "WWAN8_BK"
			, "CALCULATE_BK"."WWIDLN_BK" AS "WWIDLN_BK"
			, "CALCULATE_BK"."ABAN8_FK_WWAN8_BK" AS "ABAN8_FK_WWAN8_BK"
			, "CALCULATE_BK"."WWDSS5" AS "WWDSS5"
			, "CALCULATE_BK"."WWMLNM" AS "WWMLNM"
			, "CALCULATE_BK"."WWATTL" AS "WWATTL"
			, "CALCULATE_BK"."WWREM1" AS "WWREM1"
			, "CALCULATE_BK"."WWSLNM" AS "WWSLNM"
			, "CALCULATE_BK"."WWALPH" AS "WWALPH"
			, "CALCULATE_BK"."WWDC" AS "WWDC"
			, "CALCULATE_BK"."WWGNNM" AS "WWGNNM"
			, "CALCULATE_BK"."WWMDNM" AS "WWMDNM"
			, "CALCULATE_BK"."WWSRNM" AS "WWSRNM"
			, "CALCULATE_BK"."WWTYC" AS "WWTYC"
			, "CALCULATE_BK"."WWW001" AS "WWW001"
			, "CALCULATE_BK"."WWW002" AS "WWW002"
			, "CALCULATE_BK"."WWW003" AS "WWW003"
			, "CALCULATE_BK"."WWW004" AS "WWW004"
			, "CALCULATE_BK"."WWW005" AS "WWW005"
			, "CALCULATE_BK"."WWW006" AS "WWW006"
			, "CALCULATE_BK"."WWW007" AS "WWW007"
			, "CALCULATE_BK"."WWW008" AS "WWW008"
			, "CALCULATE_BK"."WWW009" AS "WWW009"
			, "CALCULATE_BK"."WWW010" AS "WWW010"
			, "CALCULATE_BK"."WWMLN1" AS "WWMLN1"
			, "CALCULATE_BK"."WWALP1" AS "WWALP1"
			, "CALCULATE_BK"."WWUSER" AS "WWUSER"
			, "CALCULATE_BK"."WWPID" AS "WWPID"
			, "CALCULATE_BK"."WWUPMJ" AS "WWUPMJ"
			, "CALCULATE_BK"."WWJOBN" AS "WWJOBN"
			, "CALCULATE_BK"."WWUPMT" AS "WWUPMT"
			, "CALCULATE_BK"."WWNTYP" AS "WWNTYP"
			, "CALCULATE_BK"."WWNICK" AS "WWNICK"
			, "CALCULATE_BK"."WWGEND" AS "WWGEND"
			, "CALCULATE_BK"."WWDDATE" AS "WWDDATE"
			, "CALCULATE_BK"."WWDMON" AS "WWDMON"
			, "CALCULATE_BK"."WWDYR" AS "WWDYR"
			, "CALCULATE_BK"."WWWN001" AS "WWWN001"
			, "CALCULATE_BK"."WWWN002" AS "WWWN002"
			, "CALCULATE_BK"."WWWN003" AS "WWWN003"
			, "CALCULATE_BK"."WWWN004" AS "WWWN004"
			, "CALCULATE_BK"."WWWN005" AS "WWWN005"
			, "CALCULATE_BK"."WWWN006" AS "WWWN006"
			, "CALCULATE_BK"."WWWN007" AS "WWWN007"
			, "CALCULATE_BK"."WWWN008" AS "WWWN008"
			, "CALCULATE_BK"."WWWN009" AS "WWWN009"
			, "CALCULATE_BK"."WWWN010" AS "WWWN010"
			, "CALCULATE_BK"."WWFUCO" AS "WWFUCO"
			, "CALCULATE_BK"."WWPCM" AS "WWPCM"
			, "CALCULATE_BK"."WWPCF" AS "WWPCF"
			, "CALCULATE_BK"."WWACTIN" AS "WWACTIN"
			, "CALCULATE_BK"."WWCFRGUID" AS "WWCFRGUID"
			, "CALCULATE_BK"."WWSYNCS" AS "WWSYNCS"
			, "CALCULATE_BK"."WWCAAD" AS "WWCAAD"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."WWAN8" AS "WWAN8"
		, "EXT_UNION"."WWIDLN" AS "WWIDLN"
		, "EXT_UNION"."WWAN8_BK" AS "WWAN8_BK"
		, "EXT_UNION"."WWIDLN_BK" AS "WWIDLN_BK"
		, "EXT_UNION"."ABAN8_FK_WWAN8_BK" AS "ABAN8_FK_WWAN8_BK"
		, "EXT_UNION"."WWDSS5" AS "WWDSS5"
		, "EXT_UNION"."WWMLNM" AS "WWMLNM"
		, "EXT_UNION"."WWATTL" AS "WWATTL"
		, "EXT_UNION"."WWREM1" AS "WWREM1"
		, "EXT_UNION"."WWSLNM" AS "WWSLNM"
		, "EXT_UNION"."WWALPH" AS "WWALPH"
		, "EXT_UNION"."WWDC" AS "WWDC"
		, "EXT_UNION"."WWGNNM" AS "WWGNNM"
		, "EXT_UNION"."WWMDNM" AS "WWMDNM"
		, "EXT_UNION"."WWSRNM" AS "WWSRNM"
		, "EXT_UNION"."WWTYC" AS "WWTYC"
		, "EXT_UNION"."WWW001" AS "WWW001"
		, "EXT_UNION"."WWW002" AS "WWW002"
		, "EXT_UNION"."WWW003" AS "WWW003"
		, "EXT_UNION"."WWW004" AS "WWW004"
		, "EXT_UNION"."WWW005" AS "WWW005"
		, "EXT_UNION"."WWW006" AS "WWW006"
		, "EXT_UNION"."WWW007" AS "WWW007"
		, "EXT_UNION"."WWW008" AS "WWW008"
		, "EXT_UNION"."WWW009" AS "WWW009"
		, "EXT_UNION"."WWW010" AS "WWW010"
		, "EXT_UNION"."WWMLN1" AS "WWMLN1"
		, "EXT_UNION"."WWALP1" AS "WWALP1"
		, "EXT_UNION"."WWUSER" AS "WWUSER"
		, "EXT_UNION"."WWPID" AS "WWPID"
		, "EXT_UNION"."WWUPMJ" AS "WWUPMJ"
		, "EXT_UNION"."WWJOBN" AS "WWJOBN"
		, "EXT_UNION"."WWUPMT" AS "WWUPMT"
		, "EXT_UNION"."WWNTYP" AS "WWNTYP"
		, "EXT_UNION"."WWNICK" AS "WWNICK"
		, "EXT_UNION"."WWGEND" AS "WWGEND"
		, "EXT_UNION"."WWDDATE" AS "WWDDATE"
		, "EXT_UNION"."WWDMON" AS "WWDMON"
		, "EXT_UNION"."WWDYR" AS "WWDYR"
		, "EXT_UNION"."WWWN001" AS "WWWN001"
		, "EXT_UNION"."WWWN002" AS "WWWN002"
		, "EXT_UNION"."WWWN003" AS "WWWN003"
		, "EXT_UNION"."WWWN004" AS "WWWN004"
		, "EXT_UNION"."WWWN005" AS "WWWN005"
		, "EXT_UNION"."WWWN006" AS "WWWN006"
		, "EXT_UNION"."WWWN007" AS "WWWN007"
		, "EXT_UNION"."WWWN008" AS "WWWN008"
		, "EXT_UNION"."WWWN009" AS "WWWN009"
		, "EXT_UNION"."WWWN010" AS "WWWN010"
		, "EXT_UNION"."WWFUCO" AS "WWFUCO"
		, "EXT_UNION"."WWPCM" AS "WWPCM"
		, "EXT_UNION"."WWPCF" AS "WWPCF"
		, "EXT_UNION"."WWACTIN" AS "WWACTIN"
		, "EXT_UNION"."WWCFRGUID" AS "WWCFRGUID"
		, "EXT_UNION"."WWSYNCS" AS "WWSYNCS"
		, "EXT_UNION"."WWCAAD" AS "WWCAAD"
	FROM "EXT_UNION" "EXT_UNION"

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	WITH "LOAD_INIT_DATA" AS 
	( 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, TO_CHAR('S') AS "RECORD_TYPE"
			, COALESCE("INI_SRC"."WWAN8", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WWAN8"
			, COALESCE("INI_SRC"."WWIDLN", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WWIDLN"
			, "INI_SRC"."WWDSS5" AS "WWDSS5"
			, "INI_SRC"."WWMLNM" AS "WWMLNM"
			, "INI_SRC"."WWATTL" AS "WWATTL"
			, "INI_SRC"."WWREM1" AS "WWREM1"
			, "INI_SRC"."WWSLNM" AS "WWSLNM"
			, "INI_SRC"."WWALPH" AS "WWALPH"
			, "INI_SRC"."WWDC" AS "WWDC"
			, "INI_SRC"."WWGNNM" AS "WWGNNM"
			, "INI_SRC"."WWMDNM" AS "WWMDNM"
			, "INI_SRC"."WWSRNM" AS "WWSRNM"
			, "INI_SRC"."WWTYC" AS "WWTYC"
			, "INI_SRC"."WWW001" AS "WWW001"
			, "INI_SRC"."WWW002" AS "WWW002"
			, "INI_SRC"."WWW003" AS "WWW003"
			, "INI_SRC"."WWW004" AS "WWW004"
			, "INI_SRC"."WWW005" AS "WWW005"
			, "INI_SRC"."WWW006" AS "WWW006"
			, "INI_SRC"."WWW007" AS "WWW007"
			, "INI_SRC"."WWW008" AS "WWW008"
			, "INI_SRC"."WWW009" AS "WWW009"
			, "INI_SRC"."WWW010" AS "WWW010"
			, "INI_SRC"."WWMLN1" AS "WWMLN1"
			, "INI_SRC"."WWALP1" AS "WWALP1"
			, "INI_SRC"."WWUSER" AS "WWUSER"
			, "INI_SRC"."WWPID" AS "WWPID"
			, "INI_SRC"."WWUPMJ" AS "WWUPMJ"
			, "INI_SRC"."WWJOBN" AS "WWJOBN"
			, "INI_SRC"."WWUPMT" AS "WWUPMT"
			, "INI_SRC"."WWNTYP" AS "WWNTYP"
			, "INI_SRC"."WWNICK" AS "WWNICK"
			, "INI_SRC"."WWGEND" AS "WWGEND"
			, "INI_SRC"."WWDDATE" AS "WWDDATE"
			, "INI_SRC"."WWDMON" AS "WWDMON"
			, "INI_SRC"."WWDYR" AS "WWDYR"
			, "INI_SRC"."WWWN001" AS "WWWN001"
			, "INI_SRC"."WWWN002" AS "WWWN002"
			, "INI_SRC"."WWWN003" AS "WWWN003"
			, "INI_SRC"."WWWN004" AS "WWWN004"
			, "INI_SRC"."WWWN005" AS "WWWN005"
			, "INI_SRC"."WWWN006" AS "WWWN006"
			, "INI_SRC"."WWWN007" AS "WWWN007"
			, "INI_SRC"."WWWN008" AS "WWWN008"
			, "INI_SRC"."WWWN009" AS "WWWN009"
			, "INI_SRC"."WWWN010" AS "WWWN010"
			, "INI_SRC"."WWFUCO" AS "WWFUCO"
			, "INI_SRC"."WWPCM" AS "WWPCM"
			, "INI_SRC"."WWPCF" AS "WWPCF"
			, "INI_SRC"."WWACTIN" AS "WWACTIN"
			, "INI_SRC"."WWCFRGUID" AS "WWCFRGUID"
			, "INI_SRC"."WWSYNCS" AS "WWSYNCS"
			, "INI_SRC"."WWCAAD" AS "WWCAAD"
		FROM {{ source('JDEDWARDS_INI', 'F0111') }} "INI_SRC"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."WWAN8" AS "WWAN8"
			, "LOAD_INIT_DATA"."WWIDLN" AS "WWIDLN"
			, "LOAD_INIT_DATA"."WWDSS5" AS "WWDSS5"
			, "LOAD_INIT_DATA"."WWMLNM" AS "WWMLNM"
			, "LOAD_INIT_DATA"."WWATTL" AS "WWATTL"
			, "LOAD_INIT_DATA"."WWREM1" AS "WWREM1"
			, "LOAD_INIT_DATA"."WWSLNM" AS "WWSLNM"
			, "LOAD_INIT_DATA"."WWALPH" AS "WWALPH"
			, "LOAD_INIT_DATA"."WWDC" AS "WWDC"
			, "LOAD_INIT_DATA"."WWGNNM" AS "WWGNNM"
			, "LOAD_INIT_DATA"."WWMDNM" AS "WWMDNM"
			, "LOAD_INIT_DATA"."WWSRNM" AS "WWSRNM"
			, "LOAD_INIT_DATA"."WWTYC" AS "WWTYC"
			, "LOAD_INIT_DATA"."WWW001" AS "WWW001"
			, "LOAD_INIT_DATA"."WWW002" AS "WWW002"
			, "LOAD_INIT_DATA"."WWW003" AS "WWW003"
			, "LOAD_INIT_DATA"."WWW004" AS "WWW004"
			, "LOAD_INIT_DATA"."WWW005" AS "WWW005"
			, "LOAD_INIT_DATA"."WWW006" AS "WWW006"
			, "LOAD_INIT_DATA"."WWW007" AS "WWW007"
			, "LOAD_INIT_DATA"."WWW008" AS "WWW008"
			, "LOAD_INIT_DATA"."WWW009" AS "WWW009"
			, "LOAD_INIT_DATA"."WWW010" AS "WWW010"
			, "LOAD_INIT_DATA"."WWMLN1" AS "WWMLN1"
			, "LOAD_INIT_DATA"."WWALP1" AS "WWALP1"
			, "LOAD_INIT_DATA"."WWUSER" AS "WWUSER"
			, "LOAD_INIT_DATA"."WWPID" AS "WWPID"
			, "LOAD_INIT_DATA"."WWUPMJ" AS "WWUPMJ"
			, "LOAD_INIT_DATA"."WWJOBN" AS "WWJOBN"
			, "LOAD_INIT_DATA"."WWUPMT" AS "WWUPMT"
			, "LOAD_INIT_DATA"."WWNTYP" AS "WWNTYP"
			, "LOAD_INIT_DATA"."WWNICK" AS "WWNICK"
			, "LOAD_INIT_DATA"."WWGEND" AS "WWGEND"
			, "LOAD_INIT_DATA"."WWDDATE" AS "WWDDATE"
			, "LOAD_INIT_DATA"."WWDMON" AS "WWDMON"
			, "LOAD_INIT_DATA"."WWDYR" AS "WWDYR"
			, "LOAD_INIT_DATA"."WWWN001" AS "WWWN001"
			, "LOAD_INIT_DATA"."WWWN002" AS "WWWN002"
			, "LOAD_INIT_DATA"."WWWN003" AS "WWWN003"
			, "LOAD_INIT_DATA"."WWWN004" AS "WWWN004"
			, "LOAD_INIT_DATA"."WWWN005" AS "WWWN005"
			, "LOAD_INIT_DATA"."WWWN006" AS "WWWN006"
			, "LOAD_INIT_DATA"."WWWN007" AS "WWWN007"
			, "LOAD_INIT_DATA"."WWWN008" AS "WWWN008"
			, "LOAD_INIT_DATA"."WWWN009" AS "WWWN009"
			, "LOAD_INIT_DATA"."WWWN010" AS "WWWN010"
			, "LOAD_INIT_DATA"."WWFUCO" AS "WWFUCO"
			, "LOAD_INIT_DATA"."WWPCM" AS "WWPCM"
			, "LOAD_INIT_DATA"."WWPCF" AS "WWPCF"
			, "LOAD_INIT_DATA"."WWACTIN" AS "WWACTIN"
			, "LOAD_INIT_DATA"."WWCFRGUID" AS "WWCFRGUID"
			, "LOAD_INIT_DATA"."WWSYNCS" AS "WWSYNCS"
			, "LOAD_INIT_DATA"."WWCAAD" AS "WWCAAD"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "WWAN8"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "WWIDLN"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WWDSS5"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWMLNM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWATTL"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWREM1"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWSLNM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWALPH"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWDC"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWGNNM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWMDNM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWSRNM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWTYC"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW001"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW002"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW003"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW004"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW005"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW006"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW007"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW008"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW009"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWW010"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWMLN1"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWALP1"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWUSER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWPID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "WWUPMJ"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWJOBN"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WWUPMT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWNTYP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWNICK"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWGEND"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WWDDATE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WWDMON"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WWDYR"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN001"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN002"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN003"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN004"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN005"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN006"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN007"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN008"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN009"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWWN010"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWFUCO"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWPCM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWPCF"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWACTIN"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WWCFRGUID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WWSYNCS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WWCAAD"
		FROM {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_EXT_SRC"
	)
	, "CALCULATE_BK" AS 
	( 
		SELECT 
			  COALESCE("PREP_EXCEP"."LOAD_CYCLE_ID","LCI_SRC"."LOAD_CYCLE_ID") AS "LOAD_CYCLE_ID"
			, "LCI_SRC"."LOAD_DATE" AS "LOAD_DATE"
			, "LCI_SRC"."LOAD_DATE" AS "CDC_TIMESTAMP"
			, "PREP_EXCEP"."JRN_FLAG" AS "JRN_FLAG"
			, "PREP_EXCEP"."RECORD_TYPE" AS "RECORD_TYPE"
			, "PREP_EXCEP"."WWAN8" AS "WWAN8"
			, "PREP_EXCEP"."WWIDLN" AS "WWIDLN"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."WWAN8")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WWAN8_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."WWIDLN")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WWIDLN_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."WWAN8")) AS "ABAN8_FK_WWAN8_BK"
			, "PREP_EXCEP"."WWDSS5" AS "WWDSS5"
			, "PREP_EXCEP"."WWMLNM" AS "WWMLNM"
			, "PREP_EXCEP"."WWATTL" AS "WWATTL"
			, "PREP_EXCEP"."WWREM1" AS "WWREM1"
			, "PREP_EXCEP"."WWSLNM" AS "WWSLNM"
			, "PREP_EXCEP"."WWALPH" AS "WWALPH"
			, "PREP_EXCEP"."WWDC" AS "WWDC"
			, "PREP_EXCEP"."WWGNNM" AS "WWGNNM"
			, "PREP_EXCEP"."WWMDNM" AS "WWMDNM"
			, "PREP_EXCEP"."WWSRNM" AS "WWSRNM"
			, "PREP_EXCEP"."WWTYC" AS "WWTYC"
			, "PREP_EXCEP"."WWW001" AS "WWW001"
			, "PREP_EXCEP"."WWW002" AS "WWW002"
			, "PREP_EXCEP"."WWW003" AS "WWW003"
			, "PREP_EXCEP"."WWW004" AS "WWW004"
			, "PREP_EXCEP"."WWW005" AS "WWW005"
			, "PREP_EXCEP"."WWW006" AS "WWW006"
			, "PREP_EXCEP"."WWW007" AS "WWW007"
			, "PREP_EXCEP"."WWW008" AS "WWW008"
			, "PREP_EXCEP"."WWW009" AS "WWW009"
			, "PREP_EXCEP"."WWW010" AS "WWW010"
			, "PREP_EXCEP"."WWMLN1" AS "WWMLN1"
			, "PREP_EXCEP"."WWALP1" AS "WWALP1"
			, "PREP_EXCEP"."WWUSER" AS "WWUSER"
			, "PREP_EXCEP"."WWPID" AS "WWPID"
			, "PREP_EXCEP"."WWUPMJ" AS "WWUPMJ"
			, "PREP_EXCEP"."WWJOBN" AS "WWJOBN"
			, "PREP_EXCEP"."WWUPMT" AS "WWUPMT"
			, "PREP_EXCEP"."WWNTYP" AS "WWNTYP"
			, "PREP_EXCEP"."WWNICK" AS "WWNICK"
			, "PREP_EXCEP"."WWGEND" AS "WWGEND"
			, "PREP_EXCEP"."WWDDATE" AS "WWDDATE"
			, "PREP_EXCEP"."WWDMON" AS "WWDMON"
			, "PREP_EXCEP"."WWDYR" AS "WWDYR"
			, "PREP_EXCEP"."WWWN001" AS "WWWN001"
			, "PREP_EXCEP"."WWWN002" AS "WWWN002"
			, "PREP_EXCEP"."WWWN003" AS "WWWN003"
			, "PREP_EXCEP"."WWWN004" AS "WWWN004"
			, "PREP_EXCEP"."WWWN005" AS "WWWN005"
			, "PREP_EXCEP"."WWWN006" AS "WWWN006"
			, "PREP_EXCEP"."WWWN007" AS "WWWN007"
			, "PREP_EXCEP"."WWWN008" AS "WWWN008"
			, "PREP_EXCEP"."WWWN009" AS "WWWN009"
			, "PREP_EXCEP"."WWWN010" AS "WWWN010"
			, "PREP_EXCEP"."WWFUCO" AS "WWFUCO"
			, "PREP_EXCEP"."WWPCM" AS "WWPCM"
			, "PREP_EXCEP"."WWPCF" AS "WWPCF"
			, "PREP_EXCEP"."WWACTIN" AS "WWACTIN"
			, "PREP_EXCEP"."WWCFRGUID" AS "WWCFRGUID"
			, "PREP_EXCEP"."WWSYNCS" AS "WWSYNCS"
			, "PREP_EXCEP"."WWCAAD" AS "WWCAAD"
		FROM "PREP_EXCEP" "PREP_EXCEP"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'LOAD_CYCLE_INFO') }} "LCI_SRC" ON  1 = 1
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  1 = 1
		WHERE  "MEX_SRC"."RECORD_TYPE" = 'N'
	)
	SELECT 
		  "CALCULATE_BK"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "CALCULATE_BK"."LOAD_DATE" AS "LOAD_DATE"
		, "CALCULATE_BK"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "CALCULATE_BK"."JRN_FLAG" AS "JRN_FLAG"
		, "CALCULATE_BK"."RECORD_TYPE" AS "RECORD_TYPE"
		, "CALCULATE_BK"."WWAN8" AS "WWAN8"
		, "CALCULATE_BK"."WWIDLN" AS "WWIDLN"
		, "CALCULATE_BK"."WWAN8_BK" AS "WWAN8_BK"
		, "CALCULATE_BK"."WWIDLN_BK" AS "WWIDLN_BK"
		, "CALCULATE_BK"."ABAN8_FK_WWAN8_BK" AS "ABAN8_FK_WWAN8_BK"
		, "CALCULATE_BK"."WWDSS5" AS "WWDSS5"
		, "CALCULATE_BK"."WWMLNM" AS "WWMLNM"
		, "CALCULATE_BK"."WWATTL" AS "WWATTL"
		, "CALCULATE_BK"."WWREM1" AS "WWREM1"
		, "CALCULATE_BK"."WWSLNM" AS "WWSLNM"
		, "CALCULATE_BK"."WWALPH" AS "WWALPH"
		, "CALCULATE_BK"."WWDC" AS "WWDC"
		, "CALCULATE_BK"."WWGNNM" AS "WWGNNM"
		, "CALCULATE_BK"."WWMDNM" AS "WWMDNM"
		, "CALCULATE_BK"."WWSRNM" AS "WWSRNM"
		, "CALCULATE_BK"."WWTYC" AS "WWTYC"
		, "CALCULATE_BK"."WWW001" AS "WWW001"
		, "CALCULATE_BK"."WWW002" AS "WWW002"
		, "CALCULATE_BK"."WWW003" AS "WWW003"
		, "CALCULATE_BK"."WWW004" AS "WWW004"
		, "CALCULATE_BK"."WWW005" AS "WWW005"
		, "CALCULATE_BK"."WWW006" AS "WWW006"
		, "CALCULATE_BK"."WWW007" AS "WWW007"
		, "CALCULATE_BK"."WWW008" AS "WWW008"
		, "CALCULATE_BK"."WWW009" AS "WWW009"
		, "CALCULATE_BK"."WWW010" AS "WWW010"
		, "CALCULATE_BK"."WWMLN1" AS "WWMLN1"
		, "CALCULATE_BK"."WWALP1" AS "WWALP1"
		, "CALCULATE_BK"."WWUSER" AS "WWUSER"
		, "CALCULATE_BK"."WWPID" AS "WWPID"
		, "CALCULATE_BK"."WWUPMJ" AS "WWUPMJ"
		, "CALCULATE_BK"."WWJOBN" AS "WWJOBN"
		, "CALCULATE_BK"."WWUPMT" AS "WWUPMT"
		, "CALCULATE_BK"."WWNTYP" AS "WWNTYP"
		, "CALCULATE_BK"."WWNICK" AS "WWNICK"
		, "CALCULATE_BK"."WWGEND" AS "WWGEND"
		, "CALCULATE_BK"."WWDDATE" AS "WWDDATE"
		, "CALCULATE_BK"."WWDMON" AS "WWDMON"
		, "CALCULATE_BK"."WWDYR" AS "WWDYR"
		, "CALCULATE_BK"."WWWN001" AS "WWWN001"
		, "CALCULATE_BK"."WWWN002" AS "WWWN002"
		, "CALCULATE_BK"."WWWN003" AS "WWWN003"
		, "CALCULATE_BK"."WWWN004" AS "WWWN004"
		, "CALCULATE_BK"."WWWN005" AS "WWWN005"
		, "CALCULATE_BK"."WWWN006" AS "WWWN006"
		, "CALCULATE_BK"."WWWN007" AS "WWWN007"
		, "CALCULATE_BK"."WWWN008" AS "WWWN008"
		, "CALCULATE_BK"."WWWN009" AS "WWWN009"
		, "CALCULATE_BK"."WWWN010" AS "WWWN010"
		, "CALCULATE_BK"."WWFUCO" AS "WWFUCO"
		, "CALCULATE_BK"."WWPCM" AS "WWPCM"
		, "CALCULATE_BK"."WWPCF" AS "WWPCF"
		, "CALCULATE_BK"."WWACTIN" AS "WWACTIN"
		, "CALCULATE_BK"."WWCFRGUID" AS "WWCFRGUID"
		, "CALCULATE_BK"."WWSYNCS" AS "WWSYNCS"
		, "CALCULATE_BK"."WWCAAD" AS "WWCAAD"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'