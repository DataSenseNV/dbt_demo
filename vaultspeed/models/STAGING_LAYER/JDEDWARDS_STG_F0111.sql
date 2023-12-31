{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='F0111',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'STG_JDE_WHOISWHO_INCR', 'STG_JDE_WHOISWHO_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."WWAN8_BK" || '\#' ||  "EXT_SRC"."WWIDLN_BK" || '\#' )) AS "WHOISWHO_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_WWAN8_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."WWAN8_BK" || '\#' ||  "EXT_SRC"."WWIDLN_BK" || '\#' || 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_WWAN8_BK" || 
			'\#' )) AS "LNK_WHOISWHO_ACCOUNT_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."WWAN8" AS "WWAN8"
		, "EXT_SRC"."WWIDLN" AS "WWIDLN"
		, "EXT_SRC"."WWAN8_BK" AS "WWAN8_BK"
		, "EXT_SRC"."WWIDLN_BK" AS "WWIDLN_BK"
		, "EXT_SRC"."ABAN8_FK_WWAN8_BK" AS "ABAN8_FK_WWAN8_BK"
		, "EXT_SRC"."WWDSS5" AS "WWDSS5"
		, "EXT_SRC"."WWMLNM" AS "WWMLNM"
		, "EXT_SRC"."WWATTL" AS "WWATTL"
		, "EXT_SRC"."WWREM1" AS "WWREM1"
		, "EXT_SRC"."WWSLNM" AS "WWSLNM"
		, "EXT_SRC"."WWALPH" AS "WWALPH"
		, "EXT_SRC"."WWDC" AS "WWDC"
		, "EXT_SRC"."WWGNNM" AS "WWGNNM"
		, "EXT_SRC"."WWMDNM" AS "WWMDNM"
		, "EXT_SRC"."WWSRNM" AS "WWSRNM"
		, "EXT_SRC"."WWTYC" AS "WWTYC"
		, "EXT_SRC"."WWW001" AS "WWW001"
		, "EXT_SRC"."WWW002" AS "WWW002"
		, "EXT_SRC"."WWW003" AS "WWW003"
		, "EXT_SRC"."WWW004" AS "WWW004"
		, "EXT_SRC"."WWW005" AS "WWW005"
		, "EXT_SRC"."WWW006" AS "WWW006"
		, "EXT_SRC"."WWW007" AS "WWW007"
		, "EXT_SRC"."WWW008" AS "WWW008"
		, "EXT_SRC"."WWW009" AS "WWW009"
		, "EXT_SRC"."WWW010" AS "WWW010"
		, "EXT_SRC"."WWMLN1" AS "WWMLN1"
		, "EXT_SRC"."WWALP1" AS "WWALP1"
		, "EXT_SRC"."WWUSER" AS "WWUSER"
		, "EXT_SRC"."WWPID" AS "WWPID"
		, "EXT_SRC"."WWUPMJ" AS "WWUPMJ"
		, "EXT_SRC"."WWJOBN" AS "WWJOBN"
		, "EXT_SRC"."WWUPMT" AS "WWUPMT"
		, "EXT_SRC"."WWNTYP" AS "WWNTYP"
		, "EXT_SRC"."WWNICK" AS "WWNICK"
		, "EXT_SRC"."WWGEND" AS "WWGEND"
		, "EXT_SRC"."WWDDATE" AS "WWDDATE"
		, "EXT_SRC"."WWDMON" AS "WWDMON"
		, "EXT_SRC"."WWDYR" AS "WWDYR"
		, "EXT_SRC"."WWWN001" AS "WWWN001"
		, "EXT_SRC"."WWWN002" AS "WWWN002"
		, "EXT_SRC"."WWWN003" AS "WWWN003"
		, "EXT_SRC"."WWWN004" AS "WWWN004"
		, "EXT_SRC"."WWWN005" AS "WWWN005"
		, "EXT_SRC"."WWWN006" AS "WWWN006"
		, "EXT_SRC"."WWWN007" AS "WWWN007"
		, "EXT_SRC"."WWWN008" AS "WWWN008"
		, "EXT_SRC"."WWWN009" AS "WWWN009"
		, "EXT_SRC"."WWWN010" AS "WWWN010"
		, "EXT_SRC"."WWFUCO" AS "WWFUCO"
		, "EXT_SRC"."WWPCM" AS "WWPCM"
		, "EXT_SRC"."WWPCF" AS "WWPCF"
		, "EXT_SRC"."WWACTIN" AS "WWACTIN"
		, "EXT_SRC"."WWCFRGUID" AS "WWCFRGUID"
		, "EXT_SRC"."WWSYNCS" AS "WWSYNCS"
		, "EXT_SRC"."WWCAAD" AS "WWCAAD"
	FROM {{ ref('JDEDWARDS_EXT_F0111') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."WWAN8_BK" || '\#' ||  "EXT_SRC"."WWIDLN_BK" || '\#' )) AS "WHOISWHO_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_WWAN8_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."WWAN8_BK" || '\#' ||  "EXT_SRC"."WWIDLN_BK" || '\#' || 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_WWAN8_BK" || 
			'\#' )) AS "LNK_WHOISWHO_ACCOUNT_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."WWAN8" AS "WWAN8"
		, "EXT_SRC"."WWIDLN" AS "WWIDLN"
		, "EXT_SRC"."WWAN8_BK" AS "WWAN8_BK"
		, "EXT_SRC"."WWIDLN_BK" AS "WWIDLN_BK"
		, "EXT_SRC"."ABAN8_FK_WWAN8_BK" AS "ABAN8_FK_WWAN8_BK"
		, "EXT_SRC"."WWDSS5" AS "WWDSS5"
		, "EXT_SRC"."WWMLNM" AS "WWMLNM"
		, "EXT_SRC"."WWATTL" AS "WWATTL"
		, "EXT_SRC"."WWREM1" AS "WWREM1"
		, "EXT_SRC"."WWSLNM" AS "WWSLNM"
		, "EXT_SRC"."WWALPH" AS "WWALPH"
		, "EXT_SRC"."WWDC" AS "WWDC"
		, "EXT_SRC"."WWGNNM" AS "WWGNNM"
		, "EXT_SRC"."WWMDNM" AS "WWMDNM"
		, "EXT_SRC"."WWSRNM" AS "WWSRNM"
		, "EXT_SRC"."WWTYC" AS "WWTYC"
		, "EXT_SRC"."WWW001" AS "WWW001"
		, "EXT_SRC"."WWW002" AS "WWW002"
		, "EXT_SRC"."WWW003" AS "WWW003"
		, "EXT_SRC"."WWW004" AS "WWW004"
		, "EXT_SRC"."WWW005" AS "WWW005"
		, "EXT_SRC"."WWW006" AS "WWW006"
		, "EXT_SRC"."WWW007" AS "WWW007"
		, "EXT_SRC"."WWW008" AS "WWW008"
		, "EXT_SRC"."WWW009" AS "WWW009"
		, "EXT_SRC"."WWW010" AS "WWW010"
		, "EXT_SRC"."WWMLN1" AS "WWMLN1"
		, "EXT_SRC"."WWALP1" AS "WWALP1"
		, "EXT_SRC"."WWUSER" AS "WWUSER"
		, "EXT_SRC"."WWPID" AS "WWPID"
		, "EXT_SRC"."WWUPMJ" AS "WWUPMJ"
		, "EXT_SRC"."WWJOBN" AS "WWJOBN"
		, "EXT_SRC"."WWUPMT" AS "WWUPMT"
		, "EXT_SRC"."WWNTYP" AS "WWNTYP"
		, "EXT_SRC"."WWNICK" AS "WWNICK"
		, "EXT_SRC"."WWGEND" AS "WWGEND"
		, "EXT_SRC"."WWDDATE" AS "WWDDATE"
		, "EXT_SRC"."WWDMON" AS "WWDMON"
		, "EXT_SRC"."WWDYR" AS "WWDYR"
		, "EXT_SRC"."WWWN001" AS "WWWN001"
		, "EXT_SRC"."WWWN002" AS "WWWN002"
		, "EXT_SRC"."WWWN003" AS "WWWN003"
		, "EXT_SRC"."WWWN004" AS "WWWN004"
		, "EXT_SRC"."WWWN005" AS "WWWN005"
		, "EXT_SRC"."WWWN006" AS "WWWN006"
		, "EXT_SRC"."WWWN007" AS "WWWN007"
		, "EXT_SRC"."WWWN008" AS "WWWN008"
		, "EXT_SRC"."WWWN009" AS "WWWN009"
		, "EXT_SRC"."WWWN010" AS "WWWN010"
		, "EXT_SRC"."WWFUCO" AS "WWFUCO"
		, "EXT_SRC"."WWPCM" AS "WWPCM"
		, "EXT_SRC"."WWPCF" AS "WWPCF"
		, "EXT_SRC"."WWACTIN" AS "WWACTIN"
		, "EXT_SRC"."WWCFRGUID" AS "WWCFRGUID"
		, "EXT_SRC"."WWSYNCS" AS "WWSYNCS"
		, "EXT_SRC"."WWCAAD" AS "WWCAAD"
	FROM {{ ref('JDEDWARDS_EXT_F0111') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'