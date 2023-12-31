{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='F0116',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'STG_JDE_ADDRESS_INCR', 'STG_JDE_ADDRESS_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ALAN8_BK" || '\#' ||  "EXT_SRC"."ALEFTB_BK" || '\#' )) AS "ADDRESS_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_ALAN8_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ALAN8_BK" || '\#' ||  "EXT_SRC"."ALEFTB_BK" || '\#' || 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_ALAN8_BK" || 
			'\#' )) AS "LNK_ADDRESS_ACCOUNT_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ALAN8" AS "ALAN8"
		, "EXT_SRC"."ALEFTB" AS "ALEFTB"
		, "EXT_SRC"."ALAN8_BK" AS "ALAN8_BK"
		, "EXT_SRC"."ALEFTB_BK" AS "ALEFTB_BK"
		, "EXT_SRC"."ABAN8_FK_ALAN8_BK" AS "ABAN8_FK_ALAN8_BK"
		, "EXT_SRC"."ALEFTF" AS "ALEFTF"
		, "EXT_SRC"."ALADD1" AS "ALADD1"
		, "EXT_SRC"."ALADD2" AS "ALADD2"
		, "EXT_SRC"."ALADD3" AS "ALADD3"
		, "EXT_SRC"."ALADD4" AS "ALADD4"
		, "EXT_SRC"."ALADDZ" AS "ALADDZ"
		, "EXT_SRC"."ALCTY1" AS "ALCTY1"
		, "EXT_SRC"."ALCOUN" AS "ALCOUN"
		, "EXT_SRC"."ALADDS" AS "ALADDS"
		, "EXT_SRC"."ALCRTE" AS "ALCRTE"
		, "EXT_SRC"."ALBKML" AS "ALBKML"
		, "EXT_SRC"."ALCTR" AS "ALCTR"
		, "EXT_SRC"."ALUSER" AS "ALUSER"
		, "EXT_SRC"."ALPID" AS "ALPID"
		, "EXT_SRC"."ALUPMJ" AS "ALUPMJ"
		, "EXT_SRC"."ALJOBN" AS "ALJOBN"
		, "EXT_SRC"."ALUPMT" AS "ALUPMT"
		, "EXT_SRC"."ALSYNCS" AS "ALSYNCS"
		, "EXT_SRC"."ALCAAD" AS "ALCAAD"
	FROM {{ ref('JDEDWARDS_EXT_F0116') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."ALAN8_BK" || '\#' ||  "EXT_SRC"."ALEFTB_BK" || '\#' )) AS "ADDRESS_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_ALAN8_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."ALAN8_BK" || '\#' ||  "EXT_SRC"."ALEFTB_BK" || '\#' || 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_ALAN8_BK" || 
			'\#' )) AS "LNK_ADDRESS_ACCOUNT_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."ALAN8" AS "ALAN8"
		, "EXT_SRC"."ALEFTB" AS "ALEFTB"
		, "EXT_SRC"."ALAN8_BK" AS "ALAN8_BK"
		, "EXT_SRC"."ALEFTB_BK" AS "ALEFTB_BK"
		, "EXT_SRC"."ABAN8_FK_ALAN8_BK" AS "ABAN8_FK_ALAN8_BK"
		, "EXT_SRC"."ALEFTF" AS "ALEFTF"
		, "EXT_SRC"."ALADD1" AS "ALADD1"
		, "EXT_SRC"."ALADD2" AS "ALADD2"
		, "EXT_SRC"."ALADD3" AS "ALADD3"
		, "EXT_SRC"."ALADD4" AS "ALADD4"
		, "EXT_SRC"."ALADDZ" AS "ALADDZ"
		, "EXT_SRC"."ALCTY1" AS "ALCTY1"
		, "EXT_SRC"."ALCOUN" AS "ALCOUN"
		, "EXT_SRC"."ALADDS" AS "ALADDS"
		, "EXT_SRC"."ALCRTE" AS "ALCRTE"
		, "EXT_SRC"."ALBKML" AS "ALBKML"
		, "EXT_SRC"."ALCTR" AS "ALCTR"
		, "EXT_SRC"."ALUSER" AS "ALUSER"
		, "EXT_SRC"."ALPID" AS "ALPID"
		, "EXT_SRC"."ALUPMJ" AS "ALUPMJ"
		, "EXT_SRC"."ALJOBN" AS "ALJOBN"
		, "EXT_SRC"."ALUPMT" AS "ALUPMT"
		, "EXT_SRC"."ALSYNCS" AS "ALSYNCS"
		, "EXT_SRC"."ALCAAD" AS "ALCAAD"
	FROM {{ ref('JDEDWARDS_EXT_F0116') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'