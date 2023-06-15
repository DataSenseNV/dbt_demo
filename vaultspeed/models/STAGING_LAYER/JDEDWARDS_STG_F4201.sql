{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='F4201',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'STG_JDE_SOHEADER_INCR', 'STG_JDE_SOHEADER_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."SHDOCO_BK" || '\#' ||  "EXT_SRC"."SHDCTO_BK" || '\#' ||  "EXT_SRC"."SHKCOO_BK" || 
			'\#' )) AS "SALESORDERHEADER_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_SHAN8_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."MCMCU_FK_SHMCU_BK" || '\#' )) AS "BUSINESSUNITMASTER_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."SHDOCO_BK" || '\#' ||  "EXT_SRC"."SHDCTO_BK" || '\#' ||  "EXT_SRC"."SHKCOO_BK" || 
			'\#' || 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_SHAN8_BK" || '\#' )) AS "LNK_SOHEADER_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."SHDOCO_BK" || '\#' ||  "EXT_SRC"."SHDCTO_BK" || '\#' ||  "EXT_SRC"."SHKCOO_BK" || 
			'\#' || "EXT_SRC"."MCMCU_FK_SHMCU_BK" || '\#' )) AS "LNK_SOHEADER_BUSINESSUNITMASTER_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."SHKCOO" AS "SHKCOO"
		, "EXT_SRC"."SHDOCO" AS "SHDOCO"
		, "EXT_SRC"."SHDCTO" AS "SHDCTO"
		, "EXT_SRC"."SHMCU" AS "SHMCU"
		, "EXT_SRC"."SHAN8" AS "SHAN8"
		, "EXT_SRC"."SHDOCO_BK" AS "SHDOCO_BK"
		, "EXT_SRC"."SHDCTO_BK" AS "SHDCTO_BK"
		, "EXT_SRC"."SHKCOO_BK" AS "SHKCOO_BK"
		, "EXT_SRC"."ABAN8_FK_SHAN8_BK" AS "ABAN8_FK_SHAN8_BK"
		, "EXT_SRC"."MCMCU_FK_SHMCU_BK" AS "MCMCU_FK_SHMCU_BK"
		, "EXT_SRC"."SHSFXO" AS "SHSFXO"
		, "EXT_SRC"."SHCO" AS "SHCO"
		, "EXT_SRC"."SHOKCO" AS "SHOKCO"
		, "EXT_SRC"."SHOORN" AS "SHOORN"
		, "EXT_SRC"."SHOCTO" AS "SHOCTO"
		, "EXT_SRC"."SHRKCO" AS "SHRKCO"
		, "EXT_SRC"."SHRORN" AS "SHRORN"
		, "EXT_SRC"."SHRCTO" AS "SHRCTO"
		, "EXT_SRC"."SHSHAN" AS "SHSHAN"
		, "EXT_SRC"."SHPA8" AS "SHPA8"
		, "EXT_SRC"."SHDRQJ" AS "SHDRQJ"
		, "EXT_SRC"."SHTRDJ" AS "SHTRDJ"
		, "EXT_SRC"."SHPDDJ" AS "SHPDDJ"
		, "EXT_SRC"."SHOPDJ" AS "SHOPDJ"
		, "EXT_SRC"."SHADDJ" AS "SHADDJ"
		, "EXT_SRC"."SHCNDJ" AS "SHCNDJ"
		, "EXT_SRC"."SHPEFJ" AS "SHPEFJ"
		, "EXT_SRC"."SHPPDJ" AS "SHPPDJ"
		, "EXT_SRC"."SHVR01" AS "SHVR01"
		, "EXT_SRC"."SHVR02" AS "SHVR02"
		, "EXT_SRC"."SHDEL1" AS "SHDEL1"
		, "EXT_SRC"."SHDEL2" AS "SHDEL2"
		, "EXT_SRC"."SHINMG" AS "SHINMG"
		, "EXT_SRC"."SHPTC" AS "SHPTC"
		, "EXT_SRC"."SHRYIN" AS "SHRYIN"
		, "EXT_SRC"."SHASN" AS "SHASN"
		, "EXT_SRC"."SHPRGP" AS "SHPRGP"
		, "EXT_SRC"."SHTRDC" AS "SHTRDC"
		, "EXT_SRC"."SHPCRT" AS "SHPCRT"
		, "EXT_SRC"."SHTXA1" AS "SHTXA1"
		, "EXT_SRC"."SHEXR1" AS "SHEXR1"
		, "EXT_SRC"."SHTXCT" AS "SHTXCT"
		, "EXT_SRC"."SHATXT" AS "SHATXT"
		, "EXT_SRC"."SHPRIO" AS "SHPRIO"
		, "EXT_SRC"."SHBACK" AS "SHBACK"
		, "EXT_SRC"."SHSBAL" AS "SHSBAL"
		, "EXT_SRC"."SHHOLD" AS "SHHOLD"
		, "EXT_SRC"."SHPLST" AS "SHPLST"
		, "EXT_SRC"."SHINVC" AS "SHINVC"
		, "EXT_SRC"."SHNTR" AS "SHNTR"
		, "EXT_SRC"."SHANBY" AS "SHANBY"
		, "EXT_SRC"."SHCARS" AS "SHCARS"
		, "EXT_SRC"."SHMOT" AS "SHMOT"
		, "EXT_SRC"."SHCOT" AS "SHCOT"
		, "EXT_SRC"."SHROUT" AS "SHROUT"
		, "EXT_SRC"."SHSTOP" AS "SHSTOP"
		, "EXT_SRC"."SHZON" AS "SHZON"
		, "EXT_SRC"."SHCNID" AS "SHCNID"
		, "EXT_SRC"."SHFRTH" AS "SHFRTH"
		, "EXT_SRC"."SHAFT" AS "SHAFT"
		, "EXT_SRC"."SHFUF1" AS "SHFUF1"
		, "EXT_SRC"."SHFRTC" AS "SHFRTC"
		, "EXT_SRC"."SHMORD" AS "SHMORD"
		, "EXT_SRC"."SHRCD" AS "SHRCD"
		, "EXT_SRC"."SHFUF2" AS "SHFUF2"
		, "EXT_SRC"."SHOTOT" AS "SHOTOT"
		, "EXT_SRC"."SHTOTC" AS "SHTOTC"
		, "EXT_SRC"."SHWUMD" AS "SHWUMD"
		, "EXT_SRC"."SHVUMD" AS "SHVUMD"
		, "EXT_SRC"."SHAUTN" AS "SHAUTN"
		, "EXT_SRC"."SHCACT" AS "SHCACT"
		, "EXT_SRC"."SHCEXP" AS "SHCEXP"
		, "EXT_SRC"."SHSBLI" AS "SHSBLI"
		, "EXT_SRC"."SHCRMD" AS "SHCRMD"
		, "EXT_SRC"."SHCRRM" AS "SHCRRM"
		, "EXT_SRC"."SHCRCD" AS "SHCRCD"
		, "EXT_SRC"."SHCRR" AS "SHCRR"
		, "EXT_SRC"."SHLNGP" AS "SHLNGP"
		, "EXT_SRC"."SHFAP" AS "SHFAP"
		, "EXT_SRC"."SHFCST" AS "SHFCST"
		, "EXT_SRC"."SHORBY" AS "SHORBY"
		, "EXT_SRC"."SHTKBY" AS "SHTKBY"
		, "EXT_SRC"."SHURCD" AS "SHURCD"
		, "EXT_SRC"."SHURDT" AS "SHURDT"
		, "EXT_SRC"."SHURAT" AS "SHURAT"
		, "EXT_SRC"."SHURAB" AS "SHURAB"
		, "EXT_SRC"."SHURRF" AS "SHURRF"
		, "EXT_SRC"."SHUSER" AS "SHUSER"
		, "EXT_SRC"."SHPID" AS "SHPID"
		, "EXT_SRC"."SHJOBN" AS "SHJOBN"
		, "EXT_SRC"."SHUPMJ" AS "SHUPMJ"
		, "EXT_SRC"."SHTDAY" AS "SHTDAY"
		, "EXT_SRC"."SHIR01" AS "SHIR01"
		, "EXT_SRC"."SHIR02" AS "SHIR02"
		, "EXT_SRC"."SHIR03" AS "SHIR03"
		, "EXT_SRC"."SHIR04" AS "SHIR04"
		, "EXT_SRC"."SHIR05" AS "SHIR05"
		, "EXT_SRC"."SHVR03" AS "SHVR03"
		, "EXT_SRC"."SHSOOR" AS "SHSOOR"
		, "EXT_SRC"."SHPMDT" AS "SHPMDT"
		, "EXT_SRC"."SHRSDT" AS "SHRSDT"
		, "EXT_SRC"."SHRQSJ" AS "SHRQSJ"
		, "EXT_SRC"."SHPSTM" AS "SHPSTM"
		, "EXT_SRC"."SHPDTT" AS "SHPDTT"
		, "EXT_SRC"."SHOPTT" AS "SHOPTT"
		, "EXT_SRC"."SHDRQT" AS "SHDRQT"
		, "EXT_SRC"."SHADTM" AS "SHADTM"
		, "EXT_SRC"."SHADLJ" AS "SHADLJ"
		, "EXT_SRC"."SHPBAN" AS "SHPBAN"
		, "EXT_SRC"."SHITAN" AS "SHITAN"
		, "EXT_SRC"."SHFTAN" AS "SHFTAN"
		, "EXT_SRC"."SHDVAN" AS "SHDVAN"
		, "EXT_SRC"."SHDOC1" AS "SHDOC1"
		, "EXT_SRC"."SHDCT4" AS "SHDCT4"
		, "EXT_SRC"."SHCORD" AS "SHCORD"
		, "EXT_SRC"."SHBSC" AS "SHBSC"
		, "EXT_SRC"."SHBCRC" AS "SHBCRC"
		, "EXT_SRC"."SHAUFT" AS "SHAUFT"
		, "EXT_SRC"."SHAUFI" AS "SHAUFI"
		, "EXT_SRC"."SHOPBO" AS "SHOPBO"
		, "EXT_SRC"."SHOPTC" AS "SHOPTC"
		, "EXT_SRC"."SHOPLD" AS "SHOPLD"
		, "EXT_SRC"."SHOPBK" AS "SHOPBK"
		, "EXT_SRC"."SHOPSB" AS "SHOPSB"
		, "EXT_SRC"."SHOPPS" AS "SHOPPS"
		, "EXT_SRC"."SHOPPL" AS "SHOPPL"
		, "EXT_SRC"."SHOPMS" AS "SHOPMS"
		, "EXT_SRC"."SHOPSS" AS "SHOPSS"
		, "EXT_SRC"."SHOPBA" AS "SHOPBA"
		, "EXT_SRC"."SHOPLL" AS "SHOPLL"
		, "EXT_SRC"."SHPRAN8" AS "SHPRAN8"
		, "EXT_SRC"."SHOPPID" AS "SHOPPID"
		, "EXT_SRC"."SHSDATTN" AS "SHSDATTN"
		, "EXT_SRC"."SHSPATTN" AS "SHSPATTN"
		, "EXT_SRC"."SHOTIND" AS "SHOTIND"
		, "EXT_SRC"."SHPRCIDLN" AS "SHPRCIDLN"
		, "EXT_SRC"."SHCCIDLN" AS "SHCCIDLN"
		, "EXT_SRC"."SHSHCCIDLN" AS "SHSHCCIDLN"
	FROM {{ ref('JDEDWARDS_EXT_F4201') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."SHDOCO_BK" || '\#' ||  "EXT_SRC"."SHDCTO_BK" || '\#' ||  "EXT_SRC"."SHKCOO_BK" || 
			'\#' )) AS "SALESORDERHEADER_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_SHAN8_BK" || '\#' )) AS "ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."MCMCU_FK_SHMCU_BK" || '\#' )) AS "BUSINESSUNITMASTER_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."SHDOCO_BK" || '\#' ||  "EXT_SRC"."SHDCTO_BK" || '\#' ||  "EXT_SRC"."SHKCOO_BK" || 
			'\#' || 'JDE' || '\#' || "EXT_SRC"."ABAN8_FK_SHAN8_BK" || '\#' )) AS "LNK_SOHEADER_ACCOUNT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."SHDOCO_BK" || '\#' ||  "EXT_SRC"."SHDCTO_BK" || '\#' ||  "EXT_SRC"."SHKCOO_BK" || 
			'\#' || "EXT_SRC"."MCMCU_FK_SHMCU_BK" || '\#' )) AS "LNK_SOHEADER_BUSINESSUNITMASTER_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."SHKCOO" AS "SHKCOO"
		, "EXT_SRC"."SHDOCO" AS "SHDOCO"
		, "EXT_SRC"."SHDCTO" AS "SHDCTO"
		, "EXT_SRC"."SHMCU" AS "SHMCU"
		, "EXT_SRC"."SHAN8" AS "SHAN8"
		, "EXT_SRC"."SHDOCO_BK" AS "SHDOCO_BK"
		, "EXT_SRC"."SHDCTO_BK" AS "SHDCTO_BK"
		, "EXT_SRC"."SHKCOO_BK" AS "SHKCOO_BK"
		, "EXT_SRC"."ABAN8_FK_SHAN8_BK" AS "ABAN8_FK_SHAN8_BK"
		, "EXT_SRC"."MCMCU_FK_SHMCU_BK" AS "MCMCU_FK_SHMCU_BK"
		, "EXT_SRC"."SHSFXO" AS "SHSFXO"
		, "EXT_SRC"."SHCO" AS "SHCO"
		, "EXT_SRC"."SHOKCO" AS "SHOKCO"
		, "EXT_SRC"."SHOORN" AS "SHOORN"
		, "EXT_SRC"."SHOCTO" AS "SHOCTO"
		, "EXT_SRC"."SHRKCO" AS "SHRKCO"
		, "EXT_SRC"."SHRORN" AS "SHRORN"
		, "EXT_SRC"."SHRCTO" AS "SHRCTO"
		, "EXT_SRC"."SHSHAN" AS "SHSHAN"
		, "EXT_SRC"."SHPA8" AS "SHPA8"
		, "EXT_SRC"."SHDRQJ" AS "SHDRQJ"
		, "EXT_SRC"."SHTRDJ" AS "SHTRDJ"
		, "EXT_SRC"."SHPDDJ" AS "SHPDDJ"
		, "EXT_SRC"."SHOPDJ" AS "SHOPDJ"
		, "EXT_SRC"."SHADDJ" AS "SHADDJ"
		, "EXT_SRC"."SHCNDJ" AS "SHCNDJ"
		, "EXT_SRC"."SHPEFJ" AS "SHPEFJ"
		, "EXT_SRC"."SHPPDJ" AS "SHPPDJ"
		, "EXT_SRC"."SHVR01" AS "SHVR01"
		, "EXT_SRC"."SHVR02" AS "SHVR02"
		, "EXT_SRC"."SHDEL1" AS "SHDEL1"
		, "EXT_SRC"."SHDEL2" AS "SHDEL2"
		, "EXT_SRC"."SHINMG" AS "SHINMG"
		, "EXT_SRC"."SHPTC" AS "SHPTC"
		, "EXT_SRC"."SHRYIN" AS "SHRYIN"
		, "EXT_SRC"."SHASN" AS "SHASN"
		, "EXT_SRC"."SHPRGP" AS "SHPRGP"
		, "EXT_SRC"."SHTRDC" AS "SHTRDC"
		, "EXT_SRC"."SHPCRT" AS "SHPCRT"
		, "EXT_SRC"."SHTXA1" AS "SHTXA1"
		, "EXT_SRC"."SHEXR1" AS "SHEXR1"
		, "EXT_SRC"."SHTXCT" AS "SHTXCT"
		, "EXT_SRC"."SHATXT" AS "SHATXT"
		, "EXT_SRC"."SHPRIO" AS "SHPRIO"
		, "EXT_SRC"."SHBACK" AS "SHBACK"
		, "EXT_SRC"."SHSBAL" AS "SHSBAL"
		, "EXT_SRC"."SHHOLD" AS "SHHOLD"
		, "EXT_SRC"."SHPLST" AS "SHPLST"
		, "EXT_SRC"."SHINVC" AS "SHINVC"
		, "EXT_SRC"."SHNTR" AS "SHNTR"
		, "EXT_SRC"."SHANBY" AS "SHANBY"
		, "EXT_SRC"."SHCARS" AS "SHCARS"
		, "EXT_SRC"."SHMOT" AS "SHMOT"
		, "EXT_SRC"."SHCOT" AS "SHCOT"
		, "EXT_SRC"."SHROUT" AS "SHROUT"
		, "EXT_SRC"."SHSTOP" AS "SHSTOP"
		, "EXT_SRC"."SHZON" AS "SHZON"
		, "EXT_SRC"."SHCNID" AS "SHCNID"
		, "EXT_SRC"."SHFRTH" AS "SHFRTH"
		, "EXT_SRC"."SHAFT" AS "SHAFT"
		, "EXT_SRC"."SHFUF1" AS "SHFUF1"
		, "EXT_SRC"."SHFRTC" AS "SHFRTC"
		, "EXT_SRC"."SHMORD" AS "SHMORD"
		, "EXT_SRC"."SHRCD" AS "SHRCD"
		, "EXT_SRC"."SHFUF2" AS "SHFUF2"
		, "EXT_SRC"."SHOTOT" AS "SHOTOT"
		, "EXT_SRC"."SHTOTC" AS "SHTOTC"
		, "EXT_SRC"."SHWUMD" AS "SHWUMD"
		, "EXT_SRC"."SHVUMD" AS "SHVUMD"
		, "EXT_SRC"."SHAUTN" AS "SHAUTN"
		, "EXT_SRC"."SHCACT" AS "SHCACT"
		, "EXT_SRC"."SHCEXP" AS "SHCEXP"
		, "EXT_SRC"."SHSBLI" AS "SHSBLI"
		, "EXT_SRC"."SHCRMD" AS "SHCRMD"
		, "EXT_SRC"."SHCRRM" AS "SHCRRM"
		, "EXT_SRC"."SHCRCD" AS "SHCRCD"
		, "EXT_SRC"."SHCRR" AS "SHCRR"
		, "EXT_SRC"."SHLNGP" AS "SHLNGP"
		, "EXT_SRC"."SHFAP" AS "SHFAP"
		, "EXT_SRC"."SHFCST" AS "SHFCST"
		, "EXT_SRC"."SHORBY" AS "SHORBY"
		, "EXT_SRC"."SHTKBY" AS "SHTKBY"
		, "EXT_SRC"."SHURCD" AS "SHURCD"
		, "EXT_SRC"."SHURDT" AS "SHURDT"
		, "EXT_SRC"."SHURAT" AS "SHURAT"
		, "EXT_SRC"."SHURAB" AS "SHURAB"
		, "EXT_SRC"."SHURRF" AS "SHURRF"
		, "EXT_SRC"."SHUSER" AS "SHUSER"
		, "EXT_SRC"."SHPID" AS "SHPID"
		, "EXT_SRC"."SHJOBN" AS "SHJOBN"
		, "EXT_SRC"."SHUPMJ" AS "SHUPMJ"
		, "EXT_SRC"."SHTDAY" AS "SHTDAY"
		, "EXT_SRC"."SHIR01" AS "SHIR01"
		, "EXT_SRC"."SHIR02" AS "SHIR02"
		, "EXT_SRC"."SHIR03" AS "SHIR03"
		, "EXT_SRC"."SHIR04" AS "SHIR04"
		, "EXT_SRC"."SHIR05" AS "SHIR05"
		, "EXT_SRC"."SHVR03" AS "SHVR03"
		, "EXT_SRC"."SHSOOR" AS "SHSOOR"
		, "EXT_SRC"."SHPMDT" AS "SHPMDT"
		, "EXT_SRC"."SHRSDT" AS "SHRSDT"
		, "EXT_SRC"."SHRQSJ" AS "SHRQSJ"
		, "EXT_SRC"."SHPSTM" AS "SHPSTM"
		, "EXT_SRC"."SHPDTT" AS "SHPDTT"
		, "EXT_SRC"."SHOPTT" AS "SHOPTT"
		, "EXT_SRC"."SHDRQT" AS "SHDRQT"
		, "EXT_SRC"."SHADTM" AS "SHADTM"
		, "EXT_SRC"."SHADLJ" AS "SHADLJ"
		, "EXT_SRC"."SHPBAN" AS "SHPBAN"
		, "EXT_SRC"."SHITAN" AS "SHITAN"
		, "EXT_SRC"."SHFTAN" AS "SHFTAN"
		, "EXT_SRC"."SHDVAN" AS "SHDVAN"
		, "EXT_SRC"."SHDOC1" AS "SHDOC1"
		, "EXT_SRC"."SHDCT4" AS "SHDCT4"
		, "EXT_SRC"."SHCORD" AS "SHCORD"
		, "EXT_SRC"."SHBSC" AS "SHBSC"
		, "EXT_SRC"."SHBCRC" AS "SHBCRC"
		, "EXT_SRC"."SHAUFT" AS "SHAUFT"
		, "EXT_SRC"."SHAUFI" AS "SHAUFI"
		, "EXT_SRC"."SHOPBO" AS "SHOPBO"
		, "EXT_SRC"."SHOPTC" AS "SHOPTC"
		, "EXT_SRC"."SHOPLD" AS "SHOPLD"
		, "EXT_SRC"."SHOPBK" AS "SHOPBK"
		, "EXT_SRC"."SHOPSB" AS "SHOPSB"
		, "EXT_SRC"."SHOPPS" AS "SHOPPS"
		, "EXT_SRC"."SHOPPL" AS "SHOPPL"
		, "EXT_SRC"."SHOPMS" AS "SHOPMS"
		, "EXT_SRC"."SHOPSS" AS "SHOPSS"
		, "EXT_SRC"."SHOPBA" AS "SHOPBA"
		, "EXT_SRC"."SHOPLL" AS "SHOPLL"
		, "EXT_SRC"."SHPRAN8" AS "SHPRAN8"
		, "EXT_SRC"."SHOPPID" AS "SHOPPID"
		, "EXT_SRC"."SHSDATTN" AS "SHSDATTN"
		, "EXT_SRC"."SHSPATTN" AS "SHSPATTN"
		, "EXT_SRC"."SHOTIND" AS "SHOTIND"
		, "EXT_SRC"."SHPRCIDLN" AS "SHPRCIDLN"
		, "EXT_SRC"."SHCCIDLN" AS "SHCCIDLN"
		, "EXT_SRC"."SHSHCCIDLN" AS "SHSHCCIDLN"
	FROM {{ ref('JDEDWARDS_EXT_F4201') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'