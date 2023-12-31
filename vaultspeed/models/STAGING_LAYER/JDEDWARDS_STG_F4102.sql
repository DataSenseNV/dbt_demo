{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='F4102',
		schema='JDEDWARDS_STG',
		tags=['JDEDWARDS', 'STG_JDE_ITEMBRANCH_INCR', 'STG_JDE_ITEMBRANCH_INIT']
	)
}}
select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."IBMCU_BK" || '\#' ||  "EXT_SRC"."IBITM_BK" || '\#' )) AS "ITEMBRANCH_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."MCMCU_FK_IBMCU_BK" || '\#' )) AS "BUSINESSUNITMASTER_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."IMITM_FK_IBITM_BK" || '\#' )) AS "PRODUCT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."IBMCU_BK" || '\#' ||  "EXT_SRC"."IBITM_BK" || '\#' || "EXT_SRC"."MCMCU_FK_IBMCU_BK" || 
			'\#' )) AS "LNK_ITEMBRANCH_BUSINESSUNITMASTER_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."IBMCU_BK" || '\#' ||  "EXT_SRC"."IBITM_BK" || '\#' || 'JDE' || '\#' || "EXT_SRC"."IMITM_FK_IBITM_BK" || 
			'\#' )) AS "LNK_ITEMBRANCH_PRODUCT_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."IBITM" AS "IBITM"
		, "EXT_SRC"."IBMCU" AS "IBMCU"
		, "EXT_SRC"."IBMCU_BK" AS "IBMCU_BK"
		, "EXT_SRC"."IBITM_BK" AS "IBITM_BK"
		, "EXT_SRC"."MCMCU_FK_IBMCU_BK" AS "MCMCU_FK_IBMCU_BK"
		, "EXT_SRC"."IMITM_FK_IBITM_BK" AS "IMITM_FK_IBITM_BK"
		, "EXT_SRC"."IBLITM" AS "IBLITM"
		, "EXT_SRC"."IBAITM" AS "IBAITM"
		, "EXT_SRC"."IBSRP1" AS "IBSRP1"
		, "EXT_SRC"."IBSRP2" AS "IBSRP2"
		, "EXT_SRC"."IBSRP3" AS "IBSRP3"
		, "EXT_SRC"."IBSRP4" AS "IBSRP4"
		, "EXT_SRC"."IBSRP5" AS "IBSRP5"
		, "EXT_SRC"."IBSRP6" AS "IBSRP6"
		, "EXT_SRC"."IBSRP7" AS "IBSRP7"
		, "EXT_SRC"."IBSRP8" AS "IBSRP8"
		, "EXT_SRC"."IBSRP9" AS "IBSRP9"
		, "EXT_SRC"."IBSRP0" AS "IBSRP0"
		, "EXT_SRC"."IBPRP1" AS "IBPRP1"
		, "EXT_SRC"."IBPRP2" AS "IBPRP2"
		, "EXT_SRC"."IBPRP3" AS "IBPRP3"
		, "EXT_SRC"."IBPRP4" AS "IBPRP4"
		, "EXT_SRC"."IBPRP5" AS "IBPRP5"
		, "EXT_SRC"."IBPRP6" AS "IBPRP6"
		, "EXT_SRC"."IBPRP7" AS "IBPRP7"
		, "EXT_SRC"."IBPRP8" AS "IBPRP8"
		, "EXT_SRC"."IBPRP9" AS "IBPRP9"
		, "EXT_SRC"."IBPRP0" AS "IBPRP0"
		, "EXT_SRC"."IBCDCD" AS "IBCDCD"
		, "EXT_SRC"."IBPDGR" AS "IBPDGR"
		, "EXT_SRC"."IBDSGP" AS "IBDSGP"
		, "EXT_SRC"."IBVEND" AS "IBVEND"
		, "EXT_SRC"."IBANPL" AS "IBANPL"
		, "EXT_SRC"."IBBUYR" AS "IBBUYR"
		, "EXT_SRC"."IBGLPT" AS "IBGLPT"
		, "EXT_SRC"."IBORIG" AS "IBORIG"
		, "EXT_SRC"."IBROPI" AS "IBROPI"
		, "EXT_SRC"."IBROQI" AS "IBROQI"
		, "EXT_SRC"."IBRQMX" AS "IBRQMX"
		, "EXT_SRC"."IBRQMN" AS "IBRQMN"
		, "EXT_SRC"."IBWOMO" AS "IBWOMO"
		, "EXT_SRC"."IBSERV" AS "IBSERV"
		, "EXT_SRC"."IBSAFE" AS "IBSAFE"
		, "EXT_SRC"."IBSLD" AS "IBSLD"
		, "EXT_SRC"."IBCKAV" AS "IBCKAV"
		, "EXT_SRC"."IBSRCE" AS "IBSRCE"
		, "EXT_SRC"."IBLOTS" AS "IBLOTS"
		, "EXT_SRC"."IBOT1Y" AS "IBOT1Y"
		, "EXT_SRC"."IBOT2Y" AS "IBOT2Y"
		, "EXT_SRC"."IBSTDP" AS "IBSTDP"
		, "EXT_SRC"."IBFRMP" AS "IBFRMP"
		, "EXT_SRC"."IBTHRP" AS "IBTHRP"
		, "EXT_SRC"."IBSTDG" AS "IBSTDG"
		, "EXT_SRC"."IBFRGD" AS "IBFRGD"
		, "EXT_SRC"."IBTHGD" AS "IBTHGD"
		, "EXT_SRC"."IBCOTY" AS "IBCOTY"
		, "EXT_SRC"."IBMMPC" AS "IBMMPC"
		, "EXT_SRC"."IBPRGR" AS "IBPRGR"
		, "EXT_SRC"."IBRPRC" AS "IBRPRC"
		, "EXT_SRC"."IBORPR" AS "IBORPR"
		, "EXT_SRC"."IBBACK" AS "IBBACK"
		, "EXT_SRC"."IBIFLA" AS "IBIFLA"
		, "EXT_SRC"."IBABCS" AS "IBABCS"
		, "EXT_SRC"."IBABCM" AS "IBABCM"
		, "EXT_SRC"."IBABCI" AS "IBABCI"
		, "EXT_SRC"."IBOVR" AS "IBOVR"
		, "EXT_SRC"."IBSHCM" AS "IBSHCM"
		, "EXT_SRC"."IBCARS" AS "IBCARS"
		, "EXT_SRC"."IBCARP" AS "IBCARP"
		, "EXT_SRC"."IBSHCN" AS "IBSHCN"
		, "EXT_SRC"."IBSTKT" AS "IBSTKT"
		, "EXT_SRC"."IBLNTY" AS "IBLNTY"
		, "EXT_SRC"."IBFIFO" AS "IBFIFO"
		, "EXT_SRC"."IBCYCL" AS "IBCYCL"
		, "EXT_SRC"."IBINMG" AS "IBINMG"
		, "EXT_SRC"."IBWARR" AS "IBWARR"
		, "EXT_SRC"."IBSRNR" AS "IBSRNR"
		, "EXT_SRC"."IBPCTM" AS "IBPCTM"
		, "EXT_SRC"."IBCMCG" AS "IBCMCG"
		, "EXT_SRC"."IBFUF1" AS "IBFUF1"
		, "EXT_SRC"."IBTX" AS "IBTX"
		, "EXT_SRC"."IBTAX1" AS "IBTAX1"
		, "EXT_SRC"."IBMPST" AS "IBMPST"
		, "EXT_SRC"."IBMRPD" AS "IBMRPD"
		, "EXT_SRC"."IBMRPC" AS "IBMRPC"
		, "EXT_SRC"."IBUPC" AS "IBUPC"
		, "EXT_SRC"."IBSNS" AS "IBSNS"
		, "EXT_SRC"."IBMERL" AS "IBMERL"
		, "EXT_SRC"."IBLTLV" AS "IBLTLV"
		, "EXT_SRC"."IBLTMF" AS "IBLTMF"
		, "EXT_SRC"."IBLTCM" AS "IBLTCM"
		, "EXT_SRC"."IBOPC" AS "IBOPC"
		, "EXT_SRC"."IBOPV" AS "IBOPV"
		, "EXT_SRC"."IBACQ" AS "IBACQ"
		, "EXT_SRC"."IBMLQ" AS "IBMLQ"
		, "EXT_SRC"."IBLTPU" AS "IBLTPU"
		, "EXT_SRC"."IBMPSP" AS "IBMPSP"
		, "EXT_SRC"."IBMRPP" AS "IBMRPP"
		, "EXT_SRC"."IBITC" AS "IBITC"
		, "EXT_SRC"."IBECO" AS "IBECO"
		, "EXT_SRC"."IBECTY" AS "IBECTY"
		, "EXT_SRC"."IBECOD" AS "IBECOD"
		, "EXT_SRC"."IBMTF1" AS "IBMTF1"
		, "EXT_SRC"."IBMTF2" AS "IBMTF2"
		, "EXT_SRC"."IBMTF3" AS "IBMTF3"
		, "EXT_SRC"."IBMTF4" AS "IBMTF4"
		, "EXT_SRC"."IBMTF5" AS "IBMTF5"
		, "EXT_SRC"."IBMOVD" AS "IBMOVD"
		, "EXT_SRC"."IBQUED" AS "IBQUED"
		, "EXT_SRC"."IBSETL" AS "IBSETL"
		, "EXT_SRC"."IBSRNK" AS "IBSRNK"
		, "EXT_SRC"."IBSRKF" AS "IBSRKF"
		, "EXT_SRC"."IBTIMB" AS "IBTIMB"
		, "EXT_SRC"."IBBQTY" AS "IBBQTY"
		, "EXT_SRC"."IBORDW" AS "IBORDW"
		, "EXT_SRC"."IBEXPD" AS "IBEXPD"
		, "EXT_SRC"."IBDEFD" AS "IBDEFD"
		, "EXT_SRC"."IBMULT" AS "IBMULT"
		, "EXT_SRC"."IBSFLT" AS "IBSFLT"
		, "EXT_SRC"."IBMAKE" AS "IBMAKE"
		, "EXT_SRC"."IBLFDJ" AS "IBLFDJ"
		, "EXT_SRC"."IBLLX" AS "IBLLX"
		, "EXT_SRC"."IBCMGL" AS "IBCMGL"
		, "EXT_SRC"."IBURCD" AS "IBURCD"
		, "EXT_SRC"."IBURDT" AS "IBURDT"
		, "EXT_SRC"."IBURAT" AS "IBURAT"
		, "EXT_SRC"."IBURAB" AS "IBURAB"
		, "EXT_SRC"."IBURRF" AS "IBURRF"
		, "EXT_SRC"."IBUSER" AS "IBUSER"
		, "EXT_SRC"."IBPID" AS "IBPID"
		, "EXT_SRC"."IBJOBN" AS "IBJOBN"
		, "EXT_SRC"."IBUPMJ" AS "IBUPMJ"
		, "EXT_SRC"."IBTDAY" AS "IBTDAY"
		, "EXT_SRC"."IBTFLA" AS "IBTFLA"
		, "EXT_SRC"."IBCOMH" AS "IBCOMH"
		, "EXT_SRC"."IBAVRT" AS "IBAVRT"
		, "EXT_SRC"."IBPOC" AS "IBPOC"
		, "EXT_SRC"."IBAING" AS "IBAING"
		, "EXT_SRC"."IBBBDD" AS "IBBBDD"
		, "EXT_SRC"."IBCMDM" AS "IBCMDM"
		, "EXT_SRC"."IBLECM" AS "IBLECM"
		, "EXT_SRC"."IBLEDD" AS "IBLEDD"
		, "EXT_SRC"."IBMLOT" AS "IBMLOT"
		, "EXT_SRC"."IBPEFD" AS "IBPEFD"
		, "EXT_SRC"."IBSBDD" AS "IBSBDD"
		, "EXT_SRC"."IBU1DD" AS "IBU1DD"
		, "EXT_SRC"."IBU2DD" AS "IBU2DD"
		, "EXT_SRC"."IBU3DD" AS "IBU3DD"
		, "EXT_SRC"."IBU4DD" AS "IBU4DD"
		, "EXT_SRC"."IBU5DD" AS "IBU5DD"
		, "EXT_SRC"."IBXDCK" AS "IBXDCK"
		, "EXT_SRC"."IBLAF" AS "IBLAF"
		, "EXT_SRC"."IBLTFM" AS "IBLTFM"
		, "EXT_SRC"."IBRWLA" AS "IBRWLA"
		, "EXT_SRC"."IBLNPA" AS "IBLNPA"
		, "EXT_SRC"."IBLOTC" AS "IBLOTC"
		, "EXT_SRC"."IBAPSC" AS "IBAPSC"
		, "EXT_SRC"."IBPRI1" AS "IBPRI1"
		, "EXT_SRC"."IBPRI2" AS "IBPRI2"
		, "EXT_SRC"."IBLTCV" AS "IBLTCV"
		, "EXT_SRC"."IBASHL" AS "IBASHL"
		, "EXT_SRC"."IBOPTH" AS "IBOPTH"
		, "EXT_SRC"."IBCUTH" AS "IBCUTH"
		, "EXT_SRC"."IBUMTH" AS "IBUMTH"
		, "EXT_SRC"."IBLMFG" AS "IBLMFG"
		, "EXT_SRC"."IBLINE" AS "IBLINE"
		, "EXT_SRC"."IBDFTPCT" AS "IBDFTPCT"
		, "EXT_SRC"."IBKBIT" AS "IBKBIT"
		, "EXT_SRC"."IBDFENDITM" AS "IBDFENDITM"
		, "EXT_SRC"."IBKANEXLL" AS "IBKANEXLL"
		, "EXT_SRC"."IBSCPSELL" AS "IBSCPSELL"
		, "EXT_SRC"."IBMOPTH" AS "IBMOPTH"
		, "EXT_SRC"."IBMCUTH" AS "IBMCUTH"
		, "EXT_SRC"."IBCUMTH" AS "IBCUMTH"
		, "EXT_SRC"."IBATPRN" AS "IBATPRN"
		, "EXT_SRC"."IBATPCA" AS "IBATPCA"
		, "EXT_SRC"."IBATPAC" AS "IBATPAC"
		, "EXT_SRC"."IBCOORE" AS "IBCOORE"
		, "EXT_SRC"."IBVCPFC" AS "IBVCPFC"
		, "EXT_SRC"."IBPNYN" AS "IBPNYN"
	FROM {{ ref('JDEDWARDS_EXT_F4102') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INCR' and '{{ var("source") }}' = 'JDEDWARDS'

UNION ALL

select * from (
	SELECT 
		  UPPER(MD5_HEX( "EXT_SRC"."IBMCU_BK" || '\#' ||  "EXT_SRC"."IBITM_BK" || '\#' )) AS "ITEMBRANCH_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."MCMCU_FK_IBMCU_BK" || '\#' )) AS "BUSINESSUNITMASTER_HKEY"
		, UPPER(MD5_HEX( 'JDE' || '\#' || "EXT_SRC"."IMITM_FK_IBITM_BK" || '\#' )) AS "PRODUCT_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."IBMCU_BK" || '\#' ||  "EXT_SRC"."IBITM_BK" || '\#' || "EXT_SRC"."MCMCU_FK_IBMCU_BK" || 
			'\#' )) AS "LNK_ITEMBRANCH_BUSINESSUNITMASTER_HKEY"
		, UPPER(MD5_HEX( "EXT_SRC"."IBMCU_BK" || '\#' ||  "EXT_SRC"."IBITM_BK" || '\#' || 'JDE' || '\#' || "EXT_SRC"."IMITM_FK_IBITM_BK" || 
			'\#' )) AS "LNK_ITEMBRANCH_PRODUCT_HKEY"
		, "EXT_SRC"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_SRC"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_SRC"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_SRC"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_SRC"."IBITM" AS "IBITM"
		, "EXT_SRC"."IBMCU" AS "IBMCU"
		, "EXT_SRC"."IBMCU_BK" AS "IBMCU_BK"
		, "EXT_SRC"."IBITM_BK" AS "IBITM_BK"
		, "EXT_SRC"."MCMCU_FK_IBMCU_BK" AS "MCMCU_FK_IBMCU_BK"
		, "EXT_SRC"."IMITM_FK_IBITM_BK" AS "IMITM_FK_IBITM_BK"
		, "EXT_SRC"."IBLITM" AS "IBLITM"
		, "EXT_SRC"."IBAITM" AS "IBAITM"
		, "EXT_SRC"."IBSRP1" AS "IBSRP1"
		, "EXT_SRC"."IBSRP2" AS "IBSRP2"
		, "EXT_SRC"."IBSRP3" AS "IBSRP3"
		, "EXT_SRC"."IBSRP4" AS "IBSRP4"
		, "EXT_SRC"."IBSRP5" AS "IBSRP5"
		, "EXT_SRC"."IBSRP6" AS "IBSRP6"
		, "EXT_SRC"."IBSRP7" AS "IBSRP7"
		, "EXT_SRC"."IBSRP8" AS "IBSRP8"
		, "EXT_SRC"."IBSRP9" AS "IBSRP9"
		, "EXT_SRC"."IBSRP0" AS "IBSRP0"
		, "EXT_SRC"."IBPRP1" AS "IBPRP1"
		, "EXT_SRC"."IBPRP2" AS "IBPRP2"
		, "EXT_SRC"."IBPRP3" AS "IBPRP3"
		, "EXT_SRC"."IBPRP4" AS "IBPRP4"
		, "EXT_SRC"."IBPRP5" AS "IBPRP5"
		, "EXT_SRC"."IBPRP6" AS "IBPRP6"
		, "EXT_SRC"."IBPRP7" AS "IBPRP7"
		, "EXT_SRC"."IBPRP8" AS "IBPRP8"
		, "EXT_SRC"."IBPRP9" AS "IBPRP9"
		, "EXT_SRC"."IBPRP0" AS "IBPRP0"
		, "EXT_SRC"."IBCDCD" AS "IBCDCD"
		, "EXT_SRC"."IBPDGR" AS "IBPDGR"
		, "EXT_SRC"."IBDSGP" AS "IBDSGP"
		, "EXT_SRC"."IBVEND" AS "IBVEND"
		, "EXT_SRC"."IBANPL" AS "IBANPL"
		, "EXT_SRC"."IBBUYR" AS "IBBUYR"
		, "EXT_SRC"."IBGLPT" AS "IBGLPT"
		, "EXT_SRC"."IBORIG" AS "IBORIG"
		, "EXT_SRC"."IBROPI" AS "IBROPI"
		, "EXT_SRC"."IBROQI" AS "IBROQI"
		, "EXT_SRC"."IBRQMX" AS "IBRQMX"
		, "EXT_SRC"."IBRQMN" AS "IBRQMN"
		, "EXT_SRC"."IBWOMO" AS "IBWOMO"
		, "EXT_SRC"."IBSERV" AS "IBSERV"
		, "EXT_SRC"."IBSAFE" AS "IBSAFE"
		, "EXT_SRC"."IBSLD" AS "IBSLD"
		, "EXT_SRC"."IBCKAV" AS "IBCKAV"
		, "EXT_SRC"."IBSRCE" AS "IBSRCE"
		, "EXT_SRC"."IBLOTS" AS "IBLOTS"
		, "EXT_SRC"."IBOT1Y" AS "IBOT1Y"
		, "EXT_SRC"."IBOT2Y" AS "IBOT2Y"
		, "EXT_SRC"."IBSTDP" AS "IBSTDP"
		, "EXT_SRC"."IBFRMP" AS "IBFRMP"
		, "EXT_SRC"."IBTHRP" AS "IBTHRP"
		, "EXT_SRC"."IBSTDG" AS "IBSTDG"
		, "EXT_SRC"."IBFRGD" AS "IBFRGD"
		, "EXT_SRC"."IBTHGD" AS "IBTHGD"
		, "EXT_SRC"."IBCOTY" AS "IBCOTY"
		, "EXT_SRC"."IBMMPC" AS "IBMMPC"
		, "EXT_SRC"."IBPRGR" AS "IBPRGR"
		, "EXT_SRC"."IBRPRC" AS "IBRPRC"
		, "EXT_SRC"."IBORPR" AS "IBORPR"
		, "EXT_SRC"."IBBACK" AS "IBBACK"
		, "EXT_SRC"."IBIFLA" AS "IBIFLA"
		, "EXT_SRC"."IBABCS" AS "IBABCS"
		, "EXT_SRC"."IBABCM" AS "IBABCM"
		, "EXT_SRC"."IBABCI" AS "IBABCI"
		, "EXT_SRC"."IBOVR" AS "IBOVR"
		, "EXT_SRC"."IBSHCM" AS "IBSHCM"
		, "EXT_SRC"."IBCARS" AS "IBCARS"
		, "EXT_SRC"."IBCARP" AS "IBCARP"
		, "EXT_SRC"."IBSHCN" AS "IBSHCN"
		, "EXT_SRC"."IBSTKT" AS "IBSTKT"
		, "EXT_SRC"."IBLNTY" AS "IBLNTY"
		, "EXT_SRC"."IBFIFO" AS "IBFIFO"
		, "EXT_SRC"."IBCYCL" AS "IBCYCL"
		, "EXT_SRC"."IBINMG" AS "IBINMG"
		, "EXT_SRC"."IBWARR" AS "IBWARR"
		, "EXT_SRC"."IBSRNR" AS "IBSRNR"
		, "EXT_SRC"."IBPCTM" AS "IBPCTM"
		, "EXT_SRC"."IBCMCG" AS "IBCMCG"
		, "EXT_SRC"."IBFUF1" AS "IBFUF1"
		, "EXT_SRC"."IBTX" AS "IBTX"
		, "EXT_SRC"."IBTAX1" AS "IBTAX1"
		, "EXT_SRC"."IBMPST" AS "IBMPST"
		, "EXT_SRC"."IBMRPD" AS "IBMRPD"
		, "EXT_SRC"."IBMRPC" AS "IBMRPC"
		, "EXT_SRC"."IBUPC" AS "IBUPC"
		, "EXT_SRC"."IBSNS" AS "IBSNS"
		, "EXT_SRC"."IBMERL" AS "IBMERL"
		, "EXT_SRC"."IBLTLV" AS "IBLTLV"
		, "EXT_SRC"."IBLTMF" AS "IBLTMF"
		, "EXT_SRC"."IBLTCM" AS "IBLTCM"
		, "EXT_SRC"."IBOPC" AS "IBOPC"
		, "EXT_SRC"."IBOPV" AS "IBOPV"
		, "EXT_SRC"."IBACQ" AS "IBACQ"
		, "EXT_SRC"."IBMLQ" AS "IBMLQ"
		, "EXT_SRC"."IBLTPU" AS "IBLTPU"
		, "EXT_SRC"."IBMPSP" AS "IBMPSP"
		, "EXT_SRC"."IBMRPP" AS "IBMRPP"
		, "EXT_SRC"."IBITC" AS "IBITC"
		, "EXT_SRC"."IBECO" AS "IBECO"
		, "EXT_SRC"."IBECTY" AS "IBECTY"
		, "EXT_SRC"."IBECOD" AS "IBECOD"
		, "EXT_SRC"."IBMTF1" AS "IBMTF1"
		, "EXT_SRC"."IBMTF2" AS "IBMTF2"
		, "EXT_SRC"."IBMTF3" AS "IBMTF3"
		, "EXT_SRC"."IBMTF4" AS "IBMTF4"
		, "EXT_SRC"."IBMTF5" AS "IBMTF5"
		, "EXT_SRC"."IBMOVD" AS "IBMOVD"
		, "EXT_SRC"."IBQUED" AS "IBQUED"
		, "EXT_SRC"."IBSETL" AS "IBSETL"
		, "EXT_SRC"."IBSRNK" AS "IBSRNK"
		, "EXT_SRC"."IBSRKF" AS "IBSRKF"
		, "EXT_SRC"."IBTIMB" AS "IBTIMB"
		, "EXT_SRC"."IBBQTY" AS "IBBQTY"
		, "EXT_SRC"."IBORDW" AS "IBORDW"
		, "EXT_SRC"."IBEXPD" AS "IBEXPD"
		, "EXT_SRC"."IBDEFD" AS "IBDEFD"
		, "EXT_SRC"."IBMULT" AS "IBMULT"
		, "EXT_SRC"."IBSFLT" AS "IBSFLT"
		, "EXT_SRC"."IBMAKE" AS "IBMAKE"
		, "EXT_SRC"."IBLFDJ" AS "IBLFDJ"
		, "EXT_SRC"."IBLLX" AS "IBLLX"
		, "EXT_SRC"."IBCMGL" AS "IBCMGL"
		, "EXT_SRC"."IBURCD" AS "IBURCD"
		, "EXT_SRC"."IBURDT" AS "IBURDT"
		, "EXT_SRC"."IBURAT" AS "IBURAT"
		, "EXT_SRC"."IBURAB" AS "IBURAB"
		, "EXT_SRC"."IBURRF" AS "IBURRF"
		, "EXT_SRC"."IBUSER" AS "IBUSER"
		, "EXT_SRC"."IBPID" AS "IBPID"
		, "EXT_SRC"."IBJOBN" AS "IBJOBN"
		, "EXT_SRC"."IBUPMJ" AS "IBUPMJ"
		, "EXT_SRC"."IBTDAY" AS "IBTDAY"
		, "EXT_SRC"."IBTFLA" AS "IBTFLA"
		, "EXT_SRC"."IBCOMH" AS "IBCOMH"
		, "EXT_SRC"."IBAVRT" AS "IBAVRT"
		, "EXT_SRC"."IBPOC" AS "IBPOC"
		, "EXT_SRC"."IBAING" AS "IBAING"
		, "EXT_SRC"."IBBBDD" AS "IBBBDD"
		, "EXT_SRC"."IBCMDM" AS "IBCMDM"
		, "EXT_SRC"."IBLECM" AS "IBLECM"
		, "EXT_SRC"."IBLEDD" AS "IBLEDD"
		, "EXT_SRC"."IBMLOT" AS "IBMLOT"
		, "EXT_SRC"."IBPEFD" AS "IBPEFD"
		, "EXT_SRC"."IBSBDD" AS "IBSBDD"
		, "EXT_SRC"."IBU1DD" AS "IBU1DD"
		, "EXT_SRC"."IBU2DD" AS "IBU2DD"
		, "EXT_SRC"."IBU3DD" AS "IBU3DD"
		, "EXT_SRC"."IBU4DD" AS "IBU4DD"
		, "EXT_SRC"."IBU5DD" AS "IBU5DD"
		, "EXT_SRC"."IBXDCK" AS "IBXDCK"
		, "EXT_SRC"."IBLAF" AS "IBLAF"
		, "EXT_SRC"."IBLTFM" AS "IBLTFM"
		, "EXT_SRC"."IBRWLA" AS "IBRWLA"
		, "EXT_SRC"."IBLNPA" AS "IBLNPA"
		, "EXT_SRC"."IBLOTC" AS "IBLOTC"
		, "EXT_SRC"."IBAPSC" AS "IBAPSC"
		, "EXT_SRC"."IBPRI1" AS "IBPRI1"
		, "EXT_SRC"."IBPRI2" AS "IBPRI2"
		, "EXT_SRC"."IBLTCV" AS "IBLTCV"
		, "EXT_SRC"."IBASHL" AS "IBASHL"
		, "EXT_SRC"."IBOPTH" AS "IBOPTH"
		, "EXT_SRC"."IBCUTH" AS "IBCUTH"
		, "EXT_SRC"."IBUMTH" AS "IBUMTH"
		, "EXT_SRC"."IBLMFG" AS "IBLMFG"
		, "EXT_SRC"."IBLINE" AS "IBLINE"
		, "EXT_SRC"."IBDFTPCT" AS "IBDFTPCT"
		, "EXT_SRC"."IBKBIT" AS "IBKBIT"
		, "EXT_SRC"."IBDFENDITM" AS "IBDFENDITM"
		, "EXT_SRC"."IBKANEXLL" AS "IBKANEXLL"
		, "EXT_SRC"."IBSCPSELL" AS "IBSCPSELL"
		, "EXT_SRC"."IBMOPTH" AS "IBMOPTH"
		, "EXT_SRC"."IBMCUTH" AS "IBMCUTH"
		, "EXT_SRC"."IBCUMTH" AS "IBCUMTH"
		, "EXT_SRC"."IBATPRN" AS "IBATPRN"
		, "EXT_SRC"."IBATPCA" AS "IBATPCA"
		, "EXT_SRC"."IBATPAC" AS "IBATPAC"
		, "EXT_SRC"."IBCOORE" AS "IBCOORE"
		, "EXT_SRC"."IBVCPFC" AS "IBVCPFC"
		, "EXT_SRC"."IBPNYN" AS "IBPNYN"
	FROM {{ ref('JDEDWARDS_EXT_F4102') }} "EXT_SRC"
	INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_SRC" ON  "MEX_SRC"."RECORD_TYPE" = 'U'

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'