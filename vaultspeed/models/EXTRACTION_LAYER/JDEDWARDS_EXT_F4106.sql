{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='F4106',
		schema='JDEDWARDS_EXT',
		tags=['JDEDWARDS', 'EXT_JDE_ITEMPRICE_INCR', 'EXT_JDE_ITEMPRICE_INIT']
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
			, "TDFV_SRC"."BPITM" AS "BPITM"
			, "TDFV_SRC"."BPMCU" AS "BPMCU"
			, "TDFV_SRC"."BPLOCN" AS "BPLOCN"
			, "TDFV_SRC"."BPLOTN" AS "BPLOTN"
			, "TDFV_SRC"."BPAN8" AS "BPAN8"
			, "TDFV_SRC"."BPIGID" AS "BPIGID"
			, "TDFV_SRC"."BPCGID" AS "BPCGID"
			, "TDFV_SRC"."BPLOTG" AS "BPLOTG"
			, "TDFV_SRC"."BPFRMP" AS "BPFRMP"
			, "TDFV_SRC"."BPCRCD" AS "BPCRCD"
			, "TDFV_SRC"."BPUOM" AS "BPUOM"
			, "TDFV_SRC"."BPEXDJ" AS "BPEXDJ"
			, "TDFV_SRC"."BPUPMJ" AS "BPUPMJ"
			, "TDFV_SRC"."BPTDAY" AS "BPTDAY"
			, "TDFV_SRC"."BPLEDG" AS "BPLEDG"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPITM")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPITM_BK"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."BPMCU"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPMCU_BK"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."BPLOCN"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPLOCN_BK"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."BPLOTN"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPLOTN_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPAN8")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPAN8_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPIGID")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPIGID_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPCGID")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPCGID_BK"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."BPLOTG"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPLOTG_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPFRMP")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPFRMP_BK"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."BPCRCD"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPCRCD_BK"
			, COALESCE(REPLACE(TRIM( "TDFV_SRC"."BPUOM"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPUOM_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPEXDJ")),"MEX_SRC"."KEY_ATTRIBUTE_NUMBER") AS "BPEXDJ_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPUPMJ")),"MEX_SRC"."KEY_ATTRIBUTE_NUMBER") AS "BPUPMJ_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."BPTDAY")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPTDAY_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."BPITM")) AS "COITM_FK_BPLEDG_BK"
			, REPLACE(TRIM( "TDFV_SRC"."BPMCU"),'\#','\\' || '\#') AS "COMCU_FK_BPLEDG_BK"
			, REPLACE(TRIM( "TDFV_SRC"."BPLOCN"),'\#','\\' || '\#') AS "COLOCN_FK_BPLEDG_BK"
			, REPLACE(TRIM( "TDFV_SRC"."BPLOTN"),'\#','\\' || '\#') AS "COLOTN_FK_BPLEDG_BK"
			, REPLACE(TRIM( "TDFV_SRC"."BPLEDG"),'\#','\\' || '\#') AS "COLEDG_FK_BPLEDG_BK"
			, REPLACE(TRIM( "TDFV_SRC"."BPMCU"),'\#','\\' || '\#') AS "IBMCU_FK_ITEMBRANCH_BPITM_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."BPITM")) AS "IBITM_FK_ITEMBRANCH_BPITM_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."BPAN8")) AS "ABAN8_FK_BPAN8_BK"
			, REPLACE(TRIM( "TDFV_SRC"."BPMCU"),'\#','\\' || '\#') AS "MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."BPITM")) AS "IMITM_FK_ITEMMASTER_BPITM_BK"
			, "TDFV_SRC"."BPLITM" AS "BPLITM"
			, "TDFV_SRC"."BPAITM" AS "BPAITM"
			, "TDFV_SRC"."BPEFTJ" AS "BPEFTJ"
			, "TDFV_SRC"."BPUPRC" AS "BPUPRC"
			, "TDFV_SRC"."BPACRD" AS "BPACRD"
			, "TDFV_SRC"."BPBSCD" AS "BPBSCD"
			, "TDFV_SRC"."BPFVTR" AS "BPFVTR"
			, "TDFV_SRC"."BPFRMN" AS "BPFRMN"
			, "TDFV_SRC"."BPURCD" AS "BPURCD"
			, "TDFV_SRC"."BPURDT" AS "BPURDT"
			, "TDFV_SRC"."BPURAT" AS "BPURAT"
			, "TDFV_SRC"."BPURAB" AS "BPURAB"
			, "TDFV_SRC"."BPURRF" AS "BPURRF"
			, "TDFV_SRC"."BPAPRS" AS "BPAPRS"
			, "TDFV_SRC"."BPUSER" AS "BPUSER"
			, "TDFV_SRC"."BPPID" AS "BPPID"
			, "TDFV_SRC"."BPJOBN" AS "BPJOBN"
		FROM {{ ref('JDEDWARDS_DFV_VW_F4106') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."BPITM" AS "BPITM"
			, "CALCULATE_BK"."BPMCU" AS "BPMCU"
			, "CALCULATE_BK"."BPLOCN" AS "BPLOCN"
			, "CALCULATE_BK"."BPLOTN" AS "BPLOTN"
			, "CALCULATE_BK"."BPAN8" AS "BPAN8"
			, "CALCULATE_BK"."BPIGID" AS "BPIGID"
			, "CALCULATE_BK"."BPCGID" AS "BPCGID"
			, "CALCULATE_BK"."BPLOTG" AS "BPLOTG"
			, "CALCULATE_BK"."BPFRMP" AS "BPFRMP"
			, "CALCULATE_BK"."BPCRCD" AS "BPCRCD"
			, "CALCULATE_BK"."BPUOM" AS "BPUOM"
			, "CALCULATE_BK"."BPEXDJ" AS "BPEXDJ"
			, "CALCULATE_BK"."BPUPMJ" AS "BPUPMJ"
			, "CALCULATE_BK"."BPTDAY" AS "BPTDAY"
			, "CALCULATE_BK"."BPLEDG" AS "BPLEDG"
			, "CALCULATE_BK"."BPITM_BK" AS "BPITM_BK"
			, "CALCULATE_BK"."BPMCU_BK" AS "BPMCU_BK"
			, "CALCULATE_BK"."BPLOCN_BK" AS "BPLOCN_BK"
			, "CALCULATE_BK"."BPLOTN_BK" AS "BPLOTN_BK"
			, "CALCULATE_BK"."BPAN8_BK" AS "BPAN8_BK"
			, "CALCULATE_BK"."BPIGID_BK" AS "BPIGID_BK"
			, "CALCULATE_BK"."BPCGID_BK" AS "BPCGID_BK"
			, "CALCULATE_BK"."BPLOTG_BK" AS "BPLOTG_BK"
			, "CALCULATE_BK"."BPFRMP_BK" AS "BPFRMP_BK"
			, "CALCULATE_BK"."BPCRCD_BK" AS "BPCRCD_BK"
			, "CALCULATE_BK"."BPUOM_BK" AS "BPUOM_BK"
			, "CALCULATE_BK"."BPEXDJ_BK" AS "BPEXDJ_BK"
			, "CALCULATE_BK"."BPUPMJ_BK" AS "BPUPMJ_BK"
			, "CALCULATE_BK"."BPTDAY_BK" AS "BPTDAY_BK"
			, "CALCULATE_BK"."COITM_FK_BPLEDG_BK" AS "COITM_FK_BPLEDG_BK"
			, "CALCULATE_BK"."COMCU_FK_BPLEDG_BK" AS "COMCU_FK_BPLEDG_BK"
			, "CALCULATE_BK"."COLOCN_FK_BPLEDG_BK" AS "COLOCN_FK_BPLEDG_BK"
			, "CALCULATE_BK"."COLOTN_FK_BPLEDG_BK" AS "COLOTN_FK_BPLEDG_BK"
			, "CALCULATE_BK"."COLEDG_FK_BPLEDG_BK" AS "COLEDG_FK_BPLEDG_BK"
			, "CALCULATE_BK"."IBMCU_FK_ITEMBRANCH_BPITM_BK" AS "IBMCU_FK_ITEMBRANCH_BPITM_BK"
			, "CALCULATE_BK"."IBITM_FK_ITEMBRANCH_BPITM_BK" AS "IBITM_FK_ITEMBRANCH_BPITM_BK"
			, "CALCULATE_BK"."ABAN8_FK_BPAN8_BK" AS "ABAN8_FK_BPAN8_BK"
			, "CALCULATE_BK"."MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK" AS "MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK"
			, "CALCULATE_BK"."IMITM_FK_ITEMMASTER_BPITM_BK" AS "IMITM_FK_ITEMMASTER_BPITM_BK"
			, "CALCULATE_BK"."BPLITM" AS "BPLITM"
			, "CALCULATE_BK"."BPAITM" AS "BPAITM"
			, "CALCULATE_BK"."BPEFTJ" AS "BPEFTJ"
			, "CALCULATE_BK"."BPUPRC" AS "BPUPRC"
			, "CALCULATE_BK"."BPACRD" AS "BPACRD"
			, "CALCULATE_BK"."BPBSCD" AS "BPBSCD"
			, "CALCULATE_BK"."BPFVTR" AS "BPFVTR"
			, "CALCULATE_BK"."BPFRMN" AS "BPFRMN"
			, "CALCULATE_BK"."BPURCD" AS "BPURCD"
			, "CALCULATE_BK"."BPURDT" AS "BPURDT"
			, "CALCULATE_BK"."BPURAT" AS "BPURAT"
			, "CALCULATE_BK"."BPURAB" AS "BPURAB"
			, "CALCULATE_BK"."BPURRF" AS "BPURRF"
			, "CALCULATE_BK"."BPAPRS" AS "BPAPRS"
			, "CALCULATE_BK"."BPUSER" AS "BPUSER"
			, "CALCULATE_BK"."BPPID" AS "BPPID"
			, "CALCULATE_BK"."BPJOBN" AS "BPJOBN"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."BPITM" AS "BPITM"
		, "EXT_UNION"."BPMCU" AS "BPMCU"
		, "EXT_UNION"."BPLOCN" AS "BPLOCN"
		, "EXT_UNION"."BPLOTN" AS "BPLOTN"
		, "EXT_UNION"."BPAN8" AS "BPAN8"
		, "EXT_UNION"."BPIGID" AS "BPIGID"
		, "EXT_UNION"."BPCGID" AS "BPCGID"
		, "EXT_UNION"."BPLOTG" AS "BPLOTG"
		, "EXT_UNION"."BPFRMP" AS "BPFRMP"
		, "EXT_UNION"."BPCRCD" AS "BPCRCD"
		, "EXT_UNION"."BPUOM" AS "BPUOM"
		, "EXT_UNION"."BPEXDJ" AS "BPEXDJ"
		, "EXT_UNION"."BPUPMJ" AS "BPUPMJ"
		, "EXT_UNION"."BPTDAY" AS "BPTDAY"
		, "EXT_UNION"."BPLEDG" AS "BPLEDG"
		, "EXT_UNION"."BPITM_BK" AS "BPITM_BK"
		, "EXT_UNION"."BPMCU_BK" AS "BPMCU_BK"
		, "EXT_UNION"."BPLOCN_BK" AS "BPLOCN_BK"
		, "EXT_UNION"."BPLOTN_BK" AS "BPLOTN_BK"
		, "EXT_UNION"."BPAN8_BK" AS "BPAN8_BK"
		, "EXT_UNION"."BPIGID_BK" AS "BPIGID_BK"
		, "EXT_UNION"."BPCGID_BK" AS "BPCGID_BK"
		, "EXT_UNION"."BPLOTG_BK" AS "BPLOTG_BK"
		, "EXT_UNION"."BPFRMP_BK" AS "BPFRMP_BK"
		, "EXT_UNION"."BPCRCD_BK" AS "BPCRCD_BK"
		, "EXT_UNION"."BPUOM_BK" AS "BPUOM_BK"
		, "EXT_UNION"."BPEXDJ_BK" AS "BPEXDJ_BK"
		, "EXT_UNION"."BPUPMJ_BK" AS "BPUPMJ_BK"
		, "EXT_UNION"."BPTDAY_BK" AS "BPTDAY_BK"
		, "EXT_UNION"."COITM_FK_BPLEDG_BK" AS "COITM_FK_BPLEDG_BK"
		, "EXT_UNION"."COMCU_FK_BPLEDG_BK" AS "COMCU_FK_BPLEDG_BK"
		, "EXT_UNION"."COLOCN_FK_BPLEDG_BK" AS "COLOCN_FK_BPLEDG_BK"
		, "EXT_UNION"."COLOTN_FK_BPLEDG_BK" AS "COLOTN_FK_BPLEDG_BK"
		, "EXT_UNION"."COLEDG_FK_BPLEDG_BK" AS "COLEDG_FK_BPLEDG_BK"
		, "EXT_UNION"."IBMCU_FK_ITEMBRANCH_BPITM_BK" AS "IBMCU_FK_ITEMBRANCH_BPITM_BK"
		, "EXT_UNION"."IBITM_FK_ITEMBRANCH_BPITM_BK" AS "IBITM_FK_ITEMBRANCH_BPITM_BK"
		, "EXT_UNION"."ABAN8_FK_BPAN8_BK" AS "ABAN8_FK_BPAN8_BK"
		, "EXT_UNION"."MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK" AS "MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK"
		, "EXT_UNION"."IMITM_FK_ITEMMASTER_BPITM_BK" AS "IMITM_FK_ITEMMASTER_BPITM_BK"
		, "EXT_UNION"."BPLITM" AS "BPLITM"
		, "EXT_UNION"."BPAITM" AS "BPAITM"
		, "EXT_UNION"."BPEFTJ" AS "BPEFTJ"
		, "EXT_UNION"."BPUPRC" AS "BPUPRC"
		, "EXT_UNION"."BPACRD" AS "BPACRD"
		, "EXT_UNION"."BPBSCD" AS "BPBSCD"
		, "EXT_UNION"."BPFVTR" AS "BPFVTR"
		, "EXT_UNION"."BPFRMN" AS "BPFRMN"
		, "EXT_UNION"."BPURCD" AS "BPURCD"
		, "EXT_UNION"."BPURDT" AS "BPURDT"
		, "EXT_UNION"."BPURAT" AS "BPURAT"
		, "EXT_UNION"."BPURAB" AS "BPURAB"
		, "EXT_UNION"."BPURRF" AS "BPURRF"
		, "EXT_UNION"."BPAPRS" AS "BPAPRS"
		, "EXT_UNION"."BPUSER" AS "BPUSER"
		, "EXT_UNION"."BPPID" AS "BPPID"
		, "EXT_UNION"."BPJOBN" AS "BPJOBN"
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
			, COALESCE("INI_SRC"."BPITM", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "BPITM"
			, COALESCE("INI_SRC"."BPMCU", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BPMCU"
			, COALESCE("INI_SRC"."BPLOCN", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BPLOCN"
			, COALESCE("INI_SRC"."BPLOTN", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BPLOTN"
			, COALESCE("INI_SRC"."BPAN8", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "BPAN8"
			, COALESCE("INI_SRC"."BPIGID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "BPIGID"
			, COALESCE("INI_SRC"."BPCGID", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "BPCGID"
			, COALESCE("INI_SRC"."BPLOTG", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BPLOTG"
			, COALESCE("INI_SRC"."BPFRMP", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "BPFRMP"
			, COALESCE("INI_SRC"."BPCRCD", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BPCRCD"
			, COALESCE("INI_SRC"."BPUOM", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BPUOM"
			, COALESCE("INI_SRC"."BPEXDJ", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_NUMBER" AS NUMBER)) AS "BPEXDJ"
			, COALESCE("INI_SRC"."BPUPMJ", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_NUMBER" AS NUMBER)) AS "BPUPMJ"
			, COALESCE("INI_SRC"."BPTDAY", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "BPTDAY"
			, COALESCE("INI_SRC"."BPLEDG", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR)) AS "BPLEDG"
			, "INI_SRC"."BPLITM" AS "BPLITM"
			, "INI_SRC"."BPAITM" AS "BPAITM"
			, "INI_SRC"."BPEFTJ" AS "BPEFTJ"
			, "INI_SRC"."BPUPRC" AS "BPUPRC"
			, "INI_SRC"."BPACRD" AS "BPACRD"
			, "INI_SRC"."BPBSCD" AS "BPBSCD"
			, "INI_SRC"."BPFVTR" AS "BPFVTR"
			, "INI_SRC"."BPFRMN" AS "BPFRMN"
			, "INI_SRC"."BPURCD" AS "BPURCD"
			, "INI_SRC"."BPURDT" AS "BPURDT"
			, "INI_SRC"."BPURAT" AS "BPURAT"
			, "INI_SRC"."BPURAB" AS "BPURAB"
			, "INI_SRC"."BPURRF" AS "BPURRF"
			, "INI_SRC"."BPAPRS" AS "BPAPRS"
			, "INI_SRC"."BPUSER" AS "BPUSER"
			, "INI_SRC"."BPPID" AS "BPPID"
			, "INI_SRC"."BPJOBN" AS "BPJOBN"
		FROM {{ source('JDEDWARDS_INI', 'F4106') }} "INI_SRC"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."BPITM" AS "BPITM"
			, "LOAD_INIT_DATA"."BPMCU" AS "BPMCU"
			, "LOAD_INIT_DATA"."BPLOCN" AS "BPLOCN"
			, "LOAD_INIT_DATA"."BPLOTN" AS "BPLOTN"
			, "LOAD_INIT_DATA"."BPAN8" AS "BPAN8"
			, "LOAD_INIT_DATA"."BPIGID" AS "BPIGID"
			, "LOAD_INIT_DATA"."BPCGID" AS "BPCGID"
			, "LOAD_INIT_DATA"."BPLOTG" AS "BPLOTG"
			, "LOAD_INIT_DATA"."BPFRMP" AS "BPFRMP"
			, "LOAD_INIT_DATA"."BPCRCD" AS "BPCRCD"
			, "LOAD_INIT_DATA"."BPUOM" AS "BPUOM"
			, "LOAD_INIT_DATA"."BPEXDJ" AS "BPEXDJ"
			, "LOAD_INIT_DATA"."BPUPMJ" AS "BPUPMJ"
			, "LOAD_INIT_DATA"."BPTDAY" AS "BPTDAY"
			, "LOAD_INIT_DATA"."BPLEDG" AS "BPLEDG"
			, "LOAD_INIT_DATA"."BPLITM" AS "BPLITM"
			, "LOAD_INIT_DATA"."BPAITM" AS "BPAITM"
			, "LOAD_INIT_DATA"."BPEFTJ" AS "BPEFTJ"
			, "LOAD_INIT_DATA"."BPUPRC" AS "BPUPRC"
			, "LOAD_INIT_DATA"."BPACRD" AS "BPACRD"
			, "LOAD_INIT_DATA"."BPBSCD" AS "BPBSCD"
			, "LOAD_INIT_DATA"."BPFVTR" AS "BPFVTR"
			, "LOAD_INIT_DATA"."BPFRMN" AS "BPFRMN"
			, "LOAD_INIT_DATA"."BPURCD" AS "BPURCD"
			, "LOAD_INIT_DATA"."BPURDT" AS "BPURDT"
			, "LOAD_INIT_DATA"."BPURAT" AS "BPURAT"
			, "LOAD_INIT_DATA"."BPURAB" AS "BPURAB"
			, "LOAD_INIT_DATA"."BPURRF" AS "BPURRF"
			, "LOAD_INIT_DATA"."BPAPRS" AS "BPAPRS"
			, "LOAD_INIT_DATA"."BPUSER" AS "BPUSER"
			, "LOAD_INIT_DATA"."BPPID" AS "BPPID"
			, "LOAD_INIT_DATA"."BPJOBN" AS "BPJOBN"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "BPITM"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPMCU"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPLOCN"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPLOTN"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "BPAN8"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "BPIGID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "BPCGID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPLOTG"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "BPFRMP"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPCRCD"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPUOM"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_NUMBER" AS NUMBER) AS "BPEXDJ"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_NUMBER" AS NUMBER) AS "BPUPMJ"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "BPTDAY"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPLEDG"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPLITM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPAITM"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "BPEFTJ"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "BPUPRC"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "BPACRD"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPBSCD"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "BPFVTR"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPFRMN"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPURCD"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "BPURDT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "BPURAT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "BPURAB"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPURRF"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPAPRS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPUSER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPPID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "BPJOBN"
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
			, "PREP_EXCEP"."BPITM" AS "BPITM"
			, "PREP_EXCEP"."BPMCU" AS "BPMCU"
			, "PREP_EXCEP"."BPLOCN" AS "BPLOCN"
			, "PREP_EXCEP"."BPLOTN" AS "BPLOTN"
			, "PREP_EXCEP"."BPAN8" AS "BPAN8"
			, "PREP_EXCEP"."BPIGID" AS "BPIGID"
			, "PREP_EXCEP"."BPCGID" AS "BPCGID"
			, "PREP_EXCEP"."BPLOTG" AS "BPLOTG"
			, "PREP_EXCEP"."BPFRMP" AS "BPFRMP"
			, "PREP_EXCEP"."BPCRCD" AS "BPCRCD"
			, "PREP_EXCEP"."BPUOM" AS "BPUOM"
			, "PREP_EXCEP"."BPEXDJ" AS "BPEXDJ"
			, "PREP_EXCEP"."BPUPMJ" AS "BPUPMJ"
			, "PREP_EXCEP"."BPTDAY" AS "BPTDAY"
			, "PREP_EXCEP"."BPLEDG" AS "BPLEDG"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPITM")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPITM_BK"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."BPMCU"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPMCU_BK"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."BPLOCN"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPLOCN_BK"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."BPLOTN"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPLOTN_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPAN8")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPAN8_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPIGID")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPIGID_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPCGID")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPCGID_BK"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."BPLOTG"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPLOTG_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPFRMP")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPFRMP_BK"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."BPCRCD"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPCRCD_BK"
			, COALESCE(REPLACE(TRIM("PREP_EXCEP"."BPUOM"),'\#','\\' || '\#'),"MEX_SRC"."KEY_ATTRIBUTE_VARCHAR") AS "BPUOM_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPEXDJ")),"MEX_SRC"."KEY_ATTRIBUTE_NUMBER") AS "BPEXDJ_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPUPMJ")),"MEX_SRC"."KEY_ATTRIBUTE_NUMBER") AS "BPUPMJ_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."BPTDAY")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "BPTDAY_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."BPITM")) AS "COITM_FK_BPLEDG_BK"
			, REPLACE(TRIM("PREP_EXCEP"."BPMCU"),'\#','\\' || '\#') AS "COMCU_FK_BPLEDG_BK"
			, REPLACE(TRIM("PREP_EXCEP"."BPLOCN"),'\#','\\' || '\#') AS "COLOCN_FK_BPLEDG_BK"
			, REPLACE(TRIM("PREP_EXCEP"."BPLOTN"),'\#','\\' || '\#') AS "COLOTN_FK_BPLEDG_BK"
			, REPLACE(TRIM("PREP_EXCEP"."BPLEDG"),'\#','\\' || '\#') AS "COLEDG_FK_BPLEDG_BK"
			, REPLACE(TRIM("PREP_EXCEP"."BPMCU"),'\#','\\' || '\#') AS "IBMCU_FK_ITEMBRANCH_BPITM_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."BPITM")) AS "IBITM_FK_ITEMBRANCH_BPITM_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."BPAN8")) AS "ABAN8_FK_BPAN8_BK"
			, REPLACE(TRIM("PREP_EXCEP"."BPMCU"),'\#','\\' || '\#') AS "MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."BPITM")) AS "IMITM_FK_ITEMMASTER_BPITM_BK"
			, "PREP_EXCEP"."BPLITM" AS "BPLITM"
			, "PREP_EXCEP"."BPAITM" AS "BPAITM"
			, "PREP_EXCEP"."BPEFTJ" AS "BPEFTJ"
			, "PREP_EXCEP"."BPUPRC" AS "BPUPRC"
			, "PREP_EXCEP"."BPACRD" AS "BPACRD"
			, "PREP_EXCEP"."BPBSCD" AS "BPBSCD"
			, "PREP_EXCEP"."BPFVTR" AS "BPFVTR"
			, "PREP_EXCEP"."BPFRMN" AS "BPFRMN"
			, "PREP_EXCEP"."BPURCD" AS "BPURCD"
			, "PREP_EXCEP"."BPURDT" AS "BPURDT"
			, "PREP_EXCEP"."BPURAT" AS "BPURAT"
			, "PREP_EXCEP"."BPURAB" AS "BPURAB"
			, "PREP_EXCEP"."BPURRF" AS "BPURRF"
			, "PREP_EXCEP"."BPAPRS" AS "BPAPRS"
			, "PREP_EXCEP"."BPUSER" AS "BPUSER"
			, "PREP_EXCEP"."BPPID" AS "BPPID"
			, "PREP_EXCEP"."BPJOBN" AS "BPJOBN"
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
		, "CALCULATE_BK"."BPITM" AS "BPITM"
		, "CALCULATE_BK"."BPMCU" AS "BPMCU"
		, "CALCULATE_BK"."BPLOCN" AS "BPLOCN"
		, "CALCULATE_BK"."BPLOTN" AS "BPLOTN"
		, "CALCULATE_BK"."BPAN8" AS "BPAN8"
		, "CALCULATE_BK"."BPIGID" AS "BPIGID"
		, "CALCULATE_BK"."BPCGID" AS "BPCGID"
		, "CALCULATE_BK"."BPLOTG" AS "BPLOTG"
		, "CALCULATE_BK"."BPFRMP" AS "BPFRMP"
		, "CALCULATE_BK"."BPCRCD" AS "BPCRCD"
		, "CALCULATE_BK"."BPUOM" AS "BPUOM"
		, "CALCULATE_BK"."BPEXDJ" AS "BPEXDJ"
		, "CALCULATE_BK"."BPUPMJ" AS "BPUPMJ"
		, "CALCULATE_BK"."BPTDAY" AS "BPTDAY"
		, "CALCULATE_BK"."BPLEDG" AS "BPLEDG"
		, "CALCULATE_BK"."BPITM_BK" AS "BPITM_BK"
		, "CALCULATE_BK"."BPMCU_BK" AS "BPMCU_BK"
		, "CALCULATE_BK"."BPLOCN_BK" AS "BPLOCN_BK"
		, "CALCULATE_BK"."BPLOTN_BK" AS "BPLOTN_BK"
		, "CALCULATE_BK"."BPAN8_BK" AS "BPAN8_BK"
		, "CALCULATE_BK"."BPIGID_BK" AS "BPIGID_BK"
		, "CALCULATE_BK"."BPCGID_BK" AS "BPCGID_BK"
		, "CALCULATE_BK"."BPLOTG_BK" AS "BPLOTG_BK"
		, "CALCULATE_BK"."BPFRMP_BK" AS "BPFRMP_BK"
		, "CALCULATE_BK"."BPCRCD_BK" AS "BPCRCD_BK"
		, "CALCULATE_BK"."BPUOM_BK" AS "BPUOM_BK"
		, "CALCULATE_BK"."BPEXDJ_BK" AS "BPEXDJ_BK"
		, "CALCULATE_BK"."BPUPMJ_BK" AS "BPUPMJ_BK"
		, "CALCULATE_BK"."BPTDAY_BK" AS "BPTDAY_BK"
		, "CALCULATE_BK"."COITM_FK_BPLEDG_BK" AS "COITM_FK_BPLEDG_BK"
		, "CALCULATE_BK"."COMCU_FK_BPLEDG_BK" AS "COMCU_FK_BPLEDG_BK"
		, "CALCULATE_BK"."COLOCN_FK_BPLEDG_BK" AS "COLOCN_FK_BPLEDG_BK"
		, "CALCULATE_BK"."COLOTN_FK_BPLEDG_BK" AS "COLOTN_FK_BPLEDG_BK"
		, "CALCULATE_BK"."COLEDG_FK_BPLEDG_BK" AS "COLEDG_FK_BPLEDG_BK"
		, "CALCULATE_BK"."IBMCU_FK_ITEMBRANCH_BPITM_BK" AS "IBMCU_FK_ITEMBRANCH_BPITM_BK"
		, "CALCULATE_BK"."IBITM_FK_ITEMBRANCH_BPITM_BK" AS "IBITM_FK_ITEMBRANCH_BPITM_BK"
		, "CALCULATE_BK"."ABAN8_FK_BPAN8_BK" AS "ABAN8_FK_BPAN8_BK"
		, "CALCULATE_BK"."MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK" AS "MCMCU_FK_BUSINESSUNITMASTER_BPMCU_BK"
		, "CALCULATE_BK"."IMITM_FK_ITEMMASTER_BPITM_BK" AS "IMITM_FK_ITEMMASTER_BPITM_BK"
		, "CALCULATE_BK"."BPLITM" AS "BPLITM"
		, "CALCULATE_BK"."BPAITM" AS "BPAITM"
		, "CALCULATE_BK"."BPEFTJ" AS "BPEFTJ"
		, "CALCULATE_BK"."BPUPRC" AS "BPUPRC"
		, "CALCULATE_BK"."BPACRD" AS "BPACRD"
		, "CALCULATE_BK"."BPBSCD" AS "BPBSCD"
		, "CALCULATE_BK"."BPFVTR" AS "BPFVTR"
		, "CALCULATE_BK"."BPFRMN" AS "BPFRMN"
		, "CALCULATE_BK"."BPURCD" AS "BPURCD"
		, "CALCULATE_BK"."BPURDT" AS "BPURDT"
		, "CALCULATE_BK"."BPURAT" AS "BPURAT"
		, "CALCULATE_BK"."BPURAB" AS "BPURAB"
		, "CALCULATE_BK"."BPURRF" AS "BPURRF"
		, "CALCULATE_BK"."BPAPRS" AS "BPAPRS"
		, "CALCULATE_BK"."BPUSER" AS "BPUSER"
		, "CALCULATE_BK"."BPPID" AS "BPPID"
		, "CALCULATE_BK"."BPJOBN" AS "BPJOBN"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'