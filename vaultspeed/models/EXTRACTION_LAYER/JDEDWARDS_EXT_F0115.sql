{{
	config(
		materialized='incremental',
		pre_hook= [
			'{% if var("load_type") == "INCR" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}',
			'{% if var("load_type") == "INIT" and var("source") == "JDEDWARDS" %} TRUNCATE TABLE {{ this }}; {% endif %}'
		],
		alias='F0115',
		schema='JDEDWARDS_EXT',
		tags=['JDEDWARDS', 'EXT_JDE_PHONE_INCR', 'EXT_JDE_PHONE_INIT']
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
			, "TDFV_SRC"."WPAN8" AS "WPAN8"
			, "TDFV_SRC"."WPIDLN" AS "WPIDLN"
			, "TDFV_SRC"."WPRCK7" AS "WPRCK7"
			, "TDFV_SRC"."WPCNLN" AS "WPCNLN"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."WPAN8")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPAN8_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."WPIDLN")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPIDLN_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."WPCNLN")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPCNLN_BK"
			, COALESCE(UPPER( TO_CHAR("TDFV_SRC"."WPRCK7")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPRCK7_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."WPAN8")) AS "WWAN8_FK_WPIDLN_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."WPIDLN")) AS "WWIDLN_FK_WPIDLN_BK"
			, UPPER( TO_CHAR("TDFV_SRC"."WPAN8")) AS "ABAN8_FK_ADDRESSBOOK_WPAN8_BK"
			, "TDFV_SRC"."WPPHTP" AS "WPPHTP"
			, "TDFV_SRC"."WPAR1" AS "WPAR1"
			, "TDFV_SRC"."WPPH1" AS "WPPH1"
			, "TDFV_SRC"."WPUSER" AS "WPUSER"
			, "TDFV_SRC"."WPPID" AS "WPPID"
			, "TDFV_SRC"."WPUPMJ" AS "WPUPMJ"
			, "TDFV_SRC"."WPJOBN" AS "WPJOBN"
			, "TDFV_SRC"."WPUPMT" AS "WPUPMT"
			, "TDFV_SRC"."WPCFNO1" AS "WPCFNO1"
			, "TDFV_SRC"."WPGEN1" AS "WPGEN1"
			, "TDFV_SRC"."WPFALGE" AS "WPFALGE"
			, "TDFV_SRC"."WPSYNCS" AS "WPSYNCS"
			, "TDFV_SRC"."WPCAAD" AS "WPCAAD"
		FROM {{ ref('JDEDWARDS_DFV_VW_F0115') }} "TDFV_SRC"
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
			, "CALCULATE_BK"."WPAN8" AS "WPAN8"
			, "CALCULATE_BK"."WPIDLN" AS "WPIDLN"
			, "CALCULATE_BK"."WPRCK7" AS "WPRCK7"
			, "CALCULATE_BK"."WPCNLN" AS "WPCNLN"
			, "CALCULATE_BK"."WPAN8_BK" AS "WPAN8_BK"
			, "CALCULATE_BK"."WPIDLN_BK" AS "WPIDLN_BK"
			, "CALCULATE_BK"."WPCNLN_BK" AS "WPCNLN_BK"
			, "CALCULATE_BK"."WPRCK7_BK" AS "WPRCK7_BK"
			, "CALCULATE_BK"."WWAN8_FK_WPIDLN_BK" AS "WWAN8_FK_WPIDLN_BK"
			, "CALCULATE_BK"."WWIDLN_FK_WPIDLN_BK" AS "WWIDLN_FK_WPIDLN_BK"
			, "CALCULATE_BK"."ABAN8_FK_ADDRESSBOOK_WPAN8_BK" AS "ABAN8_FK_ADDRESSBOOK_WPAN8_BK"
			, "CALCULATE_BK"."WPPHTP" AS "WPPHTP"
			, "CALCULATE_BK"."WPAR1" AS "WPAR1"
			, "CALCULATE_BK"."WPPH1" AS "WPPH1"
			, "CALCULATE_BK"."WPUSER" AS "WPUSER"
			, "CALCULATE_BK"."WPPID" AS "WPPID"
			, "CALCULATE_BK"."WPUPMJ" AS "WPUPMJ"
			, "CALCULATE_BK"."WPJOBN" AS "WPJOBN"
			, "CALCULATE_BK"."WPUPMT" AS "WPUPMT"
			, "CALCULATE_BK"."WPCFNO1" AS "WPCFNO1"
			, "CALCULATE_BK"."WPGEN1" AS "WPGEN1"
			, "CALCULATE_BK"."WPFALGE" AS "WPFALGE"
			, "CALCULATE_BK"."WPSYNCS" AS "WPSYNCS"
			, "CALCULATE_BK"."WPCAAD" AS "WPCAAD"
		FROM "CALCULATE_BK" "CALCULATE_BK"
	)
	SELECT 
		  "EXT_UNION"."LOAD_CYCLE_ID" AS "LOAD_CYCLE_ID"
		, "EXT_UNION"."LOAD_DATE" AS "LOAD_DATE"
		, "EXT_UNION"."CDC_TIMESTAMP" AS "CDC_TIMESTAMP"
		, "EXT_UNION"."JRN_FLAG" AS "JRN_FLAG"
		, "EXT_UNION"."RECORD_TYPE" AS "RECORD_TYPE"
		, "EXT_UNION"."WPAN8" AS "WPAN8"
		, "EXT_UNION"."WPIDLN" AS "WPIDLN"
		, "EXT_UNION"."WPRCK7" AS "WPRCK7"
		, "EXT_UNION"."WPCNLN" AS "WPCNLN"
		, "EXT_UNION"."WPAN8_BK" AS "WPAN8_BK"
		, "EXT_UNION"."WPIDLN_BK" AS "WPIDLN_BK"
		, "EXT_UNION"."WPCNLN_BK" AS "WPCNLN_BK"
		, "EXT_UNION"."WPRCK7_BK" AS "WPRCK7_BK"
		, "EXT_UNION"."WWAN8_FK_WPIDLN_BK" AS "WWAN8_FK_WPIDLN_BK"
		, "EXT_UNION"."WWIDLN_FK_WPIDLN_BK" AS "WWIDLN_FK_WPIDLN_BK"
		, "EXT_UNION"."ABAN8_FK_ADDRESSBOOK_WPAN8_BK" AS "ABAN8_FK_ADDRESSBOOK_WPAN8_BK"
		, "EXT_UNION"."WPPHTP" AS "WPPHTP"
		, "EXT_UNION"."WPAR1" AS "WPAR1"
		, "EXT_UNION"."WPPH1" AS "WPPH1"
		, "EXT_UNION"."WPUSER" AS "WPUSER"
		, "EXT_UNION"."WPPID" AS "WPPID"
		, "EXT_UNION"."WPUPMJ" AS "WPUPMJ"
		, "EXT_UNION"."WPJOBN" AS "WPJOBN"
		, "EXT_UNION"."WPUPMT" AS "WPUPMT"
		, "EXT_UNION"."WPCFNO1" AS "WPCFNO1"
		, "EXT_UNION"."WPGEN1" AS "WPGEN1"
		, "EXT_UNION"."WPFALGE" AS "WPFALGE"
		, "EXT_UNION"."WPSYNCS" AS "WPSYNCS"
		, "EXT_UNION"."WPCAAD" AS "WPCAAD"
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
			, COALESCE("INI_SRC"."WPAN8", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WPAN8"
			, COALESCE("INI_SRC"."WPIDLN", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WPIDLN"
			, COALESCE("INI_SRC"."WPRCK7", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WPRCK7"
			, COALESCE("INI_SRC"."WPCNLN", CAST("MEX_INR_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT)) AS "WPCNLN"
			, "INI_SRC"."WPPHTP" AS "WPPHTP"
			, "INI_SRC"."WPAR1" AS "WPAR1"
			, "INI_SRC"."WPPH1" AS "WPPH1"
			, "INI_SRC"."WPUSER" AS "WPUSER"
			, "INI_SRC"."WPPID" AS "WPPID"
			, "INI_SRC"."WPUPMJ" AS "WPUPMJ"
			, "INI_SRC"."WPJOBN" AS "WPJOBN"
			, "INI_SRC"."WPUPMT" AS "WPUPMT"
			, "INI_SRC"."WPCFNO1" AS "WPCFNO1"
			, "INI_SRC"."WPGEN1" AS "WPGEN1"
			, "INI_SRC"."WPFALGE" AS "WPFALGE"
			, "INI_SRC"."WPSYNCS" AS "WPSYNCS"
			, "INI_SRC"."WPCAAD" AS "WPCAAD"
		FROM {{ source('JDEDWARDS_INI', 'F0115') }} "INI_SRC"
		INNER JOIN {{ source('JDEDWARDS_MTD', 'MTD_EXCEPTION_RECORDS') }} "MEX_INR_SRC" ON  "MEX_INR_SRC"."RECORD_TYPE" = 'N'
	)
	, "PREP_EXCEP" AS 
	( 
		SELECT 
			  "LOAD_INIT_DATA"."JRN_FLAG" AS "JRN_FLAG"
			, "LOAD_INIT_DATA"."RECORD_TYPE" AS "RECORD_TYPE"
			, NULL AS "LOAD_CYCLE_ID"
			, "LOAD_INIT_DATA"."WPAN8" AS "WPAN8"
			, "LOAD_INIT_DATA"."WPIDLN" AS "WPIDLN"
			, "LOAD_INIT_DATA"."WPRCK7" AS "WPRCK7"
			, "LOAD_INIT_DATA"."WPCNLN" AS "WPCNLN"
			, "LOAD_INIT_DATA"."WPPHTP" AS "WPPHTP"
			, "LOAD_INIT_DATA"."WPAR1" AS "WPAR1"
			, "LOAD_INIT_DATA"."WPPH1" AS "WPPH1"
			, "LOAD_INIT_DATA"."WPUSER" AS "WPUSER"
			, "LOAD_INIT_DATA"."WPPID" AS "WPPID"
			, "LOAD_INIT_DATA"."WPUPMJ" AS "WPUPMJ"
			, "LOAD_INIT_DATA"."WPJOBN" AS "WPJOBN"
			, "LOAD_INIT_DATA"."WPUPMT" AS "WPUPMT"
			, "LOAD_INIT_DATA"."WPCFNO1" AS "WPCFNO1"
			, "LOAD_INIT_DATA"."WPGEN1" AS "WPGEN1"
			, "LOAD_INIT_DATA"."WPFALGE" AS "WPFALGE"
			, "LOAD_INIT_DATA"."WPSYNCS" AS "WPSYNCS"
			, "LOAD_INIT_DATA"."WPCAAD" AS "WPCAAD"
		FROM "LOAD_INIT_DATA" "LOAD_INIT_DATA"
		UNION ALL 
		SELECT 
			  TO_CHAR('I' ) AS "JRN_FLAG"
			, "MEX_EXT_SRC"."RECORD_TYPE" AS "RECORD_TYPE"
			, "MEX_EXT_SRC"."LOAD_CYCLE_ID" ::integer AS "LOAD_CYCLE_ID"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "WPAN8"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "WPIDLN"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "WPRCK7"
			, CAST("MEX_EXT_SRC"."KEY_ATTRIBUTE_FLOAT" AS FLOAT) AS "WPCNLN"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPPHTP"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPAR1"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPPH1"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPUSER"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPPID"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_NUMBER" AS NUMBER) AS "WPUPMJ"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPJOBN"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WPUPMT"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WPCFNO1"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPGEN1"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_VARCHAR" AS VARCHAR) AS "WPFALGE"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WPSYNCS"
			, CAST("MEX_EXT_SRC"."ATTRIBUTE_FLOAT" AS FLOAT) AS "WPCAAD"
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
			, "PREP_EXCEP"."WPAN8" AS "WPAN8"
			, "PREP_EXCEP"."WPIDLN" AS "WPIDLN"
			, "PREP_EXCEP"."WPRCK7" AS "WPRCK7"
			, "PREP_EXCEP"."WPCNLN" AS "WPCNLN"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."WPAN8")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPAN8_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."WPIDLN")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPIDLN_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."WPCNLN")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPCNLN_BK"
			, COALESCE(UPPER( TO_CHAR("PREP_EXCEP"."WPRCK7")),"MEX_SRC"."KEY_ATTRIBUTE_FLOAT") AS "WPRCK7_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."WPAN8")) AS "WWAN8_FK_WPIDLN_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."WPIDLN")) AS "WWIDLN_FK_WPIDLN_BK"
			, UPPER( TO_CHAR("PREP_EXCEP"."WPAN8")) AS "ABAN8_FK_ADDRESSBOOK_WPAN8_BK"
			, "PREP_EXCEP"."WPPHTP" AS "WPPHTP"
			, "PREP_EXCEP"."WPAR1" AS "WPAR1"
			, "PREP_EXCEP"."WPPH1" AS "WPPH1"
			, "PREP_EXCEP"."WPUSER" AS "WPUSER"
			, "PREP_EXCEP"."WPPID" AS "WPPID"
			, "PREP_EXCEP"."WPUPMJ" AS "WPUPMJ"
			, "PREP_EXCEP"."WPJOBN" AS "WPJOBN"
			, "PREP_EXCEP"."WPUPMT" AS "WPUPMT"
			, "PREP_EXCEP"."WPCFNO1" AS "WPCFNO1"
			, "PREP_EXCEP"."WPGEN1" AS "WPGEN1"
			, "PREP_EXCEP"."WPFALGE" AS "WPFALGE"
			, "PREP_EXCEP"."WPSYNCS" AS "WPSYNCS"
			, "PREP_EXCEP"."WPCAAD" AS "WPCAAD"
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
		, "CALCULATE_BK"."WPAN8" AS "WPAN8"
		, "CALCULATE_BK"."WPIDLN" AS "WPIDLN"
		, "CALCULATE_BK"."WPRCK7" AS "WPRCK7"
		, "CALCULATE_BK"."WPCNLN" AS "WPCNLN"
		, "CALCULATE_BK"."WPAN8_BK" AS "WPAN8_BK"
		, "CALCULATE_BK"."WPIDLN_BK" AS "WPIDLN_BK"
		, "CALCULATE_BK"."WPCNLN_BK" AS "WPCNLN_BK"
		, "CALCULATE_BK"."WPRCK7_BK" AS "WPRCK7_BK"
		, "CALCULATE_BK"."WWAN8_FK_WPIDLN_BK" AS "WWAN8_FK_WPIDLN_BK"
		, "CALCULATE_BK"."WWIDLN_FK_WPIDLN_BK" AS "WWIDLN_FK_WPIDLN_BK"
		, "CALCULATE_BK"."ABAN8_FK_ADDRESSBOOK_WPAN8_BK" AS "ABAN8_FK_ADDRESSBOOK_WPAN8_BK"
		, "CALCULATE_BK"."WPPHTP" AS "WPPHTP"
		, "CALCULATE_BK"."WPAR1" AS "WPAR1"
		, "CALCULATE_BK"."WPPH1" AS "WPPH1"
		, "CALCULATE_BK"."WPUSER" AS "WPUSER"
		, "CALCULATE_BK"."WPPID" AS "WPPID"
		, "CALCULATE_BK"."WPUPMJ" AS "WPUPMJ"
		, "CALCULATE_BK"."WPJOBN" AS "WPJOBN"
		, "CALCULATE_BK"."WPUPMT" AS "WPUPMT"
		, "CALCULATE_BK"."WPCFNO1" AS "WPCFNO1"
		, "CALCULATE_BK"."WPGEN1" AS "WPGEN1"
		, "CALCULATE_BK"."WPFALGE" AS "WPFALGE"
		, "CALCULATE_BK"."WPSYNCS" AS "WPSYNCS"
		, "CALCULATE_BK"."WPCAAD" AS "WPCAAD"
	FROM "CALCULATE_BK" "CALCULATE_BK"

) final 
where '{{ var("load_type") }}' = 'INIT' and '{{ var("source") }}' = 'JDEDWARDS'