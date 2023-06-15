CREATE OR REPLACE PROCEDURE "DEMO_PROC"."FMC_UPD_RUN_STATUS_FL_SALESFORCE"(P_LOAD_CYCLE_ID VARCHAR2,
P_SUCCESS_FLAG VARCHAR2)
 RETURNS varchar 
LANGUAGE JAVASCRIPT 

AS $$ 
/*
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.3.1.9, generation date: 2023/06/15 11:19:25
DV_NAME: DEMO - Release: Release 9(9) - Comment: Changed DV parameters en source parameters - Release date: 2023/04/18 16:49:46, 
BV release: init(1) - Comment: initial release - Release date: 2023/04/18 14:52:02, 
SRC_NAME: SALESFORCE - Release: SALESFORCE(5) - Comment: Changed CDC_UPDATE_RECORD_ALL_ATTRIBUTES parameter - Release date: 2023/04/18 16:48:01
 */



var HIST_UPD = snowflake.createStatement( {sqlText: `
	UPDATE "DEMO_FMC"."FMC_LOADING_HISTORY" "HIST_UPD"
	SET 
		 "SUCCESS_FLAG" =  '` + P_SUCCESS_FLAG + `'::integer
		,"LOAD_END_DATE" =  CURRENT_TIMESTAMP
	WHERE "HIST_UPD"."LOAD_CYCLE_ID" =  '` + P_LOAD_CYCLE_ID + `'::integer
	;
`} ).execute();

return "Done.";$$;
 
 
