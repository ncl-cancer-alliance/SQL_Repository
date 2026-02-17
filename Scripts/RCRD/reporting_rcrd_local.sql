/*
Script: DEV__REPORTING.PUBLIC.CANCER__RCRD__LOCAL
Version: 1.1

Description:
Dynamic table to create new Tumour Grouping that matches the ones in the National RCRD Data extracts. Also only selecting necessary columns for the RCRD Dashboard. Only including London Data.

Notes:
- Runtime around 3 secs


Author: Eric Pinto

Tables Used:

MODELLING.CANCER__RCRD.RCRD

Target Output Table:

 - REPORTING.CANCER__RCRD.CANCER__RCRD__LOCAL

*/

create or replace dynamic table REPORTING.CANCER__RCRD.CANCER__RCRD__LOCAL(
	SK_PATIENT_ID,
	DATE_OF_DIAGNOSIS,
	MONTH_OF_DIAGNOSIS,
	QUARTER_OF_DIAGNOSIS,
	YEAR_OF_DIAGNOSIS,
	FINANCIAL_YEAR_OF_DIAGNOSIS,
	ICD10_CODE,
	ICD10_DESCRIPTION,
	ALL_TUMOUR_GROUP_NAME,
	TUMOUR_GROUP_NAME,
	STAGE,
	STAGE_EARLY_LATE,
	IS_STAGEABLE_CANCER,
	GENDER_NAME,
	AGE_AT_DIAGNOSIS_GROUP,
	ETHNICITY_CATEGORY,
	RESIDENCE_LSOA_IMD_QUINTILE,
	ROUTE_TO_DIAGNOSIS,
	TRUST_NAME,
	CANCER_ALLIANCE_NAME,
	REGION_NAME,
	ICB_NAME,
	GP_PRACTICE_CODE,
	GP_PRACTICE_NAME,
	REG_PCN_NAME,
	REG_BOROUGH_NCL_NAME,
	RESIDENCE_LSOA_CODE,
	RESIDENCE_LSOA_NAME,
	RESIDENCE_BOROUGH,
	RESIDENCE_NEIGHBOURHOOD,
	SNAPSHOT
) target_lag = '1 day' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to create new Tumour Grouping that matches the ones in the National RCRD Data extracts. \nOnly selecting necessary columns for the RCRD Dashboard.\n\nContact: eric.pinto@nhs.net'
 as

SELECT 
SK_PATIENT_ID,
DATE_OF_DIAGNOSIS,
MONTH_OF_DIAGNOSIS,
QUARTER_OF_DIAGNOSIS,
YEAR_OF_DIAGNOSIS,
FINANCIAL_YEAR_OF_DIAGNOSIS,
rcrd.ICD10_CODE,
rcrd.ICD10_CODE || ' - ' || INITCAP(REPLACE("ShortDescription", 'Malignant neoplasm of ', '')) AS ICD10_DESCRIPTION,
TUMOUR_GROUP_NAME AS ALL_TUMOUR_GROUP_NAME,
---- CASE STATEMENT that creates a new tumour grouping in line with the groupings in the National RCRD extracts. ----
CASE
    WHEN rcrd.ICD10_CODE = 'C50' THEN 'Breast'
    WHEN rcrd.ICD10_CODE BETWEEN 'C18' AND 'C20' THEN 'Bowel'
    WHEN rcrd.ICD10_CODE BETWEEN 'C70' AND 'C72' THEN 'Brain & CNS'
    WHEN rcrd.ICD10_CODE = 'C61' THEN 'Prostate'
    WHEN rcrd.ICD10_CODE = 'C43' THEN 'Melanoma'
    WHEN rcrd.ICD10_CODE BETWEEN 'C81' AND 'C96' THEN 'Haematological'
    WHEN rcrd.ICD10_CODE IN ('C33','C34') THEN 'Lung'
    WHEN rcrd.ICD10_CODE BETWEEN 'C64' AND 'C68' OR rcrd.ICD10_CODE = 'C62' THEN 'Urological excl. prostate'
    WHEN rcrd.ICD10_CODE BETWEEN 'C40' AND 'C41'
      OR rcrd.ICD10_CODE BETWEEN 'C47' AND 'C49' THEN 'Bone & soft-tissue'
    WHEN rcrd.ICD10_CODE BETWEEN 'C00' AND 'C14'
      OR rcrd.ICD10_CODE IN ('C30','C31','C32') THEN 'Head & neck'
    WHEN rcrd.ICD10_CODE BETWEEN 'C15' AND 'C16' THEN 'Oesophago-gastric'
    WHEN rcrd.ICD10_CODE IN ('C17','C22','C23','C24','C25','C26') THEN 'Upper GI excl. OG'
    WHEN rcrd.ICD10_CODE IN ('C51','C52','C53','C54','C55','C56','C57','C58') THEN 'Gynaecological'
    WHEN rcrd.ICD10_CODE BETWEEN 'C73' AND 'C75' THEN 'Endocrine'
    ELSE 'Other'

END AS TUMOUR_GROUP_NAME,
STAGE,
STAGE_EARLY_LATE,
IS_STAGEABLE_CANCER,
GENDER_NAME,
AGE_AT_DIAGNOSIS_GROUP,
ETHNICITY_CATEGORY,
RESIDENCE_LSOA_IMD_QUINTILE,
ROUTE_TO_DIAGNOSIS,
TRUST_NAME,
CANCER_ALLIANCE_NAME,
REGION_NAME,
ICB_NAME,
GP_PRACTICE_CODE,
GP_PRACTICE_NAME,
REG_PCN_NAME,
REG_BOROUGH_NCL_NAME,
RESIDENCE_LSOA_CODE,
RESIDENCE_LSOA_NAME,
RESIDENCE_BOROUGH,
RESIDENCE_NEIGHBOURHOOD,
SNAPSHOT

FROM MODELLING.CANCER__RCRD.RCRD rcrd

---- Join to get ICD10 Descriptions ----
JOIN (
    SELECT DISTINCT
        "AltCode" AS ICD10_CODE,
        "ShortDescription"
    FROM "Dictionary"."dbo"."Diagnosis"
    WHERE "AltCode" LIKE 'C%'
    AND LEN("AltCode") = 3) icd
    ON rcrd.ICD10_CODE = icd.ICD10_CODE


WHERE CANCER_ALLIANCE_NAME = 'North Central London';