/*
Script: PATIENTENCOUNTERDATA
Version: 1.2
Description: Unions patient demographics from SUS OP/IP/A&E databases for all patients. Selects valid characteristics from the latest recorded encounter across the 3 datasets.
Required for secondary care reporting, particularly CWT, where a large proportion of patients receiving cancer care are not NCL residents or registered with NCL GP's.
Author: Graham Roberts
Run time: ~1 min

Tables Involved:
SUS
- DATA_LAKE.SUS_OP.EncounterPatient
- DATA_LAKE.SUS_OP.EncounterDetail
- DATA_LAKE.SUS_AE.EncounterPatient
- DATA_LAKE.SUS_AE.EncounterDetail
- DATA_LAKE.SUS_IP.EncounterPatient
- DATA_LAKE.SUS_IP.EncounterDetail

Mortality
- DATA_LAKE.DEATHS.Deaths

Reference / Dictionary Tables:
- Dictionary.dbo.Gender
- Dictionary.dbo.Ethnicity
- Dictionary.dbo.Ethnicity2
- Dictionary.dbo.Organisation
- Dictionary.dbo.OrganisationHierarchyPractice
- Dictionary.dbo.Postcode

Geography and Socioeconomic Lookup:
- MODELLING.LOOKUP_NCL.SR_PCN_BOROUGH_LOOKUP
- MODELLING.LOOKUP_NCL.IMD_2019

Target Output Table:
DEV__MODELLING.CANCER__REF.PATIENTENCOUNTERDATA
*/

--Create and populate the final patient encounter data table with the latest demographic details
CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__REF.SUS_PATIENT_DEMOGRAPHICS_NEW(
    SK_PATIENT_ID INT,
    SK_ENCOUNTER_ID BIGINT,
    DATETIME_SUS_ENCOUNTER DATETIME,
    --SUS_DATA_SOURCE VARCHAR(50),
    GENDER_CODE VARCHAR(50),
    GENDER VARCHAR(50),
    GENDER_LETTER VARCHAR(50),
    ETHNICITY_SK_CODE VARCHAR(5),
    ETHNICITY_HES_CODE VARCHAR(50),
    ETHNICITY_HES_DETAILED_CODE VARCHAR(50),
    ETHNICITY_CATEGORY VARCHAR(50),
    ETHNICITY_DESC VARCHAR(255),
    ETHNICITY_CATEGORY_AND_DESC VARCHAR(255),
    YEAR_OF_BIRTH INT,
    YEARS_SINCE_BIRTH INT,
    DATE_OF_DEATH DATE,
    REG_GP_PRACTICE_CODE VARCHAR(50),
    REG_GP_PRACTICE_NAME VARCHAR(255),
    REG_GP_PRACTICE_POSTCODE VARCHAR(50),
    REG_PCN_CODE VARCHAR(50),
    REG_PCN_NAME VARCHAR(255),
    REG_ICB_CODE VARCHAR(50),
    REG_ICB_NAME VARCHAR(255),
    REG_BOROUGH VARCHAR(255),
    RESIDENCE_LSOA_CODE VARCHAR(50),
    RESIDENCE_LSOA_NAME VARCHAR(50),
    RESIDENCE_LSOA_IMD_DECILE VARCHAR(50),
    RESIDENCE_LSOA_IMD_QUINTILE VARCHAR(50),
    DATETIME_RUN TIMESTAMP_NTZ(9)
)
target_lag = '1 DAY'
refresh_mode = FULL
WAREHOUSE = NCL_ANALYTICS_XS
AS

--CTE for combined SUS data with row_numbers for first non-null data for dob, ethnicity, gender
WITH CTE_COMBINED_SUS AS (

    --Combine OP, A&E, IP data and filter ethnic code
    SELECT
        *,
        --Partition over different fields to get the most recent valid data
        ROW_NUMBER() OVER (
            PARTITION BY "SK_PatientID"
            ORDER BY "EncounterStartDateTime" DESC, "SK_EncounterID" DESC
        ) AS row_number_base,
        
        ROW_NUMBER() OVER (
            PARTITION BY "SK_PatientID" 
            ORDER BY
                CASE WHEN "Date_of_Birth" IS NOT NULL THEN 0 ELSE 1 END,
                "EncounterStartDateTime" DESC, "SK_EncounterID" DESC
        ) AS row_number_dob,
        
        ROW_NUMBER() OVER (
            PARTITION BY "SK_PatientID" 
            ORDER BY
                CASE
                    WHEN "SK_EthnicityID" IS NULL THEN 2
                    WHEN "SK_EthnicityID" NOT IN (1, 58, 59, 60, 61, 62, 63, 99, 100, 101, 102, 103, 191, 192, 193) THEN 0 
                    ELSE 1 END,
                "EncounterStartDateTime" DESC, "SK_EncounterID" DESC
        ) AS row_number_ethnic,
    
        ROW_NUMBER() OVER (
            PARTITION BY "SK_PatientID" 
            ORDER BY
                CASE WHEN "LSOACode" IS NOT NULL THEN 0 ELSE 1 END,
                "EncounterStartDateTime" DESC, "SK_EncounterID" DESC
        ) AS row_number_lsoa
    
    FROM (
    
    SELECT 
    ed."SK_PatientID", ep."SK_EncounterID",
    ep."Date_of_Birth",
    ep."SK_EthnicityID",
    ep."SK_GenderID",
    ep."SK_PracticeID", ep."SK_Org_PracticeID",
    ep."LSOACode",
    ed."EncounterStartDateTime",
    'OP' AS SUS_DATA_SOURCE
    
    FROM DATA_LAKE.SUS_OP."EncounterPatient" ep
    JOIN DATA_LAKE.SUS_OP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
    
    UNION ALL
    
    SELECT 
    ed."SK_PatientID", ep."SK_EncounterID",
    ep."Date_of_Birth",
    ep."SK_EthnicityID",
    ep."SK_GenderID",
    ep."SK_PracticeID", ep."SK_Org_PracticeID",
    ep."LSOACode",
    ed."EncounterStartDateTime",
    'A&E' AS SUS_DATA_SOURCE
    
    FROM DATA_LAKE.SUS_AE."EncounterPatient" ep
    JOIN DATA_LAKE.SUS_AE."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
    
    UNION ALL
    
    SELECT 
    ed."SK_PatientID", ep."SK_EncounterID",
    ep."Date_of_Birth",
    ep."SK_EthnicityID",
    ep."SK_GenderID",
    ep."SK_PracticeID", ep."SK_Org_PracticeID",
    ep."LSOACode",
    ed."EncounterStartDateTime",
    'IP' AS SUS_DATA_SOURCE
    
    FROM DATA_LAKE.SUS_IP."EncounterPatient" ep
    JOIN DATA_LAKE.SUS_IP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
    
    )
),
--CTE to get 1 record per patient with only the latest field values
CTE_LATEST_RECORDS AS (
    SELECT
        "SK_PatientID", 
        MAX(CASE WHEN row_number_base = 1 THEN "SK_EncounterID" END) AS "SK_EncounterID",
        MAX(CASE WHEN row_number_dob = 1 THEN "Date_of_Birth" END) AS "Date_of_Birth",
        MAX(CASE WHEN row_number_ethnic = 1 THEN "SK_EthnicityID" END) AS "SK_EthnicityID",
        MAX(CASE WHEN row_number_base = 1 THEN "SK_GenderID" END) AS "SK_GenderID",
        MAX(CASE WHEN row_number_base = 1 THEN "SK_PracticeID" END) AS "SK_PracticeID",
        MAX(CASE WHEN row_number_base = 1 THEN "SK_Org_PracticeID" END) AS "SK_Org_PracticeID",
        MAX(CASE WHEN row_number_lsoa = 1 THEN "LSOACode" END) AS "LSOACode",
        MAX(CASE WHEN row_number_base = 1 THEN SUS_DATA_SOURCE END) AS SUS_DATA_SOURCE,
        MAX("EncounterStartDateTime") AS "EncounterStartDateTime"
    FROM CTE_COMBINED_SUS
    GROUP BY "SK_PatientID"
)

SELECT
    -- SK ID Fields
    sus."SK_PatientID" AS SK_PATIENT_ID,
    sus."SK_EncounterID" AS SK_ENCOUNTER_ID,

    --Datetime of the latest SUS activity
    sus."EncounterStartDateTime" AS DATETIME_SUS_ENCOUNTER,
    --sus."SUS_DATA_SOURCE", --Removed as different fields can come from different sources

    --Demographic information
    code_gender."GenderCode" AS GENDER_CODE,
    code_gender."Gender" AS GENDER,
    code_gender."GenderCode2" AS GENDER_LETTER,
    sus."SK_EthnicityID" AS ETHNICITY_SK_CODE,
    code_ethnic_1."EthnicityHESCode" AS ETHNICITY_HES_CODE,
    code_ethnic_1."EthnicityCombinedCode" AS ETHNICITY_HES_DETAILED_CODE,
    code_ethnic_2."EthnicityCategory" AS ETHNICITY_CATEGORY,
    code_ethnic_2."EthnicityDesc" AS ETHNICITY_DESC,
    code_ethnic_1."EthnicityDesc" AS ETHNICITY_CATEGORY_AND_DESC,
    YEAR(sus."Date_of_Birth") AS YEAR_OF_BIRTH,
    CASE 
        WHEN (sus."Date_of_Birth" IS NOT NULL AND code_death.REG_DATE_OF_DEATH IS NULL) THEN 
        DATEDIFF(YEAR, sus."Date_of_Birth", GETDATE())
        ELSE NULL
    END AS YEARS_SINCE_BIRTH,
    
    -- Prefer death registry for date of death
    code_death.REG_DATE_OF_DEATH AS DATE_OF_DEATH,

    -- GP Registration information
    dict_org_gp."Organisation_Code" AS REG_GP_PRACTICE_CODE,
    dict_org_gp."Organisation_Name" AS REG_GP_PRACTICE_NAME,
    dict_post."Postcode_8_chars" AS REG_GP_PRACTICE_POSTCODE,
    dict_org_pcn."Organisation_Code" AS REG_PCN_CODE,
    dict_org_pcn."Organisation_Name" AS REG_PCN_NAME,
    dict_org_healthauth."Organisation_Code" AS REG_ICB_CODE,
    dict_org_healthauth."Organisation_Name" AS REG_ICB_NAME,
    CASE 
        WHEN dict_org_borough."Borough" IS NULL THEN 'Non-NCL'
        ELSE dict_org_borough."Borough"
    END AS REG_BOROUGH,

    --Residence information
    sus."LSOACode" AS RESIDENCE_LSOA_CODE,
    imd.LSOA_NAME_2011 AS RESIDENCE_LSOA_NAME,
    imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE AS RESIDENCE_LSOA_IMD_DECILE,
    CASE 
        WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('1','2') THEN '1 - Most Deprived'
        WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('3','4') THEN '2'
        WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('5','6') THEN '3'
        WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('7','8') THEN '4'
        WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('9','10') THEN '5 - Least Deprived'
        ELSE 'Unknown' 
    END AS RESIDENCE_LSOA_IMD_QUINTILE,
    GETDATE() AS DATETIME_RUN
    
FROM CTE_LATEST_RECORDS sus

--Organisation fields
LEFT JOIN "Dictionary"."dbo"."Organisation" dict_org_gp ON sus."SK_Org_PracticeID" = dict_org_gp."SK_OrganisationID"
LEFT JOIN "Dictionary"."dbo"."Postcode" dict_post on dict_org_gp."SK_PostcodeID" = dict_post."SK_PostcodeID"

--Select only GP records from Org table for performance
LEFT JOIN (SELECT * FROM "Dictionary"."dbo"."OrganisationHierarchyPractice" WHERE "Level" = 4) dict_hierarchy 
ON dict_org_gp."SK_OrganisationID" = dict_hierarchy."SK_OrganisationID"

--Join GP onto parent PCN
LEFT JOIN "Dictionary"."dbo"."Organisation" dict_org_pcn ON dict_hierarchy."SK_OrganisationID_Parent" = dict_org_pcn."SK_OrganisationID" 
LEFT JOIN "Dictionary"."dbo"."Organisation" dict_org_healthauth ON dict_org_gp."SK_OrganisationID_HealthAuthority" = dict_org_healthauth."SK_OrganisationID"

--Local Steve Reiners lookup table for NCL PCN to Borough
LEFT JOIN MODELLING.LOOKUP_NCL.SR_PCN_BOROUGH_LOOKUP dict_org_borough ON dict_org_pcn."Organisation_Code" = dict_org_borough."pcn_code"

--Local lookup table for deprivation decile based on LSOA
LEFT JOIN MODELLING.LOOKUP_NCL.IMD_2019 imd ON sus."LSOACode" = imd."LSOA_CODE_2011"

--Code lookups
LEFT JOIN "Dictionary"."dbo"."Gender" code_gender ON sus."SK_GenderID" = code_gender."SK_GenderID"
LEFT JOIN "Dictionary"."dbo"."Ethnicity" code_ethnic_1 ON sus."SK_EthnicityID" = code_ethnic_1."SK_EthnicityID"
LEFT JOIN "Dictionary"."dbo"."Ethnicity2" code_ethnic_2 ON sus."SK_EthnicityID" = code_ethnic_2."SK_EthnicityID"
LEFT JOIN DATA_LAKE.DEATHS."Deaths" code_death ON sus."SK_PatientID" = code_death."Pseudo NHS Number"