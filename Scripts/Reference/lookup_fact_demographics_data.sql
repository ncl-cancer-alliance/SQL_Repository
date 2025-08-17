/*
Script: LOOKUP_FACT_DEMOGRAPHICS_DATA
Version: 1.2

Description: 
Stored procedure to generate a complete list of patients and their demographics that have been or currently are resident within NCL LSOAs.  
Exclusively pulls from FACT tables (GP data).  
Also derives a complete list of patients with QoF Long-Term Conditions. 

Notes: 
- Accuracy of LTC data has not been fully validated (derived from raw GP data).  
- Availability limitations exist in the data (e.g. Barnet has limited demographic information).

Author: Graham Roberts  
Run time: TBC  

Source Tables:

Dictionary Tables (shared):
 - Dictionary.dbo.LSOA_NCL
 - Dictionary.dbo.IMD_LSOA
 - Dictionary.dbo.EthnicityLookup
 - Dictionary.dbo.OrganisationLookup
 - Dictionary.dbo.Ethnicity
 - Dictionary.dbo.Ethnicity2
 - Dictionary.dbo.Organisation
 - Dictionary.dbo.Postcode
 - Dictionary.dbo.OrganisationHierarchyPractice
 - Dictionary.dbo.OutputArea
 - Dictionary.dbo.Gender

Activity Tables:
 - DATA_LAKE.FACT_PATIENT."FactPractice"
 - DATA_LAKE.FACT_PATIENT."FactProfile"
 - DATA_LAKE.FACT_PATIENT."FactResidence"
 - DATA_LAKE.FACT_PATIENT."FactCondition"
 - DATA_LAKE.FACT_PATIENT."FactLifetimeCondition"
 - DATA_LAKE.FACT_PATIENT."DimConditionType"
 - DATA_LAKE.FACT_PATIENT."DimLifetimeConditionType"
 - DATA_LAKE.DEATHS."Deaths"

Modelling Tables:
 - MODELLING.LOOKUP_NCL.SR_PCN_BOROUGH_LOOKUP
 - MODELLING.LOOKUP_NCL.IMD_2019
 - MODELLING.LOOKUP_NCL.NEIGHBOURHOODS_2011
 - DEV__MODELLING.CANCER__REF.LOOKUP_PRIMARY_CARE_ORGS

Target Output Tables:
 - DEV__MODELLING.CANCER__REF.LOOKUP_FACT_DEMOGRAPHICS_DATA
*/

CREATE OR REPLACE PROCEDURE DEV__MODELLING.CANCER__REF.LOOKUP_FACT_DEMOGRAPHICS_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

/* ================================
   Step 1: Find latest valid Ethnicity data for patients from FactProfile (table includes multiple instances and unknowns).
   ================================ */
CREATE OR REPLACE TEMPORARY TABLE DEV__MODELLING.CANCER__REF.GP_ETH AS
    WITH LatestPeriod AS (
        SELECT 
            "SK_PatientID", 
            MAX("PeriodEnd") AS "LatestPeriodEnd"
        FROM DATA_LAKE.FACT_PATIENT."FactProfile"
        WHERE "SK_DataSourceID" = 5
        GROUP BY "SK_PatientID"
    )
    SELECT DISTINCT 
        f."SK_PatientID",
        f."SK_EthnicityID",
        e."EthnicityHESCode",
        e."EthnicityCombinedCode",
        e2."EthnicityCategory",
        e2."EthnicityDesc",
        e."EthnicityDesc" AS EthnicityDesc2
    FROM DATA_LAKE.FACT_PATIENT."FactProfile" f
    JOIN LatestPeriod lp 
        ON f."SK_PatientID" = lp."SK_PatientID" 
       AND f."PeriodEnd" = lp."LatestPeriodEnd"
    LEFT JOIN "Dictionary"."dbo"."Ethnicity" e  
        ON f."SK_EthnicityID" = e."SK_EthnicityID"
    LEFT JOIN "Dictionary"."dbo"."Ethnicity2" e2 
        ON f."SK_EthnicityID" = e2."SK_EthnicityID"
    WHERE f."SK_DataSourceID" = 5
      AND e."SK_EthnicityID" <> 1;  -- Exclude 'Unknown'

/* ======================================
   Step 2: Identify list of all NCL residents accross FACT Practice, Profile, and Residence databases.
   ====================================== */
CREATE OR REPLACE TEMPORARY TABLE DEV__MODELLING.CANCER__REF.NCL_RESIDENTS AS
WITH RankedPatientsPra AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY "SK_PatientID" ORDER BY "PeriodEnd" DESC) AS rn
    FROM DATA_LAKE.FACT_PATIENT."FactPractice"
),
RankedPatientsPro AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY "SK_PatientID" ORDER BY "PeriodEnd" DESC) AS rn
    FROM DATA_LAKE.FACT_PATIENT."FactProfile"
    WHERE "SK_DataSourceID" = 7
),
RankedPatientsRes AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY "SK_PatientID" ORDER BY "PeriodEnd" DESC) AS rn
    FROM DATA_LAKE.FACT_PATIENT."Factresidence"
    WHERE "SK_DataSourceID" = 7
)
SELECT DISTINCT 
    fpro."SK_PatientID",
    CASE WHEN fpra."PeriodEnd" = '9999-12-31' THEN 'Y' ELSE 'N' END AS "IsCurrent",
    g."GenderCode",
    g."Gender",
    g."GenderCode2",
    gp_eth."SK_EthnicityID",
    gp_eth."EthnicityHESCode",
    gp_eth."EthnicityCombinedCode",
    gp_eth."EthnicityCategory",
    gp_eth."EthnicityDesc",
    gp_eth.EthnicityDesc2,
    fpro."DateOfBirth",
    CASE 
        WHEN fpro."DateOfBirth" IS NOT NULL 
         AND COALESCE(fpro."DateOfDeath", d."REG_DATE_OF_DEATH") IS NULL
        THEN DATEDIFF(YEAR, fpro."DateOfBirth", CURRENT_DATE)
        ELSE NULL
    END AS "YearsSinceBirth",
    -- Prefer Death Registry date over EMIS
    COALESCE(d."REG_DATE_OF_DEATH", fpro."DateOfDeath") AS "DateOfDeath",
    CASE 
        WHEN d."REG_DATE_OF_DEATH" IS NOT NULL THEN 'Death Register'
        WHEN fpro."DateOfDeath" IS NOT NULL THEN 'EMIS'
        ELSE NULL
    END AS "DateOfDeathSource",
    gp.ORGANISATION_CODE,
    gp.ORGANISATION_NAME,
    gp.POSTCODE,
    gp.ORGANISATION_IMD_DECILE,
    gp.ORGANISATION_IMD_QUINTILE,
    gp.PCN_CODE,
    gp.PCN_NAME,
    gp.ICB_CODE,
    gp.ICB_NAME,
    gp.BOROUGH_NCL,
    oa."OACode",
    OA."OAName",
    n."Neighbourhood",
    imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE,
    CASE 
        WHEN imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE IN ('1','2') THEN '1 - Most Deprived'
        WHEN imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE IN ('3','4') THEN '2'
        WHEN imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE IN ('5','6') THEN '3'
        WHEN imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE IN ('7','8') THEN '4'
        WHEN imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE IN ('9','10') THEN '5 - Least Deprived'
        ELSE NULL
    END AS "IMD_Quintile",
     GETDATE() AS "LoadDate"
FROM RankedPatientsPro fpro
LEFT JOIN (SELECT * FROM RankedPatientsPra WHERE rn = 1) fpra 
       ON fpro."SK_PatientID" = fpra."SK_PatientID"
LEFT JOIN (SELECT * FROM RankedPatientsRes WHERE rn = 1) fres 
       ON fpro."SK_PatientID" = fres."SK_PatientID"
LEFT JOIN DEV__MODELLING.CANCER__REF.GP_ETH gp_eth 
       ON fpro."SK_PatientID" = gp_eth."SK_PatientID"
LEFT JOIN DEV__MODELLING.CANCER__REF.LOOKUP_PRIMARY_CARE_ORGS gp
       ON fpra."SK_OrganisationID" = gp.SK_ORGANISATION_CODE
LEFT JOIN "Dictionary"."dbo"."OutputArea" oa 
       ON fres."SK_OutputAreaID" = oa."SK_OutputAreaID"
LEFT JOIN MODELLING.LOOKUP_NCL.IMD_2019 imd 
       ON oa."OACode" = imd.LSOA_CODE_2011
LEFT JOIN MODELLING.LOOKUP_NCL.NEIGHBOURHOODS_2011 n
       ON oa."OACode" = n."LSOA11CD"
LEFT JOIN "Dictionary"."dbo"."Gender" g
       ON fpro."SK_GenderID" = g."SK_GenderID"
LEFT JOIN DATA_LAKE.DEATHS."Deaths" d 
       ON fpro."SK_PatientID" = d."Pseudo NHS Number"
WHERE fpro.rn = 1;

/* =================================
   Step 3: Create QoF LTC table
   ================================= */
CREATE OR REPLACE TEMPORARY TABLE DEV__MODELLING.CANCER__REF.QOFLTC AS
    SELECT fc."SK_PatientID",
           ct."ConditionType",
           COUNT(*) AS num
    FROM DATA_LAKE.FACT_PATIENT."FactCondition" fc
    JOIN DEV__MODELLING.CANCER__REF.NCL_RESIDENTS pop 
         ON fc."SK_PatientID" = pop."SK_PatientID"
    JOIN DATA_LAKE.FACT_PATIENT."DimConditionType" ct 
         ON fc."SK_ConditionTypeID" = ct."SK_ConditionTypeID"
    WHERE ct."SK_ConditionTypeID" = 247  -- QoF Obesity
    GROUP BY fc."SK_PatientID", ct."ConditionType"

    UNION ALL

    SELECT flc."SK_PatientID",
           ltc."LifetimeConditionType" AS ConditionType,
           COUNT(*) AS num
    FROM DATA_LAKE.FACT_PATIENT."FactLifetimeCondition" flc
    JOIN DATA_LAKE.FACT_PATIENT."DimLifetimeConditionType" ltc 
         ON flc."SK_LifetimeConditionTypeID" = ltc."SK_LifetimeConditionTypeID"
    WHERE ltc."LifetimeConditionType" LIKE 'QoF%'
    GROUP BY flc."SK_PatientID", ltc."LifetimeConditionType";

/* =================================
   Step 4: Pivot LTC into residents records
   ================================= */
CREATE OR REPLACE TABLE DEV__MODELLING.CANCER__REF.NCL_RESIDENTS_LTC AS
    SELECT 
        pop.*,
        COUNT_IF("ConditionType" = 'QoF Asthma') AS QOF_ASTHMA,
        COUNT_IF("ConditionType" = 'QoF Atrial Fibrillation') AS QOF_ATRIAL_FIBRILLATION,
        COUNT_IF("ConditionType" = 'QoF Cancer') AS QOF_CANCER,
        COUNT_IF("ConditionType" = 'QoF CKD (Stage 1-2)') AS QOF_CKD_STAGE_1_2,
        COUNT_IF("ConditionType" = 'QoF CKD (Stage 3-5)') AS QOF_CKD_STAGE_3_5,
        COUNT_IF("ConditionType" = 'QoF CHD') AS QOF_CHD,
        COUNT_IF("ConditionType" = 'QoF COPD') AS QOF_COPD,
        COUNT_IF("ConditionType" = 'QoF Dementia') AS QOF_DEMENTIA,
        COUNT_IF("ConditionType" = 'QoF Depression') AS QOF_DEPRESSION,
        COUNT_IF("ConditionType" = 'QoF Diabetes') AS QOF_DIABETES,
        COUNT_IF("ConditionType" = 'QoF Epilepsy') AS QOF_EPILEPSY,
        COUNT_IF("ConditionType" = 'QoF Heart Failure') AS QOF_HEART_FAILURE,
        COUNT_IF("ConditionType" = 'QoF Hypertension') AS QOF_HYPERTENSION,
        COUNT_IF("ConditionType" = 'QoF Learning Disabilities') AS QOF_LEARNING_DISABILITIES,
        COUNT_IF("ConditionType" = 'QoF Mental Health') AS QOF_MENTAL_HEALTH,
        COUNT_IF("ConditionType" = 'QoF Obesity') AS QOF_OBESITY,
        COUNT_IF("ConditionType" = 'QoF Osteoporosis') AS QOF_OSTEOPOROSIS,
        COUNT_IF("ConditionType" = 'QoF PAD') AS QOF_PAD,
        COUNT_IF("ConditionType" = 'QoF Rheumatoid Arthritis') AS QOF_RHEUMATOID_ARTHRITIS,
        COUNT_IF("ConditionType" = 'QoF Stroke') AS QOF_STROKE
    FROM DEV__MODELLING.CANCER__REF.NCL_RESIDENTS pop
    LEFT JOIN DEV__MODELLING.CANCER__REF.QOFLTC ltc 
           ON pop."SK_PatientID" = ltc."SK_PatientID"
    GROUP BY ALL;

/* =================================
   Step 5: Create combined dataset
   ================================= */
CREATE OR REPLACE TABLE DEV__MODELLING.CANCER__REF.LOOKUP_FACT_DEMOGRAPHICS_DATA (
    SK_PATIENT_ID INT,
    CURRENT_RESIDENT_FLAG VARCHAR(1),
    GENDER_CODE INT,
    GENDER_NAME VARCHAR(10),
    GENDER_LETTER VARCHAR(1),
    ETHNICITY_SK_CODE VARCHAR(5),
    ETHNICITY_HES_CODE VARCHAR(1),
    ETHNICITY_HES_DETAILED_CODE VARCHAR(2),
    ETHNICITY_CATEGORY VARCHAR(10),
    ETHNICITY_DESC VARCHAR(255),
    ETHNICITY_CATEGORY_AND_DESC VARCHAR(255),
    YEAR_OF_BIRTH DATE,
    YEARS_SINCE_BIRTH INT,
    DATE_OF_DEATH DATE,
    DATE_OF_DEATH_SOURCE VARCHAR(50),
    REG_GP_PRACTICE_CODE VARCHAR(6),
    REG_GP_PRACTICE_NAME VARCHAR(100),
    REG_GP_PRACTICE_POSTCODE VARCHAR(10),
    REG_GP_PRACTICE_IMD_DECILE INT,
    REG_GP_PRACTICE_IMD_QUINTILE INT,
    REG_PCN_CODE VARCHAR(6),
    REG_PCN_NAME VARCHAR(255),
    REG_ICB_CODE VARCHAR(9),
    REG_ICB_NAME VARCHAR(255),
    REG_BOROUGH_NCL VARCHAR(10),
    RESIDENCE_LSOA_CODE VARCHAR(9),
    RESIDENCE_LSOA_NAME VARCHAR(100),
    RESIDENCE_NEIGHBOURHOOD VARCHAR(100),
    RESIDENCE_LSOA_IMD_DECILE VARCHAR(1),
    RESIDENCE_LSOA_IMD_QUINTILE VARCHAR(1),
    DATETIME_RUN DATETIME,
    QOF_ASTHMA INT, 
    QOF_ATRIAL_FIBRILLATION INT,
    QOF_CANCER INT,
    QOF_CKD_STAGE_1_2 INT,
    QOF_CKD_STAGE_3_5 INT,
    QOF_CHD INT,
    QOF_COPD INT,
    QOF_DEMENTIA INT,
    QOF_DEPRESSION INT,
    QOF_DIABETES INT,
    QOF_EPILEPSY INT,
    QOF_HEART_FAILURE INT,
    QOF_HYPERTENSION INT,
    QOF_LEARNING_DISABILITIES INT,
    QOF_MENTAL_HEALTH INT,
    QOF_OSTEOPOROSIS INT,
    QOF_OBESITY INT,
    QOF_PAD INT,
    QOF_RHEUMATOID_ARTHRITIS INT,
    QOF_STROKE INT
    )
AS SELECT * FROM DEV__MODELLING.CANCER__REF.NCL_RESIDENTS_LTC;

/* =================================
   Step 6: Clean-up temp tables
   ================================= */
DROP TABLE IF EXISTS DEV__MODELLING.CANCER__REF.GP_ETH;
DROP TABLE IF EXISTS DEV__MODELLING.CANCER__REF.NCL_RESIDENTS;
DROP TABLE IF EXISTS DEV__MODELLING.CANCER__REF.QOFLTC;
DROP TABLE IF EXISTS DEV__MODELLING.CANCER__REF.NCL_RESIDENTS_LTC;

END;
$$;