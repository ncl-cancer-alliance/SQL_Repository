/*
Script: PATIENTENCOUNTERDATA
Version: 1.1
Description: Unions patient demographics from SUS OP/IP/A&E databases for all patients. Selects valid characteristics from the latest recorded encounter across the 3 datasets.
Required for secondary care reporting, particularly CWT, where a large proportion of patients receiving cancer care are not NCL residents or registered with NCL GP's.
Author: Graham Roberts
Run time: TBC (previously 6-7 mins in Sandpit)

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
DEV__MODELLING.CANCER__REF.PATIENTENCOUNTERDATA (a dynamic table, replacing GrahamR.PatientEncounterData)
*/

-- Select requisite database and schema
    
    USE DATABASE DEV_MODELLING;
    USE SCHEMA CANCER__REF;

   --Create and populate the main SUS table which unions OP, IP, and A&E patient and SUS encounter tables
	--OP data
    CREATE OR REPLACE TABLE RankedAggSUSData AS
    WITH CTE_AggSUSData AS (
        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."Date_of_Birth",
            ep."SK_EthnicityID",
            ep."SK_GenderID",
            ep."SK_PracticeID",
            ep."SK_Org_PracticeID",
            ep."WardCode",
            ep."LSOACode",
            ed."EncounterStartDateTime",
            'OP' AS SUS_Data_Source
        FROM DATA_LAKE.SUS_OP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_OP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"

        UNION ALL
		--A&E data
        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."Date_of_Birth",
            ep."SK_EthnicityID",
            ep."SK_GenderID",
            ep."SK_PracticeID",
            ep."SK_Org_PracticeID",
            ep."WardCode",
            ep."LSOACode",
            ed."EncounterStartDateTime",
            'AE' AS SUS_Data_Source
        FROM DATA_LAKE.SUS_AE."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_AE."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"

        UNION ALL
		--IP data
        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."Date_of_Birth",
            ep."SK_EthnicityID",
            ep."SK_GenderID",
            ep."SK_PracticeID",
            ep."SK_Org_PracticeID",
            ep."WardCode",
            ep."LSOACode",
            ed."EncounterStartDateTime",
            'IP' AS SUS_Data_Source
        FROM DATA_LAKE.SUS_IP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_IP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
    )
	--Order by latest SUS encounter start date/time descending to find latest demographic details for each patient.
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "SK_PatientID" ORDER BY "EncounterStartDateTime" DESC) AS RankedRecord
    FROM CTE_AggSUSData;

    --Subquery's required for DOB, Ethnicity, and LSOA to handle instances where the latest record for patients is blank but prior records contain data.
	--Subquery to populate latest valid DOB captured
	CREATE OR REPLACE TABLE RankedAggDOBData AS
    WITH CTE_AggDOBData AS (
        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."Date_of_Birth",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_OP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_OP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."Date_of_Birth" IS NOT NULL

        UNION ALL

        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."Date_of_Birth",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_AE."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_AE."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."Date_of_Birth" IS NOT NULL

        UNION ALL

        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."Date_of_Birth",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_IP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_IP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."Date_of_Birth" IS NOT NULL
    )

    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "SK_PatientID" ORDER BY "EncounterStartDateTime" DESC) AS RankedRecord
    FROM CTE_AggDOBData;

    --Subquery to populate latest valid Ethnicity captured
    CREATE OR REPLACE TABLE RankedAggEthData AS
    WITH CTE_AggEthData AS (
        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."SK_EthnicityID",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_OP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_OP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."SK_EthnicityID" NOT IN (1, 58, 59, 60, 61, 62, 63, 99, 100, 101, 102, 103, 191, 192, 193)

        UNION ALL

        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."SK_EthnicityID",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_AE."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_AE."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."SK_EthnicityID" NOT IN (1, 58, 59, 60, 61, 62, 63, 99, 100, 101, 102, 103, 191, 192, 193)

        UNION ALL

        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."SK_EthnicityID",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_IP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_IP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."SK_EthnicityID" NOT IN (1, 58, 59, 60, 61, 62, 63, 99, 100, 101, 102, 103, 191, 192, 193)
    )

    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "SK_PatientID" ORDER BY "EncounterStartDateTime" DESC) AS RankedRecord
    FROM CTE_AggEthData;

    --Subquery to populate latest valid LSOA captured
    CREATE OR REPLACE TABLE RankedAggLSOAData AS
    WITH CTE_AggLSOAData AS (
        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."LSOACode",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_OP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_OP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."LSOACode" IS NOT NULL

        UNION ALL

        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."LSOACode",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_AE."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_AE."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."LSOACode" IS NOT NULL

        UNION ALL

        SELECT 
            ed."SK_PatientID",
            ep."SK_EncounterID",
            ep."LSOACode",
            ed."EncounterStartDateTime"
        FROM DATA_LAKE.SUS_IP."EncounterPatient" ep
        JOIN DATA_LAKE.SUS_IP."EncounterDetail" ed ON ep."SK_EncounterID" = ed."SK_EncounterID"
        WHERE ep."LSOACode" IS NOT NULL
    )

    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "SK_PatientID" ORDER BY "EncounterStartDateTime" DESC) AS RankedRecord
    FROM CTE_AggLSOAData;

    --Create and populate the final patient encounter data table with the latest demographic details
      CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__REF.SUS_PATIENT_DEMOGRAPHICS(
    RUN_DATE_TIME DATETIME,
    PSEUDO_ID INT,
    SUS_ENCOUNTER_ID BIGINT,
    SUS_ENCOUNTER_DATE_TIME DATETIME,
    SUS_DATA_SOURCE VARCHAR(50),
    GENDER_CODE VARCHAR(50),
    GENDER VARCHAR(50),
    GENDER_LETTER VARCHAR(50),
    ETHNICITY_SANDPIT_CODE VARCHAR(5),
    ETHNICITY_HES_CODE VARCHAR(50),
    ETHNICITY_HES_CODE_DETAIL VARCHAR(50),
    ETHNICITY_CATEGORY VARCHAR(50),
    ETHNICITY_DESC VARCHAR(255),
    ETHNICITY_CATEGORY_AND_DESC VARCHAR(255),
    DATE_OF_BIRTH DATE,
    CURRENT_AGE INT,
    DATE_OF_DEATH DATE,
    GP_PRACTICE_CODE VARCHAR(50),
    GP_PRACTICE_NAME VARCHAR(255),
    GP_PRACTICE_POSTCODE VARCHAR(50),
    PCN_CODE VARCHAR(50),
    PCN_NAME VARCHAR(255),
    ICB_CODE VARCHAR(50),
    ICB_NAME VARCHAR(255),
    BOROUGH_NCL VARCHAR(255),
    LSOA_CODE VARCHAR(50),
    LSOA_NAME VARCHAR(50),
    IMD_DECILE_LSOA VARCHAR(50),
    IMD_QUINTILE_LSOA VARCHAR(50)
    )
    target_lag = '1 DAY'
    refresh_mode = FULL
    WAREHOUSE = NCL_ANALYTICS_XS
    AS
    SELECT
		GETDATE(),
        s."SK_PatientID",
        s."SK_EncounterID",
        s."EncounterStartDateTime",
        s."SUS_DATA_SOURCE",
		g."GenderCode",
        g."Gender",
		g."GenderCode2",
		eth."SK_EthnicityID",
        e."EthnicityHESCode",
        e."EthnicityCombinedCode",
        e2."EthnicityCategory",
        e2."EthnicityDesc",
        e."EthnicityDesc",
        dob."Date_of_Birth",
		CASE 
			WHEN (dob."Date_of_Birth" IS NOT NULL AND d."REG_DATE_OF_DEATH" IS NULL) THEN 
            DATEDIFF(YEAR, dob."Date_of_Birth", GETDATE())
        ELSE NULL
    END AS Current_Age,
	-- Prefer death registry for date of death
	    d."REG_DATE_OF_DEATH" AS Date_of_Death,
        a."Organisation_Code",
        a."Organisation_Name",
		po."Postcode_8_chars",
        c."Organisation_Code",
        c."Organisation_Name",
        i."Organisation_Code",
        i."Organisation_Name",
        t."Borough",
		lsoa."LSOACode",
		imd.LSOA_NAME_2011,
        imd.INDEX_OF_MULTIPLE_DEPRIVATION_DECILE,
        CASE 
            WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('1','2') THEN '1 - Most Deprived'
            WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('3','4') THEN '2'
            WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('5','6') THEN '3'
            WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('7','8') THEN '4'
            WHEN imd."INDEX_OF_MULTIPLE_DEPRIVATION_DECILE" IN ('9','10') THEN '5 - Least Deprived'
            ELSE 'Unknown' 
        END
    FROM RankedAggSUSData s
	--Subquery's in joins to select only latest populated data for Ethnicity, DOB, and LSOA
	LEFT JOIN (select * from RankedAggEthData where rankedrecord = 1) eth ON s."SK_PatientID" = eth."SK_PatientID"
	LEFT JOIN (select * from RankedAggDOBData where rankedrecord = 1) dob ON s."SK_PatientID" = dob."SK_PatientID"
	LEFT JOIN (select * from RankedAggLSOAData where rankedrecord = 1) lsoa ON s."SK_PatientID" = lsoa."SK_PatientID"
	LEFT JOIN "Dictionary"."dbo"."Organisation" a ON s."SK_Org_PracticeID" = a."SK_OrganisationID"
	LEFT JOIN "Dictionary"."dbo"."Postcode" po on a."SK_PostcodeID" = po."SK_PostcodeID"
	--Select only GP records from Org table for performance
	LEFT JOIN (SELECT * FROM "Dictionary"."dbo"."OrganisationHierarchyPractice" WHERE "Level" = 4) b ON a."SK_OrganisationID" = b."SK_OrganisationID" 
	--Join GP onto parent PCN
	LEFT JOIN "Dictionary"."dbo"."Organisation" c ON b."SK_OrganisationID_Parent" = c."SK_OrganisationID" 
	LEFT JOIN "Dictionary"."dbo"."Organisation" i ON a."SK_OrganisationID_HealthAuthority" = i."SK_OrganisationID"
	--Local Steve Reiners lookup table for NCL PCN to Borough
	LEFT JOIN MODELLING.LOOKUP_NCL.SR_PCN_BOROUGH_LOOKUP t ON c."Organisation_Code" = t."pcn_code"
	--Local lookup table for deprivation decile based on LSOA
	LEFT JOIN MODELLING.LOOKUP_NCL.IMD_2019 imd ON lsoa."LSOACode" = imd."LSOA_CODE_2011"
	LEFT JOIN "Dictionary"."dbo"."Gender" g ON s."SK_GenderID" = g."SK_GenderID"
	LEFT JOIN "Dictionary"."dbo"."Ethnicity" e ON eth."SK_EthnicityID" = e."SK_EthnicityID"
	LEFT JOIN "Dictionary"."dbo"."Ethnicity2" e2 ON eth."SK_EthnicityID" = e2."SK_EthnicityID"
	LEFT JOIN DATA_LAKE.DEATHS."Deaths" d ON s."SK_PatientID" = d."Pseudo NHS Number"
	WHERE s."RANKEDRECORD" = 1;

	-- Clean up intermediary tables
    DROP TABLE IF EXISTS RankedAggSUSData;
	DROP TABLE IF EXISTS RankedAggDOBData;
	DROP TABLE IF EXISTS RankedAggEthData;
	DROP TABLE IF EXISTS RankedAggLSOAData;