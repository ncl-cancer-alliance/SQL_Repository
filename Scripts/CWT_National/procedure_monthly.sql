CREATE OR REPLACE PROCEDURE DEV__MODELLING.CANCER__CWT_NATIONAL.CREATE_CWT_NATIONAL_MONTHLY()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

--Description: Cleaned dataset for the CWT National level data
--Author: Jake Kealey

CREATE OR REPLACE TABLE DEV__MODELLING.CANCER__CWT_NATIONAL.CWT_NATIONAL_MONTHLY
AS

--CTE for Source data. 
--Source data is pulled by unioning the Comm (GP Registered) and Prov (Provider) tables
WITH pmct AS (
    --GP Registered data
    SELECT
        ''GP Registered'' AS ROW_POPULATION_TYPE,
        CCG_CODE AS ORGANISATION_CODE,
        CANCER__CWT_NATIONAL.CLEAN_TYPE_OF_CANCER(TYPE_OF_CANCER) AS TYPE_OF_CANCER_CLEAN,
        * EXCLUDE(CCG_CODE, CLINICAL_COMMISSIONING_GROUP)
    FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseComm"
    
    UNION ALL
    
    --Provider data
    SELECT
        ''Provider'' AS ROW_POPULATION_TYPE,
        PROVIDER_CODE AS ORGANISATION_CODE,
        CANCER__CWT_NATIONAL.CLEAN_TYPE_OF_CANCER(TYPE_OF_CANCER) AS TYPE_OF_CANCER_CLEAN,
        "ReportDate", CARE_SETTING, STANDARD, TYPE_OF_CANCER, CANCER_TYPE, TOTAL,
        WITHIN_14_DAYS, AFTER_14_DAYS, PERCENTAGE_SEEN_WITHIN_14_DAYS,
        WITHIN_31_DAYS, AFTER_31_DAYS, PERCENTAGE_TREATED_WITHIN_31_DAYS,
        AFTER_62_DAYS, WITHIN_62_DAYS, PERCENTAGE_TREATED_WITHIN_62_DAYS,
        TARGET, ORGANISATION_TYPE, "CreateTS", WITHIN_28_DAYS, AFTER_28_DAYS,
        PERCENTAGE_TOLD_WITHIN_28_DAYS, IN_15_TO_16_DAYS, IN_17_TO_21_DAYS, IN_22_TO_28_DAYS,
        WITHIN_32_TO_38_DAYS, WITHIN_39_TO_48_DAYS, WITHIN_49_TO_62_DAYS, WITHIN_63_TO_76_DAYS,
        WITHIN_77_TO_90_DAYS, WITHIN_91_TO_104_DAYS, AFTER_104_DAYS,
        IN_15_TO_28_DAYS, IN_29_TO_42_DAYS, IN_43_TO_62_DAYS
    FROM DATA_LAKE.PMCT."CwtMonthlySourceAppendReviseProv"
),

base_query AS (
    SELECT
        --States if the row is grouped by Provider or GP Registration (ICB)
        ROW_POPULATION_TYPE,
        
        --Date Fields
        "ReportDate" AS DATE_PERIOD,
        CASE 
            WHEN MONTH("ReportDate") < 4 
            THEN CONCAT(YEAR("ReportDate") - 1, ''-'', YEAR("ReportDate") - 2000)
            ELSE CONCAT(YEAR("ReportDate"), ''-'', YEAR("ReportDate") - 1999)
        END AS FIN_YEAR,
    
        CASE 
            WHEN MONTH("ReportDate") < 4 
            THEN MONTH("ReportDate") + 9
            ELSE MONTH("ReportDate") - 3
        END AS FIN_MONTH_NUMBER,
        MONTHNAME("ReportDate") AS FIN_MONTH_NAME,
    
        --Organisation Fields
        ORGANISATION_TYPE,
        ORGANISATION_CODE,
        org."Organisation_Name" AS ORGANISATION_NAME,
        CASE ORGANISATION_TYPE
            WHEN ''Provider'' THEN par."Organisation_Code"
            WHEN ''CCG'' THEN COALESCE(par."Organisation_Code", org_ccg.ICB_CODE)
            WHEN ''Sub-ICB'' THEN par."Organisation_Code"
            WHEN ''ICB'' THEN org."Organisation_Code"
        END AS ORGANISATION_ICB_CODE,
        CASE ORGANISATION_TYPE
            WHEN ''Provider'' THEN par."Organisation_Name"
            WHEN ''CCG'' THEN COALESCE(par."Organisation_Name", org_ccg.ICB_NAME)
            WHEN ''Sub-ICB'' THEN par."Organisation_Name"
            WHEN ''ICB'' THEN org."Organisation_Name"
        END AS ORGANISATION_ICB_NAME,
        org_ca.CANCER_ALLIANCE,
        rt.RADIOTHERAPY_NETWORK,
    
        --Breakdown Fields
        ----Reformat for consistency
        CASE pmct.STANDARD
            WHEN ''2-WEEK WAIT'' THEN ''2WW''
            WHEN ''28 DAY'' THEN ''FDS''
            WHEN ''31-DAY WAIT'' THEN ''31 DAY''
            ELSE pmct.STANDARD
        END AS STANDARD,
        
        ----When the TYPE_OF_CANCER value has a hyphen in it and can be split between a main category and subcategory
        CASE 
            --Exception for fields with hyphens but no subcategory
            WHEN TYPE_OF_CANCER_CLEAN IN(''Cancer - Non-Specific Symptoms'', ''Exhibited (Non-Cancer) Breast Symptoms - Cancer Not Initially Suspected'')
                THEN TYPE_OF_CANCER_CLEAN
            --For fields with hyphens, take the value before the hyphen
            WHEN TYPE_OF_CANCER_CLEAN LIKE ''% - %'' 
                THEN LEFT(TYPE_OF_CANCER, CHARINDEX(''-'', TYPE_OF_CANCER, 0) - 2)
            ELSE TYPE_OF_CANCER_CLEAN
        END AS CANCER_TYPE,
    
        CASE 
            --Exception for fields with hyphens but no subcategory
            WHEN TYPE_OF_CANCER_CLEAN IN(''Cancer - Non-Specific Symptoms'', ''Exhibited (Non-Cancer) Breast Symptoms - Cancer Not Initially Suspected'')
                THEN NULL
            --For fields with hyphens, take the value after the hyphen
            WHEN TYPE_OF_CANCER_CLEAN LIKE ''% - %'' 
                THEN RIGHT(TYPE_OF_CANCER_CLEAN, LEN(TYPE_OF_CANCER_CLEAN) - CHARINDEX(''-'', TYPE_OF_CANCER_CLEAN, 0))
        END AS CANCER_TYPE_SUBCATEGORY,
    
        ----Derive cancer pathway from the original CANCER_TYPE field. 
        ----The logic and grouping for this can vary on a case by case basis for different STANDARD types.
        CASE 
            --2WW
            WHEN CANCER_TYPE = ''2-WEEK WAIT - ALL SUSPECTED CANCER'' THEN ''Combined''
            WHEN CANCER_TYPE = ''2-WEEK WAIT - BREAST SYMPTOMS (CANCER NOT INITALLY SUSPECTED)'' THEN ''Breast Symptomatic''
            WHEN CANCER_TYPE = ''2-WEEK WAIT - SUSPECTED BY CANCER TYPE'' THEN ''USC''
            --FDS
            WHEN pmct.STANDARD = ''28 DAY'' THEN ''FDS''
            --31 Day
            WHEN CANCER_TYPE LIKE ''31-DAY - FIRST%'' THEN ''First''
            WHEN CANCER_TYPE LIKE ''31-DAY - 2nd%'' THEN ''Subsequent''
            WHEN CANCER_TYPE LIKE ''31-DAY COMBINED%'' THEN ''Combined''
            WHEN CANCER_TYPE LIKE ''31-DAY WAIT - RARE%'' THEN ''Rare Cancer''
            --62 Day
            WHEN CANCER_TYPE LIKE ''62-DAY - BREAST%'' THEN ''Breast Symptomatic''
            WHEN CANCER_TYPE LIKE ''62-DAY BREAST%'' THEN ''Breast Symptomatic''
            WHEN CANCER_TYPE LIKE ''62-DAY - CONSULTANT%'' THEN ''Consultant Upgrade''
            WHEN CANCER_TYPE LIKE ''62-DAY - SCREENING%'' THEN ''Screening''
            WHEN CANCER_TYPE LIKE ''62-DAY URGENT%'' THEN ''USC''
            WHEN CANCER_TYPE LIKE ''62-DAY COMBINED%'' THEN ''Combined''
            ELSE NULL
        END AS CANCER_PATHWAY,
    
        ----Group rows into groups
        CASE
            --Give grand totals their own group (avoids aggregating with other rows and double counting)
            WHEN TYPE_OF_CANCER_CLEAN = ''All Cancers'' THEN ''All Cancers''
            --Cancer type group for rows split by a tumour category
            WHEN CANCER_TYPE LIKE ''%BY CANCER TYPE'' THEN ''Cancer Type''
            WHEN CANCER_TYPE = ''28 DAY FAST DIAGNOSIS (BY ROUTE)'' THEN ''Cancer Type''
            --Treatment group, use LIKE %(OTHER) for the last catgeory to future proof against other groups having an "other" option
            WHEN TYPE_OF_CANCER_CLEAN IN (''Drugs'', ''Surgery'', ''Radiotherapy'') OR (CANCER_TYPE LIKE ''%(OTHER)'') THEN ''Treatment''
            --Standalone pathways as their own category, put here to only label 2ww Breast Symptomatic as a group. 
            --For other standards, Breast Symptomatic is included in the Cancer Type group
            WHEN CANCER_PATHWAY IN (''Breast Symptomatic'', ''Rare Cancer'') THEN CANCER_PATHWAY
            ELSE NULL
        END AS CANCER_TYPE_GROUP,
        CASE CANCER_PATHWAY
            WHEN ''FDS''
            THEN 
                CASE CARE_SETTING
                    WHEN ''URGENT SUSPECTED CANCER'' THEN ''USC''
                    WHEN ''NATIONAL SCREENING PROGRAMME'' THEN ''Screening''
                    WHEN ''BREAST SYMPTOMATIC, CANCER NOT SUSPECTED'' THEN ''Breast Symptomatic''
                    WHEN ''ALL CARE'' THEN ''All Cancers''
                    ELSE ''Unknown''
                END
            ELSE NULL
        END AS FDS_PATHWAY,
        --Standard Metrics (Base)
        TOTAL AS NO_PATIENTS,
        ----Combine metrics across standards so they can be viewed under the same columns
        CASE pmct.STANDARD
            WHEN ''2-WEEK WAIT'' THEN WITHIN_14_DAYS
            WHEN ''28 DAY''      THEN WITHIN_28_DAYS
            WHEN ''31-DAY WAIT'' THEN WITHIN_31_DAYS
            WHEN ''62 DAY''      THEN WITHIN_62_DAYS
        END AS NO_COMPLIANT,
        CASE pmct.STANDARD
            WHEN ''2-WEEK WAIT'' THEN AFTER_14_DAYS
            WHEN ''28 DAY''      THEN AFTER_28_DAYS
            WHEN ''31-DAY WAIT'' THEN AFTER_31_DAYS
            WHEN ''62 DAY''      THEN AFTER_62_DAYS
        END AS BREACHES,
        ----Performance = NO_COMPLIANT / NO_PATIENTS
        ----DIV0NULL is used to get around rows with 0 patients in the data, performance will be NULL in these cases
        DIV0NULL(
            NO_COMPLIANT,
            NO_PATIENTS) AS STANDARD_PERFORMANCE,
        TARGET,
    
        --pmct.STANDARD Metrics (Breakdown)--
        ----To prevent confusion and unintended aggregation, value breakdowns are split by standard.
        ----Downside of making the table wider than the source.
        --2WW
        CASE pmct.STANDARD WHEN ''2-WEEK WAIT'' THEN WITHIN_14_DAYS        END AS TWO_WEEK_WAIT_DAYS_WITHIN_14,
        CASE pmct.STANDARD WHEN ''2-WEEK WAIT'' THEN IN_15_TO_16_DAYS      END AS TWO_WEEK_WAIT_DAYS_15_TO_16,
        CASE pmct.STANDARD WHEN ''2-WEEK WAIT'' THEN IN_17_TO_21_DAYS      END AS TWO_WEEK_WAIT_DAYS_17_TO_21,
        CASE pmct.STANDARD WHEN ''2-WEEK WAIT'' THEN IN_22_TO_28_DAYS      END AS TWO_WEEK_WAIT_DAYS_22_TO_28,
        CASE pmct.STANDARD WHEN ''2-WEEK WAIT'' THEN AFTER_28_DAYS         END AS TWO_WEEK_WAIT_DAYS_MORE_THAN_28,
        
        --FDS 28 Days
        CASE pmct.STANDARD WHEN ''28 DAY''      THEN WITHIN_14_DAYS        END AS FDS_DAYS_WITHIN_14,
        CASE pmct.STANDARD WHEN ''28 DAY''      THEN IN_15_TO_28_DAYS      END AS FDS_DAYS_15_TO_28,
        CASE pmct.STANDARD WHEN ''28 DAY''      THEN IN_29_TO_42_DAYS      END AS FDS_DAYS_29_TO_42,
        CASE pmct.STANDARD WHEN ''28 DAY''      THEN IN_43_TO_62_DAYS      END AS FDS_DAYS_43_TO_62,
        CASE pmct.STANDARD WHEN ''28 DAY''      THEN AFTER_62_DAYS         END AS FDS_DAYS_MORE_THAN_62,
        
        --31 Days
        CASE pmct.STANDARD WHEN ''31-DAY WAIT'' THEN WITHIN_31_DAYS        END AS D31_DAYS_WITHIN_31,
        CASE pmct.STANDARD WHEN ''31-DAY WAIT'' THEN WITHIN_32_TO_38_DAYS  END AS D31_DAYS_32_TO_38,
        CASE pmct.STANDARD WHEN ''31-DAY WAIT'' THEN WITHIN_39_TO_48_DAYS  END AS D31_DAYS_39_TO_48,
        CASE pmct.STANDARD WHEN ''31-DAY WAIT'' THEN WITHIN_49_TO_62_DAYS  END AS D31_DAYS_49_TO_62,
        CASE pmct.STANDARD WHEN ''31-DAY WAIT'' THEN AFTER_62_DAYS         END AS D31_DAYS_MORE_THAN_62,
    
        --62 Days
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN WITHIN_31_DAYS        END AS D62_DAYS_WITHIN_31,
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN WITHIN_32_TO_38_DAYS  END AS D62_DAYS_32_TO_38,
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN WITHIN_39_TO_48_DAYS  END AS D62_DAYS_39_TO_48,
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN WITHIN_49_TO_62_DAYS  END AS D62_DAYS_49_TO_62,
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN WITHIN_63_TO_76_DAYS  END AS D62_DAYS_63_TO_76,
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN WITHIN_77_TO_90_DAYS  END AS D62_DAYS_77_TO_90,
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN WITHIN_91_TO_104_DAYS END AS D62_DAYS_91_TO_104,
        CASE pmct.STANDARD WHEN ''62 DAY''      THEN AFTER_104_DAYS        END AS D62_DAYS_MORE_THAN_104
    
    --Source data is pulled by unioning the Comm (GP Registered) and Prov (Provider) tables
    FROM pmct pmct
    
    --Join for Organisation name and Parent SK Code
    LEFT JOIN "Dictionary"."dbo"."Organisation" org
    ON ORGANISATION_CODE = org."Organisation_Code"
    
    --Join for parent name (ICB) for Providers, Sub-ICBs
    LEFT JOIN "Dictionary"."dbo"."Organisation" par
    ON org."SK_OrganisationID_ParentOrg" = par."SK_OrganisationID"
    
    --Join for old CCG codes
    LEFT JOIN (
        SELECT 
            orgd."OrganisationCode_Child" AS CCG_CODE,
            org."Organisation_Name" AS ICB_NAME,
            org."Organisation_Code" AS ICB_CODE
        FROM "Dictionary"."dbo"."OrganisationDescendent" orgd
        
        LEFT JOIN "Dictionary"."dbo"."Organisation" org
        ON orgd."OrganisationCode_Root" = org."Organisation_Code"
        
        WHERE "OrganisationPrimaryRole_Root" = ''RO261''
        AND "OrganisationPrimaryRole_Child" = ''RO98''
    ) org_ccg
    ON ORGANISATION_CODE = org_ccg.CCG_CODE
    
    --Join for Organisation to Cancer Alliance mapping
    LEFT JOIN DEV__MODELLING.CANCER__REF.DIM_ORGANISATIONS org_ca
    ON ORGANISATION_CODE = org_ca.CODE
    
    --Join for Provider to Radiotherapy Network
    ----This mapping is exclusive to (some) provider rows, the RADIOTHERAPY_NETWORK will be NULL for other cases
    LEFT JOIN DEV__MODELLING.CANCER__REF.DIM_CANCER_RADIOTHERAPY_NETWORK rt
    ON ORGANISATION_CODE = rt.PROVIDER_CODE
    AND ROW_POPULATION_TYPE = ''Provider''
)

SELECT 
    ROW_POPULATION_TYPE, 
    DATE_PERIOD, 
    FIN_YEAR, 
    FIN_MONTH_NUMBER, 
    FIN_MONTH_NAME, 
    ORGANISATION_TYPE, 
    ORGANISATION_CODE, 
    ORGANISATION_NAME, 
    ORGANISATION_ICB_CODE, 
    ORGANISATION_ICB_NAME, 
    CANCER_ALLIANCE, 
    RADIOTHERAPY_NETWORK, 
    STANDARD, 
    CANCER_TYPE, 
    CANCER_TYPE_SUBCATEGORY, 
    CANCER_PATHWAY, 
    CANCER_TYPE_GROUP, 
    FDS_PATHWAY, 
    SUM(NO_PATIENTS) AS NO_PATIENTS,
    SUM(NO_COMPLIANT) AS NO_COMPLIANT,
    SUM(BREACHES) AS BREACHES,
    
    --Performance = NO_COMPLIANT / NO_PATIENTS
    --DIV0NULL is used to get around rows with 0 patients in the data, performance will be NULL in these cases
    DIV0NULL(
        SUM(NO_COMPLIANT),
        SUM(NO_PATIENTS)
    ) AS STANDARD_PERFORMANCE,
    
    SUM(TARGET) AS TARGET,
    SUM(TWO_WEEK_WAIT_DAYS_WITHIN_14) AS TWO_WEEK_WAIT_DAYS_WITHIN_14,
    SUM(TWO_WEEK_WAIT_DAYS_15_TO_16) AS TWO_WEEK_WAIT_DAYS_15_TO_16,
    SUM(TWO_WEEK_WAIT_DAYS_17_TO_21) AS TWO_WEEK_WAIT_DAYS_17_TO_21,
    SUM(TWO_WEEK_WAIT_DAYS_22_TO_28) AS TWO_WEEK_WAIT_DAYS_22_TO_28,
    SUM(TWO_WEEK_WAIT_DAYS_MORE_THAN_28) AS TWO_WEEK_WAIT_DAYS_MORE_THAN_28,
    SUM(FDS_DAYS_WITHIN_14) AS FDS_DAYS_WITHIN_14,
    SUM(FDS_DAYS_15_TO_28) AS FDS_DAYS_15_TO_28,
    SUM(FDS_DAYS_29_TO_42) AS FDS_DAYS_29_TO_42,
    SUM(FDS_DAYS_43_TO_62) AS FDS_DAYS_43_TO_62,
    SUM(FDS_DAYS_MORE_THAN_62) AS FDS_DAYS_MORE_THAN_62,
    SUM(D31_DAYS_WITHIN_31) AS D31_DAYS_WITHIN_31,
    SUM(D31_DAYS_32_TO_38) AS D31_DAYS_32_TO_38,
    SUM(D31_DAYS_39_TO_48) AS D31_DAYS_39_TO_48,
    SUM(D31_DAYS_49_TO_62) AS D31_DAYS_49_TO_62,
    SUM(D31_DAYS_MORE_THAN_62) AS D31_DAYS_MORE_THAN_62,
    SUM(D62_DAYS_WITHIN_31) AS D62_DAYS_WITHIN_31,
    SUM(D62_DAYS_32_TO_38) AS D62_DAYS_32_TO_38,
    SUM(D62_DAYS_39_TO_48) AS D62_DAYS_39_TO_48,
    SUM(D62_DAYS_49_TO_62) AS D62_DAYS_49_TO_62,
    SUM(D62_DAYS_63_TO_76) AS D62_DAYS_63_TO_76,
    SUM(D62_DAYS_77_TO_90) AS D62_DAYS_77_TO_90,
    SUM(D62_DAYS_91_TO_104) AS D62_DAYS_91_TO_104,
    SUM(D62_DAYS_MORE_THAN_104) AS D62_DAYS_MORE_THAN_104

FROM base_query
GROUP BY ALL;

END;
';