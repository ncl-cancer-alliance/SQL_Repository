-- Dynamic table to pull data required to populate the Referrals Received by NCL Providers dashboard in the Cancer Alliance folder of NCL PBI workspace.
-- Note, field names not following NCL naming conventions to mirror dashboard build using Sandpit data.
-- Contact: rachel.bryant7@nhs.net

CREATE DYNAMIC TABLE DEV__PUBLISHED_REPORTING__SECONDARY_USE.MAINDATA__ERS.ERS_REFS_RECEIVED (
IS_GENERAL_ACUTE,
POD,
"EndDate",
"Rolling12MCategory",
"ProviderName",
"ProviderSite",
"ProviderGroup",
"ProviderNameNCL",
"ProviderSiteNCL",
"MainSpecName",
"Age_Group",
"EthnicityDesc",
"GenderDesc",
"DeprivationDecile",
"ReferringOrgName",
REFERRING_ORG_TYPE,
REFERRING_ORG_TYPE_GROUP,
REFERRING_ORG_COMMISSIONER,
"Referring_Org_Commissioner_Group",
"Postcode",
"Longitude",
"Latitude",
"OPPriorityTypeDesc",
"eRSApptType",
"eRSClinicType",
"eRSServiceName",
"Activity",
NCL_REFERRING_ORG_BOROUGH
)
    TARGET_LAG = '2 hours' 
    REFRESH_MODE = FULL 
    INITIALIZE = ON_CREATE 
    WAREHOUSE = NCL_ANALYTICS_XS
    COMMENT='Dynamic table to query ERS maindata referrals received for use in the Referrals Received Dashboard.'
  AS

WITH max_date_cte AS (
    SELECT MAX(DATE_END) AS max_date
    FROM DEV__MODELLING.MAIN_DATA.ERS_REFERRALS_RECEIVED 
),

max_sunday_cte AS (
    SELECT MAX(DATE_END) AS max_date
    FROM DEV__MODELLING.MAIN_DATA.ERS_REFERRALS_RECEIVED 
    WHERE DAYOFWEEK(DATE_END) = 0
),

 org_cte AS (
    SELECT org."Organisation_Name"
        ,org."Organisation_Code"
        ,org."SK_OrganisationTypeID"
        ,org."SK_OrganisationID_ParentOrg"
        ,orgt."OrganisationType"
        ,post."Postcode"
        ,post."Longitude"
        ,post."Latitude"
        ,b."Borough"
        ,bor."Geography_Name"

    FROM "Dictionary"."dbo"."Organisation" as org

    LEFT JOIN "Dictionary"."dbo"."OrganisationType" AS orgt 
    ON org."SK_OrganisationTypeID" = orgt."SK_OrganisationTypeID" 

    LEFT JOIN "Dictionary"."dbo"."Postcode" AS post 
    ON org."SK_Postcode_ID" = post."SK_PostcodeID" 
    
    LEFT JOIN "Dictionary"."dbo"."OrganisationMatrixPractice" x 
    ON org."SK_OrganisationID" = x."SK_OrganisationID_Practice"
    
    LEFT JOIN "Dictionary"."dbo"."Organisation" c 
    ON x."SK_OrganisationID_Network" = c."SK_OrganisationID"
    
    LEFT JOIN (
    SELECT DISTINCT "pcn_code", "Borough"
    FROM MODELLING.LOOKUP_NCL.SR_PCN_BOROUGH_LOOKUP
    ) b
    ON c."Organisation_Code" = b."pcn_code"

    LEFT JOIN "Dictionary"."dbo"."ONSCodeEquivalent" AS bor
    ON post."Local_Authority_District_Unitary_Authority" = bor."Geography_Code"
    
),

 org_cte_gp_comm AS (
    SELECT org2.PRACTICE_CODE
        ,org2.ICB_NAME

    FROM DEV__MODELLING.CANCER__REF.DIM_PRACTICE as org2

 ),

  org_cte_prov_comm AS (
    SELECT org3.CODE
        ,org3.NAME
        ,org3.ICB_NAME
        ,bor."Geography_Name"

    FROM DEV__MODELLING.CANCER__REF.DIM_ORGANISATIONS as org3

    LEFT JOIN "Dictionary"."dbo"."Organisation" as org
    ON org3.CODE = org."Organisation_Code"

    LEFT JOIN "Dictionary"."dbo"."Postcode" AS post 
    ON org."SK_Postcode_ID" = post."SK_PostcodeID"

    LEFT JOIN "Dictionary"."dbo"."ONSCodeEquivalent" AS bor
    ON post."Local_Authority_District_Unitary_Authority" = bor."Geography_Code"
    
    WHERE ( org3.PRIMARY_ROLE_NAME = 'NHS TRUST SITE'
    OR org3.PRIMARY_ROLE_NAME = 'NHS TRUST'
    OR org3.PRIMARY_ROLE_NAME = 'CLINICAL COMMISSIONING GROUP'
    OR org3.PRIMARY_ROLE_NAME = 'CANCER NETWORK'
    OR org3.PRIMARY_ROLE_NAME = 'PRIMARY CARE TRUST'
    OR org3.PRIMARY_ROLE_NAME = 'INTEGRATED CARE BOARD')
 ),


ers_data AS (

SELECT
    IS_GENERAL_ACUTE,
    ers.POD,
    ers.DATE_END AS "EndDate",

CASE 
    WHEN ers.DATE_END >= DATEADD(MONTH, -12, max_date_cte.max_date) THEN '0'
    WHEN ers.DATE_END >= DATEADD(MONTH, -24, max_date_cte.max_date) 
         AND ers.DATE_END < DATEADD(MONTH, -12, max_date_cte.max_date) THEN '-1'
    WHEN ers.DATE_END >= DATEADD(MONTH, -36, max_date_cte.max_date)
         AND ers.DATE_END < DATEADD(MONTH, -24, max_date_cte.max_date) THEN '-2'
    WHEN ers.DATE_END >= DATEADD(MONTH, -48, max_date_cte.max_date)
         AND ers.DATE_END < DATEADD(MONTH, -36, max_date_cte.max_date) THEN '-3'
    WHEN ers.DATE_END >= DATEADD(MONTH, -60, max_date_cte.max_date)
         AND ers.DATE_END < DATEADD(MONTH, -48, max_date_cte.max_date) THEN '-4'
    WHEN ers.DATE_END >= DATEADD(MONTH, -72, max_date_cte.max_date)
         AND ers.DATE_END < DATEADD(MONTH, -60, max_date_cte.max_date) THEN '-5'
    WHEN ers.DATE_END >= DATEADD(MONTH, -84, max_date_cte.max_date)
         AND ers.DATE_END < DATEADD(MONTH, -72, max_date_cte.max_date) THEN '-6'
    WHEN ers.DATE_END >= DATEADD(MONTH, -96, max_date_cte.max_date)
         AND ers.DATE_END < DATEADD(MONTH, -84, max_date_cte.max_date) THEN '-7'
    WHEN ers.DATE_END >= DATEADD(MONTH, -108, max_date_cte.max_date)
         AND ers.DATE_END < DATEADD(MONTH, -96, max_date_cte.max_date) THEN '-8'
    ELSE 'Older'
END AS "Rolling12MCategory",

    PROVIDER_NAME AS "ProviderName",
    PROVIDER_SITE_NAME AS "ProviderSite",

    CASE 
        WHEN ers.PROVIDER_CODE LIKE 'RAL%' 
          OR ers.PROVIDER_CODE LIKE 'RAN%'
          OR ers.PROVIDER_CODE LIKE 'RAP%'
          OR ers.PROVIDER_CODE LIKE 'RKE%'
          OR ers.PROVIDER_CODE LIKE 'RP4%'
          OR ers.PROVIDER_CODE LIKE 'RP6%'
          OR ers.PROVIDER_CODE LIKE 'RRV%' THEN 'NCL'
        WHEN ers.PROVIDER_CODE IS NULL THEN 'Unknown'
        ELSE 'Non-NCL'
    END AS "ProviderGroup",

    CASE   
        WHEN PROVIDER_CODE LIKE 'RAL%' 
          OR PROVIDER_CODE LIKE 'RAN%'
          OR PROVIDER_CODE LIKE 'RAP%'
          OR PROVIDER_CODE LIKE 'RKE%'
          OR PROVIDER_CODE LIKE 'RP4%'
          OR PROVIDER_CODE LIKE 'RP6%'
          OR PROVIDER_CODE LIKE 'RRV%' THEN PROVIDER_NAME
        WHEN PROVIDER_CODE IS NULL THEN 'Unknown'
        ELSE 'Non-NCL'
    END AS "ProviderNameNCL",

    CASE  
        WHEN PROVIDER_CODE LIKE 'RAL%' 
          OR PROVIDER_CODE LIKE 'RAN%'
          OR PROVIDER_CODE LIKE 'RAP%'
          OR PROVIDER_CODE LIKE 'RKE%'
          OR PROVIDER_CODE LIKE 'RP4%'
          OR PROVIDER_CODE LIKE 'RP6%'
          OR PROVIDER_CODE LIKE 'RRV%' THEN PROVIDER_SITE_NAME
        WHEN PROVIDER_CODE IS NULL THEN 'Unknown'
        ELSE 'Non-NCL'
    END AS "ProviderSiteNCL",
    ers.tfc_name AS "MainSpecName",

    CASE 
        WHEN PATIENT_AGE BETWEEN 0 AND  19 THEN '0-19'
        WHEN PATIENT_AGE BETWEEN 20 AND 39 THEN '20-39'
        WHEN PATIENT_AGE BETWEEN 40 AND 59 THEN '40-59'
        WHEN PATIENT_AGE BETWEEN 60 AND 79 THEN '60-79'
        ELSE '80+'
    END AS "Age_Group",

    ETHNICITY_NAME AS "EthnicityDesc",
    GENDER_NAME AS "GenderDesc",
    DEPRIVATION_DECILE AS "DeprivationDecile",

    REFERRING_ORGANISATION_NAME AS "ReferringOrgName",
    org_cte."OrganisationType" AS REFERRING_ORG_TYPE,
    CASE
        WHEN org_cte."OrganisationType" LIKE 'Independent%' THEN 'Independent'
        WHEN org_cte."OrganisationType" LIKE 'Unknown%' THEN 'Unknown'
        WHEN org_cte."OrganisationType" LIKE 'Optical%' THEN 'Optical'
        WHEN org_cte."OrganisationType" LIKE 'Dental%' THEN 'Dental'
        WHEN org_cte."OrganisationType" LIKE 'GP%' THEN 'GP Practice'
        ELSE 'Other NHS Organisation'
    END AS REFERRING_ORG_TYPE_GROUP,
        
    CASE
        WHEN REFERRING_ORGANISATION_NAME LIKE 'HMP%' THEN 'Regional Health & Justice Commissioner'
        WHEN org_cte_gp_comm.ICB_NAME IS NOT NULL THEN REGEXP_REPLACE(SPLIT_PART(org_cte_gp_comm.ICB_NAME, ' - ', 1), '^NHS ', '')
        WHEN org_cte_prov_comm.ICB_NAME IS NOT NULL THEN REGEXP_REPLACE(SPLIT_PART(org_cte_prov_comm.ICB_NAME, ' - ', 1), '^NHS ', '')
        ELSE 'Independant/Optical/Unknown' 
    END AS REFERRING_ORG_COMMISSIONER,

    CASE 
        WHEN org_cte."OrganisationType" LIKE 'Unknown' THEN 'Independent/Optical/Unknown'
        WHEN (org_cte_prov_comm.ICB_NAME LIKE '%ICB%' 
            AND org_cte."OrganisationType" LIKE 'Unknown') THEN 'Non-NCL'
        WHEN org_cte_gp_comm.ICB_NAME LIKE '%North Central London ICB%' THEN 'NCL'
        WHEN org_cte_prov_comm.ICB_NAME LIKE '%North Central London ICB%' THEN 'NCL'
        WHEN org_cte_gp_comm.ICB_NAME NOT LIKE '%North Central London ICB%' THEN 'Non-NCL'
        WHEN org_cte_prov_comm.ICB_NAME NOT LIKE '%North Central London ICB%' THEN 'Non-NCL'
        ELSE 'Independent/Optical/Unknown'         
    END AS "Referring_Org_Commissioner_Group",
    
    org_cte."Postcode" AS "Postcode",
    org_cte."Longitude" AS "Longitude",
    org_cte."Latitude" AS "Latitude",

    OP_PRIORITY_TYPE_NAME AS "OPPriorityTypeDesc",
    ERS_APPOINTMENT_TYPE AS "eRSApptType",
    ERS_CLINIC_TYPE AS "eRSClinicType",
    ERS_SERVICE_NAME AS "eRSServiceName",
    ACTIVTIY AS "Activity",
    CASE
        WHEN (org_cte_prov_comm."Geography_Name" LIKE 'Camden'
            OR org_cte_prov_comm."Geography_Name" LIKE 'Islington'
            OR org_cte_prov_comm."Geography_Name" LIKE 'Haringey'
            OR org_cte_prov_comm."Geography_Name" LIKE 'Enfield'
            OR org_cte_prov_comm."Geography_Name" LIKE 'Barnet' ) THEN org_cte_prov_comm."Geography_Name"
        WHEN org_cte."Borough" IS NOT NULL THEN org_cte."Borough"
        WHEN (org_cte."Borough" IS NULL AND ( org_cte."Geography_Name" LIKE 'Camden'
                                        OR org_cte."Geography_Name" LIKE 'Islington'
                                        OR org_cte."Geography_Name" LIKE 'Haringey'
                                        OR org_cte."Geography_Name" LIKE 'Enfield'
                                        OR org_cte."Geography_Name" LIKE 'Barnet')) THEN org_cte."Geography_Name"
        ELSE 'Outside NCL'
    END AS NCL_REFERRING_ORG_BOROUGH
    
    
    FROM DEV__MODELLING.MAIN_DATA.ERS_REFERRALS_RECEIVED AS ers
    CROSS JOIN max_date_cte AS max_date_cte
    CROSS JOIN max_sunday_cte AS max_sunday_cte

    LEFT JOIN org_cte AS org_cte
    ON  ers.REFERRING_ORGANISATION_CODE = org_cte."Organisation_Code"

    LEFT JOIN org_cte_gp_comm AS org_cte_gp_comm
    ON org_cte."Organisation_Code" = org_cte_gp_comm.PRACTICE_CODE

    LEFT JOIN org_cte_prov_comm AS org_cte_prov_comm
    ON org_cte."Organisation_Code" = org_cte_prov_comm.CODE

    WHERE ers.DATE_END <= max_sunday_cte.max_date
    AND  (
        PROVIDER_CODE LIKE 'RAL%' 
        OR PROVIDER_CODE LIKE 'RAN%'
        OR PROVIDER_CODE LIKE 'RAP%'
        OR PROVIDER_CODE LIKE 'RKE%'
        OR PROVIDER_CODE LIKE 'RP4%'
        OR PROVIDER_CODE LIKE 'RP6%'
        OR PROVIDER_CODE LIKE 'RRV%'
        )
)

SELECT *
FROM ers_data
