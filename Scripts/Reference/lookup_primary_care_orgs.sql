/*
Script: LOOKUP_PRIMARY_CARE_ORGS
Version: 1.2
Description: Creates a normalised organisation reference table for primary care organisations 
             (mainly GP practices, but also NHS dental practices and pharmacies).

This table is used to provide a reference for organisation demographics, including:
- Organisation details (code, name, address, status, type)
- Location information (Postcode, LSOA, MSOA, Neighbourhood, Borough, Region, ICB)
- Deprivation measures (IMD decile and quintile, 2019) for NCL GP practices.
- Links to PCNs and NCL boroughs where applicable

Notes:
- Includes all organisations nationally, not just those in NCL.
- A staging table (LOOKUP_OUTPUTAREA) is created because Snowflake Dynamic Tables cannot directly reference objects from a Snowflake Data Share.
- Target dynamic table replaces sandpit table GrahamR.dim_GP_Practice.

Author: Graham Roberts
Run time: < 30 seconds

Source Tables:

Dictionary Tables (shared):
- Dictionary.dbo.Organisation
- Dictionary.dbo.OrganisationMatrixPractice
- Dictionary.dbo.Postcode
- Dictionary.dbo.OrganisationStatus
- Dictionary.dbo.OrganisationType
- Dictionary.dbo.OutputArea  (staged locally as LOOKUP_OUTPUTAREA)

Modelling Tables (local):
- MODELLING.LOOKUP_NCL.NEIGHBOURHOODS_2011
- MODELLING.LOOKUP_NCL.SR_PCN_BOROUGH_LOOKUP
- MODELLING.LOOKUP_NCL.DIM_PRACTICE_DEPRIVATION

Target Output Table:
- DEV__MODELLING.CANCER__REF.LOOKUP_PRIMARY_CARE_ORGS
*/


/* 
Step 1: Stage OutputArea locally 
- Required because Snowflake Dynamic Tables cannot directly reference shared objects (Dictionary)
- Used to provide LSOA and MSOA names for practices
*/
CREATE OR REPLACE TABLE DEV__MODELLING.CANCER__REF.LOOKUP_OUTPUTAREA AS
SELECT * FROM "Dictionary"."dbo"."OutputArea";
/* 
Step 2: Create the Dynamic Table for primary care organisations
- Includes GP practices, NHS dental practices, and pharmacies
- Refreshes daily (target_lag = 1 DAY)
*/
CREATE OR REPLACE DYNAMIC TABLE DEV__MODELLING.CANCER__REF.LOOKUP_PRIMARY_CARE_ORGS (
    DB_ORGANISATION_CODE INT,
    ORGANISATION_TYPE VARCHAR(255),
    ORGANISATION_CODE VARCHAR(255),
    ORGANISATION_NAME VARCHAR(255),
    ADDRESS_LINE_1 VARCHAR(255),
    ADDRESS_LINE_2 VARCHAR(255),
    ADDRESS_LINE_3 VARCHAR(255),
    ADDRESS_LINE_4 VARCHAR(255),
    ADDRESS_LINE_5 VARCHAR(255),
    POSTCODE VARCHAR(255),
    DB_POSTCODE INT,
    LSOA_CODE_2011 VARCHAR(255),
    LSOA_NAME_2011 VARCHAR(255),
    MSOA_CODE_2011 VARCHAR(255),
    MSOA_NAME_2011 VARCHAR(255),
    DATE_ORGANISATION_OPEN DATE,
    DATE_ORGANISATION_CLOSE DATE,
    ORGANISATION_STATUS VARCHAR(255),
    ORGANISATION_IMD_DECILE INT,
    ORGANISATION_IMD_QUINTILE INT,
    DB_PCN_CODE INT,
    PCN_CODE VARCHAR(255),
    PCN_NAME VARCHAR(255),
    PCN_POSTCODE VARCHAR(255),
    NEIGHBOURHOOD_NAME VARCHAR(255),
    BOROUGH_NCL VARCHAR(255),
    DB_REGION_CODE INT,
    REGION_CODE VARCHAR(255),
    REGION_NAME VARCHAR(255),
    DB_ICB_CODE INT,
    ICB_CODE VARCHAR(255),
    ICB_NAME VARCHAR(255),
    DATETIME_RUN DATETIME
)
target_lag = '1 DAY'
refresh_mode = FULL
warehouse = NCL_ANALYTICS_XS
AS
SELECT
    -- Internal database identifier (useful for troubleshooting joins)
    a."SK_OrganisationID" AS DB_ORGANISATION_CODE,

    -- Organisation details
    t."OrganisationType" AS ORGANISATION_TYPE,        -- e.g. Dental Practices, GPs, Pharmacies
    a."Organisation_Code" AS ORGANISATION_CODE,
    a."Organisation_Name" AS ORGANISATION_NAME,

    -- Address fields
    a."Address_Line_1" AS ADDRESS_LINE_1,
    a."Address_Line_2" AS ADDRESS_LINE_2,
    a."Address_Line_3" AS ADDRESS_LINE_3,
    a."Address_Line_4" AS ADDRESS_LINE_4,
    a."Address_Line_5" AS ADDRESS_LINE_5,

    -- Postcode and geographical lookup
    p."Postcode_single_space_e_Gif" AS POSTCODE,
    p."SK_PostcodeID" AS DB_POSTCODE,
    p."LSOA" AS LSOA_CODE_2011,
    l."OAName" AS LSOA_NAME_2011,
    p."MSOA" AS MSOA_CODE_2011,
    m."OAName" AS MSOA_NAME_2011,

    -- Organisation status
    a."StartDate" AS DATE_ORGANISATION_OPEN,
    a."EndDate" AS DATE_ORGANISATION_CLOSE,
    s."OrganisationStatus" AS ORGANISATION_STATUS,    -- Active, Closed, etc

    -- Deprivation (NCL GP Practice specific: 2019 IMD)
    d."Deprivation_Decile_2019_Fingertips" AS ORGANISATION_IMD_DECILE,
    d."Deprivation_Quintile_2019_Fingertips" AS ORGANISATION_IMD_QUINTILE,

    -- PCN
    c."SK_OrganisationID" AS DB_PCN_CODE,
    c."Organisation_Code" AS PCN_CODE,
    c."Organisation_Name" AS PCN_NAME,
    p2."Postcode_single_space_e_Gif" AS PCN_POSTCODE,

    -- Neighbourhood & Borough
    n."Neighbourhood" AS NEIGHBOURHOOD_NAME,
    b."Borough" AS BOROUGH_NCL,

    -- Region & ICB
    r."SK_OrganisationID" AS DB_REGION_CODE,
    r."Organisation_Code" AS REGION_CODE,
    r."Organisation_Name" AS REGION_NAME,
    i."SK_OrganisationID" AS DB_ICB_CODE,
    i."Organisation_Code" AS ICB_CODE,
    i."Organisation_Name" AS ICB_NAME,

    -- Audit timestamp
    GETDATE() AS DATETIME_RUN

FROM "Dictionary"."dbo"."Organisation" a
LEFT JOIN "Dictionary"."dbo"."OrganisationMatrixPractice" x 
    ON a."SK_OrganisationID" = x."SK_OrganisationID_Practice"
LEFT JOIN "Dictionary"."dbo"."Organisation" c 
    ON x."SK_OrganisationID_Network" = c."SK_OrganisationID"
LEFT JOIN "Dictionary"."dbo"."Organisation" i 
    ON a."SK_OrganisationID_ParentOrg" = i."SK_OrganisationID"
LEFT JOIN "Dictionary"."dbo"."Organisation" r 
    ON a."SK_OrganisationID_NationalGrouping" = r."SK_OrganisationID"
LEFT JOIN "Dictionary"."dbo"."Postcode" p 
    ON a."SK_PostcodeID" = p."SK_PostcodeID"
LEFT JOIN "Dictionary"."dbo"."Postcode" p2 
    ON c."SK_PostcodeID" = p2."SK_PostcodeID"
LEFT JOIN "Dictionary"."dbo"."OrganisationStatus" s 
    ON a."SK_OrganisationStatusID" = s."SK_OrganisationStatusID"
LEFT JOIN MODELLING.LOOKUP_NCL.NEIGHBOURHOODS_2011 n
    ON p."LSOA" = n."LSOA11CD"
LEFT JOIN DEV__MODELLING.CANCER__REF.LOOKUP_OUTPUTAREA l 
    ON p."LSOA" = l."OACode" AND l."CensusYear" = 2011
LEFT JOIN DEV__MODELLING.CANCER__REF.LOOKUP_OUTPUTAREA m 
    ON p."MSOA" = m."OACode" AND m."CensusYear" = 2011
LEFT JOIN (
    SELECT DISTINCT "pcn_code", "Borough"
    FROM MODELLING.LOOKUP_NCL.SR_PCN_BOROUGH_LOOKUP
) b 
    ON c."Organisation_Code" = b."pcn_code"
LEFT JOIN MODELLING.LOOKUP_NCL.DIM_PRACTICE_DEPRIVATION d 
    ON a."Organisation_Code" = d."Practice_Code"
LEFT JOIN "Dictionary"."dbo"."OrganisationType" t 
    ON a."SK_OrganisationTypeID" = t."SK_OrganisationTypeID"

-- Only include primary care providers: Dental Practices, GPs, Pharmacies
WHERE a."SK_OrganisationTypeID" IN (43, 44, 8);