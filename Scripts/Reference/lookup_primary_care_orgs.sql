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
    SK_ORGANISATION_CODE INT,
    ORGANISATION_TYPE VARCHAR(100),
    ORGANISATION_CODE VARCHAR(6),
    ORGANISATION_NAME VARCHAR(255),
    ADDRESS_LINE_1 VARCHAR(255),
    ADDRESS_LINE_2 VARCHAR(255),
    ADDRESS_LINE_3 VARCHAR(255),
    ADDRESS_LINE_4 VARCHAR(255),
    ADDRESS_LINE_5 VARCHAR(255),
    SK_POSTCODE INT,
    POSTCODE VARCHAR(10),
    LSOA_CODE VARCHAR(9),
    LSOA_NAME VARCHAR(100),
    MSOA_CODE VARCHAR(9),
    MSOA_NAME VARCHAR(100),
    DATE_ORGANISATION_OPEN DATE,
    DATE_ORGANISATION_CLOSE DATE,
    ORGANISATION_STATUS VARCHAR(255),
    ORGANISATION_IMD_DECILE INT,
    ORGANISATION_IMD_QUINTILE INT,
    SK_PCN_CODE INT,
    PCN_CODE VARCHAR(6),
    PCN_NAME VARCHAR(255),
    PCN_POSTCODE VARCHAR(10),
    NEIGHBOURHOOD_NCL VARCHAR(100),
    BOROUGH_NCL VARCHAR(10),
    SK_REGION_CODE INT,
    REGION_CODE VARCHAR(9),
    REGION_NAME VARCHAR(255),
    SK_PARENT_ORG_CODE INT,
    PARENT_ORG_CODE VARCHAR(9),
    PARENT_ORG_NAME VARCHAR(255),
    PARENT_ORG_TYPE VARCHAR(50),
    DATETIME_RUN DATETIME
)
target_lag = '1 DAY'
refresh_mode = FULL
warehouse = NCL_ANALYTICS_XS
AS
SELECT
    -- Organisation details including address
    -- Internal database identifiers (useful for troubleshooting joins), prefix SK
    a."SK_OrganisationID",
    t1."OrganisationType",        -- e.g. Dental Practices, GPs, Pharmacies
    a."Organisation_Code",
    a."Organisation_Name",
    a."Address_Line_1",
    a."Address_Line_2",
    a."Address_Line_3",
    a."Address_Line_4",
    a."Address_Line_5",
    p."SK_PostcodeID",
    p."Postcode_single_space_e_Gif",

    -- LSOA and MSOA lookup
    p."LSOA",
    l."OAName",
    p."MSOA",
    m."OAName",

    -- Organisation status
    a."StartDate",
    a."EndDate",
    s."OrganisationStatus",    -- Active, Closed, etc

    -- Deprivation (NCL GP Practice specific: 2019 IMD)
    d."Deprivation_Decile_2019_Fingertips",
    d."Deprivation_Quintile_2019_Fingertips",

    -- PCN
    c."SK_OrganisationID",
    c."Organisation_Code",
    c."Organisation_Name",
    p2."Postcode_single_space_e_Gif",

    -- Neighbourhood & Borough
    n."Neighbourhood",
    b."Borough",

    -- Region & ICB
    r."SK_OrganisationID",
    r."Organisation_Code",
    r."Organisation_Name",
    i."SK_OrganisationID",
    i."Organisation_Code",
    i."Organisation_Name",
    t2."OrganisationType",

    -- Audit timestamp
    GETDATE()

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
LEFT JOIN "Dictionary"."dbo"."OrganisationType" t1 
    ON a."SK_OrganisationTypeID" = t1."SK_OrganisationTypeID"
LEFT JOIN "Dictionary"."dbo"."OrganisationType" t2 
    ON i."SK_OrganisationTypeID" = t2."SK_OrganisationTypeID"

-- Only include primary care providers: Dental Practices, GPs, Pharmacies
WHERE a."SK_OrganisationTypeID" IN (43, 44, 8);