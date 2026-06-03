-- Transition the existing PTL scripts from MSSQL to Snowflake, PTL_3
-- The script is fairly raw and unaltered from the previous MSSQL script
-- Contact: ben.goretzki@nhs.net
CREATE OR REPLACE DYNAMIC TABLE MODELLING.CANCER__PTL.PTL3(
    "DateKey" DATE,
    "Week Ending" VARCHAR,
    "Org Code" VARCHAR,
    "Org Name" VARCHAR,
    "Section" VARCHAR,
    "RowType" VARCHAR,
    "TumourType" VARCHAR,
    "Day 0-28" NUMBER,
    "Day 29-62" NUMBER,
    "Day 63-104" NUMBER,
    "Day >104" NUMBER,
    "Day >62" NUMBER,
    "Total PTL" NUMBER,
    "Number passing day 28 in last 7 days" NUMBER,
    "Number passing day 62 in last 7 days" NUMBER,
    "Number passing day 104 in last 7 days" NUMBER,
    "Number of patients treated by day 62" NUMBER,
    "Number of patients treated day 63-104" NUMBER,
    "Number of patients treated > 104" NUMBER,
    "Referrals and Upgrades Made" NUMBER,
    "Referrals Seen" NUMBER,
    "Day 0-33" NUMBER,
    "Day 34-62" NUMBER,
    "Region" VARCHAR,
    "Cancer_Alliance" VARCHAR,
    "Shortname" VARCHAR,
    "Row Type" VARCHAR,
    "Inc" VARCHAR,
    "MonthKey" DATE,
    "Last Week" VARCHAR
)
COMMENT="PTL3 table for PTL output"
TARGET_LAG = "2 hours"
REFRESH_MODE = FULL
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS

WITH ExpectedWeeks AS (
    -- Generate all expected Sundays within the relevant date range
    SELECT DISTINCT 
        "DateKey"
    FROM DATA_LAKE.PMCT."Cwt63DayPlusWeeklySourceAppendReviseProv"
    WHERE "DateKey" > TO_DECIMAL(TO_VARCHAR(DATEADD(DAY, -375, GETDATE()), 'YYYYMMDD'))
),

ActualSubmissions AS (
    SELECT DISTINCT 
        "DateKey", "Org Code"
    FROM DATA_LAKE.PMCT."Cwt63DayPlusWeeklySourceAppendReviseProv"
    WHERE "Org Code" NOT IN ('RVY','RA4', 'RBZ', 'RW6', 'RAP') and "DateKey" > TO_DECIMAL(TO_VARCHAR(DATEADD(DAY, -375, GETDATE()), 'YYYYMMDD'))
),

MissingWeeks AS (
    -- Find the weeks where submissions are missing
    SELECT 
        e."DateKey" AS "MissingDateKey",
        o."Org Code"
    FROM ExpectedWeeks e
    CROSS JOIN (SELECT DISTINCT "Org Code" FROM ActualSubmissions) o
    LEFT JOIN ActualSubmissions a 
        ON e."DateKey" = a."DateKey" AND o."Org Code" = a."Org Code"
    WHERE a."DateKey" IS NULL
),

MostRecentValidWeek AS (
    -- For each missing week, find the most recent valid submission week
    SELECT 
        m."Org Code",
        m."MissingDateKey",
        MAX(p."DateKey") AS "PreviousDateKey"
    FROM MissingWeeks m
    JOIN DATA_LAKE.PMCT."Cwt63DayPlusWeeklySourceAppendReviseProv" p
        ON p."Org Code" = m."Org Code"
       AND p."DateKey" < m."MissingDateKey"
    GROUP BY m."Org Code", m."MissingDateKey"
),

FilledData AS (
    -- Combine actual submissions with filled-in missing weeks
    SELECT 
        COALESCE(p."MissingDateKey", a."DateKey") AS "DateKey",
        COALESCE(p."Week Ending", a."Week Ending") AS "Week Ending",


        CASE 
            WHEN COALESCE(p."Org Code", a."Org Code") = 'RAP' THEN 'RAL'
            ELSE COALESCE(p."Org Code", a."Org Code")
                    END AS "Org Code",

        
        CASE 
            WHEN COALESCE(p."Org Name", a."Org Name") = 'NORTH MIDDLESEX UNIVERSITY HOSPITAL NHS TRUST' THEN 'ROYAL FREE LONDON NHS FOUNDATION TRUST'
            ELSE COALESCE(p."Org Name", a."Org Name")
                    END AS "Org Name",

        COALESCE(p."Section", a."Section") AS "Section",
        COALESCE(p."RowType", a."RowType") AS "RowType",
        COALESCE(p."TumourType", a."TumourType") AS "TumourType",
        COALESCE(p."Day 0-28", a."Day 0-28") AS "Day 0-28",
        COALESCE(p."Day 29-62", a."Day 29-62") AS "Day 29-62",
        COALESCE(p."Day 63-104", a."Day 63-104") AS "Day 63-104",
        COALESCE(p."Day >104", a."Day >104") AS "Day >104",
        COALESCE(p."Number passing day 28 in last 7 days", a."Number passing day 28 in last 7 days") AS "Number passing day 28 in last 7 days",
        COALESCE(p."Number passing day 62 in last 7 days", a."Number passing day 62 in last 7 days") AS "Number passing day 62 in last 7 days",
        COALESCE(p."Number passing day 104 in last 7 days", a."Number passing day 104 in last 7 days") AS "Number passing day 104 in last 7 days",
        COALESCE(p."Number of patients treated by day 62", a."Number of patients treated by day 62") AS "Number of patients treated by day 62",
        COALESCE(p."Number of patients treated day 63-104", a."Number of patients treated day 63-104") AS "Number of patients treated day 63-104",
        COALESCE(p."Number of patients treated day >104", a."Number of patients treated day >104") AS "Number of patients treated day >104",
        COALESCE(p."Referrals and Upgrades Made", a."Referrals and Upgrades Made") AS "Referrals and Upgrades Made",
        COALESCE(p."Referrals Seen", a."Referrals Seen") AS "Referrals Seen",
        COALESCE(p."Day 0-33", a."Day 0-33") AS "Day 0-33",
        COALESCE(p."Day 34-62", a."Day 34-62") AS "Day 34-62"
    FROM DATA_LAKE.PMCT."Cwt63DayPlusWeeklySourceAppendReviseProv" a
    FULL OUTER JOIN (
        SELECT 
            r."MissingDateKey",
            p.*
        FROM MostRecentValidWeek r
        JOIN DATA_LAKE.PMCT."Cwt63DayPlusWeeklySourceAppendReviseProv" p
            ON p."Org Code" = r."Org Code"
           AND p."DateKey" = r."PreviousDateKey"
    ) p ON a."DateKey" = p."MissingDateKey" AND a."Org Code" = p."Org Code"
),

BaseData AS (
    SELECT 
        c."DateKey",
        TO_DATE(CAST(c."DateKey" AS CHAR(8)), 'YYYYMMDD') AS "DateKeyFormatted",
        c."Week Ending",
        c."Org Code",
        c."Org Name",
        CASE 
            WHEN c."Section" IN ('Section1 Without DTT', 'Section2 With DTT') THEN 'Section 0'
            ELSE c."Section"
        END AS "Section",
        c."RowType",
        c."TumourType",
        c."Day 0-28",
        c."Day 29-62",
        c."Day 63-104",
        c."Day >104",
        c."Number passing day 28 in last 7 days",
        c."Number passing day 62 in last 7 days",
        c."Number passing day 104 in last 7 days",
        c."Number of patients treated by day 62",
        c."Number of patients treated day 63-104",
        c."Number of patients treated day >104",
        c."Referrals and Upgrades Made",
        c."Referrals Seen",
        c."Day 0-33",
        c."Day 34-62",
        o.REGION,
        o.CANCER_ALLIANCE,
        o.PTL_SHORT_NAME,
        CASE 
            WHEN o.REGION = 'London' THEN c."Org Name"
            ELSE 'Outside London'
        END AS "GroupingField",
        CASE 
            WHEN o.REGION = 'London' THEN c."Org Code"
            ELSE 'AAA'
        END AS "GroupingOrgCode",
        CASE 
            WHEN o.REGION = 'London' THEN 'London'
            ELSE 'Outside London'
        END AS "GroupingRegion",
        CASE 
            WHEN o.REGION = 'London' THEN o.SHORT_NAME
            ELSE 'Outside London'
        END AS "GroupingShortName",
        CASE 
            WHEN o.REGION = 'London' THEN o.CANCER_ALLIANCE
            ELSE 'Outside London'
        END AS "GroupingAlliance"
    FROM FilledData c
    LEFT JOIN MODELLING.CANCER__REF.DIM_ORGANISATIONS o
        ON c."Org Code" = o.CODE
),

MaxDateCTE AS (
    SELECT MAX(TO_DATE(CAST("DateKey" AS CHAR(8)), 'YYYYMMDD')) AS "MaxDate"
    FROM FilledData
)

SELECT
    b."DateKeyFormatted" AS "DateKey",
    b."Week Ending",
    b."GroupingOrgCode" AS "Org Code",
    b."GroupingField" AS "Org Name",
    b."Section",
    b."RowType",
    b."TumourType",
    SUM(b."Day 0-28") AS "Day 0-28",
    SUM(b."Day 29-62") AS "Day 29-62",
    SUM(b."Day 63-104") AS "Day 63-104",
    SUM(b."Day >104") AS "Day >104",
    SUM(b."Day 63-104") + SUM(b."Day >104") AS "Day >62",
    SUM(b."Day 0-28") + SUM(b."Day 29-62") + SUM(b."Day 63-104") + SUM(b."Day >104") AS "Total PTL",
    SUM(b."Number passing day 28 in last 7 days") AS "Number passing day 28 in last 7 days",
    SUM(b."Number passing day 62 in last 7 days") AS "Number passing day 62 in last 7 days",
    SUM(b."Number passing day 104 in last 7 days") AS "Number passing day 104 in last 7 days",
    SUM(b."Number of patients treated by day 62") AS "Number of patients treated by day 62",
    SUM(b."Number of patients treated day 63-104") AS "Number of patients treated day 63-104",
    SUM(b."Number of patients treated day >104") AS "Number of patients treated > 104",
    SUM(b."Referrals and Upgrades Made") AS "Referrals and Upgrades Made",
    SUM(b."Referrals Seen") AS "Referrals Seen",
    SUM(b."Day 0-33") AS "Day 0-33",
    SUM(b."Day 34-62") AS "Day 34-62",
    MAX(b."GroupingRegion") AS REGION, 
    MAX(b."GroupingAlliance") AS CANCER_ALLIANCE,
    MAX(b.PTL_SHORT_NAME) AS "Shortname",
    CASE 
        WHEN b."RowType" IN ('Urgent Bowel Screening','Urgent Breast Screening', 'Urgent Cervical Screening') THEN 'Urgent Screening' 
        ELSE b."RowType"
    END AS "Row Type",
    CASE 
        WHEN b."TumourType" = 'Non site specific symptoms' THEN 'No' 
        ELSE 'Yes' 
    END AS "Inc",
    DATEFROMPARTS(YEAR(b."DateKeyFormatted"), MONTH(b."DateKeyFormatted"), 1) AS "MonthKey",
    CASE
        
WHEN b."DateKey" = '20190203' then '20190101'
WHEN b."DateKey" = '20190303' then '20190201'
WHEN b."DateKey" = '20190331' then '20190301'
WHEN b."DateKey" = '20190428' then '20190401'
WHEN b."DateKey" = '20190602' then '20190501'
WHEN b."DateKey" = '20190630' then '20190601'
WHEN b."DateKey" = '20190728' then '20190701'
WHEN b."DateKey" = '20190901' then '20190801'
WHEN b."DateKey" = '20190929' then '20190901'
WHEN b."DateKey" = '20191103' then '20191001'
WHEN b."DateKey" = '20191201' then '20191101'
WHEN b."DateKey" = '20191229' then '20191201'
WHEN b."DateKey" = '20200202' then '20200101'
WHEN b."DateKey" = '20200301' then '20200201'
WHEN b."DateKey" = '20200329' then '20200301'
WHEN b."DateKey" = '20200503' then '20200401'
WHEN b."DateKey" = '20200531' then '20200501'
WHEN b."DateKey" = '20200628' then '20200601'
WHEN b."DateKey" = '20200802' then '20200701'
WHEN b."DateKey" = '20200830' then '20200801'
WHEN b."DateKey" = '20200927' then '20200901'
WHEN b."DateKey" = '20201101' then '20201001'
WHEN b."DateKey" = '20201129' then '20201101'
WHEN b."DateKey" = '20210103' then '20201201'
WHEN b."DateKey" = '20210131' then '20210101'
WHEN b."DateKey" = '20210228' then '20210201'
WHEN b."DateKey" = '20210328' then '20210301'
WHEN b."DateKey" = '20210502' then '20210401'
WHEN b."DateKey" = '20210530' then '20210501'
WHEN b."DateKey" = '20210627' then '20210601'
WHEN b."DateKey" = '20210801' then '20210701'
WHEN b."DateKey" = '20210829' then '20210801'
WHEN b."DateKey" = '20211003' then '20210901'
WHEN b."DateKey" = '20211031' then '20211001'
WHEN b."DateKey" = '20211128' then '20211101'
WHEN b."DateKey" = '20220102' then '20211201'
WHEN b."DateKey" = '20220130' then '20220101'
WHEN b."DateKey" = '20220227' then '20220201'
WHEN b."DateKey" = '20220403' then '20220301'
WHEN b."DateKey" = '20220501' then '20220401'
WHEN b."DateKey" = '20220605' then '20220501'
WHEN b."DateKey" = '20220703' then '20220601'
WHEN b."DateKey" = '20220731' then '20220701'
WHEN b."DateKey" = '20220904' then '20220801'
WHEN b."DateKey" = '20221002' then '20220901'
WHEN b."DateKey" = '20221030' then '20221001'
WHEN b."DateKey" = '20221204' then '20221101'
WHEN b."DateKey" = '20230101' then '20221201'
WHEN b."DateKey" = '20230129' then '20230101'
WHEN b."DateKey" = '20230226' then '20230201'
WHEN b."DateKey" = '20230402' then '20230301'
WHEN b."DateKey" = '20230430' then '20230401'
WHEN b."DateKey" = '20230528' then '20230501'
WHEN b."DateKey" = '20230702' then '20230601'
WHEN b."DateKey" = '20230730' then '20230701'
WHEN b."DateKey" = '20230903' then '20230801'
WHEN b."DateKey" = '20231001' then '20230901'
WHEN b."DateKey" = '20231029' then '20231001'
WHEN b."DateKey" = '20231203' then '20231101'
WHEN b."DateKey" = '20231231' then '20231201'
WHEN b."DateKey" = '20240128' then '20240101'
WHEN b."DateKey" = '20240303' then '20240201'
WHEN b."DateKey" = '20240331' then '20240301'
WHEN b."DateKey" = '20240428' then '20240401'
WHEN b."DateKey" = '20240602' then '20240501'
WHEN b."DateKey" = '20240630' then '20240601'
WHEN b."DateKey" = '20240728' then '20240701'
WHEN b."DateKey" = '20240901' then '20240801'
WHEN b."DateKey" = '20240929' then '20240901'
WHEN b."DateKey" = '20241103' then '20241001'
WHEN b."DateKey" = '20241201' then '20241101'
WHEN b."DateKey" = '20241229' then '20241201'
WHEN b."DateKey" = '20250202' then '20250101'
WHEN b."DateKey" = '20250302' then '20250201'
WHEN b."DateKey" = '20250330' then '20250301'
WHEN b."DateKey" = '20250427' then '20250401'
WHEN b."DateKey" = '20250601' then '20250501'
WHEN b."DateKey" = '20250629' then '20250601'
WHEN b."DateKey" = '20250803' then '20250701'
WHEN b."DateKey" = '20250831' then '20250801'
WHEN b."DateKey" = '20250928' then '20250901'
WHEN b."DateKey" = '20251102' then '20251001'
WHEN b."DateKey" = '20251130' then '20251101'
WHEN b."DateKey" = '20251228' then '20251201'
WHEN b."DateKey" = '20260201' then '20260101'
WHEN b."DateKey" = '20260301' then '20260201'
WHEN b."DateKey" = '20260329' then '20260301'
WHEN b."DateKey" = '20260503' then '20260401'
WHEN b."DateKey" = '20260531' then '20260501'
WHEN b."DateKey" = '20260628' then '20260601'
WHEN b."DateKey" = '20260802' then '20260701'
WHEN b."DateKey" = '20260830' then '20260801'
WHEN b."DateKey" = '20260927' then '20260901'
WHEN b."DateKey" = '20261101' then '20261001'
WHEN b."DateKey" = '20261129' then '20261101'
WHEN b."DateKey" = '20270103' then '20261201'
        ELSE null 
    END AS "Last Week"
FROM 
    BaseData b
CROSS JOIN 
    MaxDateCTE
WHERE 
    b."DateKeyFormatted" > DATEADD(DAY, -375, MaxDateCTE."MaxDate")
    
GROUP BY 
    b."DateKeyFormatted",
    b."DateKey",
    b."Week Ending",
    b."GroupingOrgCode",
    b."GroupingField",
    b."Section", 
    b."RowType",
    b."TumourType",
    b."GroupingRegion",
    b."GroupingAlliance"
ORDER BY 
    b."DateKeyFormatted" DESC,
    b."Week Ending",
    MAX(b."GroupingAlliance"),
    b."Section",
    b."RowType",
    b."TumourType",
    b."GroupingRegion",
    b."GroupingField";
