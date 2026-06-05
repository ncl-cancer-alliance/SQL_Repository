-- Steps to insert latest PTL data from SharePoint into staging and refresh published dynamic table


-- Step 1: truncate landing and upload new file

-- Step 2: remove existing rows for the same dates from stage
DELETE FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__PTL_SHAREPOINT_STAGE
WHERE "DateKey" IN (
    SELECT DISTINCT "DateKey" 
    FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__PTL_SHAREPOINT_LANDING
);

-- Step 3: insert new rows from landing into stage
INSERT INTO DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__PTL_SHAREPOINT_STAGE
SELECT
    "DateKey",
    "Week Ending",
    "Org Code",
    "Org Name",
    "Section",
    "RowType",
    "TumourType",
    "Day 0-28",
    "Day 29-62",
    "Day 63-104",
    "Day >104",
    "Number passing day 28 in last 7 days",
    "Number passing day 62 in last 7 days",
    "Number passing day 104 in last 7 days",
    "Number of patients treated by day 62",
    "Number of patients treated day 63-104",
    "Number of patients treated day >104",
    "Referrals and Upgrades Made",
    "Referrals Seen",
    CURRENT_TIMESTAMP AS "CreateTS"
FROM DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__PTL_SHAREPOINT_LANDING;

-- Step 4 Refresh PTL published dynamic table
ALTER DYNAMIC TABLE DEV__PUBLISHED_REPORTING__SECONDARY_USE.CANCER__PTL.PTL3 REFRESH;
