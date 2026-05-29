create or replace view DEV__MODELLING.CANCER__CCO.PTL_SOURCE_COMBINED(
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
	"Day 0-33",
	"Day 34-62",
	"CreateTS"
) COMMENT='Description: Combined view unioning the original automated PTL source (DATA_LAKE.PMCT.Cwt63DayPlusWeeklySourceAppendReviseProv) with manually uploaded Futures data (DATA_LAKE__NCL.CANCER__PTL.PTL_SHAREPOINT_STAGE). SharePoint rows are only included where DateKey exceeds the maximum DateKey in the automated source, preventing overlap.\n\nCreated: 28/05/2026\nOwner: Eric Pinto (eric.pinto@nhs.net)'
 as

    SELECT * 
    FROM DATA_LAKE.PMCT."Cwt63DayPlusWeeklySourceAppendReviseProv"

    UNION ALL

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
        NULL AS "Day 0-33",
        NULL AS "Day 34-62",
        "CreateTS"
    FROM DATA_LAKE__NCL.CANCER__PTL.PTL_SHAREPOINT_STAGE
    WHERE "DateKey" > (
        SELECT MAX("DateKey") 
        FROM DATA_LAKE.PMCT."Cwt63DayPlusWeeklySourceAppendReviseProv"
    );