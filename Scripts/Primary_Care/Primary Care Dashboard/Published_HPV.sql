-- View to prepare HPV data for use in the Primary Care Dashboard. London Indicator hardcoded until this data can be accessed in a refrence table.
-- Contact: eric.pinto@nhs.net

CREATE OR REPLACE VIEW DEV__PUBLISHED_REPORTING__SECONDARY_USE.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__VACCINATIONS__HPV(
	LOCAL_AUTHORITY,
	YEAR_GROUP,
	GENDER,
	NUMBER,
	NUMBER_VACCINATED,
	ACADEMIC_YEAR_END_DATE,
	ACADEMIC_YEAR_TEXT,
	IS_NCL,
	IS_LONDON,
	IS_ENGLAND
) COMMENT='VIEW that SELECTs HPV data for use in the Primary Care Dashboard.'
 as 

SELECT * FROM DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__VACCINATIONS__HPV;