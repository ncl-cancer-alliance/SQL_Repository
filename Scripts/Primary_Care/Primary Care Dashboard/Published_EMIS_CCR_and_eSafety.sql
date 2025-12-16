-- View to prepare Safety_Netting data for use in the Primary Care Dashboard. Joins Practice Reference for Names.
-- Contact: eric.pinto@nhs.net

create or replace view DEV__PUBLISHED_REPORTING__SECONDARY_USE.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__EMIS__SAFETY_NETTING_AND_CCR(
	INDICATOR_NAME,
	PRACTICE_CODE,
	PRACTICE_NAME,
	DEPRIVATION_QUINTILE,
	PCN_NAME,
	BOROUGH,
	POPULATION_COUNT,
	PARENT_COUNT,
	DATE_FULL
) COMMENT='View to SELECT Safety Netting data for use in the Primary Care Dashboard.'
 as

SELECT * FROM DEV__REPORTING.CANCER__PRIMARY_CARE_DASHBOARD.CANCER__EMIS__SAFETY_NETTING_AND_CCR;
