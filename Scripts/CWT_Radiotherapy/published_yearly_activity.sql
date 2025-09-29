--Published View
CREATE OR REPLACE VIEW DEV__PUBLISHED_REPORTING__SECONDARY_USE.CANCER__RADIOTHERAPY.YEARLY_ACTIVITY
COMMENT = "View to get yearly activity and calculate year on year growth. Contact: jake.kealey@nhs.net"
AS

SELECT 
FIN_YEAR AS "Financial Year",
RADIOTHERAPY_NETWORK AS "Radiotherapy Network",
CANCER_PATHWAY AS "Cancer Pathway",
NO_PATIENTS_CUR AS "Patients Referred",
NO_PATIENTS_PREV AS "Last Year Patients Referred"
FROM DEV__REPORTING.CANCER__RADIOTHERAPY.YEARLY_ACTIVITY
WHERE FIN_YEAR >= '2022-23';