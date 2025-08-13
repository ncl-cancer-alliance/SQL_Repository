-- View that SELECTs Population Health data for use in the Primary Care Dashboard. UNION Social Prescribing Data as they are joined in the Dashboard.
-- Contact: eric.pinto@nhs.net

CREATE OR REPLACE VIEW DEV__PUBLISHED_REPORTING__SECONDARY_USE.PUBLIC.CANCER__FINGERTIPS__INDICATOR_DATA
    
COMMENT='View that SELECTs Population Health data for use in the Primary Care Dashboard. UNION Social Prescribing Data as they are joined in the Dashboard.'

AS

SELECT * FROM DEV__REPORTING.PUBLIC.CANCER__FINGERTIPS__INDICATOR_DATA

UNION ALL 

SELECT * FROM DEV__REPORTING.PUBLIC.CANCER__EMIS__SOCIAL_PRESCRIBING
