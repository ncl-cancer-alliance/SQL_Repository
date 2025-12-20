/*
Script: DEV__REPORTING.CANCER__RCRD.CANCER__RCRD__NATIONAL

Description:
Dynamic table to clean RCRD National data, and align columns with Local Extract

Author: Eric Pinto

Target Output Tables:
 - DEV__REPORTING.CANCER__RCRD.CANCER__RCRD__NATIONAL
*/

create or replace dynamic table DEV__REPORTING.CANCER__RCRD.CANCER__RCRD__NATIONAL(
	GEOGRAPHY_TYPE,
	GEOGRAPHY_NAME,
	DATE_FULL,
	CANCER_GROUP,
	CANCER_GROUP_BROAD,
	CANCER_GROUP_DETAILED,
	IS_STAGEABLE,
	METRIC_NAME,
	BREAKDOWN_NAME,
	DEMOGRAPHIC_NAME,
	COMPLETENESS_TREATMENT_FOLLOWUP,
	NUMERATOR,
	DENOMINATOR,
	STATISTIC,
	NUMERATOR_12M,
	DENOMINATOR_12M,
	STATISTIC_12M
) target_lag = '2 hours' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Dynamic table to clean RCRD National data, and align columns with Local Extract \n\nContact: eric.pinto@nhs.net'
 as

SELECT 

	GEOGRAPHY_TYPE,
    TRIM(REGEXP_REPLACE(
        GEOGRAPHY_NAME,
        '(Integrated Care Board|NHS )',
        ''
    )) AS GEOGRAPHY_NAME_CLEAN,
	DATE_FULL,
	CANCER_GROUP,
	CASE WHEN CANCER_GROUP_BROAD = 'Blood cancer (haematological neoplasms)' THEN 'Haematological'
        WHEN CANCER_GROUP_BROAD = 'Respiratory and intrathoracic' THEN 'Lung'
        ELSE REPLACE(CANCER_GROUP_BROAD, ' and ', ' & ')
        END AS CANCER_GROUP_BROAD_FIXED,

	CASE WHEN CANCER_GROUP_DETAILED = 'Broad group total' THEN CONCAT(CANCER_GROUP_BROAD_FIXED,' - Total')
        WHEN CANCER_GROUP_DETAILED = 'NA' THEN 'All Cancer Sites - Total'
        ELSE CANCER_GROUP_DETAILED
        END AS NEW_CANCER_GROUP_DETAILED,
    
    CASE 
        WHEN CANCER_GROUP_BROAD IN (
            'Breast',
            'Cervix',
            'Colon',
            'Corpus Uteri',
            'Lung',
            'Melanoma',
            'Non-Hodgkin lymphoma',
            'Ovary',
            'Pancreas',
            'Prostate',
            'Rectal',
            'Stomach',
            'Uterus',
            'Bladder',
            'Kidney',
            'Hodgkin lymphoma',
            'Oesophagus',
            'Bowel',
            'Gynaecological',
            'Oesophago-gastric',
            'Respiratory and intrathoracic',
            'Blood cancer (haematological neoplasms)',
            'Urological excl. prostate',
            'Upper GI excl. OG'
        ) THEN TRUE
        ELSE FALSE
        END AS IS_STAGEABLE,
	METRIC_NAME,
	BREAKDOWN_NAME,
	DEMOGRAPHIC_NAME,
	COMPLETENESS_TREATMENT_FOLLOWUP,
	NUMERATOR,
	DENOMINATOR,
	STATISTIC,
	NUMERATOR_12M,
	DENOMINATOR_12M,
	STATISTIC_12M

FROM DEV__MODELLING.CANCER__RCRD.RCRD_NATIONAL
-- Changed Respiratory and intrathoracic to Lung, so this exclusion avoids double counting Lung
WHERE NEW_CANCER_GROUP_DETAILED <> 'Lung';