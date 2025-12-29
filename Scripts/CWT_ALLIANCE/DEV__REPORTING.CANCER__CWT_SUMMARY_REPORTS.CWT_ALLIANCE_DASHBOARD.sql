create or replace dynamic table DEV__REPORTING.CANCER__CWT_SUMMARY_REPORTS.CWT_ALLIANCE_DASHBOARD(
	LEVEL,
	STANDARD,
	TARGET,
	TARGET_ASPIRATION,
	FIN_YEAR,
	FIN_MONTH_NAME,
	DATE,
	ORG_CODE,
	ORG_NAME,
	PATHWAY,
	TREATMENT_STAGE,
	TREATMENT_MODALITY,
	TREATMENT_GROUP,
	CANCER_REPORT_CATEGORY,
	CANCER_REPORT_CATEGORY_LG,
	FLTR_GROUP1,
	FLTR_GROUP2,
	FLTR_GROUP3,
	FLTR_GROUP4,
	NO_PATHWAYS,
	NO_BREACHES,
	WITHIN_STANDARD,
	DAY_GROUP1,
	DAY_GROUP2,
	DAY_GROUP3,
	DAY_GROUP4,
	DAY_GROUP5,
	TRAJECTORY_NUMERATOR,
	TRAJECTORY_DENOMINATOR
) target_lag = '1 day' refresh_mode = FULL initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS
 COMMENT='Unifies the 4 CADEAS CWT standards into one dataset (volumes, breaches, trajectories), mapping NMUH to RFH and standard-specific day bands and category fields into shared columns for Power BI reporting. PowerBI Dim table logic listed at the end of the code. Contact: ben.goretzki@nhs.net'
 as



WITH base AS (

/* =========================
   31 DAY - ICB
   ========================= */
SELECT
    'ICB'                               AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,
    'QMJ'                               AS ORG_CODE,
    'NCL ICB'                           AS ORG_NAME,
    NULL::VARCHAR                       AS PATHWAY,
    TREATMENT_STAGE,
    TREATMENT_MODALITY,
    TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY,
    CANCER_REPORT_CATEGORY_LG,

    'NCL ICB'                 AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_LG  AS FLTR_GROUP2,
    TREATMENT_STAGE            AS FLTR_GROUP3,
    SUBSEQUENT_GROUPING        AS FLTR_GROUP4,

    NO_TREATED                          AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_31                      AS DAY_GROUP1,
    DAYS_32_TO_38                       AS DAY_GROUP2,
    DAYS_39_TO_48                       AS DAY_GROUP3,
    DAYS_49_TO_62                       AS DAY_GROUP4,
    DAYS_MORE_THAN_62                   AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.ICB__31_DAY
WHERE
    (
        ( TREATMENT_STAGE = 'First Treatment'
          AND TREATMENT_MODALITY <> 'All treatment declined'
        )
        OR
        ( TREATMENT_STAGE = 'Subsequent'
          AND TREATMENT_MODALITY NOT IN (
                'Specialist Palliative Care',
                'Active Monitoring (excluding Non-Specialist Palliative Care)',
                'Non-Specialist Palliative Care (excluding Active Monitoring)',
                'All treatment declined'
          )
        )
    )

UNION ALL

/* =========================
   31 DAY - TRUST
   ========================= */
SELECT
    'TRUST'                             AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,

    CASE WHEN PROVIDER_CODE IN('RAP','RRP') THEN 'RAL' ELSE PROVIDER_CODE END AS ORG_CODE,
    CASE WHEN PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE PROVIDER_NAME END AS ORG_NAME,

    NULL::VARCHAR                       AS PATHWAY,
    TREATMENT_STAGE,
    TREATMENT_MODALITY,
    TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY,
    CANCER_REPORT_CATEGORY_LG,

    CASE WHEN PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE PROVIDER_NAME END AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_LG  AS FLTR_GROUP2,
    TREATMENT_STAGE            AS FLTR_GROUP3,
    SUBSEQUENT_GROUPING        AS FLTR_GROUP4,

    NO_TREATED                          AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_31                      AS DAY_GROUP1,
    DAYS_32_TO_38                       AS DAY_GROUP2,
    DAYS_39_TO_48                       AS DAY_GROUP3,
    DAYS_49_TO_62                       AS DAY_GROUP4,
    DAYS_MORE_THAN_62                   AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.TRUST__31_DAY
WHERE
    (
        ( TREATMENT_STAGE = 'First Treatment'
          AND TREATMENT_MODALITY <> 'All treatment declined'
        )
        OR
        ( TREATMENT_STAGE = 'Subsequent'
          AND TREATMENT_MODALITY NOT IN (
                'Specialist Palliative Care',
                'Active Monitoring (excluding Non-Specialist Palliative Care)',
                'Non-Specialist Palliative Care (excluding Active Monitoring)',
                'All treatment declined'
          )
        )
    )

UNION ALL

/* =========================
   62 DAY - ICB
   ========================= */
SELECT
    'ICB'                               AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,
    'QMJ'                               AS ORG_CODE,
    'NCL ICB'                           AS ORG_NAME,
    PATHWAY,
    NULL::VARCHAR                       AS TREATMENT_STAGE,
    TREATMENT_MODALITY,
    TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY,
    CANCER_REPORT_CATEGORY_LG,

    'NCL ICB'                 AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_LG  AS FLTR_GROUP2,
    PATHWAY                    AS FLTR_GROUP3,
    SUBSEQUENT_GROUPING        AS FLTR_GROUP4,

    NO_PATIENTS                         AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_31 + DAYS_32_TO_62      AS DAY_GROUP1,
    DAYS_63_TO_76                       AS DAY_GROUP2,
    DAYS_77_TO_90                       AS DAY_GROUP3,
    DAYS_91_TO_104                      AS DAY_GROUP4,
    DAYS_MORE_THAN_104                  AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.ICB__62_DAY

UNION ALL

/* =========================
   62 DAY - TRUST
   ========================= */
SELECT
    'TRUST'                             AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,

    CASE WHEN ACCOUNTABLE_62_DAY_PROVIDER_CODE IN('RAP','RRP') THEN 'RAL' ELSE ACCOUNTABLE_62_DAY_PROVIDER_CODE END AS ORG_CODE,
    CASE WHEN ACCOUNTABLE_62_DAY_PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE ACCOUNTABLE_62_DAY_PROVIDER_NAME END AS ORG_NAME,

    PATHWAY,
    NULL::VARCHAR                       AS TREATMENT_STAGE,
    TREATMENT_MODALITY,
    TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY,
    CANCER_REPORT_CATEGORY_LG,

    CASE WHEN ACCOUNTABLE_62_DAY_PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE ACCOUNTABLE_62_DAY_PROVIDER_NAME END AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_LG  AS FLTR_GROUP2,
    PATHWAY                    AS FLTR_GROUP3,
    SUBSEQUENT_GROUPING        AS FLTR_GROUP4,

    NO_PATIENTS                         AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_31 + DAYS_32_TO_62      AS DAY_GROUP1,
    DAYS_63_TO_76                       AS DAY_GROUP2,
    DAYS_77_TO_90                       AS DAY_GROUP3,
    DAYS_91_TO_104                      AS DAY_GROUP4,
    DAYS_MORE_THAN_104                  AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.TRUST__62_DAY

UNION ALL

/* =========================
   FDS - ICB
   ========================= */
SELECT
    'ICB'                               AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,
    'QMJ'                               AS ORG_CODE,
    'NCL ICB'                           AS ORG_NAME,
    NULL::VARCHAR                       AS PATHWAY,
    NULL::VARCHAR                       AS TREATMENT_STAGE,
    NULL::VARCHAR                       AS TREATMENT_MODALITY,
    NULL::VARCHAR                       AS TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY_SHORT        AS CANCER_REPORT_CATEGORY,
    NULL::VARCHAR                       AS CANCER_REPORT_CATEGORY_LG,

    'NCL ICB'                 AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_SHORT AS FLTR_GROUP2,
    PRIORITY_TYPE             AS FLTR_GROUP3,
    PATHWAY_END_REASON        AS FLTR_GROUP4,

    NO_PATIENTS                         AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_7 + DAYS_8_TO_14 + DAYS_15_TO_21 + DAYS_22_TO_28        AS DAY_GROUP1,
    DAYS_29_TO_35                                                    AS DAY_GROUP2,
    DAYS_36_TO_42                                                    AS DAY_GROUP3,
    DAYS_43_TO_49 + DAYS_50_TO_62                                    AS DAY_GROUP4,
    DAYS_63_TO_76 + DAYS_77_TO_90 + DAYS_91_TO_104 + DAYS_MORE_THAN_104 AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.ICB__FDS

UNION ALL

/* =========================
   FDS - TRUST
   ========================= */
SELECT
    'TRUST'                             AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,

    CASE WHEN FDS_PROVIDER_CODE IN('RAP','RRP') THEN 'RAL' ELSE FDS_PROVIDER_CODE END AS ORG_CODE,
    CASE WHEN FDS_PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE FDS_PROVIDER_NAME END AS ORG_NAME,

    NULL::VARCHAR                       AS PATHWAY,
    NULL::VARCHAR                       AS TREATMENT_STAGE,
    NULL::VARCHAR                       AS TREATMENT_MODALITY,
    NULL::VARCHAR                       AS TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY_SHORT        AS CANCER_REPORT_CATEGORY,
    NULL::VARCHAR                       AS CANCER_REPORT_CATEGORY_LG,

    CASE WHEN FDS_PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE FDS_PROVIDER_NAME END AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_SHORT AS FLTR_GROUP2,
    PRIORITY_TYPE             AS FLTR_GROUP3,
    PATHWAY_END_REASON        AS FLTR_GROUP4,

    NO_PATIENTS                         AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_7 + DAYS_8_TO_14 + DAYS_15_TO_21 + DAYS_22_TO_28        AS DAY_GROUP1,
    DAYS_29_TO_35                                                    AS DAY_GROUP2,
    DAYS_36_TO_42                                                    AS DAY_GROUP3,
    DAYS_43_TO_49 + DAYS_50_TO_62                                    AS DAY_GROUP4,
    DAYS_63_TO_76 + DAYS_77_TO_90 + DAYS_91_TO_104 + DAYS_MORE_THAN_104 AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.TRUST__FDS

UNION ALL

/* =========================
   2WW - ICB
   ========================= */
SELECT
    'ICB'                               AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,
    'QMJ'                               AS ORG_CODE,
    'NCL ICB'                           AS ORG_NAME,
    NULL::VARCHAR                       AS PATHWAY,
    NULL::VARCHAR                       AS TREATMENT_STAGE,
    NULL::VARCHAR                       AS TREATMENT_MODALITY,
    NULL::VARCHAR                       AS TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY_SHORT        AS CANCER_REPORT_CATEGORY,
    NULL::VARCHAR                       AS CANCER_REPORT_CATEGORY_LG,

    'NCL ICB'                 AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_SHORT AS FLTR_GROUP2,
    NULL::VARCHAR             AS FLTR_GROUP3,
    NULL::VARCHAR             AS FLTR_GROUP4,

    NO_SEEN                             AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_14                      AS DAY_GROUP1,
    DAYS_15_TO_16                       AS DAY_GROUP2,
    DAYS_17_TO_21                       AS DAY_GROUP3,
    DAYS_22_TO_28                       AS DAY_GROUP4,
    DAYS_MORE_THAN_28                   AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.ICB__TWO_WEEK_WAIT

UNION ALL

/* =========================
   2WW - TRUST
   ========================= */
SELECT
    'TRUST'                             AS LEVEL,
    STANDARD,
    FIN_YEAR,
    FIN_MONTH_NAME,
    DATE,

    CASE WHEN PROVIDER_CODE IN('RAP','RRP') THEN 'RAL' ELSE PROVIDER_CODE END AS ORG_CODE,
    CASE WHEN PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE PROVIDER_NAME END AS ORG_NAME,

    NULL::VARCHAR                       AS PATHWAY,
    NULL::VARCHAR                       AS TREATMENT_STAGE,
    NULL::VARCHAR                       AS TREATMENT_MODALITY,
    NULL::VARCHAR                       AS TREATMENT_GROUP,
    CANCER_REPORT_CATEGORY_SHORT        AS CANCER_REPORT_CATEGORY,
    NULL::VARCHAR                       AS CANCER_REPORT_CATEGORY_LG,

    CASE WHEN PROVIDER_NAME IN ('North Middlesex University Hospital NHS Trust','Barnet, Enfield and Haringey Mental Health NHS Trust')
         THEN 'Royal Free London NHS Foundation Trust' ELSE PROVIDER_NAME END AS FLTR_GROUP1,
    CANCER_REPORT_CATEGORY_SHORT AS FLTR_GROUP2,
    NULL::VARCHAR             AS FLTR_GROUP3,
    NULL::VARCHAR             AS FLTR_GROUP4,

    NO_SEEN                             AS NO_PATHWAYS,
    BREACHES,

    DAYS_WITHIN_14                      AS DAY_GROUP1,
    DAYS_15_TO_16                       AS DAY_GROUP2,
    DAYS_17_TO_21                       AS DAY_GROUP3,
    DAYS_22_TO_28                       AS DAY_GROUP4,
    DAYS_MORE_THAN_28                   AS DAY_GROUP5

FROM MODELLING.CANCER__CWT_ALLIANCE.TRUST__TWO_WEEK_WAIT
),

enriched AS (
    SELECT
        b.*,

        COALESCE(lu_main.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY) AS CANCER_REPORT_CATEGORY_MAPPED,

        CASE
            WHEN NULLIF(TRIM(COALESCE(lu_lg.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY_LG)), '') IS NULL
                THEN NULLIF(TRIM(COALESCE(lu_main.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY)), '')

            WHEN TRIM(COALESCE(lu_lg.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY_LG)) = 'Other'
                 AND TRIM(COALESCE(lu_main.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY)) <> 'Other'
                THEN TRIM(COALESCE(lu_main.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY)) || ' - Other'

            WHEN TRIM(COALESCE(lu_lg.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY_LG)) = 'Other'
                 AND TRIM(COALESCE(lu_main.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY)) = 'Other'
                THEN 'Other'

            ELSE TRIM(COALESCE(lu_lg.CANCER_TYPE_SHORT, b.CANCER_REPORT_CATEGORY_LG))
        END AS CANCER_REPORT_CATEGORY_LG_FINAL,

        CASE
            WHEN b.LEVEL = 'ICB' THEN b.ORG_NAME
            ELSE ncl.PROVIDER_SHORTHAND
        END AS ORG_NAME_FINAL

    FROM base b
    LEFT JOIN MODELLING.LOOKUP_NCL.NCL_PROVIDER ncl
      ON b.LEVEL = 'TRUST'
     AND b.ORG_CODE = ncl.REPORTING_CODE
     AND ncl.ROW_TYPE = 'trust'

    LEFT JOIN MODELLING.CANCER__REF.CANCER_TYPE_LOOKUP lu_main
      ON b.CANCER_REPORT_CATEGORY = lu_main.CANCER_TYPE_SOURCE

    LEFT JOIN MODELLING.CANCER__REF.CANCER_TYPE_LOOKUP lu_lg
      ON b.CANCER_REPORT_CATEGORY_LG = lu_lg.CANCER_TYPE_SOURCE
),

final_agg AS (
    SELECT
        e.LEVEL,
        e.STANDARD,
        tgt.TARGET AS TARGET,
        tgt_asp.TARGET AS TARGET_ASPIRATION,
        e.FIN_YEAR,
        e.FIN_MONTH_NAME,
        e.DATE,
        e.ORG_CODE,
        e.ORG_NAME_FINAL AS ORG_NAME,
        e.PATHWAY,
        e.TREATMENT_STAGE,
        e.TREATMENT_MODALITY,
        e.TREATMENT_GROUP,

        e.CANCER_REPORT_CATEGORY_MAPPED    AS CANCER_REPORT_CATEGORY,
        e.CANCER_REPORT_CATEGORY_LG_FINAL  AS CANCER_REPORT_CATEGORY_LG,

        e.ORG_NAME_FINAL AS FLTR_GROUP1,

        CASE
            WHEN UPPER(e.STANDARD) IN ('31 DAY', '62 DAY') THEN e.CANCER_REPORT_CATEGORY_LG_FINAL
            ELSE e.FLTR_GROUP2
        END AS FLTR_GROUP2,

        e.FLTR_GROUP3,
        e.FLTR_GROUP4,

        SUM(e.NO_PATHWAYS) AS NO_PATHWAYS,
        SUM(e.BREACHES) AS NO_BREACHES,
        SUM(e.NO_PATHWAYS) - SUM(e.BREACHES) AS WITHIN_STANDARD,

        SUM(e.DAY_GROUP1) AS DAY_GROUP1,
        SUM(e.DAY_GROUP2) AS DAY_GROUP2,
        SUM(e.DAY_GROUP3) AS DAY_GROUP3,
        SUM(e.DAY_GROUP4) AS DAY_GROUP4,
        SUM(e.DAY_GROUP5) AS DAY_GROUP5

    FROM enriched e

    LEFT JOIN MODELLING.CANCER__REF.CWT_STANDARD_TARGETS tgt
      ON REPLACE(e.FIN_YEAR, '/', '-') = tgt.FIN_YEAR
     AND tgt.STANDARD = e.STANDARD
     AND tgt.STANDARD NOT ILIKE '%Aspiration%'

    LEFT JOIN MODELLING.CANCER__REF.CWT_STANDARD_TARGETS tgt_asp
      ON REPLACE(e.FIN_YEAR, '/', '-') = tgt_asp.FIN_YEAR
     AND tgt_asp.STANDARD = e.STANDARD || ' Aspiration'

    GROUP BY
        e.LEVEL,
        e.STANDARD,
        tgt.TARGET,
        tgt_asp.TARGET,
        e.FIN_YEAR,
        e.FIN_MONTH_NAME,
        e.DATE,
        e.ORG_CODE,
        e.ORG_NAME_FINAL,
        e.PATHWAY,
        e.TREATMENT_STAGE,
        e.TREATMENT_MODALITY,
        e.TREATMENT_GROUP,
        e.CANCER_REPORT_CATEGORY_MAPPED,
        e.CANCER_REPORT_CATEGORY_LG_FINAL,
        e.FLTR_GROUP2,
        e.FLTR_GROUP3,
        e.FLTR_GROUP4
),

traj AS (
    SELECT DISTINCT
        DATE_FULL,
        PROVIDER_SHORTHAND,
        STANDARD,
        NUMERATOR,
        DENOMINATOR
    FROM DEV__MODELLING.CANCER__CCO.CANCER_TRAJECTORY
)

SELECT
    /* ===== required column order ===== */
    fa."LEVEL",
    fa."STANDARD",
    fa."TARGET",
    fa."TARGET_ASPIRATION",
    fa."FIN_YEAR",
    fa."FIN_MONTH_NAME",
    fa."DATE",
    fa."ORG_CODE",
    fa."ORG_NAME",
    fa."PATHWAY",
    fa."TREATMENT_STAGE",
    fa."TREATMENT_MODALITY",
    fa."TREATMENT_GROUP",
    fa."CANCER_REPORT_CATEGORY",
    fa."CANCER_REPORT_CATEGORY_LG",
    fa."FLTR_GROUP1",
    fa."FLTR_GROUP2",
    fa."FLTR_GROUP3",
    fa."FLTR_GROUP4",
    fa."NO_PATHWAYS",
    fa."NO_BREACHES",
    fa."WITHIN_STANDARD",
    fa."DAY_GROUP1",
    fa."DAY_GROUP2",
    fa."DAY_GROUP3",
    fa."DAY_GROUP4",
    fa."DAY_GROUP5",
    t.NUMERATOR   AS "TRAJECTORY_NUMERATOR",
    t.DENOMINATOR AS "TRAJECTORY_DENOMINATOR"

FROM final_agg fa
LEFT JOIN traj t
  ON t.DATE_FULL = fa."DATE"
 AND t.STANDARD  = fa."STANDARD"
 AND t.PROVIDER_SHORTHAND =
        CASE
            WHEN fa."ORG_NAME" = 'NCL ICB' THEN 'NCL'
            ELSE fa."ORG_NAME"
        END

/* =========================
   PowerBI Dim table script - Filter Group Labels
   ========================= 
   
   Dim - Filter Group Labels = 
    DATATABLE (
    "Standard", STRING,
    "Filter", STRING,
    "Description", STRING,
    {

    
        { "FDS",    "FLTR_GROUP1", "Organisation"},
        { "31 day", "FLTR_GROUP1", "Organisation" },
        { "62 day", "FLTR_GROUP1", "Organisation" },
        { "2WW",    "FLTR_GROUP1", "Organisation" },
    
    
        { "FDS",    "FLTR_GROUP2", "Suspected Cancer Type" },
        { "31 day", "FLTR_GROUP2", "Cancer Category (Lower Granularity)" },
        { "62 day", "FLTR_GROUP2", "Cancer Category (Lower Granularity)" },
        { "2WW",    "FLTR_GROUP2", "Suspected Cancer Type" },

        { "FDS",    "FLTR_GROUP3", "Priority Type" },
        { "31 day", "FLTR_GROUP3", "Treatment Event" },
        { "62 day", "FLTR_GROUP3", "Pathway Type" },
        { "2WW",    "FLTR_GROUP3", BLANK() },

        { "FDS",    "FLTR_GROUP4", "FDS End Reason" },
        { "31 day", "FLTR_GROUP4", "Treatment Type" },
        { "62 day", "FLTR_GROUP4", "Treatment Type" },
        { "2WW",    "FLTR_GROUP4", BLANK() }

    }
)

    =========================
    PowerBI Dim table script - Day Group Labels
    ========================= 
   
   Dim - Day Group Labels = 
    DATATABLE (
    "Standard", STRING,
    "Volumes", STRING,
    "Field", STRING,
    "Description", STRING,
    {
        { "FDS",    "pathways",             "DAY_GROUP1", "within 28" },
        { "31 day", "treatments",           "DAY_GROUP1", "within 31" },
        { "62 day", "accountable pathways", "DAY_GROUP1", "within 62" },
        { "2WW",    "pathways",             "DAY_GROUP1", "within 14" },

        { "FDS",    "pathways",             "DAY_GROUP2", "29-35" },
        { "31 day", "treatments",           "DAY_GROUP2", "32-38" },
        { "62 day", "accountable pathways", "DAY_GROUP2", "63-76" },
        { "2WW",    "pathways",             "DAY_GROUP2", "15-16" },

        { "FDS",    "pathways",             "DAY_GROUP3", "36-42" },
        { "31 day", "treatments",           "DAY_GROUP3", "39-48" },
        { "62 day", "accountable pathways", "DAY_GROUP3", "77-90" },
        { "2WW",    "pathways",             "DAY_GROUP3", "17-21" },

        { "FDS",    "pathways",             "DAY_GROUP4", "43-62" },
        { "31 day", "treatments",           "DAY_GROUP4", "49-62" },
        { "62 day", "accountable pathways", "DAY_GROUP4", "91-104" },
        { "2WW",    "pathways",             "DAY_GROUP4", "22-28" },

        { "FDS",    "pathways",             "DAY_GROUP5", ">62" },
        { "31 day", "treatments",           "DAY_GROUP5", ">62" },
        { "62 day", "accountable pathways", "DAY_GROUP5", ">104" },
        { "2WW",    "pathways",             "DAY_GROUP5", ">28" }
    }
)
   */
   ;