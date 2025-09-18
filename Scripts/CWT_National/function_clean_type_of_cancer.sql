CREATE OR REPLACE FUNCTION DEV__MODELLING.CANCER__CWT_NATIONAL.CLEAN_TYPE_OF_CANCER(TYPE_OF_CANCER STRING)
RETURNS STRING
LANGUAGE SQL

AS
$$

REPLACE(
    CASE
        --All Cancer pseudonyms
        WHEN TYPE_OF_CANCER IN ('ALL REFERRAL ROUTES','ALL SUSPECTED CANCER') THEN 'All Cancers'

        --Drug Formatting
        WHEN TYPE_OF_CANCER = 'ANTI-CANCER DRUG REGIMEN' THEN 'Drugs'

        --Urology Formatting
        WHEN TYPE_OF_CANCER IN (
                'UROLOGICAL (Excluding Testicular)', 
                'Urological (Excluding Testicular)',
                'SUSPECTED UROLOGICAL MALIGNANCIES (EXCLUDING TESTICULAR)',
                'Suspected urological malignancies (excluding testicular)'
        ) THEN 'Urological'
        
        --Suspected X Cancer
        WHEN LEFT(TYPE_OF_CANCER, 9) IN ('Suspected', 'SUSPECTED') AND RIGHT(TYPE_OF_CANCER, 6) IN ('cancer', 'CANCER') THEN (
            INITCAP(SUBSTRING(TYPE_OF_CANCER, 11, LEN(TYPE_OF_CANCER) - 10 - 7))
        )
        --Suspected X
        WHEN LEFT(TYPE_OF_CANCER, 9) IN ('Suspected', 'SUSPECTED') THEN (
            INITCAP(RIGHT(TYPE_OF_CANCER, LEN(TYPE_OF_CANCER) - 10))
        )
        -- X Other (a)
        WHEN RIGHT(TYPE_OF_CANCER, 3) = '(a)' THEN (
            INITCAP(LEFT(TYPE_OF_CANCER, LEN(TYPE_OF_CANCER) - 4))
        )
        ELSE INITCAP(TYPE_OF_CANCER)
    END,
    'Or',
    'or'
)

$$;