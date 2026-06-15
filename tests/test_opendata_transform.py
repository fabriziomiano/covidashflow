"""Regression tests for Italia Open Data transformation outputs."""

import pandas as pd

from covidashflow.common.vars import (
    F_SEX_KEY,
    M_SEX_KEY,
    OD_NUTS1_KEY,
    OD_NUTS2_KEY,
    OD_REGION_CODE,
    POP_KEY,
    VAX_AGE_KEY,
    VAX_AREA_KEY,
    VAX_DATE_KEY,
    VAX_FIRST_DOSE_KEY,
    VAX_PROVIDER_KEY,
    VAX_SECOND_DOSE_KEY,
)
from covidashflow.opendata.transform import (
    preprocess_vax_admins_df,
    preprocess_vax_admins_summary_df,
)


def test_vax_admins_groups_older_age_bands_and_builds_id():
    """Administration preprocessing should merge 80+ age bands and build IDs."""
    df = pd.DataFrame(
        [
            {
                VAX_DATE_KEY: pd.Timestamp("2021-01-01"),
                VAX_PROVIDER_KEY: "Pfizer",
                VAX_AREA_KEY: "ABR",
                VAX_AGE_KEY: "80-89",
                OD_NUTS1_KEY: "ITF",
                OD_NUTS2_KEY: "ITF1",
                OD_REGION_CODE: 13,
                F_SEX_KEY: 1,
                M_SEX_KEY: 2,
                VAX_FIRST_DOSE_KEY: 3,
                VAX_SECOND_DOSE_KEY: 4,
            },
            {
                VAX_DATE_KEY: pd.Timestamp("2021-01-01"),
                VAX_PROVIDER_KEY: "Pfizer",
                VAX_AREA_KEY: "ABR",
                VAX_AGE_KEY: "90+",
                OD_NUTS1_KEY: "ITF",
                OD_NUTS2_KEY: "ITF1",
                OD_REGION_CODE: 13,
                F_SEX_KEY: 5,
                M_SEX_KEY: 6,
                VAX_FIRST_DOSE_KEY: 7,
                VAX_SECOND_DOSE_KEY: 8,
            },
        ]
    )

    result = preprocess_vax_admins_df(df)

    assert len(result.index) == 1
    assert result.iloc[0][VAX_AGE_KEY] == "80+"
    assert result.iloc[0]["totale"] == 14
    assert result.iloc[0]["_id"].endswith("ABR80+Pfizer")


def test_vax_summary_resamples_dates_and_adds_population():
    """Summary preprocessing should fill missing dates and attach population."""
    df = pd.DataFrame(
        [
            {VAX_DATE_KEY: pd.Timestamp("2021-01-01"), VAX_AREA_KEY: "ABR", "totale": 1},
            {VAX_DATE_KEY: pd.Timestamp("2021-01-03"), VAX_AREA_KEY: "ABR", "totale": 3},
        ]
    )

    result = preprocess_vax_admins_summary_df(df, {"Abruzzo": 100})

    assert len(result.index) == 3
    assert result.iloc[1]["totale"] == 0
    assert result.iloc[0][POP_KEY] == 100
