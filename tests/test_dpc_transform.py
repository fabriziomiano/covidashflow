"""Regression tests for PCM-DPC transformation outputs."""

import pandas as pd

from covidashflow.common.vars import (
    DAILY_ICU_KEY,
    DATE_KEY,
    ICU_KEY,
    NEW_POSITIVE_KEY,
    REGION_CODE,
    REGION_KEY,
    SELF_ISOLATION_KEY,
    TOTAL_CASES_KEY,
    TOTAL_DEATHS_KEY,
    TOTAL_HEALED_KEY,
    TOTAL_HOSPITALIZED_KEY,
    TOTAL_POSITIVE_KEY,
    TOTAL_SWABS_KEY,
)
from covidashflow.dpc.transform import (
    build_national_series,
    build_national_trends,
    build_regional_breakdown,
    preprocess_national_df,
    preprocess_regional_df,
)


def national_frame(days: int = 9) -> pd.DataFrame:
    """Build a compact national dataframe with enough rows for weekly trends."""
    rows = []
    for day in range(days):
        rows.append(
            {
                DATE_KEY: pd.Timestamp("2021-01-01") + pd.Timedelta(days=day),
                NEW_POSITIVE_KEY: 10 + day,
                DAILY_ICU_KEY: 1 + day,
                TOTAL_DEATHS_KEY: 100 + day,
                TOTAL_SWABS_KEY: 1000 + (day * 100),
                TOTAL_CASES_KEY: 500 + (day * 10),
                TOTAL_HEALED_KEY: 200 + day,
                TOTAL_POSITIVE_KEY: 300 + day,
                ICU_KEY: 20 + day,
                TOTAL_HOSPITALIZED_KEY: 40 + day,
                SELF_ISOLATION_KEY: 200 + day,
            }
        )
    return pd.DataFrame(rows)


def test_national_preprocess_adds_derived_columns_and_series():
    """National preprocessing should add derived columns consumed by series builders."""
    df = preprocess_national_df(national_frame())

    assert "tamponi_g" in df.columns
    assert "nuovi_positivi_ma" in df.columns
    assert "indice_positivita" in df.columns

    series = build_national_series(df)
    assert set(series) == {"dates", "daily", "current"}
    assert series["daily"][0]["data"]


def test_national_preprocess_dedupes_dates_and_ignores_cumulative_backtracks():
    """Temporary cumulative source corrections should not create daily spikes."""
    df = national_frame(days=10)
    df.loc[8, TOTAL_SWABS_KEY] = 900
    df.loc[9, TOTAL_SWABS_KEY] = 1900
    duplicated = pd.concat([df, df.tail(1)], ignore_index=True)

    transformed = preprocess_national_df(duplicated)

    assert len(transformed) == len(df)
    assert transformed[DATE_KEY].is_unique
    assert transformed["tamponi_g"].iloc[8] == 0
    assert transformed["tamponi_g"].iloc[9] == 200


def test_national_trends_match_card_shape():
    """Trend documents should keep the frontend card contract."""
    df = preprocess_national_df(national_frame())

    trends = build_national_trends(df)

    assert trends
    assert {"id", "title", "count", "percentage_difference", "last_week_count"} <= set(
        trends[0]
    )


def test_regional_preprocess_is_grouped_per_region():
    """Regional preprocessing should compute independent per-region outputs."""
    df = pd.concat(
        [
            national_frame().assign(**{REGION_CODE: 1, REGION_KEY: "Abruzzo"}),
            national_frame().assign(**{REGION_CODE: 3, REGION_KEY: "Lombardia"}),
        ],
        ignore_index=True,
    )

    transformed = preprocess_regional_df(df)
    breakdown = build_regional_breakdown(transformed)

    assert len(transformed[REGION_CODE].unique()) == 2
    assert "totale_casi" in breakdown
    assert {item["area"] for item in breakdown["totale_casi"]} == {"Abruzzo", "Lombardia"}
