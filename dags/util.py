import datetime as dt
import logging
import zipfile

import pandas as pd
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from constants import TEST_DATA_FILE_PATH, DATA_PATH, METADATA_PATH
from constants import DATA_ANALYSIS_CSV_PATH


def extract_headers() -> list:
    """function to extract headers from the metadata file."""

    def filter_line(line):
        if not line.strip():
            return False
        if "ignore" in line:
            return False
        if not line[0].isalpha():
            return False
        return True

    columns = []
    with zipfile.ZipFile(DATA_PATH, "r") as archive:
        with archive.open(METADATA_PATH, "r") as file:
            for line in file:
                column = line.decode("utf-8")
                if filter_line(column):
                    column = (
                        column.split(":")[0].strip().replace("'", "").replace("-", "_")
                    )
                    column = "_".join(column.split(" ")).lower()
                    columns.append(column)

    return columns + ["total_person_income"]


def read_csv(header: list):
    """function to read the csv file containing test data."""

    with zipfile.ZipFile(DATA_PATH, "r") as archive:
        with archive.open(TEST_DATA_FILE_PATH, "r") as file:
            data = pd.read_csv(file, names=header)
    return data


def appending_predicate(
    hook: PostgresHook, schema_name: str, table_name: str, column_name: str, date
) -> bool:
    """function to check if the table exists and if today's data is already loaded."""

    if strtobool(Variable.get("force_append")):
        return True

    table_exists = hook.get_first(
        f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='{table_name}' AND table_schema='{schema_name}');"
    )[0]

    if not table_exists:
        return True

    creation_date = hook.get_first(
        f"SELECT MAX({column_name}) FROM {schema_name}.{table_name};"
    )[0]

    if creation_date == date:
        return False

    return True


def strtobool(val: str) -> bool:
    """Convert a string representation of truth to true (1) or false (0)."""

    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    elif val in ("n", "no", "f", "false", "off", "0"):
        return False
    else:
        raise ValueError("invalid truth value %r" % (val,))


def get_date() -> dt.date:
    """function to get the date to use on the data."""

    try:
        return pd.to_datetime(Variable.get("execution_date")).date()
    except Exception:
        return dt.date.today()


def frequency_analysis(data: pd.DataFrame, date: dt.date) -> None:
    """function to get a frequency table from a with model columns."""

    data_selected = data[
        [
            "sex",
            "education",
            "marital_stat",
            "tax_filer_stat",
            "date_creation",
            "total_person_income",
        ]
    ]

    cross_tab = pd.crosstab(
        data_selected.date_creation,
        columns=[
            data_selected.education,
            data_selected.marital_stat,
            data_selected.tax_filer_stat,
            data_selected.total_person_income,
            data_selected.sex,
        ],
    )

    cross_tab = (
        cross_tab.groupby(pd.Grouper(freq=pd.offsets.Week(weekday=0)))
        .apply(lambda x: x.sum() if not x.empty else None)
        .dropna()
    )

    cross_tab = cross_tab.stack(level=["education"]).unstack(level="date_creation")

    tmp = cross_tab.copy()

    tmp["total"] = tmp.sum(axis=1)
    tmp.loc["total"] = tmp.sum(axis=0)

    cross_tab = cross_tab.div(cross_tab.sum(axis=1), axis=0) * 100
    cross_tab = round(cross_tab, 2)

    cross_tab["total"] = tmp["total"].astype(int)
    cross_tab.loc["total"] = tmp.loc["total"]

    ## save as csv
    cross_tab.to_csv(f"{DATA_ANALYSIS_CSV_PATH}_frequency_{date}.csv")

    # save as web page
    cross_tab.to_html(f"{DATA_ANALYSIS_CSV_PATH}_frequency_{date}.html")


def numeric_distribution(data: pd.DataFrame, date: dt.date) -> None:
    """function to calculate numerical data distribution."""

    result = (
        data.groupby(pd.Grouper(key="date_creation", freq=pd.offsets.Week(weekday=0)))
        .apply(lambda x: x.describe() if not x.empty else None)
        .dropna()
        .unstack(0)
    )

    ## save as csv
    result.to_csv(f"{DATA_ANALYSIS_CSV_PATH}_numeric_distribution_{date}.csv")

    # save as web page
    result.to_html(f"{DATA_ANALYSIS_CSV_PATH}_numeric_distribution_{date}.html")


def correlation_analysis(data: pd.DataFrame, date: dt.date) -> None:
    """function to calculate correlation between numerical data."""

    result = (
        data.groupby(pd.Grouper(key="date_creation", freq=pd.offsets.Week(weekday=0)))
        .apply(lambda x: x.corr() if not x.empty else None)
        .unstack(0)
    )

    ## save as csv
    result.to_csv(f"{DATA_ANALYSIS_CSV_PATH}_correlation_{date}.csv")

    # save as web page
    result.to_html(f"{DATA_ANALYSIS_CSV_PATH}_correlation_{date}.html")
