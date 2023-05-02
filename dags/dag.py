import datetime as dt
import logging
import os

import numpy as np
import pandas as pd
import xgboost as xgb
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.email import send_email
from constants import (
    CLASSIFICATION_TABLE_NAME,
    DATA_ANALYSIS_CSV_PATH,
    MODEL_MONITORING_CSV_PATH,
    MODEL_PATH,
    SCHEMA_NAME,
    TEST_TABLE_NAME,
)
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from util import (
    appending_predicate,
    correlation_analysis,
    extract_headers,
    frequency_analysis,
    get_date,
    numeric_distribution,
    read_csv,
)

logger = logging.getLogger()
default_args = {"owner": "chronotruck", "schedule": "@daily"}


@dag(default_args=default_args, start_date=dt.datetime(2023, 3, 15))
def us_census_mlops() -> None:
    # get database connection hook
    db_hook = PostgresHook(postgres_conn_id="postgres_conn_id")
    engine = db_hook.get_sqlalchemy_engine()

    data_date = get_date()

    prepare_schema = PostgresOperator(
        postgres_conn_id="postgres_conn_id",
        task_id="prepare_schema",
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
          """,
    )

    @task(task_id="load_test_data")
    def load_test_data() -> None:
        header = extract_headers()
        data = read_csv(header)
        data["date_creation"] = data_date

        if appending_predicate(
            db_hook, SCHEMA_NAME, TEST_TABLE_NAME, "date_creation", data_date
        ):
            data.convert_dtypes().to_sql(
                name=TEST_TABLE_NAME,
                schema=SCHEMA_NAME,
                con=engine.execution_options(isolation_level="SERIALIZABLE"),
                if_exists="append",
                index=False,
            )
            logger.info("test data loaded successfully")
        else:
            logger.info("test data loading skipped due to earlier insertions")

    @task(task_id="load_classifications")
    def load_classifications() -> None:
        try:
            model = xgb.XGBClassifier()
            model.load_model(MODEL_PATH)
        except Exception as e:
            logger.error("Error while loading model ", e)
            raise (e)

        model_features = model.get_booster().feature_names

        data = pd.read_sql(
            f"""
            select age,sex, marital_stat, education, tax_filer_stat, total_person_income 
            from {SCHEMA_NAME}.{TEST_TABLE_NAME} where date_creation = '{data_date}'
            """,
            con=engine,
        )

        X = data.drop("total_person_income", axis=1)
        X = pd.get_dummies(
            X, columns=["sex", "marital_stat", "education", "tax_filer_stat"]
        )

        try:
            assert X.columns.tolist() == model_features
        except AssertionError as e:
            logger.error("Columns mismatch between model and test data", e)
            raise e

        X = X[model_features]

        result = X.copy()
        precictions = model.predict(X)

        result["classification"] = precictions
        result["classification_date"] = data_date
        result["total_person_income"] = data["total_person_income"]

        if appending_predicate(
            db_hook,
            SCHEMA_NAME,
            CLASSIFICATION_TABLE_NAME,
            "classification_date",
            data_date,
        ):
            result.convert_dtypes().to_sql(
                name=CLASSIFICATION_TABLE_NAME,
                schema=SCHEMA_NAME,
                con=engine.execution_options(isolation_level="SERIALIZABLE"),
                if_exists="append",
                index=False,
            )
            logger.info("classification data loaded successfully")
        else:
            logger.info("classification data loading skipped due to earlier insertions")

    @task(task_id="test_data_analysis")
    def test_data_analysis() -> None:
        data = pd.read_sql_table(TEST_TABLE_NAME, schema=SCHEMA_NAME, con=engine)

        os.makedirs(DATA_ANALYSIS_CSV_PATH, exist_ok=True)

        # apply frequency analysis
        frequency_analysis(data, data_date)

        # apply numeric distribution analysis
        numeric_distribution(data, data_date)

        # apply correlation analysis
        correlation_analysis(data, data_date)

    @task(task_id="model_monitoring")
    def model_monitoring() -> None:
        data = pd.read_sql(
            f"""select classification_date, classification, total_person_income from {SCHEMA_NAME}.{CLASSIFICATION_TABLE_NAME}
            """,
            con=engine,
        )

        def calculate_scores(table):
            y_true = table["total_person_income"]
            y_pred = table["classification"]

            return pd.Series(
                {
                    "f1_score": f1_score(
                        y_true, y_pred, average="binary", pos_label=" 50000+."
                    ),
                    "accuracy_score": accuracy_score(y_true, y_pred),
                    "recall_score": recall_score(
                        y_true, y_pred, average="binary", pos_label=" 50000+."
                    ),
                    "precision_score": precision_score(
                        y_true, y_pred, average="binary", pos_label=" 50000+."
                    ),
                }
            )

        result = (
            data.groupby(pd.Grouper(key="classification_date"))
            .apply(calculate_scores)
            .sort_index()
        )

        for col in result.columns:
            result[f"{col}_pct_change"] = round(result[col].pct_change() * 100, 2)

        result = result.replace([np.inf, -np.inf], np.nan)

        os.makedirs(MODEL_MONITORING_CSV_PATH, exist_ok=True)

        # save result to csv
        result.to_csv(
            f"{MODEL_MONITORING_CSV_PATH}_model_monitoring_{data_date}.csv", index=True
        )

        # save result as web page
        result.to_html(f"{MODEL_MONITORING_CSV_PATH}_model_monitoring_{data_date}.html")

    chain(
        prepare_schema,
        load_test_data(),
        load_classifications(),
        [test_data_analysis(), model_monitoring()],
    )


us_census_mlops_dag = us_census_mlops()
