from datetime import datetime
from functools import reduce
from typing import List

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window


def load_csvs(spark: SparkSession, paths: List[str], schema: StructType) -> DataFrame:
    """
    Loads list of paths to CSV files and union into a single dataframe, applying schema on read
    """
    return reduce(lambda df1, df2: df1.union(df2), [spark.read.csv(p, header=True, schema=schema) for p in paths])


def filter_date(df: DataFrame, date: datetime) -> DataFrame:
    """
    Filter timestamp column on given date
    """
    return df.where(F.to_date("timestamp") == F.to_date(F.lit(date)))


def clean(df: DataFrame) -> DataFrame:
    """
    Replace outliers with null values. Outliers defined invalid values, i.e. below zero for wind speed & power output,
    not in range 0, 360 for wind direction. Also defined as values above 99th percentile for wind speed and power output
    """
    p99 = df.approxQuantile(["wind_speed", "power_output"], [0.99], 0.001)
    return (
        df.withColumn(
            "wind_speed",
            F.when((F.col("wind_speed") < 0) | (F.col("wind_speed") > p99[0][0]), None).otherwise(F.col("wind_speed")),
        )
        .withColumn(
            "wind_direction", F.when(~F.col("wind_direction").between(0, 360), None).otherwise(F.col("wind_direction"))
        )
        .withColumn(
            "power_output",
            F.when((F.col("power_output") < 0) | (F.col("power_output") > p99[1][0]), None).otherwise(
                F.col("power_output")
            ),
        )
    )


def impute(df: DataFrame) -> DataFrame:
    """
    Impute missing values, including nulls replacing outliers. Mean of prior and next values is used to impute for wind
    speed and power output. This is not appropriate for wind_direction as mean could break down over [0, 360] boundary,
    so we use max value instead
    """
    window = Window.partitionBy("turbine_id").orderBy("timestamp").rowsBetween(-1, 1)
    return (
        df.withColumn("wind_speed", F.coalesce(F.col("wind_speed"), F.mean("wind_speed").over(window)))
        .withColumn("wind_direction", F.coalesce(F.col("wind_direction"), F.max("wind_direction").over(window)))
        .withColumn("power_output", F.coalesce(F.col("power_output"), F.mean("power_output").over(window)))
    )


def compute_summary_statistics(df: DataFrame) -> DataFrame:
    """
    Compute summary statistcs for each turbine for each day, and add an anomaly flag
    """
    grouped = df.groupBy("turbine_id", F.date_trunc("day", "timestamp").alias("date")).agg(
        F.mean("power_output").alias("power_output_mean"),
        F.max("power_output").alias("power_output_max"),
        F.min("power_output").alias("power_output_min"),
    )

    mean_stddev = grouped.select(
        F.mean("power_output_mean").alias("mean"), F.stddev("power_output_mean").alias("stddev")
    ).collect()
    mean = mean_stddev[0]["mean"]
    stddev = mean_stddev[0]["stddev"]

    return grouped.withColumn("anomaly", ~F.col("power_output_mean").between(mean - 2 * stddev, mean + 2 * stddev))


def append_to_table(spark: SparkSession, table_name: str, values: DataFrame) -> None:
    """
    Append data to specified delta tables
    """
    table = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(values.schema).execute()
    table.merge(source=values, condition=F.lit(False)).whenNotMatchedInsertAll().execute()
