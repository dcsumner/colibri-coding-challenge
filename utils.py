from datetime import datetime
from functools import reduce
from typing import List

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window


def load_csvs(spark: SparkSession, paths: List[str], schema: StructType) -> DataFrame:
    return reduce(lambda df1, df2: df1.union(df2), [spark.read.csv(p, header=True, schema=schema) for p in paths])


def filter_date(df: DataFrame, date: datetime) -> DataFrame:
    return df.where(F.to_date("timestamp") == F.to_date(F.lit(date)))


def clean(df: DataFrame) -> DataFrame:
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
    window = Window.partitionBy("turbine_id").orderBy("timestamp").rowsBetween(-1, 1)
    return (
        df.withColumn("wind_speed", F.coalesce(F.col("wind_speed"), F.mean("wind_speed").over(window)))
        .withColumn("wind_direction", F.coalesce(F.col("wind_direction"), F.max("wind_direction").over(window)))
        .withColumn("power_output", F.coalesce(F.col("power_output"), F.mean("power_output").over(window)))
    )


def compute_summary_statistics(df: DataFrame) -> DataFrame:
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
    table = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(values.schema).execute()
    table.merge(source=values, condition=F.lit(False)).whenNotMatchedInsertAll().execute()
