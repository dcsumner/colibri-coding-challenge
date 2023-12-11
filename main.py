from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import types as T

import utils


def main():
    # In a production setting these variables would be parametrised
    date = datetime(year=2022, month=3, day=1)
    paths = ["./Copy of data/data_group_1.csv", "./Copy of data/data_group_2.csv", "./Copy of data/data_group_3.csv"]
    spark = SparkSession.builder.getOrCreate()

    schema = T.StructType(
        [
            T.StructField("timestamp", T.TimestampType(), False),
            T.StructField("turbine_id", T.IntegerType(), False),
            T.StructField("wind_speed", T.DoubleType(), False),
            T.StructField("wind_direction", T.DoubleType(), False),
            T.StructField("power_output", T.DoubleType(), False),
        ]
    )

    cleaned = (
        utils.load_csvs(spark, paths, schema)
        .transform(utils.filter_date, date=date)
        .transform(utils.clean)
        .transform(utils.impute)
    )

    summary_statistics = cleaned.transform(utils.compute_summary_statistics)

    utils.append_to_table(spark, "turbines_cleaned", cleaned)
    utils.append_to_table(spark, "turbine_summary_statistics", summary_statistics)


if __name__ == "__main__":
    main()
