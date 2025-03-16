from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def convert_celsius_to_fahrenheit(celsius: float) -> float:
    return (celsius * 9.0 / 5.0) + 32


def create_df_with_fahreheit(celsius_df: DataFrame) -> DataFrame:
    fahrenheit_udf = udf(convert_celsius_to_fahrenheit, DoubleType())
    df_with_fahrenheit = celsius_df.withColumn("Fahrenheit", fahrenheit_udf(celsius_df["Celsius"]))
    return df_with_fahrenheit


def create_data_frame(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([(0,), (30,), (100,)], ["Celsius"])


def main():
    with SparkSession.builder.appName("UDFExample").getOrCreate() as spark:
        df = create_data_frame(spark)

        df_with_fahrenheit = create_df_with_fahreheit(df)
        df_with_fahrenheit.show()


if __name__ == "__main__":
    main()
