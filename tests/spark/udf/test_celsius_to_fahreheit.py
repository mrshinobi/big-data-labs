"""


+-------+----------+
|Celsius|Fahrenheit|
+-------+----------+
|      0|      32.0|
|     30|      86.0|
|    100|     212.0|
+-------+----------+


"""

import pytest
from pyspark.sql import SparkSession

from spark.udf.celsius_to_fahrenheit import convert_celsius_to_fahrenheit, create_data_frame


def test_celsius_to_fahrenheit_function():
    assert convert_celsius_to_fahrenheit(0) == 32.0
    assert convert_celsius_to_fahrenheit(30) == 86.0
    assert convert_celsius_to_fahrenheit(100) == 212.0


@pytest.mark.parametrize(
    "celsius, expected",
    [
        (0, 32.0),
        (30, 86.0),
        (100, 212.0),
    ],
)
def test_celsius_to_fahrenheit_function2(celsius: float, expected: float):
    assert convert_celsius_to_fahrenheit(celsius) == expected


def test_create_data_frame(spark: SparkSession):
    df = create_data_frame(spark)
    assert df.count() == 3
    assert df.columns == ["Celsius"]
    assert df.dtypes == [("Celsius", "bigint")]
    assert df.collect() == [(0,), (30,), (100,)]
