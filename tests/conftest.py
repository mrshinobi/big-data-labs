import logging

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.config("spark.driver.memory", "1g").appName("UnitTest").enableHiveSupport().getOrCreate()
    )
    # turn down spark logging for the context
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)
    return session
