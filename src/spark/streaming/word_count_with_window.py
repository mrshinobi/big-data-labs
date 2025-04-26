from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, current_timestamp


def main():
    spark = SparkSession.builder.appName("StructuredNetworkWordCountWithWindow").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    # transformacja: lines -> words + timestamp
    words = lines.select(
        explode(split(lines.value, " ")).alias("word"),
        current_timestamp().alias("timestamp")
    )

    windowedCounts = (
        words
        .groupBy(
            window(
                words.timestamp,
                "2 seconds",
                "1 seconds"
            ),
            words.word
        )
        .count()
    )

    query = (
        windowedCounts.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
