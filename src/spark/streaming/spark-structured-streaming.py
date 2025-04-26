from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def main():
    spark = SparkSession.builder.appName("StructuredStreamingFileWriter").getOrCreate()

    lines = (
        spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )

    enriched_lines = lines.withColumn("received_time", current_timestamp())

    query = (
        enriched_lines.writeStream.outputMode("append")
        .format("csv")
        .option("path", "output-data")
        .option("checkpointLocation", "checkpoint")
        .trigger(processingTime="1 second")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
