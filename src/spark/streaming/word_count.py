from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

def main():
    spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    # Split the lines into words
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )

    # Count the words
    wordCounts = words.groupBy("word").count()

    # Output the counts to the console
    query = (
        wordCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
