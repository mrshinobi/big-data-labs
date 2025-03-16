from pyspark.sql import SparkSession

# text_file = "s3://uwm-bigdata/data/books/ulysses.txt"
text_file = "data/books/ulysses.txt"

with SparkSession.builder.appName("SimpleApp").getOrCreate() as spark:
    text_df = spark.read.text(text_file).cache()

    count_a = text_df.filter(text_df.value.contains("a")).count()
    count_b = text_df.filter(text_df.value.contains("b")).count()

    print(f"Lines with a: {count_a}, lines with b: {count_b}")

    # text_df.write.mode("overwrite").parquet("s3://uwm-bigdata/users/shinobi/ulysses.parquet")
    text_df.write.mode("overwrite").parquet("data/books/results/ulysses.parquet")
