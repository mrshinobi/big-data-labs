from pyspark import SparkContext

sc = SparkContext.getOrCreate()

accumulator = sc.accumulator(0)


def count_words(line: str) -> list[str]:
    global accumulator
    words = line.split()
    for word in words:
        if word.lower() == "ulysses":
            accumulator += 1
    return words


rdd = sc.textFile("../../data/books/ulysses.txt")
rdd.flatMap(count_words).collect()

print(f"Count of 'ulysses': {accumulator.value}")
