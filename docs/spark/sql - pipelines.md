# Co to jest potok danych?

Cechy potoku danych:
- zbiór kroków przetwarzania danych ze źródła aż do ujścia
- dowolna liczba kroków / komponentów
- może obejmować wiele systemów

Apache Spark pozwala zamknąć cały potok przetwarzania w jednym skrypcie, będącym aplikacją Spark uruchamianą w klastrze.

Możliwe źrodła / wejścia:
- CSV
- JSON
- usługi sieciowe
- bazy danych

Transformacje:
- `.withColumn()`
- `.filter()`
- `.drop()`

Możliwe ujścia / wyjścia:
- CSV
- Parquet
- baza danych


### Przykład


```python
schema = StructType([
    StructField('name', StringType(), False),
    StructField('age', StringType(), False)
])

df = spark.read.format('csv').load('datafile').schema(schema)
df = df.withColumn('id', monotonically_increasing_id())
...
df.write.parquet('outdata.parquet')
df.write.json('outdata.json')
```



## Techniki obsługi danych

Co chcemy sparsować?
- niepoprawne dane
  - puste wiersze
  - zakomentowane linie
  - nagłówki
- zagnieżdżone struktury
  - wiele stylów ograniczania pól
- nieregularne dane
  - zmienna liczba kolumn w wierszu
- skupienie na danych CSV

```
width, height, image

# This is a comment
```

```
200     300      affenpinscher;0
```

```
600     450     Collie;307 Collie;101
600     449     Japanese_spaniel;23
```




## Walidacja danych

Obejmuje:
- weryfikowanie, czy zbiór danych spełnia wymogi oczekiwanego formatu
- sprawdzanie wierszy i kolumn
- sprawdzanie typów danych
- czasami złożone reguły

Walidacja poprzez złączenia:
- porównuje dane ze znanymi wartościami
- łatwe do znalezienia dane w zadanym zbiorze
- porównywalnie szybkie


### Walidacja z użyciem funkcji `join`
Przykład *automatycznie* usuwujący wszystkie wiersze z wartością `company` nie znajdującą się w `valid_df` czyli `company_df`.

```python
parsed_df = spark.read.parquet("parsed_data.parquet")
company_df = spark.read.parquet("copanies.parquet")
verified_df = parsed_df.join(company_df, parsed_df.company == company_df.company)
```

### Walidacja regułowa

Walidacja logiczna:
- wyliczenia
- weryfikowanie wobec zewnętrznego źródła danych
- można użyć UDF


#### Przykład - funkcja UDF

Funkcja wyliczająca średnią sprzedaży:
```python
def get_avg_sale(sales_list):
    total = 0
    count = 0
    for sale in sales_list:
        total += sale[2] + sale[3]
        count += 2
    return total / count
```

Użycie:
```python
udf_get_avg_sale = udf(get_avg_sale, DoubleType())

df = df.withColumn('avg_sale', udf_get_avg_sale(df.sales_list))
```

#### Przykład - wyliczenia inline

```python
df = df.read.csv("datafile")

df = df.withColumn("avg", (df.total_sales / df.sales_count))

df = df.withColumn("sq_ft", df.width * df.length)

df = df.withColumn("total_avg_size", udf_compute_total(df.entries) / df.num_entries)
```
