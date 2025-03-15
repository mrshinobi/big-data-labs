# Apache Spark

## Zastosowanie Sparka

## Czyszczenie danych z użyciem Sparka

Problemy z typowymi systemami danych:

* wydajność
* organizacja przepływu danych

Przewaga Sparka:

* Skalowalność
* Mocny silnik do obsługi przetwarzania danych


## Czyszczenie danych

Czyszczenie danych - przygotowanie surowych danych do użycia w przewodach przetwarzania danych.

Obejmuje:

* Przeformatowanie lub zamianę tekstu
* Wykonanie obliczeń
* Usunięcie śmieciowych lub niekompletnych danych


### Przykład

Surowe dane

|imię|wiek (L)|miasto|
|----------|:---:|------:|
Kowalski, Jan|37|Olsztyn |
Adamowicz, M.|25|Kraków  |
null|215

Oczyszczone dane

|nazwisko|imię |wiek (M)|miasto|
|--------|-----|:------:|:------:|
Kowalski |Jan|444|OL|
Adamowicz|M.|708|KR|


## Schematy danych

Schematy w Sparku:

1. Definiują format ramki danych.
2. Mogą zawierać różne typy danych
    - stringi
    - daty
    - liczby całkowite
    - tablice
3. Mogą odfiltrować śmieciowe dane podczas importu.
4. Zwiększają wydajność odczytu danych.

### Przykład schematu danych

Importowanie schematu:
```python
import pyspark.sql.types

people_schema = StructType([
    StructField('imie', StringType(), True),
    StructField('wiek', IntegerType(), True),
    StructField('miasto', StringType(), True)
])
```

Czytanie pliku CSV zawierającego dane:
```python
people_df = spark.read.format('csv').load(name='rawdata.csv', schema=people_schema)
```




## Niemutowalność i leniwe przetwarzanie

Zmienne w języku Python:
* mutowalne
* elastyczne
* możliwe problemy ze współbieżnością
* mogą dodać nadmiarową złożoność



###  Niemutowalność

Zmienne niemutowalne są:
* komponentami programowania funkcyjnego
* zdefiniowane jeden raz
* nie są bezpośrednio modyfikowalne
* odtwarzane przy ponownym przypisaniu
* wydajnie współdzielone


### Przykład niemutowalności

```python
voter_df = spark.read.csv('voterdata.csv')
```

Dokonanie zmian:
```python
voter_df = voter_df.withColumn("fullyear", voter_df.year + 2000)
voter_df = voter_df.drop()
```

### Leniwe przetwarzanie

* Czy to nie jest wolne?
* Transformacje
* Akcje
* Pozwala na wydajne planowanie

```python
voter_df = voter_df.withColumn("fullyear", voter_df.year + 2000)
voter_df = voter_df.drop()

voter_df.count()
```



## Format Parquet

Strona domowa projektu Apache Parquet: https://parquet.apache.org

Cechy:
* kolumnowy format danych
* wspierany przez Sparka i inne frameworki przetwarzania danych
* wspiera `predicate pushdown`
* automatycznie przechowuje informację o schemacie danych

Ogólne trudności z plikami CSV:
* brak zdefiniowanego schematu
* zagnieżdżone dane wymagają specjalnego traktowania
* ograniczony format kodowania

Minusy obsługi plików CSV w Sparku:
* dość powolne parsowanie
* pliki nie mogą być filtrowane (brak `predicate pushdown`)
* nadmiarowość w ilości przetwarzanych danych (problemy formatów wierszowych)
* każde użycie pośrednie wymaga redefiniowania schematu


### Praca z Parquet

Czytanie plików Parquet:
```python
df = spark.read.format('parquet').load('filename.parquet')
```
```python
df = spark.read.parquet('filename.parquet')
```

Zapisywanie plików Parquet:
```python
df.write.format('parquet').save('filename.parquet')
```
```python
df.write.parquet('filename.parquet')
```

### Parquet i SQL

Parquet jako magazyn danych dla operacji Spark SQL:
```python
flight_df = spark.read.parquet('flights.parquet')

flight_df.createOrReplaceTempView('flights')

short_flights_df = spark.sql('SELECT * FROM flights WHERE flightduration < 100')
```

