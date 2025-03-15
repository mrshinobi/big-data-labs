# Spark SQL

SparkSQL to moduł Sparka do przetwarzania danych strukturalnych. Interfejsy pakietu SQL operuje już strukturami danych o pewnych schematach.

Istnieje kilka sposobów pracy ze SparkSQL:
- czysty język SQL
- interfejs API DataFrame
- interfejs API DataSets

## SQL
- SparkSQL pozwala wywoływać zapytania SQL o podstawowej składni bądź składni HiveQL.
- Może podłączać się do bazy Hive.
- Dane zwracane są jako DataFrame.
- Istnieje powłoka spark-sql (komunikacja JDBC/ODBC).

## Ramki danych DataFrames
Podstawowym typem danych w SparkSQL jest DataFrame, który jest rozproszoną kolekcją danych z nazwanymi kolumnami.
Jest jakby tabelą w relacyjnej bazie danych lub ramką danych w R/Python.

Sposoby tworzenia DataFrame:
- Structured data files
- Tables in Hive
- External databases
- RDD

Programowe tworzenie schemata danych

Trzy kroki do stworzenia DF:
1. Stwórz krotkę lub listę RDD z oryginalnego RDD.
2. Stwórz schemat reprezentowany przez obiekt StructType pasujący do struktury krotek (list) z punktu 1.
3. Zastosuj schemat do RDD przez metodę createDataFrame sesji Sparka.


## Temporarily Views

- Spark pozwala tworzyć tymczasowe widoki na bazie danych w DataFrame, do których następnie można odwoływać się w zapytaniu SQL.
- Widoki tymczasowe są definiowane na czas trwania sesji I znikną, jeśli zakończy się tworząca je sesja.
- Jest możliwość stworzenia globalnego widoku, który będzie istniał aż do zakończenia całej aplikacji Sparka.
  Tworzy się on w ramach bazy danych **global_temp**.

## Data Sources

### Czytanie i zapisywanie danych

Domyślnym formatem źródła danych jest parquet, co można zmienić ustawieniem `spark.sql.sources.default`.

Przykład 1 - Odczyt i zapis jako Parquet:

```python
df = spark.read.load("examples/src/main/resources/users.parquet")

df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
```

Przykład 2 - JSON jako źródło, zapis jako Parquet

```python
df = spark.read.load("examples/src/main/resources/people.json", format="json")
df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
```

Przykład 3 - CSV

Można podać więcej parametrów:
```
df = spark.read.load("examples/src/main/resources/people.csv", format="csv", sep=":", inferSchema="true", header="true")
```

### Uruchamianie SQL bezpośrednio
Zamiast wczytywać pliku można również bezpośrednio go użyć tworząc odpowiednie zapytanie SQL:
```python
df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
```

## Optymalizacja wykonania przekształceń SQL

Dwa rodzaje trybów optymalizacji:
- Optymalizator regułowy: optymalizator używa zestawu reguł do wykonania zapytania.
- Optymalizator kosztowy: optymalizator szacuje koszty wykonania zapytania i tworzy wiele planów wykonania dla różnych zestawów reguł.


Fazy optymalizacji:
- Wstępna analiza zapytania.
- Drzewo AST (abstract syntax tree) uzyskane od parsera SQL na bazie zapytania.
- Obiekt DataFrame.
- Brakujące / nieznane typy atrybutów, relacji.
- Logiczna optymalizacja
- Zwijanie stałych (constant folding).
- Spychanie predykatów (predicate pushdown).
- Rzutowanie typów.
- Fizyczne planowanie.
- Tworzenie kilku fizycznych planów wykonania.
- Wybór planu wg modelu kosztowego.
- Generacja kodu wykonania planu.
- Java bytecode.

## Funkcje statystyczne i matematyczne

W pakiecie `pyspark.sql.functions` dostępnych jest kilka ciekawych i ważnych funkcji statystyczne.

### Generowanie danych losowych

Generacja danych losowych przydaje się do testów istniejących już algorytmów.
W pakiecie `pyspark.sql.functions` istnieją funkcje do generowania kolumn, które mogą zawierać przykładowo wartości
z rozkładu normalnego (funkcja randn) lub ciągłego (funkcja rand).


### Statystyki sumaryczne

Funkcja `df.describe()` pozwala poznać szereg wartości statystycznych dla zbioru danych.
Zwraca obiekt DataFrame zawierający następujące informacje dla każdej z kolumn:
- liczba nie-pustych (nie-nullowych) wpisów w zbiorze
- wartość średnia (mean)
- standardowe odchylenie (standard deviation)
- wartości min i max dla każdej kolumny liczbowej

Jeśli mamy DataFrame z dużą liczbą kolumn, można wyświetlić tylko ich podzbiór za pomocą metody `select()`.

### Konwersja na bibliotekę Pandas

Łatwo uzyskać konwersję z DataFrame na obiekty Pandas. Wystarczy wywołać metodę `df.toPandas()`.

**Co to daje?**
Łatwo odwrócić wtedy tabelę – `transpose()`.

Można utworzyć DataFrame z obiektów Pandas DataFrame używając metody `createDataFrame(pandas_df)`.
```python
import numpy as np
import pandas as pd

# Generate a Pandas DataFrame
pdf = pd.DataFrame(np.random.rand(100, 3))
# Create a Spark DataFrame from a Pandas DataFrame using Arrow
df = spark.createDataFrame(pdf)
# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
result_pdf = df.select("*").toPandas()
```
