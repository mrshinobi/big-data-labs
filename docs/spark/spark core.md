# Spark Core

## RDD (Resilient Distributed Datasets)

Rozproszone obiekty RDD są reprezentacją rozproszonego zbioru danych.

Dwa typy operacji na obiektach RDD:
1. Transformacje
2. Akcje


Problemy z RDD:
- brak schematu danych
    - brak sprawdzania poprawności typów danych
- zwykłe obiekty Java
    - kosztowna serializacja
    - kosztowne odśmiecacanie pamięci
- brak złożonych optymalizacji przekształceń
    - nie można zastosować optymalizatora Catalyst
    - nie można użyć silnika wykonywania Tungsten


## Transformacje
Transformacja obiektu RDD daje w wyniku inny obiekt RDD i tworzy jego historię przekształceń.

Przykłady:
- map
- filter
- sample
- mapValues
- union

Dwie kategorie transformacji:
- Wąskie transformacje, operujące na danych z pojedynczej partycji RDD.
- Szerokie transformacja, wymagające przenoszenia dnych między partycjami przed wykonaniem obliczeń w klastrze.

## Akcje
Akcje zwracają konkretną wartość na podstawie wykonanych obliczeń na obiektach RDD.

Przykłady:
- reduce
- count
- first
- foreach

Wywołanie akcji uruchamia wykonanie pracy na klastrze Sparka.

## Cache
Spark dostarcza mechanizm cache’owania zbioru danych w pamięci. Jest to funkcja cache() dostępna dla obiektu RDD.
Cache przydaje się, gdy wykonywana na Sparku praca wymaga kilku akcji wykonujących transformacje na konkretnym zestawie danych.


## Leniwa ewaluacja

Przekształcenia zbiorów RDD takie jak map, filter, czy nawet textFile są ewaluowane w sposób leniwy, co oznacza,
że Spark nie zacznie ich wykonywania, dopóki nie zobaczy końcowej akcji.

Poprzez leniwą ewaluację oraz grupowanie działań Spark optymalizuje liczbę operacji na danych.


## DAG (Directed Acyclic Graph)

W Sparku przetwarzanie zadań modelowane jest z użyciem koncepcji grafu DAG.

> DAG to skierowany graf acykliczny, czyli bez cykli (pętli) między węzłami grafu.
> Tworzony jest jako kolekcja wierzchołków oraz krawędzi skierowanych, gdzie każda krawędź łączy jeden wierzchołek z innym.

Graf DAG budowany jest dla łańcucha transformacji i akcji.

### Przykład:


### Planista

Budową grafu DAG zajmuje się moduł Sparka `DAGScheduler` (`planista DAG`), który dzieli go również na etapy.

Zadania planisty `DAGScheduler`:
1. Tworzy graf DAG transformacji i akcji operacji RDD.
2. Optymalizuje graf, by zwiększyć wydajność wykonania pracy (przykładowo map, a później filter zostaną zamienione miejscami w kolejności uruchomienia).
3. Śledzi pochodzenie obiektów RDD.
4. Określa preferowane miejsce uruchomienia, biorąc pod uwagę status cache.
5. Uruchamia transformacje.
6. Wysyła zbiory zadań (traktowane jako etapy) do niskopoziomowego Planisty Zadań (Task Scheduler).
7. Obsługuje błędne sytuacje.

Zadania planisty `TaskScheduler`:
1. wysyłanie zadań do klastra,
2. uruchamianie zadań,
3. obsługa błędów (w tym ponawianie zadań).
4. wysyłanie komunikatów do nadrzędnego planisty.

Liczbę etapów można wyliczyć następująco:

```
liczba etapów = liczba szerokich transformacji + 1
```

W ogólności etapy są wykonywane sekwencyjnie oprócz etapu `join`, dla którego dwa etapy poprzedzające złączenie mogą być uruchamiane równolegle.

Zadania (taski) zgrupowane w etapie uruchamiane są równolegle.
Liczba tasków zależy od liczby partycji RDD.

Powyższy schemat przetwarzania w Sparku można więc zobrazować w ten sposób:

W aplikacji WWW sterownika Sparka można podejrzeć szczęgółowy obraz zbudowanego grafu:

## Obsługa różnych formatów plików

Spark potrafi obsługiwać wiele typów plików, a że powstał z myślą o zastosowaniu m.in. w systemie Hadoop to potrafi
obsługiwać również dane w formatach tej platformy.

Przykładowe formaty plików obsługiwane przez Sparka:
- Hadoop SequenceFile
- CSV
- JSON
- Avro
- ORC
- Parquet
- XML
- text

> Parquet to kolumnowy format przechowywania danych. Zawiera w sobie schemat danych oraz pozwala go wersjonować.
> Ma wsparcie dla struktur zagnieżdżonych.

> Avro to wierszowy format przechowywania danych popularny w świecie Big Data. Zawiera wsobie schemat danych.

> ORC to kolumnowy format przechowywania danych.


Tabela poniżej dla tych samych danych pliku pokazuje rozmary pliku w różnym formacie:

|format|rozmiar (MB)|
|---|---|
|csv|365|
|json|632|
|avro|157|
|parquet|49.4|
|orv|45|
|xml|1100|

Dużą efektywność przechowywania danych mają pliki ORC oraz Parquet.



## Wysłanie aplikacji PySpark do wykonania

Aplikacja PySpark to aplikacja wykorzystująca Python API programistycznie.

Przykład:
```python
from pyspark.sql import SparkSession

logFile = "YOUR_SPARK_HOME/README.md" # plik w systemie lokalnym

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
```

Program liczy w tekście liczbę linii zawierających literę `a` oraz `b`. Do stworzenia kolekcji został użyty obiekt SparkSession.

Do jego wywołania służy skrypt `spark-submit`.

Przykład wywołania:
```shell script
> YOUR_SPARK_HOME/bin/spark-submit --master local[4] --py-files my_python_lib.zip my_application.py
...
Lines with a: 46, Lines with b: 23
```

Jeżeli PySpark jest zainstalowany jako biblioteka środowiska Python (przez pip install pyspark)
to można uruchomić standardowy interpreter języka Python:
```shell script
> python SimpleApp.py
...
Lines with a: 46, Lines with b: 23
```
