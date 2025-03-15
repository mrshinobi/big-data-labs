# Apache Spark

Apache Spark to platforma do przetwarzania danych w czasie rzeczywistym, stworzona w celu zastąpienia
Apache Hadoop MapReduce, który jest wolniejszy i mniej wydajny.


Cechy:
- analityczny silnik do rozproszonego przetwarzania danych dużej skali
- rozwijany przez Apache Foundation, UC Berkeley, DataBricks
- prace rozpoczęte w 2009 roku, publicznie dostępna w 2013 roku
- obecna stabilna wersja: 3.1.1
- posiada API w języku Scala, Java, Python oraz R
- wsparcie dla składni języka SQL
- wsparcie dla przetwarzania grafów
- wsparcie dla strumieniowego przetwarzania danych
- wbudowana biblioteka do uczenia maszynowego


## Instalacja Apache Spark

Instrukcja dotycząca instalacji Apache Sparka na środowisku lokalnym znajduje się w pliku [instalowanie.md](instalowanie.md).


## Komponenty platformy Apache Spark

### Spark Core

Spark Core jest podstawą całej platformy Spark.

Funkcje:
- rozsyła rozproszone zadania,
- planuje wykonywanie zadań,
- zarządza podstawowymi funkcjonalnościami I/O,
- wystawia programistyczny interfejs aplikacji API w języku Java, Python, Scala oraz R,
- opiera się na abstrakcji rozproszonych obiektów RDD (Resilient Distributed Datasets - rozproszone zbiory danych).

Interfejs API spełnia cechy modelu programowania funkcyjnego:

1. Program sterownika wywołuje operacje równoległe takie jak map, filter czy reduce na zbiorach RDD
przez przekazanie funkcji do Sparka.
2. Spark następnie planuje wykonanie operacji w trybie równoległym na klastrze obliczeniowym.
3. Operacje typu transformacje tworzą nowe RDD na swoim wyjściu.
4. Obiekty RDD są niemodyfikowalne / niemutowalne (ang. immutable).
5. Operacje są ewaluowane leniwie (ang. lazy evaluation).
6. Odporność na błędy (ang. fault tolerance) jest osiągana przez ponawianie wykonania zadań na RDD.
7. RDD może zawierać obiekty różnych typów danych z dowolnego wspieranego języka.

Spark Core dostarcza również dwa rodzeje współdzielonych zmiennych:
1. Zmienne typu rozgłoszeniowego (ang. broadcast variables)
2. Akumulatory (ang. accumulators).


### Spark SQL

Funkcje:
- Dodaje strukturę danych zw. DataFrame, ułatwiającą manipulowanie danymi i pozwalającą na
tworzenie danych strukturalnych oraz semi-strukturalnych.
- Dostarcza tzw. język do manipulowania danych / DSL (Domain Specific Language) jako wsparcie dla
języków Scala, Java, czy Python.
- Dostarcza wsparcie dla języka SQL.
- Dostarcza dostęp do baz danych poprzez CLI oraz serwer ODBC/JDBC.


### Spark Streaming

Spark Streaming jest modułem Sparka do analityki strumieniowej, wykorzystającym do tego celu
możliwości Spark Core do szybkiego planowania zadań.
Cechy:
- Wczytuje dane w formie mini-wsadów (mini-batches) i wykonuje na nich operacje.
- Architektura modułu pozwala na użycie tego samego kodu do analizy wsadowej jak i strumieniowej,
dzięki czemu łatwo jest zaimplementować architekturę Lambda.
- Mini-batche wprowadzają jednak opóźnienia równe czasie trwania mini-wsadu. Inne systemy
(Storm, Flink, Gearpump) przetwarzają strumień zdarzenie po zdarzeniu.
- Wbudowane wsparcie dla Kafka, Flume, Twitter, ZeroMQ, Kinesis, czy gniazd TCP/IP.
- Nowa wersja (Structured Streaming) posiada interfej kontroli typów danych.

Schemat działania przestawia rysunek:


Środowisko działania przedstawia rysunek:


### Spark MLlib - Machine Learning Library

Spark `MLlib` to rozproszona biblioteka algorytmów uczących się działająca na bazie Spark Core oraz SQL.

Cechy:
- Wiele wbudowanych algorytmów statystycznych oraz uczących się.
- Ułatwia obróbkę danych poprzez łączenie zadań w potoki.
- Ze względu na sposób korzystania z pamięci jest wiele razy szybsza niż oparta na dyskach implementacja Apache Mahout.

Zaimplementowane algorytmy:
- Statystyka sumaryczna, korelacje, generacja danych losowych.
- Klasyfikacja i regresja: rzadkie i gęste wektory oraz macierze, regresja logistyczna, regresja liniowa,
  drzewa decyzyjne, klasyfikacja metodą naiwnego Bayesa.
- Filtrowanie kolaboratywne: ALS (metoda naprzemiennych najmniejszych kwadratów, ang. alternating least squeares).
- Klastrowanie: k-means, alokacja metodą Dirichleta (ang. latent Dirichlet allocation, LDA).
- Techniki redukcji wymiarów: dekompozycja wartości osobliwej (ang. singular value decomposition, SVD)
  oraz analiza głównych składowych (ang. principal component analysis, PCA).
- Ekstrakcja cech oraz funkcje transformujące.
- Algorytmy optymalizacyjne: stochastic gradient descent, limited-memory BFGS (L-BFGS).


### Spark GraphX

Cechy:
- Spark GraphX to rozproszona biblioteka do przetwarzania danych grafowych na bazie Spark Core oraz Spark SQL.
- Służy do analizy danych z użyciem koncepcji grafów skierowanych.
- Zawiera kilka popularnych algorytmów, m.in. PageRank, zliczanie trójkątów.

> W grafach skierowanych do oznaczania obiektów używa się wierzchołków grafów, a jako relacje traktowane
są krawędzie między wierzchołkami.


## Klaster Apache Spark

Spark może być uruchomiony w `trybie klastrowym`, czyli w grupie maszyn współpracujących ze sobą.
Aplikacje Sparka działają wtedy jako niezależne zestawy procesów w klastrze, koordynowane przez obiekt SparkContext
w programie głównym (w sterowniku lub w skrypcie driver'a).

### Zarządca klastra

Istnieje kilka używanych typów zarządców klastra` (`managerów`), które przydzielają zasoby aplikacjom:
- tryb samodzielny (ang. *standalone*) – zarządca dołączany standardowo do Sparka, pozwala łatwo zbudować klaster.
- YARN – podstawowy zarządca zasobów w systemie Hadoop2.
- Mesos – system zarządzania zasobami, potrafiący uruchamiać aplikacje Spark i MapReduce.
- Kubernetes – system open-source do automatyzacji instalacji, skalowania i zarządzania aplikacjami kontenerowymi.

Zarządca klastra kontroluje fizyczne maszyny i przydziela zasoby dla aplikacji Sparka.

Zasobami mogą być:
- pamięć RAM
- ilość procesorów (rdzeni, CPU)
- miejsce dla danych na dyskach

Po połączeniu z managerem klastra, posiadając dane o lokalizacji wolnych zasobów, Spark tworzy wykonawców (ang. executors)
na węzłach w klastrze, a dokłądniej tworzy na nich procesy wykonujące obliczenia i trzymające dane aplikacji.

Następnie Spark rozsyła kod aplikacji do wykonawców, a SparkContext wysyła im zadania do wykonania.

### Monitorowanie klastra

Każdy program sterownika ma swój interfejs graficzny w postaci aplikacji WWW, typowo uruchamianej na porcie `4040`.

Cechy aplikacji monitorującej Sparka:
- wyświetla informacje na temat uruchamianych zadań, wykonawców oraz zużycia magazynu danych
- dostępna w przeglądarce internetowej np. pod adresem lokalnym http://localhost:4040
- również uruchomienie konsoli pyspark lokalnie uruchamia aplikację dedykowaną
- porty przydzielane aplikacji są kolejnymi po 4040, jeśli już jest ona zajęty (czyli 4041, 4042, ...)


## Interpreter PySpark

### Wykonywanie kodu w klastrze

- Kiedy wystartuje interpreter Python w PySparku uruchamia się również maszyna `JVM`, z którą on się komunikuje
  przez `gniazdo sieciowe` (ang. *socket*), czyli wewnętrzny mechanizm system operacyjnego.
- By obsłużyć komunikację PySpark używa do tego projektu `Py4J`.
- JVM funkcjonuje jako właściwy `sterownik` (ang. *driver*).
- JVM ładuje kontekst `JavaSparkContext`, który komunikuje się z wykonawcami (executorami) Sparka w klastrze.
- `Interfejs API Pythona` woła obiekt `SparkContext`, który tłumaczy te wołania na wołania interfejsu Java API modułu JavaSparkContext.
- Na przykład implementacja metody `sc.textFile()` rozsyła wołanie do metody `.textFile()` obiektu JavaSparkContext,
  który ostatecznie komunikuje się z `maszyną JVM wykonawcy`, by ten załadował tekst z `HDFS`.
- Wykonawcy Sparka na klastrze startują swój `własny interpreter` i komunikują się z nim przez `potok`,
  kiedy muszą wykonać `kod użytkownika`.
- Obiekt Python RDD na lokalnym kliencie PySpark koresponduje z obiektem klasy `PythonRDD` w lokalnym JVM.
- Dane związane z RDD żyją właściwie w maszynie SparkJVM jako obiekty Java.
- Na przykład, uruchomienie `sc.textFile()` w interpreterze Pythona będzie wołało metodę `JavaSparkContext.textFile()`,
  która załaduje dane jako obiekty Java String w klastrze.
- Podobnie, ładowanie pliku `Parquet`/`Avro` używając metody `newAPIHadoopFile` będzie ładowało obiekty jako obiekty `Java Avro`.
- Kiedy wołania API są tworzone w obiekcie Python RDD, każdy związany kod (np. funkcja lambda Pythona) jest serializowany
  jako tzw. moduł `cloudpickle` (format `PiCloud`) i jest rozsyłany do wykonawców.
- Dane są konwertowane z obiektów Java na reprezentację w języku Python i strumieniowane do skojarzonego z wykonawcą
  interpretera Pythona poprzez utworzone wcześniej gniazdo.

### Powłoka pyspark

Konsola czy też powłoka Sparka pozwala analizować dane interaktywnie.

Istnieją dwie wersje powłoki:
1. domyślna w języku Scala -> uruchamiana poprzez `./bin/spark-shell`
2. dla języka Python -> `./bin/pyspark`
3. SQL - `./bin/spark-sql`

Wersja dla języka Java nie posiada własnej konsoli.


## Linki

### Linki bezpośrednio związane z projektem

1. Dokumentacja do najnowszej wersji
    - https://spark.apache.org/docs/latest/
2. Podręczniki programowania
    - [Wprowadzenie](https://spark.apache.org/docs/latest/quick-start.html)
    - [Obiekty RDD, Akumulatory, Rozgłaszanie ](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
    - [Programowanie w SQL, Ramki danych](https://spark.apache.org/docs/latest/sql-programming-guide.html)
    - [Strumieniowanie Strukturalne](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
    - [Uczenie Maszynowe](https://spark.apache.org/docs/latest/ml-guide.html)
    - [GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html)
    - [PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/index.html) oraz
    - [PySpark User Guide](https://spark.apache.org/docs/latest/api/python/user_guide/index.html)
    - [SparkR](https://spark.apache.org/docs/latest/sparkr.html)
3. Specyfikacja API
    - [PySpark API](https://spark.apache.org/docs/latest/api/python/index.html)
    - [Referencja](https://spark.apache.org/docs/latest/api/python/reference/index.html)
    - [Wbudowane funkcje SQL](https://spark.apache.org/docs/latest/api/sql/index.html)

4. Przykłady
    - [Przykłady na GitHub](https://github.com/apache/spark/tree/v3.1.1-rc3/examples/src/main/python)

5. PySpark
    - [Strona projektu PySpark na pipi.org](https://pypi.org/project/pyspark/)
    - [Projekt Koalas]()




