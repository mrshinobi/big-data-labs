
# Instalowanie Apache Spark

### Instalacja Apache Spark w środowisku lokalnym

Pakiet Apache Spark można pobrać ze strony: http://spark.apache.org/downloads.html

Kroki instalacji dla wersji Apache Spark 3.5.5:
1. Ściągnąć paczkę: https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
2. Rozpakować ją przykładowo do katalogu `~/bin/spark-3.5.5-bin-hadoop3` lub `C:/bin/spark-3.5.5-bin-hadoop3`
3. Ustawić zmienną `SPARK_HOME`, by wskazywała na katalog ze Sparkiem.


#### Windows

W środowisku Windows potrzebne jest dodatkowe narzędzie `winutils.exe`, 
które dostarcza skompilowane biblioteki platformy Apache Hadoop (hadoop.dll).

Dzięki temu Spark będzie mógł korzystać z HDFS, czyli systemu plików Hadoopa, a także z innych funkcji, które wymagają bibliotek Hadoopa.
To oznacza, że Spark nie będzie działał poprawnie bez tego narzędzia.

Można je bezpośrednio ściągnąć z tego adresu:
https://github.com/cdarlint/winutils

Program `winutils.exe` należy następnie skopiować do katalogu `bin`, 
który znajduje się w rozpakowanej paczce ze Sparkiem, czyli `%SPARK_HOME%\bin`.

Ostatnim krokiem jest ustawienie zmiennej środowiskowej `HADOOP_HOME`, by wskazywała na ten sam katalog co `SPARK_HOME`.


#### Linux / MacOS

W środowisku systemu Linux/MacOS należy dodać do pliku ~/.bashrc albo ~/.zshrc (zależnie od stosowanego shella):

```shell
export SPARK_HOME=~/bin/spark-3.5.5-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

### Uruchamianie Sparka w środowisku Jupyter Notebook

Apache Spark bardzo dobrze integruje się ze środowiskiem Jupyter Notebook.

#### Konfiguracja zmiennych środowiskowych

Konfiguracja i uruchomienie PySparka w systemie Linux/MacOS:

```shell
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

$SPARK_HOME/bin/pyspark
```

lub pod Windows:

```windows batch
set PYSPARK_DRIVER_PYTHON="jupyter"
set PYSPARK_DRIVER_PYTHON_OPTS="notebook"

%SPARK_HOME%\bin\pyspark
```

Od tej chwili w notatniku w Kernelu Python3 będzie dostępna zmienna `spark`, reprezentująca obiekt SparkSession.

Użycie w notatniku:
```python
df = spark.createDataFrame(...)
```

#### Pakiet findspark


Możliwe jest również użycie pakietu `findspark`, który ustawi referencje do zainstalowanego w systemie pysparka.
W tym przypadku nie potrzeba nic ustawiać, jedynie wskazać na katalog domowy Sparka, czyli zazwyczaj ten, 
do którego go rozpakowaliśmy na swoim domowym komputerze.

Instalacja:
```bash shell
pip install findspark
```

Użycie w notatniku:

```python
import findspark
findspark.init()  # gdy zmienna SPARK_HOME jest ustawiona w systemie

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BigDataLabs").getOrCreate()
```
