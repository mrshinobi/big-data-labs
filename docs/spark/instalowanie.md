
# Instalowanie Apache Spark

### Instalacja Apache Spark w środowisku lokalnym

Pakiet Apache Spark można pobrać ze strony: http://spark.apache.org/downloads.html

Kroki instalacji dla wersji Apache Spark 3.5.5:
1. Ściągnąć paczkę: https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
2. Rozpakować ją przykładowo do katalogu `~/bin/spark-3.5.5-bin-hadoop3` lub `C:\bin\spark-3.5.5-bin-hadoop3`
3. Ustawić zmienną `SPARK_HOME`, by wskazywała na katalog ze Sparkiem.
4. Dodać `$SPARK_HOME/bin` do zmiennej `PATH`, by system mógł znaleźć plik `spark-shell` lub `pyspark`.
5. Ustawić zmienną `PYSPARK_PYTHON`, by wskazywała na zainstalowanego Pythona, np. `python3` lub `python`.
6. Ustawić zmienną `PYTHONPATH` na katalog `$SPARK_HOME/python`

#### Wymagania

##### Python

Apache Spark wymaga zainstalowanego Pythona w wersji 3.8 - 3.11.

##### Java

Apache Spark wymaga zainstalowanej Javy JDK, najlepiej w wersji 17.
Wersję OpenJDK można pobrać z tej strony: https://www.openlogic.com/openjdk-downloads


#### Windows

#### Java
Adres pobrania OpenJDK 17 dla Windows:
https://builds.openlogic.com/downloadJDK/openlogic-openjdk/17.0.15+6/openlogic-openjdk-17.0.15+6-windows-x64.msi

#### WinUtils

W środowisku Windows potrzebne jest dodatkowe narzędzie `winutils.exe`, 
które dostarcza skompilowane biblioteki platformy Apache Hadoop (`hadoop.dll`, `hdfs.dll`).

Dzięki temu Spark będzie mógł korzystać z HDFS, czyli systemu plików Hadoopa, a także z innych funkcji, 
które wymagają bibliotek Hadoopa.

Można je bezpośrednio ściągnąć z tego adresu:
https://github.com/cdarlint/winutils

W repozytorium dostępne są wersje dla różnych wersji Hadoopa, w tym dla wersji 3.3.x, która jest wymagana przez Sparka 3.5.5.

Kroki:
1. ściągnąć paczkę zip repozytorium `winutils` z GitHuba
2. rozpakować ją do katalogu `C:\bin\winutils\`
3. ustawić zmienną środowiskową `HADOOP_HOME`, by wskazywała na ten katalog, czyli `C:\bin\winutils\hadoop-3.3.6`
4. dodać `%HADOOP_HOME%\bin` do zmiennej `PATH`, by system mógł znaleźć plik `winutils.exe`


#### Linux / MacOS

W środowisku systemu Linux/MacOS należy dodać do pliku ~/.bashrc albo ~/.zshrc (zależnie od stosowanego shella):

```shell
export SPARK_HOME=~/bin/spark-3.5.5-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:src:$PYTHONPATH
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
