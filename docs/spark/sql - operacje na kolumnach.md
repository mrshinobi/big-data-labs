# Praca z kolumnami w Sparku


## DataFrame

Ramki danych:
- stworzone z wierszy i kolumn
- niemutowalne
- transformacje zmieniają dane

```python
# zwraca wiersze gdzie name zaczyna się od "M"
voter_df.filter(voter_df.name.like('M%'))

# zwraca tylko name i position
voters = voter_df.select('name', 'position')
```

### Typowe transformacje na ramkach danych

* filtrowanie: `filter` / `where`
```python
voter_df.filter(voter_df.date > '1/1/2019')
```
lub zamiennie
```python
voter df.where(voter_df.date > '1/1/2019')
```

* wybieranie: `select`
```python
voter_df.select(voter_df.name)
```

* dodanie nowej kolumny
```python
voter_df.withColumn('year', voter_df.date.year)
```

* usunięcie kolumny
```python
voter_df.drop('unused_column')
```

## Filtrowanie danych

Obejmuje:
* Usuwanie wartości `null`
* Usuwanie niepoprawnych wpisów
* Rozdzielanie danych ze złączonych źródeł danych
* Negacja za pomocą `~`

```python
voter_df.filter(voter_df['name'].isNotNull())
voter_df.filter(voter_df.date.year > 1800)
voter_df.where(voter_df['_c0'].contains('VOTE'))
voter_df.where(~ voter_df._c1.isNull())
```

### Transformacje na kolumnach typu string

Zawarte w pakiecie funkcji **pyspark.sql.functions**:
```python
import pyspark.sql.functions as F
```
Stosowane do każdej z kolumn jako transformacja:
```python
voter_df.withColumn('upper', F.upper('name'))
```
Mogą tworzyć pośrednie kolumny:
```python
voter_df.withColumn('splits', F.split('name', ' '))
```
Mogą być rzucane na inne typy danych:
```python
voter_df.withColumn('year', voter_df['_c4'].cast(IntegerType()))
```

### Transformacje na kolumnach typu ArrayType

Różne funkcje / transformacje narzędziowe do pracy z **ArrayType**:

`.size(<column>)` - zwraca liczbę kolumn

`.getItem(<index>)` - zwraca z listy kolumn konkretny obiekt o indeksie `index`



### Operaje warunkowe na kolumnach

Klauzule warunkowe obejmują:
* wersję inline wyrażenia `if` / `then` / `else`
* `.when()`
* `.otherwise()`


### Przykłady operacji warunkowych

```python
.when(<if condition>, <then x>)
```
```python
df.select(df.imie, df.wiek, F.when(df.wiek >= 18, "adult"))
```

|imię|wiek||
|---|---|---|
|Alicja|14
|Borys|18|adult|
|Celina|38|adult|


Wielokrotne użycie transformacji `.when()`:
```python
df.select(df.name, df.wiek,
          .when(df.wiek >= 18, "adult")
          .when(df.wiek < 18, "minor"))
```

|imię|wiek||
|---|---|---|
|Alicja|14|minor|
|Borys|18|adult|
|Celina|38|adult|


### Otherwise

`.otherwise()` jest podobne do `else`:
```python
df.select(df.imie, df.wiek,
          .when(df.wiek >= 18, "adult")
          .otherwise("minor"))
```


## User defined functions

User Defined Functions czyli UDF-y:
- User = Użytkownik
- Defined = Definiuje własne
- Functions = Funkcje

Cechy:
- funkcja w języku Python
- opakowane przez funkcję adaptującą `pyspark.sql.functions.udf`
- trzymane jako zmienna
- wołane jak normalna funkcja Sparka

### Przykład UDF

#### Przykładowa funkcja odwracająca string

Stwórz funkcję Python
```python
def reverse_string(string_value):
    return string_value[::-1]
```

Opakuj funkcję i trzymaj jako zmienną:
```python
udf_reverse_string = udf(reverse_string, StringType())
```

Użycie w Sparku:
```
user_df = user_df.withColumn('reverse_name', udf_reverse_string(user_df.name))
```

### Przykład bezparametrowy - sortowanie

```python
def sorting_cap():
    return random.choice(['G', 'H', 'R', 'S'])

udf_sorting_cap = udf(sorting_cap, StringType())

user_df = user_df.withColumn('clazz', udf_sorting_cap())
```
|imię|wiek|klasa|
|---|---|---|
|Alicja|14|A|
|Adam|18|S|
|Cecylia|63|G|


## Partycjonowanie i leniwe przetwarzanie

Partycjonowanie:
- ramki danych są rozbite na partycje
- rozmiar partycji może się zmieniać
- każda partycja jest obsługiwana niezależnie

Leniwe przetwarzanie:
- transformacje są leniwe: `.withColumn(...)`, `.select(...)`
- nic nie jest wykonywane aż do momentu wywołania akcji: `.count()`, `.write(...)`
- transformacje mogą zmienić kolejność dla lepszego wykonania



## Identyfikatory ID

Cechy normalnych pól typu ID:
- popularne w relacyjnych bazach danych
- najczęściej zwiększająca się liczba całkowita, sekwencyjna, unikalna
- nie za bardzo równoległe

|id|nazwisko|imię|województwo|
|---|---|---|---|
|0|Kowalski|Jan|WM|
|1|Adamowicz|A.|MP|
|2|Janikowska|Danuta|PO|

Spark ma wsparcie dla takich wartości: `pyspark.sql.functions.monotonically_increasing_id()`
- liczba całkowita 64-bitowa
- zwiększająca swoją wartość
- unikalna
- niekoniecznie sekwencyjna (istnieją luki)
- całkowicie równoległa

|id|nazwisko|imię|województwo|
|---|---|---|---|
|0|Kowalski|Jan|WM|
|134520871|Adamowicz|A.|MP|
|675824594|Janikowska|Danuta|PO|
