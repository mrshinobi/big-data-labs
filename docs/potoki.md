# Potoki w Unix/Linux

Potoki (ang. "pipes") to narzędzie w systemach Unix i Linux, które pozwala na przekazywanie danych
z wyjścia jednego programu do wejścia innego programu.


## Jak działają?

Potoki pozwalają na tworzenie ciągów poleceń, gdzie wynik jednego polecenia jest natychmiast przekazywany
jako wejście do kolejnego polecenia. 
To przekazywanie odbywa się w czasie rzeczywistym, dzięki czemu drugie polecenie może zacząć przetwarzanie danych 
od razu po otrzymaniu pierwszych fragmentów danych z pierwszego polecenia.

Fizycznym mechanizmem komunikacji jest komunikacja międzyprocesowa (IPC), która pozwala na przekazywanie danych między
procesami w systemie operacyjnym. Potoki są jednym z najczęściej używanych mechanizmów IPC w systemach Unix/Linux.


### Przykład użycia potoku

Przykład użycia potoku do wyszukiwania określonych danych w plikach i policzenia ich ilości:

```shell
grep 'szukany_tekst' plik.txt | wc -l
```

gdzie:

- `grep 'szukany_tekst' plik.txt` - wyszukuje w pliku `plik.txt` linie zawierające tekst `szukany_tekst`
- `wc -l` - zlicza ilość linii przekazanych do niego z poprzedniego polecenia
- `|` - to operator potoku, który przekazuje wynik z lewej strony do polecenia po prawej stronie


### Przykłady potoków

- `cat plik.txt | grep 'szukany_tekst' | wc -l` - wyświetla zawartość pliku `plik.txt`, następnie filtruje linie zawierające `szukany_tekst` i zlicza ich ilość
- `ls -l | grep plik` - wyświetla listę plików w bieżącym katalogu, a następnie filtruje wynik, wyświetlając tylko te linie, które zawierają słowo `plik`
- `cat plik.txt | grep 'szukany_tekst' | sort` - wyświetla zawartość pliku `plik.txt`, następnie filtruje linie zawierające `szukany_tekst` i sortuje wynik
- `ps aux | grep proces | awk '{print $2}'` - wyświetla listę wszystkich procesów, filtruje te zawierające słowo `proces`, a następnie wyświetla ich identyfikatory PID
