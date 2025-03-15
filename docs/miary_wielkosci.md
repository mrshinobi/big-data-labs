# Miary wielkości danych



## Tabela wielkości bajtowych dla big data

Poniżej znajduje się tabela z jednostkami używanymi do mierzenia danych w kontekście big data, z uwzględnieniem potęg dwójki i przybliżonych wartości w systemie dziesiętnym:

| Jednostka (PL/ENG)        | Potęga dwójki (`2^n` bajtów) | Przybliżenie dziesiętne (`10^n` bajtów) | Nazwa w przybliżeniu        |
|---------------------------|------------------------------|-----------------------------------------|-----------------------------|
| Kilobajt / Kilobyte (KB)  | `2^10` = 1,024 bajtów       | `10^3` = 1,000 bajtów                   | Tysiąc bajtów               |
| Megabajt / Megabyte (MB)  | `2^20` = 1,048,576 bajtów   | `10^6` = 1,000,000 bajtów               | Milion bajtów               |
| Gigabajt / Gigabyte (GB)  | `2^30` = 1,073,741,824 bajtów | `10^9` = 1,000,000,000 bajtów         | Miliard bajtów              |
| Terabajt / Terabyte (TB)  | `2^40` = 1,099,511,627,776 bajtów | `10^12` = 1,000,000,000,000 bajtów   | Bilion bajtów               |
| Petabajt / Petabyte (PB)  | `2^50` = 1,125,899,906,842,624 bajtów | `10^15` = 1,000,000,000,000,000 bajtów | Biliard bajtów            |
| Eksabajt / Exabyte (EB)   | `2^60` = 1,152,921,504,606,846,976 bajtów | `10^18` = 1,000,000,000,000,000,000 bajtów | Trylion bajtów          |
| Zettabajt / Zettabyte (ZB)| `2^70` = 1,180,591,620,717,411,303,424 bajtów | `10^21` = 1,000,000,000,000,000,000,000 bajtów | Tryliard bajtów       |
| Jottabajt / Yottabyte (YB)| `2^80` = 1,208,925,819,614,629,174,706,176 bajtów | `10^24` = 1,000,000,000,000,000,000,000,000 bajtów | Kwadrylion bajtów   |

### Uwagi:

- Wartości `2^n` są dokładnymi ilościami bajtów definiowanymi przez system binarny używany w informatyce.
- Wartości `10^n` są zaokrągleniami często używanymi w celach marketingowych lub obliczeniowych, gdzie dokładność do najbliższej potęgi dziesięciu jest wystarczająca.
- W praktyce, szczególnie w kontekście sprzedaży nośników danych, producenci mogą używać wartości dziesiętnych (10^n) do opisania pojemności, co może prowadzić do nieporozumień i różnic w oczekiwanej pojemności.


## Jak mierzyć dane big data?

Mierzenie big data może odbywać się na różne sposoby, w zależności od specyfiki danych i potrzeb organizacji. Oto kilka podstawowych metod:

1. **Objętość danych** - Podstawowa miara, często wyrażana w jednostkach takich jak te wymienione w tabeli. Mierzy się całkowitą ilość danych zgromadzonych w systemach.

    Przykłady:
    - 1 TB - 1 terabajt danych
    - 1 PB - 1 petabajt danych
    - 1 EB - 1 eksabajt danych


2. **Szybkość przesyłu danych** - Określa, jak szybko dane są generowane, przetwarzane i analizowane. Może być wyrażona w jednostkach takich jak GB na sekundę.

    Przykłady:
    - 1 GB/s - 1 gigabajt danych na sekundę
    - 1 TB/d - 1 terabajt danych dziennie
    - 1 PB/m - 1 petabajt danych miesięcznie


3. **Różnorodność danych** - Mierzy różnorodność typów i formatów danych, które system musi przetwarzać. To może obejmować strukturalne dane, niesktrukturalne, obrazy, wideo, dane z sensorów itp.

    Przykłady:
    - Dane strukturalne - dane w formie tabelarycznej, np. bazy danych SQL
    - Dane niesktrukturalne - dane w formie tekstu, obrazów, wideo, itp.
    - Dane półstrukturalne - dane w formie JSON, XML, itp.
    - Dane czasowe - dane związane z czasem, np. dane z sensorów IoT
    - Dane geolokalizacyjne - dane związane z lokalizacją, np. dane GPS
    - Dane tekstowe - dane w formie tekstu, np. dokumenty, e-maile, itp.
    - Dane multimedialne - dane w formie obrazów, wideo, dźwięku, itp.


4. **Wartość danych** - Analiza, jak bardzo dane są wartościowe dla organizacji. To bardziej jakościowa miara, która ocenia, jak dane przyczyniają się do osiągania celów biznesowych.

    Przykłady:
    - Dane transakcyjne - dane związane z transakcjami finansowymi, np. sprzedaż, płatności
    - Dane klientów - dane związane z klientami, np. dane demograficzne, preferencje, historie zakupów
    - Dane operacyjne - dane związane z operacjami biznesowymi, np. zamówienia, dostawy, zapasy
    - Dane analityczne - dane związane z analizą biznesową, np. raporty, wskaźniki, prognozy
    - Dane jakościowe - dane związane z jakością produktów, usług, procesów
    - Dane ryzyka - dane związane z ryzykiem biznesowym, np. zabezpieczenia, ubezpieczenia, zgodność


5. **Prawdziwość danych** - Odnosi się do wiarygodności i dokładności danych. Ważne jest, aby systemy big data były w stanie skutecznie zarządzać jakością danych.

    Przykłady:
    - Dane zewnętrzne - dane pochodzące z zewnętrznych źródeł, np. API, strony internetowe
    - Dane wewnętrzne - dane pochodzące z wewnętrznych systemów, np. bazy danych, aplikacje
    - Dane czyste - dane o wysokiej jakości, bez błędów, niejednoznaczności, itp.
    - Dane brudne - dane o niskiej jakości, z błędami, niejednoznaczności, itp.
    - Dane niekompletne - dane, które nie zawierają wszystkich potrzebnych informacji
    - Dane nieaktualne - dane, które są przestarzałe, nieaktualne
    - Dane niezgodne - dane, które są niezgodne z innymi danymi, regułami, itp.
    - Dane niebezpieczne - dane, które są potencjalnie niebezpieczne, np. dane osobowe, finansowe
    - Dane poufne - dane, które są poufne, wymagające ochrony, np. dane osobowe, finansowe

Mierzenie danych big data nie ogranicza się tylko do ich objętości, ale również do szybkości, 
z jaką są one tworzone i przetwarzane, ich różnorodności, wartości oraz prawdziwości.
Te aspekty są kluczowe dla efektywnego zarządzania i wykorzystania dużych zbiorów danych.


## Tabela pojemności różnych nośników danych

Poniżej przedstawiono przybliżone pojemności dla różnych nośników danych, używanych w różnych zastosowaniach:

| Nośnik danych                             | Typowy zakres pojemności    |
|-------------------------------------------|-----------------------------|
| Dyskietka (Floppy Disk)                   | 1.44 MB                     |
| Płyta CD                                  | 700 MB                      |
| Płyta DVD                                 | 4.7 GB (single layer)       |
|                                           | 8.5 GB (dual layer)         |
| Blu-ray Disc                              | 25 GB (single layer)        |
|                                           | 50 GB (dual layer)          |
| Pamięć USB (Flash Drive)                  | 2 GB do 1 TB                |
| Karta pamięci (SD Card)                   | 2 GB do 1 TB                |
| Dysk twardy (HDD)                         | 250 GB do kilku TB          |
| Dysk SSD                                  | 128 GB do kilku TB          |
| Serwer NAS (Network Attached Storage)     | Kilka TB do kilkudziesięciu TB |
| Chmura (Cloud Storage)                    | Elastyczna, od GB do PB     |
