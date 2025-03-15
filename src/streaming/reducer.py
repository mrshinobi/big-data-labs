#!/usr/bin/python3
import sys

current_word = None
current_count = 0

# przetwarzamy posortowane dane wejściowe
for line in sys.stdin:
    word, count = line.split(',', 1)
    count = int(count)

    # jesli przetwarzamy ten sam klucz, dodajemy wartość
    if current_word == word:
        current_count += count
    else:
        # Gdy zmieniamy klucz, wypisujemy wynik poprzedniego słowa jako krotkę
        if current_word is not None:
            # format: (slowo, liczba)
            print(f"('{current_word}', {current_count})")

        current_word = word
        current_count = count

# wypisujemy wynik dla ostatniego klucza
if current_word is not None:
    print(f"('{current_word}', {current_count})")
