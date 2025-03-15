#!/usr/bin/python3
import sys

stop_words = [
    "i", "me", "my", "myself", "we", "our", "ours",
    "ourselves", "you", "you're", "you've", "you'll", "you'd", "your",
    "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she",
    "she's", "her", "hers", "herself", "it", "it's", "its", "itself", "they",
]

for line in sys.stdin:
    for word in line.split():
        word = word.lower().replace(",", "")

        print(word, 1, sep=",")
