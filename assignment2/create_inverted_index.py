import os
import string
from map_reduce_lib import *

DATA_DIRECTORY = "invertedIndexInput"


def map(line):
    output = []

    line_number = line.split('\t')[0]

    line_text = line[len(line_number):]
    line_text = line_text.lower().strip()
    line_text = line_text.translate(str.maketrans(
        string.punctuation, ' ' * len(string.punctuation)))

    # List with stopwords (low information words that should be removed from the string).
    STOP_WORDS = set([
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    ])

    for word in line_text.split():

        if word not in STOP_WORDS:
            output.append((word, line_number))

    return output


def reduce(kv):
    return kv


def map2(input):
    story, inverted_index = input

    output = []

    for word, line_numbers in inverted_index:
        for line_number in line_numbers:
            output.append((word, f"{story}@{line_number}"))

    return output


def reduce2(kv):
    return kv


if __name__ == '__main__':

    temp_indices = []

    for filename in os.listdir(DATA_DIRECTORY):
        with open(DATA_DIRECTORY + "/" + filename, 'r') as f:

            print("Inverting file: {}".format(filename))

            lines = list(f.readlines())

            lines.append(filename)

            # print(lines)
            map_reduce = MapReduce(map, reduce, 8)
            temp_indices.append((filename, map_reduce(lines, debug=True)))

    print("Combining everything...")

    map_reduce = MapReduce(map2, reduce2, 8)
    inverted_index = map_reduce(temp_indices, debug=True)

    f = open("inverted_index.txt", "w+")

    for word, locations in inverted_index:
        locations_string = ','.join(locations)
        f.write(f"{word} \t {locations_string}\n")

    f.close()

    print("inverted_index.txt has been created!")
