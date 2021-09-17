import os
import string
from map_reduce_lib import *

DATA_DIRECTORY = "invertedIndexInput"

def map_line_to_words(kv):

    file_name, line = kv

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
            output.append((word, f"{file_name}@{line_number}"))

    return output


def reduce(kv):
    word, locations = kv
    return f"{word} \t { ','.join(locations)}\n"

if __name__ == '__main__':

    # Combine all the lines
    lines = []
    for filename in os.listdir(DATA_DIRECTORY):
        with open(DATA_DIRECTORY + "/" + filename, 'r') as f:
            file_lines = list(f.readlines())
            lines.extend(map(lambda line: (filename, line), file_lines))

    # Call map_reduce to create the inverted index
    map_reduce = MapReduce(map_line_to_words, reduce, 8)
    output = map_reduce(lines, debug=True)

    f = open("inverted_index.txt", "w+")

    for entry in output:
        f.write(entry)

    f.close()

    print("inverted_index.txt has been created!")
