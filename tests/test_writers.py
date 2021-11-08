from os import path, listdir

DATA_DIR = "/home/stephenx/Dokumenty/python/Wiki_books_search/to_index_writers/"

for f in listdir(DATA_DIR):
    filee = open(DATA_DIR+f)
    print(filee.readline())
    print(filee.readline())
    print(filee.readline())

    print("\n---------------------\n")

    filee.close()
