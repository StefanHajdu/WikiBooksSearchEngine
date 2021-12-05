import sys, lucene, math, re, argparse, pyfiglet
from os import path, listdir

from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.store import RAMDirectory
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version
from datetime import datetime

# imports for indexing
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import \
    FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions

# Retriever imports:
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser


DATA_DFILE = "to_index.txt"
INDEX_DIR = "/home/stephenx/Dokumenty/python/Wiki_books_search/index/"

def extract_from_file(field, file_name):
    file = open(file_name)
    for line in file:
        if line.startswith(field):
            return line[:-1]
    file.close()

def create_document(book_doc):
    # Define properties macros for Fields
    # explained here: https://lucene.apache.org/core/7_7_0/core/org/apache/lucene/document/FieldType.html
    field_type_1 = FieldType() # Create a new FieldType with default properties
    field_type_1.setStored(True) # Set to true to store this field
    field_type_1.setTokenized(False) # Set to true to tokenize this field's contents via the configured
    field_type_1.setIndexOptions(IndexOptions.DOCS_AND_FREQS) # to control how much information is stored in the postings lists

    field_type_2 = FieldType()
    field_type_2.setStored(True)
    field_type_2.setStoreTermVectors(True)
    field_type_2.setTokenized(True)
    field_type_2.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

    # print(book_doc)

    doc = Document()
    # add the title field
    doc.add(Field("title", book_doc.title, field_type_2))
    # add the author field
    doc.add(Field("author", book_doc.author, field_type_2))
    # add the genre field
    doc.add(Field("genre", book_doc.genre, field_type_2))
    # add the country field
    doc.add(Field("country", book_doc.country, field_type_2))
    # add the key-words field
    doc.add(Field("words", book_doc.key_words, field_type_2))

    return doc

def query(command, arg):
    command = command.lower()
    aq_regex = re.search(f'(;{arg}.*?;)', command)
    if aq_regex:
        aq = aq_regex.group(1)[3:-2]
    else:
        aq_regex = re.search(f'(;{arg}.*)', command)
        aq = aq_regex.group(1)[3:]
    if arg == 'kw':
        return aq
    if ' & ' in aq:
        aq.replace(' & ', '')
    return aq.replace(' ', 'X')

def search_by_field(command, searcher, analyzer, ireader, field):
    field_set = set()
    print ("-> Searching for: ", command)
    query = QueryParser(field, analyzer).parse(command)
    # Searching returns hits in the form of a TopDocs object
    scoreDocs = searcher.search(query, 100000).scoreDocs    # 10 means : 10 best results
    print ("-> %s total matching documents." % len(scoreDocs))
    for scoreDoc in scoreDocs:
        # Note that the TopDocs object contains only references to the underlying documents.
        # In other words, instead of being loaded immediately upon search, matches are loaded
        # from the index in a lazy fashion—only when requested with the Index-
        # Searcher.doc(int) call. That call returns a Document object from which we can then
        # retrieve individual field values.
        doc = searcher.doc(scoreDoc.doc)
        book = BookDoc(
                    doc.get("title"),
                    doc.get("author"),
                    doc.get("genre"),
                    doc.get("county"),
                    doc.get("words")
                )
        field_set.add(book)
    return field_set

def print_nice(book):
    title = book.title
    title = title.replace(',', ' ').title()
    author = book.author
    author = author.replace('X', ' ').title()
    print(f"FOUND: \"{title}\" by {author}")

def count_hist(book):
    freq_histogram = {}
    for item in book.key_words.split():
        if (item.lower() in freq_histogram):
            freq_histogram[item.lower()] += 1
        else:
            freq_histogram[item.lower()] = 1

    return freq_histogram

def evaluate_result(final_set, command):
    if ';kw' not in command:
        return list(final_set)
    else:
        words = query(command, 'kw')
        words = words.split()
        for book in final_set:
            book_hist = count_hist(book)
            for word in words:
                if word in book_hist:
                    book.score += book_hist[word]
        book_list = list(final_set)
        book_list.sort(key=lambda x: x.score, reverse=True)
        return book_list

def run_search(searcher, analyzer, ireader):
    ascii_banner = pyfiglet.figlet_format("Search")
    print(ascii_banner)
    book_set = set()
    list_of_sets = []
    while True:
        print ("Hit enter with no input to quit.")
        # read input from user
        command = str(input("Query:"))
        if command == '':
            return
        print ("Searching for:", command)

        if ';a' in command:
            author_set = search_by_field(query(command, 'a'), searcher, analyzer, ireader, 'author')
            list_of_sets.append(author_set)
        if ';g' in command:
            genre_set = search_by_field(query(command, 'g'), searcher, analyzer, ireader, 'genre')
            list_of_sets.append(genre_set)
        if ';c' in command:
            country_set = search_by_field(query(command, 'c'), searcher, analyzer, ireader, 'country')
            list_of_sets.append(country_set)
        if ';kw' in command:
            words_set = search_by_field(query(command, 'kw'), searcher, analyzer, ireader, 'words')
            list_of_sets.append(words_set)

        #print(list_of_sets)

        final_set = set.intersection(*list_of_sets)
        if len(final_set) == 0:
            print("-> No results found")
        else:
            book_list = evaluate_result(final_set, command)
            for item in book_list:
                print("\n")
                print_nice(item)
            print("\n")

        final_set.clear()
        list_of_sets.clear()

def extract_author(line):
    author = line.replace('author\t', '').lower()
    if len(author) == 0 or author == '\n':
        return 'unknown'
    if '(' in author:
        author = re.sub(r'\(.*', '', author)
    if ' and ' in author:
        author = author.replace(' and ', '&')
    if '[[' in author:
        author = author.replace('[[', '&')[2:]
        author = author.replace(']]', '')
        return author.replace(' ', 'X').replace('&', ' ').lower()[:-1]
    author = author.replace(' ', 'X')
    return author[1:-1]

def extract_genre(line):
    genre = line.replace('genre\t', '').lower()
    genre = genre.replace("\\'", '')
    genre = genre.replace("\\n", '')
    genre = genre.strip()
    if genre == 'none' or genre == '' or genre == '\n':
        return 'unknown'
    genre = genre.replace(']],', '&')
    genre = genre.replace(', [[', '&')
    genre = genre.replace(', ', '&')
    genre = genre.replace('[[', '&')[1:]
    genre = genre.replace(']]', '')
    genre = genre.replace(')', '')
    genre = genre.replace('(', '')

    genre = genre.replace(' ', 'X')
    return genre.replace('&', ' ')

def extract_country(line):
    country = line.replace('nationality\t', '').lower()
    country = country.replace("\\'", '')
    country = country.replace("\\n", '')
    country = country.strip()

    if country == 'none' or country == '' or country == '\n':
        return 'unknown'
    country = country.replace('[[', '')
    country = country.replace(']]', '')
    country = country.replace(')', '')
    country = country.replace('(', '')
    country = country.replace('.', '')

    return country

stopwords_set = set()
stopwords_file = open("stopwords.txt", "r")
for line in stopwords_file:
    stopwords_set.add(line)

def remove_stopwords(key_words, stopwords_set):
    new_words = ''
    words = key_words.split()
    for word in reversed(words):
        if (word+'\n').lower() in stopwords_set:
            words.remove(word)
        else:
            new_words += word+' '
    return new_words

def extract_key_words(line):
    key_words = line.replace('abstract\t', '').lower()
    key_words = re.sub(r'({{.*?}})', '', key_words)
    key_words = key_words.replace("\\'", '')
    key_words = key_words.replace("\\n", '')
    key_words = key_words.replace('[', '')
    key_words = key_words.replace('(', '')
    key_words = key_words.replace(')', '')
    key_words = key_words.replace('|', '')
    key_words = key_words.replace(']', '')
    key_words = key_words.replace(',', '')
    key_words = key_words.replace('.', '')
    key_words = key_words.replace('=', '')
    key_words = remove_stopwords(key_words, stopwords_set)

    return key_words

arg_parser = argparse.ArgumentParser(description='Searching script')
arg_parser.add_argument('-i', '--index',
                        action='store_true',
                        help='index book database with Lucene')
args = arg_parser.parse_args()

# Initialize lucene and the JVM
lucene.initVM(vmargs=['-Djava.awt.headless=true'])

class BookDoc:

    def __init__(self, title, author, genre, country, key_words):
        self.score = 0
        self.title = title
        self.author = author
        self.genre = genre
        self.country = country
        self.key_words = key_words

    def __str__(self):
        return f"SCORE = {self.score}; TITLE = {self.title}; AUTHOR = {self.author}; GENRE = {self.genre}; COUNTRY = {self.country}; KEY-WORDS = {self.key_words}\n"

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__ and
            self.title == other.title
        )

    def __hash__(self):
        return hash(self.title)

if args.index:
    ascii_banner = pyfiglet.figlet_format("Index")
    print(ascii_banner)
    # Create a new directory. As a SimpleFSDirectory
    directory = SimpleFSDirectory(Paths.get(INDEX_DIR))
    # create StandardAnalyzer
    analyzer = StandardAnalyzer()
    # This Analyzer limits the number of tokens while indexing. It is a replacement for the maximum field length setting inside IndexWriter.
    analyzer = LimitTokenCountAnalyzer(analyzer, 1048576)
    # IndexWriterConfig determnies whether a new index is created or whether an existing index is opened
    config = IndexWriterConfig(analyzer)
    # OpenMode.CREATE -> we create new index writer
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(directory, config)

    book_file = open(DATA_DFILE, 'r')

    title = ''
    author = ''
    genre = ''
    for line in book_file:
        if line.startswith('title'):
            title = line.replace('title\t', '')
            title = title.replace(' ', ',').lower()[:-1]
        if line.startswith('author'):
            author = extract_author(line)
        if line.startswith('genre'):
            genre = extract_genre(line)
        if line.startswith('abstract'):
            key_words = extract_key_words(line)
        if line.startswith('nationality'):
            country = extract_country(line)
        if line.startswith('$'):
            # create documents
            doc = create_document(BookDoc(title, author, genre, country, key_words))
            # write document
            writer.addDocument(doc)
    print ("\nNumber of indexed documents: %d\n" % writer.numDocs())
    writer.close()

directory = SimpleFSDirectory(Paths.get(INDEX_DIR))
# Create a searcher for the above defined RAMDirectory
searcher = IndexSearcher(DirectoryReader.open(directory))
# Create a new retrieving analyzer
analyzer = StandardAnalyzer()

ireader = DirectoryReader.open(directory)

# ... and start searching!
run_search(searcher, analyzer, ireader)
del searcher