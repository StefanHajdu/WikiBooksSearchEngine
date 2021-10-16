import sys, lucene, math, re
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

DATA_DIR = "/home/stephenx/Dokumenty/python/Wiki_books_search/to_index/"
INDEX_DIR = "/home/stephenx/Dokumenty/python/Wiki_books_search/index/"

def extract_from_file(field, file_name):
    file = open(file_name)
    for line in file:
        if line.startswith(field):
            return line[:-1]
    file.close()

def create_document(file_name):
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

    path = DATA_DIR + file_name
    file = open(path)
    type = file.readline()
    file.close()
    doc = Document()
    # add the title field
    doc.add(Field("file_name", file_name, field_type_1))
    # add contents
    #doc.add(Field("random_bs", "title Hijo de hombre", field_type_2))

    if type[:-1] == 'book':
        # add author to index
        field_val = extract_from_file("author", path)
        doc.add(Field("author", field_val, field_type_2))
        # add abstract to index
        field_val = extract_from_file("abstract", path)
        doc.add(Field("abstract", field_val, field_type_2))

    return doc

    # if type == 'book':
    #     # add type to index
    #     doc.add(Field("type", type, field_type_1))
    #     # add title to index
    #     field_val = extract_from_file("title", file_name)
    #     doc.add(Field("title", field_val, field_type_1))
    #     # add author to index
    #     field_val = extract_from_file("author", file_name)
    #     doc.add(Field("author", field_val, field_type_1))
    #     # add genre to index
    #     field_val = extract_from_file("genre", file_name)
    #     doc.add(Field("genre", field_val, field_type_1))
    #     # add publ_year to index
    #     field_val = extract_from_file("publ_year", file_name)
    #     doc.add(Field("publ_year", field_val, field_type_1))
    #     # add num_page to index
    #     field_val = extract_from_file("num_pages", file_name)
    #     doc.add(Field("num_page", field_val, field_type_1))
    #     # add abstract to index
    #     field_val = extract_from_file("abstract", file_name)
    #     doc.add(Field("abstract", field_val, field_type_2))
    #
    # elif type == 'writer':
    #     doc.add(Field("title", field_val, field_type_1))
    #     # add nationality to index
    #     field_val = extract_from_file("nationality", file_name)
    #     doc.add(Field("nationality", field_val, field_type_1))


# Initialize lucene and the JVM
lucene.initVM(vmargs=['-Djava.awt.headless=true'])

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

print ("Number of indexed documents: %d\n" % writer.numDocs())

# Iterate over file in data directory
for f in listdir(DATA_DIR):
    print("Current file:", f)
    # create documents
    doc = create_document(f)
    # write document
    writer.addDocument(doc)

print ("\nNumber of indexed documents: %d\n" % writer.numDocs())
writer.close()

#
# SEARCHING PART
#

class TermScore:
    def __init__(self, term_name):
        self.name = term_name
        self.freq_in_term = 0
        self.freq_in_collection = 0
        self.idf = 0
        self.weight_in_term = 0
        self.freq_in_document = 0
        self.wf = 0
        self.cosine_weight = 0
        self.score = 0

def find_freq_in_doc(hist, term):
    regex = "('" + term + "':) ([\d*])"
    value_regex = re.search(regex, hist)
    value = value_regex.group(2)
    return int(value)

def evalute_doc(command, df_hist, N, doc_file):
    terms = command.split()
    doc_score = 0
    term_score_list = []
    for term in terms:
        curr_term = TermScore(term.lower())
        # print(curr_term.name)
        # for now only single occurs is allowed
        curr_term.freq_in_term = 1
        curr_term.freq_in_collection = df_hist[curr_term.name]
        curr_term.idf = abs(math.log10(N/curr_term.freq_in_collection))
        # print(curr_term.freq_in_term)
        # print(curr_term.freq_in_collection)
        # print(curr_term.idf)
        curr_term.weight_in_term = curr_term.freq_in_term * curr_term.idf
        f = open(DATA_DIR+doc_file, "r")
        doc_hist = extract_from_file("histogram:", DATA_DIR+doc_file)[9:]
        f.close()
        curr_term.freq_in_document = find_freq_in_doc(doc_hist, curr_term.name)
        # print(curr_term.freq_in_document)
        curr_term.wf = 1+math.log10(curr_term.freq_in_document) if curr_term.freq_in_document > 0 else 0
        # print(curr_term.wf)
        term_score_list.append(curr_term)

    wf_cosine_divider = 0
    for term_score in term_score_list:
        wf_cosine_divider += pow(term_score.wf, 2)

    wf_cosine_divider = math.sqrt(wf_cosine_divider)

    for term_score in term_score_list:
        term_score.cosine_weight = term_score.wf / wf_cosine_divider
        term_score.score = term_score.cosine_weight * term_score.weight_in_term
        doc_score += term_score.score

    return doc_score

def count_df_histogram(DATA_DIR):
    df_histogram = {}
    # Iterate over file in data directory
    for f in listdir(DATA_DIR):
        # locate abstract string
        abstract_str = extract_from_file("abstract", DATA_DIR+f)[8:]
        for item in abstract_str.split():
            if (item.lower() in df_histogram):
                df_histogram[item.lower()] += 1
            else:
                df_histogram[item.lower()] = 1
    return df_histogram

def run_search(searcher, analyzer, ireader, df_hist):
    while True:
        print ("Hit enter with no input to quit.")
        # read input from user
        command = str(input("Query:"))
        if command == '':
            return
        print ("Searching for:", command)
        query = QueryParser("abstract", analyzer).parse(command)
        # Searching returns hits in the form of a TopDocs object
        scoreDocs = searcher.search(query, 10).scoreDocs    # 10 means : 10 best results
        print ("%s total matching documents." % len(scoreDocs))

        for scoreDoc in scoreDocs:
            # Note that the TopDocs object contains only references to the underlying documents.
            # In other words, instead of being loaded immediately upon search, matches are loaded
            # from the index in a lazy fashion—only when requested with the Index-
            # Searcher.doc(int) call. That call returns a Document object from which we can then
            # retrieve individual field values.
            doc = searcher.doc(scoreDoc.doc)
            # here you can only print doc and it is fine
            doc_score = evalute_doc(command, df_hist, len(scoreDocs), doc.get("file_name"))
            print(doc.get("file_name"))
            print(f"score: {doc_score}\n")

# Create a searcher for the above defined RAMDirectory
searcher = IndexSearcher(DirectoryReader.open(directory))
# Create a new retrieving analyzer
analyzer = StandardAnalyzer()

ireader = DirectoryReader.open(directory)

df_hist = count_df_histogram(DATA_DIR)

# ... and start searching!
run_search(searcher, analyzer, ireader, df_hist)
del searcher
