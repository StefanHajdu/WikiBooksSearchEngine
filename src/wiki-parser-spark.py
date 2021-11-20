from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import re

appName = "Python Example - PySpark Read XML"
master = "local"

def delete_newlines(x):
    x[1] = str(x[1]).replace('\n', '')
    x[1] = str(x[1]).replace('<br>', '')
    return x

def extract_textTag(x):
    regex_text = re.search('(<text.*>.*</text>)', str(x[1]))
    if regex_text:
        x[1] = regex_text.group(1)
    return x

def clear_content(content_str):
    # clear html tags especialy <ref></ref> tag content
    clean_ref_regex = re.compile('<.*?>.*?<.*?>')
    content_str = re.sub(clean_ref_regex, '', content_str)
    # clear {{}} parts
    clean_ref_regex = re.compile('\{\{.*?\}\}')
    content_str = re.sub(clean_ref_regex, '', content_str)
    # clear balast characters
    content_str = content_str.replace('|', " ")
    useless_chars = ['[', ']', '#', ',', '(', ')', ';', '.', ':', '"', "''"]
    for balast in useless_chars:
        content_str = content_str.replace(balast, "")
    content_wordlist = content_str.split()
    # clear stop words
    content_wordlist = delete_stopwords(content_wordlist)
    return content_wordlist

def append_infobox(x):
    infobox = 'none'
    # extract infobox from text tag
    regex_infobox = re.search("(\{\{Infobox .*?)(\}\})", str(x[1]))
    if regex_infobox:
        infobox = regex_infobox.group(1)
        x.append(infobox + '}')
        return x

    x.append(infobox)
    return x

def append_firstSentence(x):
    firstSentence = 'none'
    # extract 1st sentence from text tag
    regex_firstSentence = re.search("(\}\}.*?\.)", str(x[1]))
    if regex_firstSentence:
        firstSentence = regex_firstSentence.group(1)

    x.append(firstSentence)
    return x

def append_abstract(x):
    abstract = 'none'
    # extract abstract of article from text tag
    regex_abstract = re.search("(\}\}.*?={2,3})", str(x[1]))
    if regex_abstract:
        abstract = regex_abstract.group(1)

    x.append(abstract)
    return x

def delete_text(x):
    del x[1]

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

schema = StructType([
    StructField('title', StringType(), True),
    StructField('revision', StringType(), True)
])

df = spark.read.format('com.databricks.spark.xml') \
    .option('rowTag','page').load('file:///home/spark-programming/enwiki-latest-pages-articles27.xml', schema=schema)

# convert DataFrame to string RDD
xmlRdd = df.rdd.map(list)
xmlRdd = xmlRdd.map(lambda x: [str(c).encode('utf-8') for c in x])

# delete newlines
xmlRdd = xmlRdd.map(lambda x: delete_newlines(x))

# extract text tag
xmlRdd = xmlRdd.map(lambda x: extract_textTag(x))

# append infobox
xmlRdd = xmlRdd.map(lambda x: append_infobox(x))

# append 1st sentence
xmlRdd = xmlRdd.map(lambda x: append_firstSentence(x))

# append abstract
xmlRdd = xmlRdd.map(lambda x: append_abstract(x))

# delete text tag bcause it is no longe useful
xmlRdd = xmlRdd.map(lambda x: delete_text(x))

resultRdd = xmlRdd.collect()

output = open("output.txt", "w")

for rRdd in resultRdd:
    output.write(f"\nTitle: {rRdd[0]}\n")
    #output.write(f"\nText: {rRdd[1]}\n")
    output.write(f"\nInfobox: {rRdd[1]}\n")
    output.write(f"\nFirst sentence: {rRdd[2]}\n")
    output.write(f"\nAbstract: {rRdd[3]}\n")
    output.write("\n----------\n")
