from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import re

appName = "PySpark Wikipedia parser"
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

def clear_html(content_str):
    # clear html tags
    clearHTML_regex = re.compile("<.*?>")
    content_str = re.sub(clearHTML_regex, '', content_str)
    content_str = content_str.replace("\'", "'")
    return content_str

# extract everything from <text> tag from "{{Infobox" to start of new section "==="
def extract_importantText(x):
    important_content = 'none'
    # check if Infobox is present
    if re.search("\{\{Infobox", str(x[1])):
        content_regex = re.search("(\{\{Infobox .*?={2,3})", str(x[1]))
        if content_regex:
            important_content = content_regex.group(1)
    else:
        # if Infobox is not present take everything from "<text>" to start of new section "==="
        content_regex = re.search("(<text.*?={2,3})", str(x[1]))
        if content_regex:
            important_content = content_regex.group(1)

    x.append(important_content)
    return x

# clear useless characters
def delete_useless(x):
    x[1] = clear_html(str(x[1]))
    return x

# delete all "{{cite blocks"
def delete_citeUrl(x):
    # clearURL_regex = re.compile("\{\{[cC]ite.*?\}\}")
    useless_blocks = ['[cC]ite', 'flatlist', '[uU][rR][lL]', 'plainlist', 'ill', '[uU][sS][eE]', 'Italic', 'Unbulleted list', 'lang']
    for block in useless_blocks:
        regex = '\{\{' + block + '.*?\}\}'
        x[1] = re.sub(regex, '', str(x[1]))
    return x

# delete from RDD list by index
def delete_fromRDD(x, index):
    del x[index]
    return x

# filter function to filter out nonbook and nonwriter articles
def is_bookORwriter(x):
    if re.search("\{\{Infobox book", str(x[1])):
        return True
    elif re.search("\{\{Infobox writer", str(x[1])):
        return True
    else:
        return False

# extract given item from Infobox
def extract_from_infobox(infobox, infobox_item):
    regex = "(" + infobox_item + ")( *?= *?)(.*?((\|)|(\})))"
    item_regex = re.search(regex, infobox)
    if item_regex:
        item_text = item_regex.group(3)[:-1]
        if infobox_item == "author" or infobox_item == "editor":
            item_text = item_text.replace("[[", "")
            item_text = item_text.replace("]]", "")
            return item_text
        return item_text
    return 'none'

# extract book info
def extract_BookWriter_info(x):
    # if x is about book:
    if re.search("\{\{Infobox book", str(x[1])):
        x[0] = '[book]' + str(x[0])
        # 1. extract author from Infobox:
        author = extract_from_infobox(str(x[1]), "author")
        if author == 'none':
            # to catch at least 1st
            author = extract_from_infobox(str(x[1]), "authors")
        if author == 'none':
            # sometimes author is refered as editor
            author = extract_from_infobox(str(x[1]), "editor")
        if author == 'none':
            # to catch at least 1st
            author = extract_from_infobox(str(x[1]), "editors")
        x.append(author.strip('\n'))

        # 2. extract genre from Infobox:
        genre = extract_from_infobox(str(x[1]), "genre")
        x.append(genre)

        # 3. extract release year from Infobox:
        publ_year_variations = ["release_date", "published", "pub_date"]
        for variation in  publ_year_variations:
            publ_year = extract_from_infobox(str(x[1]), variation)
            if not(publ_year == 'none'):
                year_regex = re.search('(\d{4})', publ_year)
                if year_regex:
                    publ_year = year_regex.group(1)
                    break
        x.append(publ_year)

        # 4. extract number of pages from Infobox:
        num_pages = extract_from_infobox(str(x[1]), "pages")
        x.append(num_pages)
    elif re.search("\{\{Infobox writer", str(x[1])):
        x[0] = '[writer]' + str(x[0])           # .decode('utf-8') to remove b''
        nationality = extract_from_infobox(str(x[1]), "birth_place")
        if not(nationality == 'none'):
            country =  nationality.split(',').pop()
            x.append(country)
            return x
        x.append(nationality)
    return x

def remove_stopwords(wordlist, stopwords_set):
    # iterate list from end so you can delete elements
    for word in reversed(wordlist):
        if (word+'\n').lower() in stopwords_set:
            wordlist.remove(word)
    return wordlist

def leave_literals(str):
    useless_chars = ['[', ']', '#', ',', '(', ')', ';', '.', ':', '"', '\'']
    for item in useless_chars:
        str = str.replace(item, "")
    return str

def clean_content(x, stopwords):
    if x[0].startswith('[book]'):
        book_content_regex = re.search("\}\}(.*?)={2,3}", x[1])
        if book_content_regex:
            book_content = book_content_regex.group(1)
            book_content = leave_literals(book_content)
            book_content = book_content.split()
            book_content = remove_stopwords(book_content, stopwords)
            x.append(book_content)
        else:
            x.append('none')
    return x

def filter_books(x):
    if re.search("\{\{Infobox book", str(x[1])):
        return True
    return False

def filter_writers(x):
    if re.search("\{\{Infobox writer", str(x[1])):
        return True
    return False

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# define XML schema
schema = StructType([
    StructField('title', StringType(), True),
    StructField('revision', StringType(), True)
])

# parse XML
df = spark.read.format('com.databricks.spark.xml') \
    .option('rowTag','page').load('file:///home/enwiki-latest-pages-articles.xml', schema=schema)

# convert DataFrame to string RDD
xmlRdd = df.rdd.map(list)
xmlRdd = xmlRdd.map(lambda x: [str(c).encode('utf-8') for c in x])

# delete newlines
xmlRdd = xmlRdd.map(lambda x: delete_newlines(x))

# extract text tag
xmlRdd = xmlRdd.map(lambda x: extract_textTag(x))

# extract important parts of article: Infobox, 1st sentence, abstract (0th section)
xmlRdd = xmlRdd.map(lambda x: extract_importantText(x))

# delete text tag bcause it is no longe useful
xmlRdd = xmlRdd.map(lambda x: delete_fromRDD(x, 1))

# clear content
xmlRdd = xmlRdd.map(lambda x: delete_useless(x))

# filter books
xmlRdd = xmlRdd.filter(lambda x: is_bookORwriter(x))

# clear useless {{...}} blocks
xmlRdd = xmlRdd.map(lambda x: delete_citeUrl(x))

# extract book(author, genre, publ_year, number of pages ...) info from text
xmlRdd = xmlRdd.map(lambda x: extract_BookWriter_info(x))

# filter book articles
bookRdd = xmlRdd.filter(lambda x: filter_books(x))

# filter writer articles
writerRdd = xmlRdd.filter(lambda x: filter_writers(x))

# create key/value rdd -> key = author_name / value -> article
bookRdd = bookRdd.map(lambda x: (str(x[2]).strip()[:-2], x))

# create key/value rdd -> key = author_name / value -> author's nationality
writerRdd = writerRdd.map(lambda x: (x[0].replace('[writer]','')[2:-1], x[2]))

# join books with writers by name
finalRdd = bookRdd.leftOuterJoin(writerRdd)

# load stopwords to set
stopwords_set = set()
stopwords_file = open("stopwords.txt", mode="r", encoding="utf-8")
for line in stopwords_file:
    stopwords_set.add(line)

# remove stopwords & useless chars from book content
# finalRdd = finalRdd.map(lambda x: clean_content(x, stopwords_set))

resultRdd = finalRdd.collect()

output = open("output.txt", "w")
for rRdd in resultRdd:
    output.write(f"title\t{rRdd[1][0][0].replace('[book]', '')[2:-1]}\n")
    output.write(f"author\t{str(rRdd[1][0][2])[:-2]}\n")
    output.write(f"genre\t{rRdd[1][0][3]}\n")
    output.write(f"pub_year\t{rRdd[1][0][4]}\n")
    output.write(f"pages\t{rRdd[1][0][5]}\n")
    output.write(f"abstract\t{rRdd[1][0][1]}\n")
    output.write(f"nationality\t{rRdd[1][1]}\n")
    output.write("$\n")
