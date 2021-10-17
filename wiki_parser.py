import re

# FLAGS
in_article_flag = False
first_sentence_captured_flag = False

# open data file and read it line by line
# "Data/Wiki/enwiki-latest-pages-article_metas27.xml"
# "problematic.xml"
xml_data_name = "Data/wikiData.xml"

class ArticleMetaData:
    def __init__(self):
        self.title = 'none'
        self.infobox = 'none'
        self.first_sentence = 'none'

    def __str__(self):
        return f"Title>\n {self.title}\n \ninfobox>\n {self.infobox}\n \n1st sentence>\n {self.first_sentence}\n"

    def reset(self):
        self.title = 'none'
        self.infobox = 'none'
        self.first_sentence = 'none'

class Book:
    def __init__(self):
        self.title = 'none'
        self.author = 'none'
        self.genre = 'none'
        self.publ_year = 'none'
        self.num_pages = 'none'
        self.abstract = []
        self.histogram = {}
        self.plot = 'none'

    def __str__(self):
        return f"Title>\n {self.title}\n \nAuthor>\n {self.author}\n \nGenre>\n {self.genre}\n \
                 \nPubl. year>\n {self.publ_year}\n \nNum. pages>\n {self.num_pages}\n \
                 \nAbstract>\n {self.abstract}\n \nPlot>\n {self.plot}\n \nHistogram>\n {self.histogram}\n"

    def reset(self):
        self.title = 'none'
        self.author = 'none'
        self.genre = 'none'
        self.publ_year = 'none'
        self.num_pages = 'none'
        self.abstract.clear()
        self.histogram.clear()
        self.plot = 'none'

class Writer:
    def __init__(self):
        self.title = 'none'
        self.nationality = 'none'

    def __str__(self):
        return f"Title>\n {self.title}\n \nNationality>\n {self.nationality}\n"

    def reset(self):
        elf.title = 'none'
        self.nationality = 'none'


def is_article_writer(article_meta):
    accepted_writers = ["writer",
                        "poet",
                        "novelist"]
    if not(article_meta.infobox == "none") and re.search("\s*\{\{Infobox writer", article_meta.infobox):
        return True
    elif not(article_meta.first_sentence == "none"):
        for writer in accepted_writers:
            # sentence structure for writers is: was|is an|a [country name] [writer, novelist, poet]
            is_writer_regex = f"((was (a|an) )|(is (a|an)) )(.*?)({writer})"
            if re.search(is_writer_regex, article_meta.first_sentence):
                return True


def is_in_genreGazeteer(genre):
    with open('Genre_Gazeteer.txt') as f:
        if genre in f.read():
            return True
        else:
            return False

def is_article_book(article_meta):
    if not(article_meta.infobox == "none") and re.search("\s*\{\{Infobox book", article_meta.infobox):
        return True
    elif re.search("('{3,5}.*?'{3,5})( \(.*\))? (is a) ((\[\[)?(.*? ){1,3}(\]\])?by)", article_meta.first_sentence):
        # regex> if 1st sentence starts with article main heading '''''...'''''
        #        after that there is 0-1 information in ()
        #        after that threse is "is a"
        #        after that there is genre with max lenght of 3 words in optional [[genre]]
        book_genre_regex = re.search("('{3,5}.*?'{3,5})( \(.*\))? (is a) ((\[\[)?(.*? ){1,3}(\]\])?by)", article_meta.first_sentence)
        # book genre is in form: "genre by" and it is 4th group
        book_genre = book_genre_regex.group(4)
        return True
        # drop " by"
        book_genre = book_genre[:-3]
        # delete [[]] if necessary
        re.sub("\w*?", book_genre)
        # is lowercase version in genre gazeteer?
        if is_in_genreGazeteer(book_genre.lower()):
         return True
    else:
        return False

def extract_item_from_infobox(infobox, infobox_item):
    # f"" not working for some reason
    regex = "(" + infobox_item + ")( = )(.*?((\|)|(\})))"
    item_regex = re.search(regex, infobox)
    if item_regex:
        item_text = item_regex.group(3)[:-1]
        if infobox_item == "author":
            if item_text.startswith("[["):
                return item_text[2:-3]
            else:
                return item_text
        return item_text

    return ''

def extract_author(article_meta):
    if not(article_meta.infobox == "none"):
        author = extract_item_from_infobox(article_meta.infobox, "author")
        return author
    elif not(article_meta.first_sentence == "none"):
        book_author = re.search("(by) (.*\[\[)(.*\]\])", article_meta.first_sentence)
        if book_author:
            author = book_author.group(3)[:-2]
            return author
        else:
            return ''
    else:
        return ''

def extract_year(str):
    year_regex = re.search('(\d{4})', str)
    if year_regex:
        return year_regex.group(0)
    else:
        return 'none'

stopwords_set = set()
stopwords_file = open("stop_words.txt", "r")
for line in stopwords_file:
    stopwords_set.add(line)

def delete_stopwords(content_wordlist):
    # iterate list from end so you can delete elements
    for word in reversed(content_wordlist):
        if (word+'\n').lower() in stopwords_set:
            content_wordlist.remove(word)
    return content_wordlist

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

def count_freq(abstract_list):
    freq_histogram = {}
    for item in abstract_list:
        if (item.lower() in freq_histogram):
            freq_histogram[item.lower()] += 1
        else:
            freq_histogram[item.lower()] = 1

    return freq_histogram

def extract_book_doc(article_meta, article_content):
    noise_chars = ['(',')']
    book_doc = Book()
    # extract Title
    xml_text = re.search(">(.*?)<", article_meta.title)
    book_doc.title = xml_text.group(1)

    # extract Author
    author = extract_author(article_meta)
    if author:
        book_doc.author = author

    # extract genre
    genre = extract_item_from_infobox(article_meta.infobox, "genre")
    if genre:
        for item in noise_chars:
            genre = genre.replace(item, "")
        book_doc.genre = genre

    # extract publ. year
    publ_year_variations = ["release_date", "published", "pub_date"]
    for variation in  publ_year_variations:
        publ_year = extract_item_from_infobox(article_meta.infobox, variation)
        if publ_year:
            book_doc.publ_year = extract_year(publ_year)

    # extract abstract
    article_abstract_regex = re.search("('''''.*?''''')(.*?)(==)", article_content)
    if article_abstract_regex:
        abstract_list = clear_content(article_abstract_regex.group(2))
        book_doc.abstract = abstract_list
        book_doc.histogram = count_freq(abstract_list)

    return book_doc

def extract_nationality(first_sentence):
    if not(article_meta.first_sentence == "none"):
        # regex not working for RoaBastos
        nationality_regex = re.search("((was (a|an) )|(is (a|an) ))(.*? )", article_meta.first_sentence)
        if nationality_regex:
            nationality = nationality_regex.group(6)[:-1]
            return nationality
        else:
            return ''
    elif not(article_meta.infobox == "none"):
        nationality = extract_item_from_infobox(article_meta.infobox, "birth_place")
        if nationality:
            writer_doc.nationality = nationality
        return nationality
    else:
        return ''

def extract_writer_doc(article_meta):
    writer_doc = Writer()
    # extract Title
    xml_text = re.search(">(.*?)<", article_meta.title)
    writer_doc.title = xml_text.group(1)

    # extract Nationality
    nationality = extract_nationality(article_meta.first_sentence)
    if nationality:
        writer_doc.nationality = nationality

    return writer_doc

def write_book_doc(book_doc):
    file_name = "to_index/" + book_doc.title + ".txt"
    f = open(file_name, "w")
    f.write(f"book\n")
    f.write(f"title {book_doc.title}\n")
    f.write(f"author {book_doc.author}\n")
    f.write(f"genre {book_doc.genre}\n")
    f.write(f"publ_year {book_doc.publ_year}\n")
    f.write(f"num_pages {book_doc.num_pages}\n")
    f.write(f"abstract ")
    for item in book_doc.abstract:
        f.write(f"{item} ")
    f.write(f"\nhistogram: {book_doc.histogram}\n")
    f.write(f"plot:{book_doc.plot}\n")
    f.close()

def write_writer_doc(writer_doc):
    file_name = "to_index/" + writer_doc.title + ".txt"
    f = open(file_name, "w")
    f.write(f"writer\n")
    f.write(f"name {writer_doc.title}\n")
    f.write(f"nationality {writer_doc.nationality}\n")
    f.close()

article_meta = ArticleMetaData()
article_content = ''
xml_data = open(xml_data_name, "r")
for line in xml_data:
    line = line.replace('\n', '')
    article_content += line
    if not in_article_flag and re.search("\s*\<page\>", line):
        in_article_flag = True
        continue

    elif in_article_flag:
        # find title of article_meta
        if re.search("\s*\<title\>", line):
            #print(f"{line[:-1].strip()}\n")
            article_meta.title = line[:-1]
            continue

        # find infobox of article_meta
        # find otherway to collect infobox
        elif re.search("\s*\{\{Infobox", line):
            while not "}}" in line:
                next_line = xml_data.readline().replace('\n', '')
                article_content += next_line
                line += next_line

            infobox_regex = re.search("\s*\{\{Infobox.*?\}\} '", line)
            if infobox_regex:
                article_meta.infobox = infobox_regex.group(0).strip()
                #print(f"{infobox_regex.group(0).strip()}\n")

        # find 1st sentence of article_meta
        elif not first_sentence_captured_flag and re.search("'{3,5}.*?'{3,5}", line):
            first_sentence_captured_flag = True
            first_para_regex = re.search("('{3,5}.*?'{3,5}).*", line)
            first_para = first_para_regex.group(0).strip()
            # if there are no '.' append newly read lines to end sentence
            while not '.' in first_para:
                next_line = xml_data.readline().replace('\n', '')
                article_content += next_line
                first_para += next_line
            first_sen_regex = re.search("('{3,5}.*?'{3,5}).*?\.", first_para)
            if first_sen_regex:
                article_meta.first_sentence = first_sen_regex.group(0).strip()
                #print(first_sen)

        # find end of article_meta
        elif re.search("\s*\</page\>", line):
            in_article_flag = False
            first_sentence_captured_flag = False
            if is_article_book(article_meta):
                # Extract: Title, Author, Genre, Publ. year, Num. of pages from article metadata
                book_doc = extract_book_doc(article_meta, article_content)

                # print(book_doc)
                # #print(article_meta)
                # print("\n----------------------------------------------------\n")

                # write book_doc items to .txt file with name b1.txt, ...
                write_book_doc(book_doc)

            elif is_article_writer(article_meta):
                writer_doc = extract_writer_doc(article_meta)

                # print(writer_doc)
                # # print(article_meta)
                # print("\n----------------------------------------------------\n")

                # write writer_doc items to .txt file with name a1.txt, ...
                # write_writer_doc(writer_doc)

            article_meta.reset()
            article_content = ''