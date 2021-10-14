import re

class Article:
    def __init__(self):
        self.title = 'none'
        self.infobox = 'none'
        self.first_sentence = 'none'

    def __str__(self):
        return f"Title> {self.title}; infobox> {self.infobox}; 1st sentence> {self.first_sentence}"

in_article_flag = False
first_sentence_captured_flag = False

# open data file and read it line by line
xml_data = open("Data/wikiData.xml", "r")
for line in xml_data:
    if not in_article_flag and re.search("\s*\<page\>", line):
        in_article_flag = True
        continue
    elif in_article_flag:
        # find title of article
        if re.search("\s*\<title\>", line):
            print(f"{line[:-1].strip()}\n")
            continue
        # find infobox of article
        elif re.search("\s*\{\{Infobox", line):
            infobox = re.search("\s*\{\{Infobox.*?\}\}", line)
            print(f"{infobox.group(0).strip()}\n")
        # find 1st sentence of article
        elif not first_sentence_captured_flag and re.search("'{3,5}.*?'{3,5}", line):
            first_sentence_captured_flag = True
            first_para_regex = re.search("('{3,5}.*?'{3,5}).*", line)
            first_para = first_para_regex.group(0).strip()
            # if there are no '.' append newly read lines to end sentence
            while not '.' in first_para:
                first_para += xml_data.readline()
            # REMOVE all '\n', NOT WORKING FOR ALL, NO IDEA WHY
            first_para.replace('\n', '')
            first_sen_regex = re.search("('{3,5}.*?'{3,5}).*?\.", first_para)
            if first_sen_regex:
                first_sen = first_sen_regex.group(0).strip()
                print(first_sen)
        # find end of article
        elif re.search("\s*\</page\>", line):
            in_article_flag = False
            first_sentence_captured_flag = False
            print("\n----------------------------------------------------\n")
