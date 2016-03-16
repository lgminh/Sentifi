__author__ = 'tuanta', 'minhloc'

import pandas as pd
from nltk import WordNetLemmatizer
import nltk
# avoid unicode bug when exporting to excel
# http://stackoverflow.com/questions/21129020/how-to-fix-unicodedecodeerror-ascii-codec-cant-decode-byte
import sys
reload(sys)  
sys.setdefaultencoding('utf8')


# global variables
GNIP_RULE_MAX_LENGTH = 2048
USE_CASES = [1, 2] # only generate for these use cases


from nltk.stem import WordNetLemmatizer

wnl = WordNetLemmatizer()

def isplural(word):
    lemma = wnl.lemmatize(word, 'n')
    plural = True if word is not lemma else False
    return plural, lemma


def load_requirement(file_path):
    return pd.read_csv(file_path)

# if file_path = true => load from csv, else get from
def load_aliases(file_path=''):
    '''
    Load all classes and their aliases into dataframe
    If file_path is specified, then load from csv file; otherwise load from Postgres database
    '''
    # load from file
    if file_path:
        return pd.read_csv(file_path, header=None, names=['class_name', 'lang', 'string_agg'])

    # otherwise, load from database
    from psycopg2 import connect, IntegrityError
    from psycopg2.extras import RealDictCursor
    conn = connect(
        host='psql-dev-1.ireland.sentifi.internal', port=5432,
        database='sentifi_dev_quang',
        user='dbo', password='sentifi')
    query = """
        SELECT li.class_name, la.lang, string_agg(la.alias, ';')
        FROM lexical_instance AS li
        LEFT OUTER JOIN lexical_alias AS la ON (la.lex_id = li.id)
        GROUP BY li.class_name, la.lang;
        """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        conn.commit()
        df = pd.DataFrame([row for row in cur])
        conn.close()
        return df


def load_aliases_from_database():

    # otherwise, load from database
    from psycopg2 import connect, IntegrityError
    from psycopg2.extras import RealDictCursor
    conn = connect(
        host='psql-dev-1.ireland.sentifi.internal', port=5432,
        database='sentifi_dev_quang',
        user='dbo', password='sentifi')
    query = """
        SELECT li.class_name, li.name, la.lang, string_agg(la.alias, ';')
        FROM lexical_instance AS li
        LEFT OUTER JOIN lexical_alias AS la ON (la.lex_id = li.id)
        GROUP BY li.class_name, li.name, la.lang;
        """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        conn.commit()
        df = pd.DataFrame([row for row in cur])
        conn.close()
        return df




def keywords_to_sentences(terms, max_length=1024, join_term=' OR '):
    '''
    Example:
        terms = ['a', 'b', 'c'], join_term=' OR ', max_length=6,
        then sentences = ['a OR b', 'c']
    '''
    cursor = 0
    result = ['']
    for p in xrange(len(terms)):
        if cursor < p:
            sen = join_term.join(terms[cursor:p])
            if len(sen) > max_length:
                cursor = p
                result.append('')
            else:
                result[-1] = sen
    return [s for s in result if s]


keyword_add_bio_contains = \
    lambda keyword: 'bio_contains:' + '"' + keyword + '"' \
    if len(keyword.split(' ')) > 1 \
    else   'bio_contains:' +  keyword



def uc_1(class_name, aliases):
    rules = []

    for (lang, terms) in aliases.items():
        subfix = ' (bio_lang:%s OR lang:%s OR twitter_lang:%s)'\
                 % tuple([lang.lower() for i in xrange(3)])


        #modified_terms = []
        #for term in terms:
        #     modified_terms.append(keyword_bio_contains(term))


        #modified_terms = map(lambda term:
        #                     'bio_contains:' + '"' + term + '"'
        #                     if (' ' in term)
        #                     else 'bio_contains:' + term , terms)

        modified_terms = map(lambda term:
                             'bio_contains:" ' + term + ' "' + ' OR ' + 'bio_contains:"' + term + ' "'
                            ,terms)



        # max length of rule body, 2 spaces for '(' and ')'
        max_length = GNIP_RULE_MAX_LENGTH - len(subfix) - 2
        rule_bodies = keywords_to_sentences(modified_terms, max_length=max_length) # return list of rules

        for body in rule_bodies:
            rules.append([class_name, lang, '(%s)%s' % (body, subfix)])
    return rules


def uc_2(class_name, aliases):
    rules = []

    bio_locations = [
        ' (bio_location:"london" OR bio_location:"united kingdom")',
        ' (bio_location:"vienna" OR bio_location:"austria")',
        ' (bio_location:"germany" OR bio_location:"berlin" OR bio_location:"frankfurt am main")',
        ' (bio_location:"hong kong" OR bio_location:"hongkong")',
        ' (bio_location:"singapore")',
        ' (bio_location:"switzerland" OR bio_location:"zurich")'
    ]

    for (lang, terms) in aliases.items():
        subfix = ' (bio_lang:%s OR lang:%s OR twitter lang:%s)'\
                    % tuple([lang.lower() for i in xrange(3)])

        for loc in bio_locations:
            subfix_loc = subfix + loc


            #modified_terms = []

            #for term in terms:
            #    modified_terms.append(keyword_bio_contains(term))

            modified_terms = map(lambda term:
                                 'bio_contains:" ' + term + ' "' + ' OR ' + 'bio_contains:"' + term + ' "', terms)



            #max length of rule body, 2 space for '(' and ')'
            max_length = GNIP_RULE_MAX_LENGTH - len(subfix_loc) - 2
            rule_bodies = keywords_to_sentences(modified_terms, max_length= max_length)

            for body in rule_bodies:
                rules.append([class_name, lang, '(%s)%s' % (body, subfix_loc)])

    return rules


def process(file_req, file_alias=''):
    '''
    Generate rules
    '''
    # load data
    dfr = load_requirement('%s.csv' % file_req)
    dfa = load_aliases('%s.csv' % file_alias if file_alias else file_alias)

    # list of generating functions
    gen_funcs = [uc_1, uc_2]
    # use to store rules for each use case during generating process
    # then import to df before exporting
    rules = [[] for i in xrange(len(gen_funcs))]

    # process each class for all use cases
    for (i, r) in dfr.iterrows():
        aliases = { # get list keywords
            row.lang:row.string_agg.split(';')\
            for (idx, row) in dfa[dfa.class_name == r.lexicon_class].iterrows()
        }
        for uc in USE_CASES:
            if r['use_case_%i' % uc] == 'T':
                rules[uc - 1] += gen_funcs[uc - 1](r.lexicon_class, aliases)

    # export to excel
    writer = pd.ExcelWriter('%s_gen.xlsx' % file_req, engine='xlsxwriter')
    for uc in USE_CASES:
        pd.DataFrame(
            rules[uc - 1],
            columns=['class_name', 'lang', 'rule']
        ).to_excel(writer, 'use_case_%i' % (uc))          # write sheet
    writer.save()






def modified_process(file_req, file_alias=''):
    '''
    Generate rules
    '''
    dfr = load_requirement('%s.csv' % file_req)
    dfa = load_aliases('%s.csv' % file_alias if file_alias else file_alias)


    # list of generating functions
    gen_funcs = [uc_1, uc_2]
    # use to store rules for each use case during generating process
    # then import to df before exporting
    rules = [[] for i in xrange(len(gen_funcs))]
    # process each class for all use cases
    for (i, r) in dfr.iterrows():
        aliases = { # get list keywords
            row.lang:row.string_agg.split(';')\
            for (idx, row) in dfa[dfa.class_name == r.lexicon_class].iterrows()
        }
        for uc in USE_CASES:
            if r['use_case_%i' % uc] == 'T':
                rules[uc - 1] += gen_funcs[uc - 1](r.lexicon_class, aliases)

    # export to excel
    writer = pd.ExcelWriter('%s_gen.xlsx' % file_req, engine='xlsxwriter')
    for uc in USE_CASES:
        pd.DataFrame(
            rules[uc - 1],
            columns=['class_name', 'lang', 'rule']
        ).to_excel(writer, 'use_case_%i' % (uc))          # write sheet
    writer.save()




def test():
    s = ['mice','mouse','keys','jet','buses','ring']
    print isplural(s)



if __name__ == "__main__":
    #process each class for all use cases
    #print process('singleclass_20160219', 'alias')
    test()