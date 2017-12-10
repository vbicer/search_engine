import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sqlite3
import re

class crawler:
    ingore_words = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'are', 'it'])
    http = urllib3.PoolManager()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Initialize the crawler with database name
    def __init__(self, db_name):
        self.con = sqlite3.connect(db_name)

    def __del__(self):
        self.con.close()

    def db_commit(self):
        self.con.commit()

    def get_entry_id(self, table, field, value, createnew= True):
        cur = self.con.execute("select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res != None:
            return res[0]
        elif createnew:
            cur = self.con.execute("insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        return None


    # Index at indivual page
    def add_to_index(self, url, soup):
        if self.is_indexed(url): return
        print('Indexing', url)

        text = self.get_text_only(soup)
        words = self.separate_words(text)

        # Get url id
        urlid = self.get_entry_id('urllist', 'url', url)

        # Link each words to this url
        for i in range(len(words)):
            word = words[i]
            if word in self.ingore_words: continue
            wordid = self.get_entry_id('wordlist', 'word', word)
            self.con.execute('insert into wordlocation(urlid, wordid, location) values (%d,%d,%d)' % (urlid, wordid, i))



    # Extract the text from HTML page
    def get_text_only(self, soup):
        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.extract()    # rip it out

        text = ''
        for s in soup.stripped_strings:
            text += repr(s) + '\n'
        return text

    # Separate the words by any non-whitespace character
    def separate_words(self, text):
        splitter = re.compile('\\W+')
        return [s.lower() for s in splitter.split(text) if s != '']

    def is_indexed(self, url):
        urlid = self.get_entry_id('urllist','url',url,False)
        if urlid != None:
            # Check if it has actually been crawled
            wordid = self.con.execute("select rowid from wordlocation where urlid=%d" % urlid).fetchone()
            if wordid != None: return True
        return False

    # Add link between two pages
    def add_link_ref(self, url_from, url_to, link_text):
        pass

    def crawl(self, pages, depth= 2):
        for i in range(depth):
            print(20*'#','Scanning', 20*'#')
            new_pages = set()
            for page in pages:
                try:
                    response = self.http.request('GET', page)
                except Exception as ex:
                    print("Couldn't open page", page)
                    continue
                soup = BeautifulSoup(response.data, 'html.parser')
                self.add_to_index(page, soup)

                links = soup.find_all('a')
                # print(links)
                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1: continue
                        url = url.split('#')[0] # remove location portion
                        protocol = url[:4]
                        if protocol == 'http'  and not self.is_indexed(url):
                            new_pages.add(url)
                        link_text = self.get_text_only(link)
                        self.add_link_ref(page, url, link_text)
                self.db_commit()
            pages = new_pages
        print(20*'#','Crawling Completed', 20*'#')

    def create_index_tables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid, wordid, location)')
        self.con.execute('create table link(fromid integer, toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlindex on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.db_commit()

class searcher:

    def __init__(self, db_name):
        self.con = sqlite3.connect(db_name)

    def __del__(self):
        self.con.close()

    def get_url_name(self, urlid):
        return self.con.execute('select url from urllist where rowid = %d' % urlid).fetchone()[0]

    def get_scored_list(self, rows, word_ids):
        total_scores = dict([(row[0], 0) for row in rows])

        weights = []

        for weight, scores in weights:
            for url in total_scores:
                total_scores[url] += weight * scores[url]
        return total_scores

    def get_match_rows(self, query):
        field_list = 'w0.urlid'
        table_list = ''
        clause_list = ''
        word_ids = []

        words = query.split(' ')
        table_number = 0

        for word in words:
            wordrow = self.con.execute("select rowid from wordlist where word='%s'" % word).fetchone()
            if wordrow != None:
                wordid = wordrow[0]
                word_ids.append(word)
                if table_number > 0:
                    table_list += ' ,'
                    clause_list += ' and w%d.urlid = w%d.urlid and ' %(table_number-1, table_number)
                field_list += ' ,w%d.location' % table_number
                table_list += ' wordlocation w%d' % table_number
                clause_list += ' w%d.wordid = %d' % (table_number, wordid)
                table_number += 1

        sql_query = "select %s from %s where %s" % (field_list, table_list, clause_list)    
        cur = self.con.execute(sql_query)
        rows = [row for row in cur]
        return rows, word_ids

    def query(self, q):
        rows, word_ids = self.get_match_rows(q)
        scores = self.get_scored_list(rows, word_ids)
        ranked_scores = sorted([(score, url)for url, score in scores.items()], reverse=1)
        for (score, url) in ranked_scores:
            print(score,' -> ', self.get_url_name(url))
