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
        words = self.separate_words(link_text)
        fromid = self.get_entry_id('urllist', 'url', url_from)
        toid = self.get_entry_id('urllist', 'url', url_to)

        if fromid == toid: return
        linkid = self.con.execute('insert into link (fromid, toid) values (%d, %d)' % (fromid, toid)).lastrowid
        for word in words:
            if word in self.ingore_words: continue
            wordid = self.get_entry_id('wordlist', 'word', word)
            self.con.execute('insert into linkwords (wordid, linkid) values (%d, %d)' % (wordid, linkid))

    def calculate_page_rank(self, iterations=20):
        # Clear current page rank table_list
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key, score)')

        # Initialize every url with page rank 1
        self.con.execute('insert into pagerank select rowid, 1.0 from urllist')
        self.db_commit()

        for i in range(iterations):
            print('Iteration', i)
            for urlid, in self.con.execute('select rowid from urllist'):
                pr = 0.15
                # Loop through all the pages that link to this one
                for linker, in self.con.execute('select distinct fromid from link where toid = %d' % urlid):
                    # Get the page rank of linker
                    linker_page_rank = self.con.execute('select score from pagerank where urlid = %d' % linker).fetchone()[0]
                    linking_count = self.con.execute('select count(*) from link where fromid = %d' % linker).fetchone()[0]
                    pr += 0.85 * (linker_page_rank/linking_count)
                    print(pr, urlid)
                    self.con.execute('update pagerank set score = %f where urlid = %d' % (pr, urlid))
                self.db_commit()

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

        weights = [(1.0, self.locaiton_scores(rows)),
                   (1.0, self.frequency_score(rows)),
                   (1.0, self.page_rank_score(rows)),
                   (1.0, self.link_text_score(rows, word_ids))]

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
                word_ids.append(wordid)
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

    # Each of the scoring functions calls this function to normalize its result
    # Return value between 0 and 1
    def normalize_scores(self, scores, small_better=0):
        vsmall = 0.00001 # Avoid division by zero errors
        if small_better:
            min_score = min(scores.values())
            return dict([(u, float(min_score/max(vsmall,l)))for u,l in scores.items()])
        else:
            max_score = max(scores.values())
            return dict([ (u, float(c)/max_score) for u, c in scores.items()])

    def frequency_score(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows: counts[row[0]] += 1
        return self.normalize_scores(counts)

    def locaiton_scores(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]: locations[row[0]] = loc
        return self.normalize_scores(locations, small_better=1)

    def distance_score(self, rows):
        # If there is only one word then everbody wins
        if len(rows[0]) < 2: return dict([(row[0], 1.0) for row in rows])

        min_distance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            distance = sum([abs(row[i], row[i-1]) for i in range(2, len(row))])
            if distance < min_distance[row[0]]: min_distance[row[0]] = distance
        return self.normalize_scores(min_distance, small_better=1)

    def inbound_link_score(self, rows):
        urls = selt([row[0] for row in rows])
        inbound_count = dict([(id, self.con.execute('select count(*) from link where toid = %d' % id)) for id in urls])
        return self.normalize_scores(inbound_count)

    def page_rank_score(self, rows):
        urls = set([row[0] for row in rows])
        page_ranks = dict([(urlid, self.con.execute('select score from pagerank where urlid = %d' % urlid).fetchone()[0]) for urlid in urls])
        return self.normalize_scores(page_ranks)

    def link_text_score(self, rows, word_ids):
        link_scores = dict([(row[0], 0) for row in rows])
        for wordid in word_ids:
            cur = self.con.execute('select link.fromid, link.toid from linkwords, link where wordid = %d and linkwords.linkid = link.rowid' % wordid)
            for fromid, toid in cur:
                if toid in link_scores:
                    page_rank = self.con.execute('select score from pagerank where urlid = %d' % fromid).fetchone()[0]
                    link_scores[toid] += page_rank
        return self.normalize_scores(link_scores)
