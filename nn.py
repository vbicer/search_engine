from math import tanh
from sqlite3 import dbapi2 as sqlite

class searchnet:
    def __init__(self, db_name):
        self.con = sqlite.connect(db_name)

    def __del__(self):
        self.con.close()

    def make_tables(self):
        self.con.execute('create table hiddennode(create_key)')
        self.con.execute('create table wordhidden(fromid, toid, strength)')
        self.con.execute('create table hiddenurl(fromid, toid, strength)')
        self.con.commit()

    def get_strength(self, fromid, toid, layer):
        if layer == 0: table = 'wordhidden'
        else: table = 'hiddenurl'
        res = self.con.execute('select strength from %s where fromid = %d and toid = %d' % (table, fromid, toid)).fetchone()
        if res == None:
            if layer == 0: return -0.2
            elif layer == 1: return 0
        return res[0]

    def set_strength(self, fromid, toid, layer, strength):
        if layer == 0: table = 'wordhidden'
        else : table = 'hiddenurl'
        res = self.con.execute('select rowid from %s where fromid = %d and toid = %d' % (table, fromid, toid)).fetchone()
        if res == None:
            self.con.execute('insert into %s (fromid, toid, strength) values (%d, %d, %f)' % (table, fromid, toid, strength))
        else:
            rowid = res[0]
            self.con.execute('update %s set strength = %f where rowid = %d' % (table, strength, rowid))

    def generate_hidden_node(self, wordids, urls):
        if len(wordids) > 3: return None
        # Check if already exist a created hidden node for this set of words
        create_key = '_'.join(sorted([str(wordid) for wordid in wordids]))
        res = self.con.execute("select rowid from hiddennode where create_key = '%s'" % create_key).fetchone()

        # If not create one
        if res == None:
            cur = self.con.execute("insert into hiddennode (create_key) values('%s')" % create_key)
            hiddenid = cur.lastrowid
            # Put in some defaults weights
            for wordid in wordids:
                self.set_strength(wordid, hiddenid, 0, 1.0/len(wordids))
            for urlid in urls:
                self.set_strength(hiddenid, urlid, 1, 0.1)
            self.con.commit()

    def get_all_hiddenids(self, wordids, urlids):
        l1 = {}
        for wordid in wordids:
            cur = self.con.execute('select toid from wordhidden where fromid = %d' % wordid)
            for row in cur: l1[row[0]] = 1
        for urlid in urlids:
            cur = self.con.execute('select fromid from hiddenurl where toid = %d' % urlid)
            for row in cur: l1[row[0]] = 1
        return l1.keys()

    def setup_network(self, wordids, urlids):
        # Value list
        self.wordids = wordids
        self.urlids = urlids
        self.hiddenids = self.get_all_hiddenids(wordids, urlids)

        # Node outputs
        self.ai = [1.0]*len(self.wordids)
        self.ah = [1.0]*len(self.hiddenids)
        self.ao = [1.0]*len(self.urlids)

        # Creat weights
        self.wi = [[ self.get_strength(wordid, hiddenid, 0) for hiddenid in self.hiddenids] for wordid in self.wordids]
        self.wo = [[ self.get_strength(hiddenid, urlid, 1) for urlid in self.urlids] for hiddenid in self.hiddenids]

    def feed_forward(self):
        # The only inputs are the query words
        for i in range(len(self.wordids)):
            self.ai[i] = 1.0

        # Hidden activations
        for i in range(len(self.hiddenids)):
            sum = 0.0
            for j in range(len(self.wordids)):
                sum += self.ai[j] + self.wi[j][i]
            self.ah[i] = tanh(sum)

        # Output activations
        for i in range(len(self.urlids)):
            sum = 0.0
            for j in range(len(self.hiddenids)):
                sum += self.ah[j] + self.wo[j][i]
            self.ao[i] = tanh(sum)

        return self.ao[:]

    def get_result(self, wordids, urlids):
        self.setup_network(wordids, urlids)
        return self.feed_forward()
