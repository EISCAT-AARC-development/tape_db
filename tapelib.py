#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Tapelib.py
SQL file catalogue routines, python3 version for AARC server
Carl-Fredrik Enell, EISCAT
'''

# warning filter CFE 20160126
import urllib.parse
import warnings
from functools import reduce

tape_tables = {
    'experiments': '''
    experiment_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    experiment_name varchar(255) NOT NULL,
    country char(2) NULL,
    antenna char(3) NOT NULL,
    comment blob,
    UNIQUE (experiment_name, antenna)
    ''',
    'resource': '''
    resource_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    experiment_id int NOT NULL,
    start datetime NOT NULL,
    end datetime NOT NULL DEFAULT '1970-01-01',
    comment blob,
    type enum('data', 'info') NOT NULL,
    account varchar(99),
    UNIQUE (experiment_id, start, end, type),
    INDEX (start)
    ''',
    'storage': '''
    location varchar(255) NOT NULL PRIMARY KEY,
    resource_id int NOT NULL,
    priority int NOT NULL DEFAULT 50,
    bytes bigint unsigned,
    comment blob,
    INDEX (resource_id)
    ''',
    'tape_comments': '''
    tape_nr int NOT NULL PRIMARY KEY UNIQUE,
    comment blob
    ''',
}

class nicedict(dict):
    """A dictionary that behaves like an object, i.e. you can access the
    variables as direct members"""
    def __getattr__(self, attr):
        return self[attr]
    def __setattr__(self, attr, value):
        self[attr] = value

class Conn:
    def __init__(self, dbi, conn):
        """Wraps a database connection with eiscat specific methods.
        dbi is the database library module and
        conn is the connection."""
        assert dbi.paramstyle == 'format', "%s is used for value substitution"
        self.dbi, self.conn = dbi, conn
        self.cur = conn.cursor()

    def check_timezone(self):
        self.cur.execute("SELECT UNIX_TIMESTAMP('1970-01-02')")
        secs = 86400 - self.cur.fetchone()[0]
        if secs:
            raise Exception("The timezone on mysqld is set wrong, %+.1f hours" % (secs/3600.0))

    def select_sql(self, sql, objs, limit=None):
        c = self.cur
        if limit:
            sql += " LIMIT %d" % limit
        c.execute(sql, objs)
        valuess = c.fetchall()
        arry = []
        for values in valuess:
            dict = nicedict()
            for col_info, value in zip(c.description, values):
                name = col_info[0]
                dict[name] = value
            arry.append(dict)
        return arry

    def union_select(self, table, limit=None, **kwords):
        sql = ""
        l = []
        for key, value in list(kwords.items()):
            if type(value) == type('') and '%' in value:
                l.append(key+" LIKE %s")
            else:
                l.append(key+" = %s")
        sql += " AND ".join(l)
        sql = "SELECT * FROM " + table + " WHERE " + sql + " UNION SELECT * FROM ***REMOVED***." + table + " WHERE " + sql
        return self.select_sql(sql, list(kwords.values())+list(kwords.values()), limit=limit)

    def select(self, table, limit=None, **kwords):
        sql = "SELECT * FROM " + table + " WHERE "
        l = []
        for key, value in list(kwords.items()):
            if type(value) == type('') and '%' in value:
                l.append(key+" LIKE %s")
            else:
                l.append(key+" = %s")
        sql += " AND ".join(l)
        return self.select_sql(sql,list(kwords.values()), limit=limit)

    def select_experiment_resource_union(self, query, values=(), what="*", limit=None):
        sql = "SELECT " + what + " FROM experiments, resource WHERE experiments.experiment_id = resource.experiment_id AND " + query + " UNION SELECT " + what + " FROM ***REMOVED***.experiments, ***REMOVED***.resource WHERE experiments.experiment_id = resource.experiment_id AND " + query
        return self.select_sql(sql, values+values, limit=limit)
        

    def select_experiment_storage_union(self, query, values=(), what="*", limit=None):
        sql = "SELECT " + what + " FROM experiments, resource, storage WHERE experiments.experiment_id = resource.experiment_id AND resource.resource_id = storage.resource_id AND " + query + "UNION SELECT " + what + " FROM ***REMOVED***.experiments, ***REMOVED***.resource, ***REMOVED***.storage WHERE experiments.experiment_id = resource.experiment_id AND resource.resource_id = storage.resource_id AND "+ query
        return self.select_sql(sql, values+values, limit=limit)

    def select_experiment_resource(self, query, values=(), what="*", limit=None):
        sql = "SELECT " + what + " FROM experiments, resource WHERE experiments.experiment_id = resource.experiment_id AND " + query
        return self.select_sql(sql, values, limit=limit)

    def select_experiment_storage(self, query, values=(), what="*", limit=None):
        sql = "SELECT " + what + " FROM experiments, resource, storage WHERE experiments.experiment_id = resource.experiment_id AND resource.resource_id = storage.resource_id AND " + query
        return self.select_sql(sql, values, limit=limit)

    def select_resource_storage(self, query, values=(), what="*", limit=None):
        sql = "SELECT " + what + " FROM resource, storage WHERE resource.resource_id = storage.resource_id AND " + query
        return self.select_sql(sql, values, limit=limit)

    def insert(self, table, **kwords):
        sql = "INSERT INTO " + table + " (" + ", ".join(list(kwords.keys())) + ")"
        sql += " VALUES (" + ','.join(["%s"]*len(kwords)) + ")"
        self.cur.execute(sql, list(kwords.values()))

    def delete(self, table, like=0, **kwords):
        sql = "DELETE FROM " + table + " WHERE "
        like = like and " LIKE " or " = "
        sql += " AND ".join([key + like + "%s" for key in list(kwords.keys())])
        return self.cur.execute(sql, list(kwords.values()))

    def create(self):
        """Create the tables needed. Beware: they will be purged if they
        exist already"""
        self.check_timezone()
        c = self.cur
        for table, content in list(tape_tables.items()):
            c.execute("DROP TABLE IF EXISTS " + table)
            c.execute("CREATE TABLE " + table + " (" + content + ")")
        self.conn.commit()

    def alter(self):
        """Create the tables needed."""
        self.check_timezone()
        c = self.cur
        for table, content in list(tape_tables.items()):
            cmd = "SHOW INDEX FROM " + table
            c.execute(cmd)
            indices = {}
            for t in c.fetchall():
                index = t[2]
                if index != 'PRIMARY':
                    indices[index] = 1
            cmd = "SHOW COLUMNS FROM " + table
            c.execute(cmd)
            columns = {}
            for t in c.fetchall():
                column = t[0]
                columns[column] = 1

            cmd = "ALTER IGNORE TABLE " + table + " DROP PRIMARY KEY"
            for index in indices:
                cmd += ",\nDROP INDEX " + index
            for s in content.split(',\n'):
                s = ' '.join(s.split())
                if s[0].isupper():
                    cmd += ',\nADD ' + s
                elif s.split()[0] in columns:
                    cmd += ',\nMODIFY ' + s
                    del columns[s.split()[0]]
                else:
                    cmd += ',\nADD '+s
            for s in columns:
                cmd += ',\nDROP ' + s
            eval(input("\n"+ cmd + " [press enter to continue] "))
            try:
                c.execute(cmd)
            except (self.dbi.Error, self.dbi.Warning) as why:
                print(why)
        self.conn.commit()

    def get_experiment_info(self, **kwords):
        """Get an dictionary containing the information available
        about an experiment"""
        return self.select("experiments", **kwords)

    def update_experiment(self, experiment_name, country, antenna, update_country=0):
        c = self.cur
        update = 0
        try:
            c.execute("INSERT INTO experiments (experiment_name, country, antenna) VALUES (%s, %s, %s)", (experiment_name, country, antenna))
        except self.dbi.IntegrityError:
            update = 1
        c.execute("SELECT experiment_id, country FROM experiments WHERE experiment_name = %s AND antenna = %s", (experiment_name, antenna))
        id, prev_country = c.fetchone()
        if update and country and (not prev_country or update_country):
            c.execute("UPDATE experiments SET country = %s WHERE experiment_id = %s", (country, id))
        return id

    def update_resource(self, experiment_id, start, end, typestring, account, bugfix=1):
        c = self.cur
        if end is None:
            end = 0
        c.execute("SELECT resource_id FROM resource WHERE experiment_id = %s AND start = FROM_UNIXTIME(%s) AND end = FROM_UNIXTIME(%s) AND type = %s", (experiment_id, start, end, typestring))
        resource_id = c.fetchone()
        resource_id = resource_id and resource_id[0]
        if typestring == 'data' and bugfix and not resource_id:
            c.execute("SELECT resource_id, UNIX_TIMESTAMP(start) FROM resource WHERE experiment_id = %s AND (UNIX_TIMESTAMP(end) BETWEEN %s AND %s) AND (UNIX_TIMESTAMP(start) BETWEEN %s AND %s)", (experiment_id, end-2, end+2, start-61, start+61))
            resource_id = c.fetchone()
            if resource_id:
                resource_id, prev_start = resource_id
                if prev_start > start:
                    c.execute("UPDATE resource SET start = FROM_UNIXTIME(%s) WHERE resource_id = %s", (start, resource_id))
                else:
                    start = prev_start
        if not resource_id:
            c.execute("INSERT INTO resource (experiment_id, start, end, type, account) VALUES (%s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), %s,%s)", (experiment_id, start, end, typestring, account))
            c.execute("SELECT LAST_INSERT_ID()")
            resource_id = c.fetchone()[0]
        return resource_id

    def update_location(self, resource_id, location, bytes, priority=50):
        self.remove_location(location)
        self.insert_location(resource_id=resource_id, location=location, bytes=bytes, priority=priority)

    def update(self, experiment_name, country, antenna, start, end,
               location, typestring, account=None, comment=None, priority=50,
               bytes=None, update_country=0):
        """Add an item and location into the database.
        If it already exists, do nothing"""
        experiment_id = self.update_experiment(experiment_name, country=country, antenna=antenna, update_country=update_country)
        resource_id = self.update_resource(experiment_id, start, end, typestring, account)
        self.update_location(resource_id, location, bytes=bytes, priority=priority)

    def set_tape_comment(self, tape_nr, comment):
        self.insert("tape_comments", tape_nr=tape_nr, comment=comment)
        self.conn.commit()

    def get_tape_comment(self, tape_nr):
        self.cur.execute("SELECT comment FROM tape_comments WHERE tape_nr = %s", (tape_nr,))
        comment = self.cur.fetchone()
        if comment:
            return comment[0]

    def get_stored_like(self, source, limit=None):
        cmd = "SELECT * FROM storage WHERE location LIKE %s"
        res = self.select_sql(cmd, (source,), limit=limit)
        return res

    def check_urls_backwards(self, locations, keys):
        """Same as multiple get_stored_like but uses an temporary table
        to fast match against the ending"""
        c = self.cur
        warnings.filterwarnings("ignore", '.*TEMPORARY.*TABLE.*')
        c.execute("CREATE TEMPORARY TABLE temp SELECT REVERSE(location) AS string FROM storage")
        c.execute("CREATE INDEX idx1 ON temp (string)")
        command = "SELECT COUNT(*) FROM temp WHERE string LIKE %s LIMIT 1"
        res = []
        for x, k in zip(locations, keys):
            c.execute(command, (x[::-1],))
            if not c.fetchone()[0]:
                res.append(k)
        c.close()
        return res

    def change_location(self, prev_location, new_location):
        cmd = "UPDATE storage SET location = %s WHERE location = %s"
        self.cur.execute(cmd, (new_location, prev_location))

    def remove_location(self, location, like=0):
        if like:
            cmd = "DELETE FROM storage WHERE location LIKE %s"
        else:
            cmd = "DELETE FROM storage WHERE location = %s"
        return self.cur.execute(cmd, (location,))

    def insert_location(self, **infodict):
        self.insert("storage", **infodict)

    def select_location(self, location):
        return self.select("storage", location=location)

    def purge_broken_links(self, dry=0, backwards=0, verbose=1):
        """Find entries which parent id does not exist.
        Delete these entries. This can probably be made automatically
        with some magic sql reference statements..."""
        c = self.cur
        def sub(child_table, parent_table, variable, c=c, dry=dry, verbose=verbose):
            cmd = "SELECT DISTINCT " + child_table + "." + variable + " FROM " + child_table + " LEFT JOIN " + parent_table + " ON " + child_table + "." + variable + " = " + parent_table + "." + variable + " WHERE " + parent_table + "." + variable + " IS NULL"
            c.execute(cmd)
            ids = [x for x, in c.fetchall()]
            if ids:
                if verbose:
                    print("Broken ids "+' '.join([str(s) for s in ids])+" from "+child_table)
                if not dry:
                    blocklen = 10000
                    i = -1
                    if verbose:
                        print("...deleting")
                    for i in range(len(ids)/blocklen):
                        cmd = "DELETE FROM " + child_table + " WHERE " + variable + " IN ("+','.join(['%s']*blocklen) + ')'
                        c.execute(cmd, ids[i*blocklen:((i+1)*blocklen)])
                    cmd = "DELETE FROM " + child_table + " WHERE " + variable + " IN ("+','.join(['%s']*(len(ids)-(i+1)*blocklen)) + ')'
                    c.execute(cmd, ids[(i+1)*blocklen:len(ids)])
        sub("resource", "experiments", "experiment_id")
        sub("storage", "resource", "resource_id")
        if backwards:
            sub("experiments", "resource", "experiment_id")
            sub("resource", "storage", "resource_id")

    def select_union_resource(self, experiment_id, start, end, limit=None):
        return self.select_resource(experiment_id, start, end, "", limit)+self.select_resource(experiment_id, start, end, "***REMOVED***", limit)

    def select_resource(self, experiment_id, start, end, db=None, limit=None):
        if not db:
            db = ""
        else:
            db = db + "."
        what = "*, UNIX_TIMESTAMP(start) AS unix_start, UNIX_TIMESTAMP(end) AS unix_end"
        cmd = "SELECT " + what + " FROM " + db + "resource WHERE experiment_id = %s AND start < FROM_UNIXTIME(%s) AND end > FROM_UNIXTIME(%s)"
        dataset1 = self.select_sql(cmd, (experiment_id, end, start), limit=limit)
        cmd = "SELECT type, MAX(start) FROM " + db + "resource WHERE experiment_id = %s AND end = FROM_UNIXTIME(0) AND start <= FROM_UNIXTIME(%s) GROUP BY type"
        c = self.cur
        c.execute(cmd, (experiment_id, start))
        special = c.fetchall()
        cmd = "SELECT " + what + " FROM " + db + "resource WHERE experiment_id = %s AND end = FROM_UNIXTIME(0) AND (start > FROM_UNIXTIME(%s) AND start < FROM_UNIXTIME(%s)"
        args = [experiment_id, start, end]
        for tuple in special:
            cmd += " OR type = %s AND start = %s"
            args.extend(tuple)
        cmd += ")"
        dataset2 = self.select_sql(cmd, args, limit=limit)
        return dataset1+dataset2


    def check_overlapping(self):
        cmd = "SELECT experiment_name, antenna, a.start, a.end, b.start, b.end FROM resource a, resource b, experiments WHERE a.experiment_id = b.experiment_id AND experiments.experiment_id = a.experiment_id AND a.resource_id != b.resource_id AND a.end > a.start AND b.end > b.start AND a.start <= b.start AND a.end > b.start AND a.type = b.type LIMIT 1"
        c = self.cur
        c.execute(cmd)
        tuple = c.fetchone()
        if tuple:
            print("Colliding data on experiment %s@%s. [%s,%s] and [%s,%s]."%tuple)

    def update_account(self, resource_id, account):
        c = self.cur
        c.execute("UPDATE resource SET account = %s WHERE resource_id = %s", (account, resource_id))

    def close(self):
        "Closes the connection. The object is unusable after this call."
        self.conn.commit()
        self.conn.close()
        del self.conn
        del self.dbi

def openMySQL(**params):
    import MySQLdb
    db = MySQLdb.connect(**params)
    return Conn(MySQLdb, db)

def openMySQL_SSH(host, port=3306, interactive=0, **params):
    import os
    import random
    import subprocess
    import shlex
    for i in range(5):
        localport = random.randrange(1025, 65535)
        cmd = "ssh -x -L %d:localhost:%d %s 'echo OK; cat'" % (localport, port, host)
        if not interactive:
            cmd = "ssh -o 'Batchmode yes'"+cmd[3:]
        subproc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        l = subproc.stdout.readline()
        if l == 'OK\n':
            break
            # st = subproc.wait()
        else:
            raise IOError("Cannot open ssh tunnel")
    conn = openMySQL(host="127.0.0.1", port=localport, **params)
    subproc.stdin.close()
    return conn

def openmaster():
    if nodename() in ("data1", "eiscathq"):
        return openMySQL(host="192.168.11.5", passwd='***REMOVED***', db='***REMOVED***', user='***REMOVED***')
    else:
        return openMySQL(host="localhost", passwd='***REMOVED***', db='***REMOVED***', user='***REMOVED***')

def opendefault():
    return openMySQL(host='localhost', db='***REMOVED***', user='***REMOVED***')

############# URL handling routines ########################
_cached_nodename = None
def nodename():
    """return a name to be used in URLs"""
    return "data1"


if 'eiscat-raid' not in urllib.parse.uses_netloc:
    urllib.parse.uses_netloc.append('eiscat-raid')
if 'eiscat-raid' not in urllib.parse.uses_query:
    urllib.parse.uses_query .append('eiscat-raid')
if 'eiscat-tape' not in urllib.parse.uses_netloc:
    urllib.parse.uses_netloc.append('eiscat-tape')
if 'eiscat-tape' not in urllib.parse.uses_query:
    urllib.parse.uses_query .append('eiscat-tape')

def parse_raidurl(url):
    """split a eiscat-raid://host/path?flag1+flag2 url into
    (node, path, flags_list). Returns None if the url is not raid-type"""
    scheme, node, path, query, fragment = urllib.parse.urlsplit(url)
    if scheme == "eiscat-raid" and ':' not in node:
        flags = query and query.split('+') or []
        return node, path, flags

def create_raidurl(node, path, flags=()):
    """create a eiscat-raid://host/path?flag1+flag2 url from parts."""
    query = flags and '+'.join(flags) or ''
    return urllib.parse.urlunsplit(("eiscat-raid", node, path, query, ''))

def parse_tapeurl(url):
    """split a eiscat-tape://tapenr/path url into
    (tapenr, path). Returns None if the url is not tape-type"""
    scheme, node, path, query, fragment = urlparse.urlsplit(url)
    if scheme == "eiscat-tape" and node.isdigit():
        return (node, path[1:])

def create_tapeurl(tapenr, path):
    """create a eiscat-tape://tapenr/path url from (tapenr, path)."""
    return urllib.parse.urlunsplit(("eiscat-tape", str(tapenr), '/'+path, '', ''))

############### merge two databases ##########
def merge(localconn, remoteconn, verbose=0):
    import time, operator
    assert localconn != remoteconn
    sql_pat = "SELECT %s FROM experiments, resource, storage WHERE experiments.experiment_id = resource.experiment_id AND resource.resource_id = storage.resource_id"
    r = remoteconn.cur
    l = localconn.cur
    r.execute(sql_pat%"COUNT(*)")
    n = r.fetchone()[0]
    if verbose:
        print("Importing %d sources..."%n)
    r.execute(sql_pat%"experiment_name, country, antenna, UNIX_TIMESTAMP(start), UNIX_TIMESTAMP(end), location, type, account, storage.comment, priority, bytes")
    i = 0
    t0 = time.time()
    if verbose:
        printr("Source %d/%d %.2f%% ETA ? min"%(i, n, i*100.0/n))
    while 1:
        res = r.fetchmany()
        if not res:
            break
        i += len(res)
        for tuple in res:
            localconn.update(*tuple, **{'update_country':0})
        if verbose:
            print("Source %d/%d %.2f%% ETA %d min"%(i, n, i*100.0/n, (n-i)*(time.time()-t0)/i/60))
    sql_pat = "SELECT %s FROM tape_comments"
    r.execute(sql_pat%"COUNT(*)")
    n = r.fetchone()[0]
    if verbose:
        print("Importing %d tape comments..."%n)
    r.execute(sql_pat%"tape_nr, comment")
    i = 0
    t0 = time.time()
    while 1:
        res = r.fetchmany()
        if not res:
            break
        i += len(res)
        l.execute("INSERT IGNORE INTO tape_comments (tape_nr, comment) VALUES (%s,%s)"+",(%s,%s)"*(len(res)-1), reduce(operator.add, res))
        if verbose:
            print("Source %d/%d %.2f%% ETA %d min"%(i, n, i*100.0/n, (n-i)*(time.time()-t0)/i/60))

############### main #########################
def parse_time(string, latest):
    import time, calendar
    for format, granularity in (
            ("%Y", 356*86400),
            ("%Y%m", 31*86400),
            ("%Y%m%d", 86400),
            ("%Y%m%d %H", 3600),
            ("%Y%m%d %H:%M", 60),
            ("%Y%m%d %H:%M:%S", 1),
            ("%Y-%m-%d", 86400),
            ("%Y-%m-%d %H", 3600),
            ("%Y-%m-%d %H:%M", 60),
            ("%Y-%m-%d %H:%M:%S", 1),
            ("%Y-%m-%d %H", 3600),
            ("%Y-%m-%d %H%M", 60),
            ("%Y-%m-%d %H%M%S", 1),
            ("%Y%m%d %H", 3600),
            ("%Y%m%d %H%M", 60),
            ("%Y%m%d %H%M%S", 1),
            ("%Y-%m", 31*86400),
    ):
        try:
            date = calendar.timegm(time.strptime(string, format))
            return date + latest*granularity
        except ValueError:
            pass
    raise

def parse_times(start, end=None):
    return parse_time(start, 0), parse_time(end or start, 1)

if __name__ == '__main__':
    import sys
    args = sys.argv[1:]
    cmd = args.pop(0)
    if cmd == 'create':
        import getpass
        p = getpass.getpass("admin passwd: ")
        c = openMySQL(passwd=p, db='***REMOVED***', user='root')
        c.create()
        c.close()
    elif cmd == 'alter':
        import getpass
        p = getpass.getpass("admin passwd: ")
        c = openMySQL(passwd=p, db='***REMOVED***', user='root')
        c.alter()
        c.close()
    elif cmd == 'clean':
        c = openmaster()
        c.purge_broken_links()
        c.close()
    elif cmd == 'cleanall':
        c = openmaster()
        c.purge_broken_links(backwards=1)
        c.close()
    elif cmd == 'check':
        c = openmaster()
        c.check_timezone()
        c.purge_broken_links(dry=1, backwards=1)
        c.check_overlapping()
        c.close()
    elif cmd == 'listlocations':
        c = openmaster()
        a = c.get_stored_like(args[0])
        c.close()
        for x in a:
            print(x.location)
    elif cmd == 'filterlocations':
        c = openmaster()
        print(' '.join(c.check_urls_backwards(args, args)))
        c.close()
    elif cmd == 'remove_experiment':
        db_conn = openmaster()
        n = 0
        for arg in args:
            n += db_conn.delete("experiments", experiment_name=arg)
        if n:
            print(n, "experiments removed")
            print("You may want to run 'python tapelib.py cleanall' to remove the affected resources and locations as well")
    elif cmd == 'remove_antennas':
        db_conn = openmaster()
        n = 0
        for arg in args:
            n += db_conn.delete("experiments", antenna=arg)
        if n:
            print(n, "experiments removed")
            print("You may want to run 'python tapelib.py cleanall' to remove the affected resources and locations as well")
    elif cmd == 'remove_urls':
        db_conn = openmaster()
        n = 0
        for arg in args:
            n += db_conn.remove_location(arg, like=1)
        if n:
            print(n, "locations removed")
            print("You may want to run 'python tapelib.py cleanall' to remove the affected resources and experiments as well")
    elif cmd == 'list':
        c = opendefault()
        try:
            experiment_name, antenna = args[0].split('@')
            start, end = parse_times(args[1], len(args) > 2 and args[2])
        except (ValueError, IndexError):
            print("Usage: list experiment@antenna date [enddate]")
            print("Example: list tau0@* '2001-12-13 12' '2001-12-14 08'")
            raise
        else:
            experiment_name = experiment_name.replace('*', '%')
            antenna = antenna.replace('*', '%')
            print("Querying", experiment_name, antenna, start, end)
            experiments = c.select("experiments", experiment_name=experiment_name, antenna=antenna)
            for e in experiments:
                datasets = c.select_resource(e.experiment_id, start, end)
                if not datasets:
                    continue
                print("Experiment "+e.experiment_name+"@"+e.antenna,)
                if e.country:
                    print("("+e.country+")",)
                print()
                for d in datasets:
                    if d.unix_end:
                        print("  "+d.type+" "+d.start+" - "+d.end)
                    else:
                        print("  "+d.type+" "+d.start+" -")
                    storages = c.select("storage", resource_id=d.resource_id)
                    for s in storages:
                        print("    "+s.location+" (%s bytes)"%s.bytes)
    elif cmd == 'fix':
        conn = openmaster()
        n = conn.cur.execute("SELECT storage.resource_id FROM resource, storage WHERE resource.resource_id = storage.resource_id AND location LIKE %s", ("eiscat-raid://eiscathq/%",))
        n = conn.cur.fetchall()
        from sets import Set
        n = Set(n)
        print(len(n))
        m = conn.cur.execute("SELECT storage.resource_id FROM resource, storage WHERE resource.resource_id = storage.resource_id AND location LIKE %s", ("eiscat-raid://data1/%",))
        m = conn.cur.fetchall()
        m = Set(m)
        print(len(m))
        print(len(n-m))
    elif cmd == 'addquota':
        sql = openmaster()
        import os
        list = open(os.environ['HOME']+'/quota.txt').read()
        for line in list.splitlines():
            l = line.split()
            ls = sql.select_experiment_storage("experiments.antenna=%s AND experiment_name=%s AND start=%s AND end=%s AND type='data'", (l[1], l[0], l[2]+' '+l[3], l[4]+' '+l[5]), what="resource.resource_id AS r,account")
            r = -1
            if ls:
                r = ls[0].r
                for lr in ls:
                    if lr.r != r:
                        r = 0
            if r < 1:
                print('Cannot add: '+line)
            else:
                acc = ls[0].account
                if acc and acc != l[6]:
                    print(line+' Old account: '+acc)
                if not acc or acc != l[6]:
                    sql.update_account(r, l[6])
    else:
        print("Command "+cmd+" not understood.")
