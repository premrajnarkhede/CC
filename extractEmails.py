from boto.s3.connection import S3Connection
import io,zlib
from boto.s3.key import Key
import warc
from gzipstream import GzipStreamFile
import boto
import re
import dataset
from unidecode import unidecode
def get_or_make_db(filename):
    import os
    if os.path.isfile(filename):
        pass
    else:
        f=open(filename,'wb+')
        f.close
    return os.path.realpath(filename)
def makeDB(nameofdb):
    from sqlitedict import SqliteDict
    sqlitedict_db=nameofdb+".db" #Do not put numbers in the name. causes some diskI.O error for some reason
    sqlitedict_db_path=get_or_make_db(sqlitedict_db)
    mydict=SqliteDict(sqlitedict_db_path,autocommit=True)
    return mydict
#conn = S3Connection('AKIAJZD2DSHMWJWLZISQ', 'vI1s7SDk7pVje38oYDGtzYldjJWTqPyvvYnnk4FD')
#prog = re.compile(r"(.{0,50}(^|\s|\*|!|:)(\S+@\S+\.\S+).{0,50})")
prog = re.compile("(^[-a-z0-9~!$%^&*_=+}{\'?]+(\.[-a-z0-9~!$%^&*_=+}{\'?]+)*@([a-z0-9_][-a-z0-9_]*(\.[-a-z0-9_]+)*\.(aero|arpa|biz|com|coop|edu|gov|info|int|mil|museum|name|net|org|pro|travel|mobi|[a-z][a-z])|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,5})?$)")
#print dir(prog)
string1="Baby Boomers (born 1945-1964) Personal characteristics: Optimistic, desire for personal gratification, highly competitive Work characteristics: Workaholic, competitive, consensus builder, mentor Education characteristics: Learner depends on educator, lecture format, process-oriented Communication style: Diplomatic Technology: Not particularly techno-saavy Generation premraj.narkhede@dolcera.com Xers (born 1964-1980) Personal characteristics: Independent, self-directed, skeptical, resilient, more accepting of diversity, self-reliant Work characteristics: Value work-life balance, comfortable with change, question authority Education characteristics: Independent learners, problem-solvers, desire to learn on the job, outcome-oriented Communication sonal.pingle@gmail.com"

result = prog.findall(string1)
db = dataset.connect('sqlite:///emailDB.db')
table=db["email"]
dicta=makeDB("CoveredKeys")
conn= boto.connect_s3(anon=True,debug=2)
bucket = conn.get_bucket('commoncrawl')
list1=bucket.list(prefix="crawl-data/CC-MAIN")

#list1=bucket.get_all_keys(maxkeys=0)
lookup=raw_input("Enter Lookup")
for key in list1:
    #print key
    #print dir(key)
    #print key.name
    if "wet" in key.name and lookup in key.name:
        print key
        if key.name in dicta:
            continue
        for l in GzipStreamFile(key):
            #print l
            result = prog.findall(l)
            for r in result:
                #print l
                #print r
                #raw_input()
                domain=r[0].split("@")[1]
                table.insert(dict(domain=unidecode(domain),email=unidecode(r[0]),text=line))
        dicta[key.name]=1
    else:
        print "wet not there"
