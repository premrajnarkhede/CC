from boto.s3.connection import S3Connection
import io,zlib
from boto.s3.key import Key
import warc
from gzipstream import GzipStreamFile
import boto
import re
import dataset
from unidecode import unidecode
import time

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

dicta=makeDB("CoveredKeys")
conn= boto.connect_s3(anon=True,debug=2)
bucket = conn.get_bucket('commoncrawl')
list1=bucket.list(prefix="crawl-data/CC-NEWS")
thefile=open("keys.txt","wb")
#list1=bucket.get_all_keys(maxkeys=0)
for key in list1:
    print dir(key)
    print key    
    # if !1:
    #     for lindex,l in enumerate(GzipStreamFile(key)):
    #         print l
    #         raw_input()
            

