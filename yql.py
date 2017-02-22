#!/usr/bin/python
#!/usr/bin/python
import sys
import os
import re
import urllib2, urllib, json
from yahoo_oauth import OAuth1, OAuth2
from bs4 import BeautifulSoup
import requests
from collections import defaultdict

from model.Base import Base
from model.HttpReq import HttpReq
from model.DnsReq import DnsReq

from sqlalchemy import create_engine, text, or_, not_, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool


# Yahoo YQL
baseurl = "https://query.yahooapis.com/v1/public/yql?"

output = "/home/apietila/work/dataprojects/ucnstudy/data/yql"

def fetch_head(domain, oauth):
    yql_query = "select * from html where url='%s' and xpath='/html/head'"%domain
    yql_url = baseurl + urllib.urlencode({'q':yql_query}) + "&format=json"

    if not oauth.token_is_valid():
        oauth.refresh_access_token()    
        
    response = oauth.session.get(yql_url)
    data = json.loads(result)
    print json.dumps(data)
    
#    with open("%s/%s_head.json"%(output,domain)) as f:
#        json.dump(data, f)

def fetch_page(domain, https=False):
    if domain == None or len(domain) == 0 or domain == u'':
        return

    raw = "%s/%s.html"%(output,domain)
    raw2 = raw.replace('.html','.json')
    url = "http://%s"%domain
    if (https):
        url = "https://%s"%domain

    if (os.path.exists(raw) or os.path.exists(raw2)):
        return

    print url,raw,raw2

    text = None
    try:
        page = requests.get(url, timeout=2)
        print 'request',page.status_code
        if (page.status_code >= 200 and page.status_code < 300):
            with open(raw,'wb') as f:
                for chunk in page.iter_content(2048):
                    f.write(chunk)
                f.flush()
        text = page.text
    except Exception as e:
        print '%s (%s)' % (e.message, type(e))

    if (text == None or len(text) == 0):
        return

    # extract all metatags to a dict (meta key -> values)
    desc = defaultdict(list)
    soup = BeautifulSoup(text, 'lxml')

    for meta in soup.findAll("meta"):
        metaname = meta.get('name', '').lower()
        if (metaname != None and len(metaname) > 0 and 'content' in meta):
            desc[metaname].append(meta['content'])
        else:
            metaname = meta.get('property', '').lower()
            if (metaname != None and len(metaname) > 0 and 'content' in meta):
                desc[metaname].append(meta['content'])

    if len(desc.keys())>0:
        with open(raw2,'w') as f:
            json.dump(desc, f)

    return

if __name__ == "__main__":
    """
    Fetch info on visited domains using yahoo APIs.
    """
    db = sys.argv[1]
                
    engine = create_engine(db, poolclass=NullPool, echo=False)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)

    dbs = Session() 
    for row in dbs.query(HttpReq.req_url_host).filter(HttpReq.user_url == True).distinct():
        domain = row[0]
        fetch_page(domain)

        # split long host names to the top-level domain aswell
        tmp = domain.split('.')
        if (domain.endswith('co.uk') and len(tmp)>3):
            domain2 = "%s.%s.%s"%(tmp[-3],tmp[-2],tmp[-1])
            fetch_page(domain2)
        elif (len(tmp)>2):
            domain2 = "%s.%s"%(tmp[-2],tmp[-1])
            fetch_page(domain2)

    for row in dbs.query(DnsReq.query).distinct():
        domain = row[0]
        fetch_page(domain, True)

    dbs.close()
