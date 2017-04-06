import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Device import Device
from model.HttpReq import HttpReq
from model.User import User

from blacklist import create_blacklist_dict
from blacklist import is_in_blacklist

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def get_urls():

    ses = Session()
    users = ses.query(User)

    for user in users:
        
        print ('user : ' + user.username)

        devids = []
        for d in user.devices:
            devids.append(str(d.id))

        devs = {}
        for d in user.devices:
            devs[d.id] = d.platform

        sqlq = """SELECT req_url_host FROM httpreqs2 \
        WHERE devid = :d_id AND matches_urlblacklist = 'f'"""

        #extra filter of blacklist
        blacklist = create_blacklist_dict()

        for dev_elem in devids:
            print devs[int(dev_elem)]
            url_host = defaultdict(int)
            detected_blacklist = defaultdict(list)
            if user.username != 'clifford.wife':
                break
            for row in ses.execute(text(sqlq).bindparams(d_id = dev_elem)):
                print 'lala'
                if (not row[0]) or (row[0] in detected_blacklist[row[0][0]]):
                    print 'entrou'
                    continue

                if not is_in_blacklist(row[0], blacklist):
                    print 'lajdid'
                    if row[0] not in url_host:
                        url_host[row[0]] = 1
                    else:
                        url_host[row[0]] += 1
                else: 
                    if row[0] not in detected_blacklist:
                        detected_blacklist[row[0][0]].append(row[0])
                    

            if url_host and user.username == 'clifford.wife':
                plot_urls(url_host, user.username, devs[int(dev_elem)])


def plot_urls(url, username, platform):

    sorted_values = sorted(url.values(), reverse=True)
    sorted_keys  = sorted(url, key=url.get, reverse=True)

    sns.set_style('darkgrid')

    if len(sorted_keys) <= 10:
        plt.figure(figsize=(15, 25), dpi=100)
    elif len(sorted_keys) <= 50 and len(sorted_keys) > 10: 
        plt.figure(figsize=(2*len(sorted_keys), 25), dpi=100)
    elif len(sorted_keys) > 50 and len(sorted_keys) <= 1000:
        plt.figure(figsize=(100, 25), dpi=100)
    else:
        plt.figure(figsize=(250, 25), dpi=100)

    X = np.arange(len(url))
    ymax = max(url.values()) + 1
    plt.title('Url accessed [user=%s, %s]'%(username, platform))
    plt.ylabel('Number of accesses')
    plt.ylim(0, ymax)
    plt.xlabel('Url')
    plt.bar(X, sorted_values, align='center', width=0.5)
    plt.xticks(X, sorted_keys, rotation = 90)
    plt.savefig('figs_histogram_url/%s-%s.png'%(username, platform))
    plt.close()



if __name__ == '__main__':
    get_urls()
