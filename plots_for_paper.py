import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Device import Device
from model.DnsReq import DnsReq
from model.HttpReq import HttpReq
from model.User import User

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def get_dns_http():

    ses = Session()
    devices = ses.query(Device)

    sql_dns = """select count(*) from dnsreqs where devid = :d_id"""

    sql_http = """select count(*) from httpreqs2 where devid = :d_id;"""

    dns_http_dict = defaultdict(list)
    for device in devices:

        print ('user: ' + device.login)

        for row in ses.execute(text(sql_dns).bindparams(d_id = device.id)):
            dns_http_dict[device.login].append(row[0])

        for row in ses.execute(text(sql_http).bindparams(d_id = device.id)):
            dns_http_dict[device.login].append(row[0])

    plot_histogram(dns_http_dict, devices)


def plot_histogram(dns_http_dict, devices):

    sns.set_style('whitegrid')

    x = []
    y_dns = []
    y_http = []
    cont = 0
    for device in devices:
        dns_http_elem = dns_http_dict[device.login] 
        x.append(cont)
        y_dns.append(dns_http_elem[0])
        y_http.append(dns_http_elem[1])
        cont += 1

    fig = plt.figure(figsize=(15, 10), dpi=100)
    ax = fig.add_subplot(111)
    
    N = 47#len(devices)
    ind = np.arange(N)                # the x locations for the groups
    width = 0.35                      # the width of the bars

    dns = ax.bar(ind, y_dns, width, color='blue')
    http = ax.bar(ind+width, y_http, width, color='red')

    ax.set_title('Http and dns traces', fontsize=30)
    ax.set_ylabel('Number of traces', fontsize=20)
    ax.set_xticks([0, 15, 30, 46], fontsize = 15)
    ax.set_xlim(0, len(x))
    ax.set_xlabel('Devices', fontsize=20)
    ax.legend( (dns[0], http[0]), ('Dns requests', 'Http requests'), fontsize = 15 )
    plt.savefig('figs_paper/dns_http_histogram.png')
    plt.close()

if __name__ == '__main__':
    get_dns_http()
