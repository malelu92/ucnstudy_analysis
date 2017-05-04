import numpy as np
import scipy as sci
import pandas as pd

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import dates

from bokeh.plotting import figure, show, output_notebook
from bokeh.charts import *

from collections import defaultdict
from datetime import datetime, timedelta

from model.Base import Base
from model.User import User
from model.Device import Device
from model.HttpReq import HttpReq
from model.DnsReq import DnsReq
from model.user_devices import user_devices;

from blacklist import create_blacklist_dict
from blacklist import is_in_blacklist

from inter_event_time_theoretical_count import get_interval_list
from inter_event_time_theoretical_count import get_free_spikes_traces

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import datautils

from IPython.display import display

output_notebook()

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

ses = Session()
users = ses.query(User)

def get_filtered_traces():
    #traces = defaultdict(list)
    blacklist = create_blacklist_dict()
    user_traces_dict = defaultdict(list)

    for user in users:
        print ('user : ' + user.username)

        devids = []
        for d in user.devices:
            devids.append(str(d.id))

        devs = {}
        for d in user.devices:
            devs[d.id] = d.platform

        sql_url = """SELECT DISTINCT req_url_host FROM \
        httpreqs2 WHERE devid =:d_id AND matches_urlblacklist = 'f';"""

        sqlq = """SELECT ts, lag(ts) OVER (ORDER BY ts) FROM httpreqs2 \
        WHERE devid =:d_id AND req_url_host =:url AND matches_urlblacklist = 'f'"""

        sql_domain = """SELECT DISTINCT query_domain FROM dnsreqs \
        WHERE devid =:d_id AND matches_blacklist = 'f'"""
        
        sql_dns = """SELECT ts, lag(ts) OVER (ORDER BY ts) FROM dnsreqs \
        WHERE devid =:d_id AND matches_blacklist = 'f' AND query_domain =:qdomain"""

        sql_dns_domain_time = """SELECT query_domain, ts, lag(ts) OVER (ORDER BY ts) FROM dnsreqs \
        WHERE devid =:d_id AND matches_blacklist = 'f'"""

        sql_userid = """SELECT login FROM devices WHERE id =:d_id"""
        print devs
        for elem_id in devids:

            valid_url_list = []
            traces_dict = defaultdict(list)
            filtered_traces = defaultdict(list)
            #idt = user.username+'.'+devs[int(elem_id)]
            user_id = ses.execute(text(sql_userid).bindparams(d_id = elem_id)).fetchone()
            idt = user_id[0]

            #if idt != 'bowen.laptop' and idt != 'bridgeman.laptop2' and idt != 'bridgeman.stuartlaptop' and idt != 'chrismaley.loungepc' and idt != 'chrismaley.mainpc' and idt != 'clifford.mainlaptop' and idt != 'gluch.laptop' and idt != 'kemianny.mainlaptop' and idt != 'neenagupta.workpc':
            if idt != 'kemianny.mainlaptop': 
                continue

            for row in ses.execute(text(sql_url).bindparams(d_id = elem_id)):
                if row[0]:# and not (is_in_blacklist(row[0], blacklist)):
                    valid_url_list.append(row[0])
            #valid_url_list.append('dns')

            for valid_url in valid_url_list:
                cdf_dist = defaultdict(int)
                total_url_traces = 0
                #get inter event times per url
                for row in ses.execute(text(sqlq).bindparams(d_id = elem_id, url = valid_url)):
                    total_url_traces += 1
                    if row[1] == None:
                        continue
                    iat = (row[0]-row[1]).total_seconds()
                    #print iat
                    if not cdf_dist[iat]:
                        cdf_dist[iat] = 1
                    else:
                        cdf_dist[iat] += 1

                row_number = 0
                for row in ses.execute(text(sqlq).bindparams(d_id = elem_id, url = valid_url)):
                    row_number +=1
                    if row[1] == None:
                        continue
                    iat = (row[0]-row[1]).total_seconds()
                    #the first event is always considereed:
                    if row_number == 2:
                        traces_dict[valid_url].append(row[1])
                        user_traces_dict[idt].append(row[1])
                    #filter by > 1s and percentage of iat 
                    if iat > 1:
                        traces_dict[valid_url].append(row[0])

                #eliminate spikes
                if traces_dict[valid_url]:
                    interval_list = get_interval_list(traces_dict[valid_url])
                    #filtered_traces[valid_url] = get_free_spikes_traces(interval_list)
                    traces_dict[valid_url] = get_free_spikes_traces(interval_list)

                """if len(traces_dict[valid_url]) != len(filtered_traces[valid_url]):
                    print '==============='
                    print valid_url
                    print 'not filtered'
                    print len(traces_dict[valid_url])
                    #for elem in traces_dict[valid_url]:
                        #print elem
                    print 'filtered'
                    print len(filtered_traces[valid_url])
                    #for elem in filtered_traces[valid_url]:
                        #print elem"""


            #get inter event times per query domain
            valid_dns_list = []
            for dnsreq in ses.execute(text(sql_domain).bindparams(d_id = elem_id)):
                if not dnsreq[0]:
                    continue
                dom = dnsreq[0]
                #if dom.rsplit('.')[-1] != 'Home':
                valid_dns_list.append(dom)
            print len(valid_dns_list)
            
            #========= filtered by domain =============
            #add dnsreqs
            #for row in ses.execute(text(sql_dns_domain_time).bindparams(d_id = elem_id)):
                #dom = row[0]
                #if dom == None:
                    #user_traces_dict[idt].append(row[1])
                #elif dom.rsplit('.')[-1] != 'Home':
                    #user_traces_dict[idt].append(row[1])
            #=======================

            for dnsreq in valid_dns_list:
                valid_url_list.append(dnsreq)
                total_domain_traces = 0
                cdf_domain_dist = defaultdict(list)
                for row in ses.execute(text(sql_dns).bindparams(d_id = elem_id, qdomain = dnsreq)):
                    total_domain_traces += 1
                    if row[1] == None:
                        continue
                    iat = (row[0]-row[1]).total_seconds()
                    if not cdf_domain_dist[iat]:
                        cdf_domain_dist[iat] = 1
                    else:
                        cdf_domain_dist[iat] += 1
                #add dnsreqs
                row_number = 0
                for row in ses.execute(text(sql_dns).bindparams(d_id = elem_id, qdomain = dnsreq)):
                    row_number += 1
                    if row[1] == None:
                        continue
                    iat = (row[0]-row[1]).total_seconds()
                    #always consider fisrt event
                    if row_number == 2:
                        user_traces_dict[idt].append(row[1])
                        traces_dict[dnsreq].append(row[1])
                    if iat > 1:
                        user_traces_dict[idt].append(row[0])
                        traces_dict[dnsreq].append(row[0])

                #eliminate spikes
                if traces_dict[dnsreq]:
                    interval_list = get_interval_list(traces_dict[dnsreq])
                    #filtered_traces[valid_url] = get_free_spikes_traces(interval_list)
                    traces_dict[dnsreq] = get_free_spikes_traces(interval_list)

            """if user.username == 'clifford.wife':
                if elem_id == str(23):
                    print elem_id
                    for valid_url in valid_url_list:
                        for timst in traces_dict[valid_url]:
                            if timst.day == 14 and timst.hour < 5 and valid_url != 'dns':
                                print valid_url + ' ' + str(timst)"""


            if traces_dict:
                plot_traces(traces_dict, valid_url_list, idt)#user.username, devs[int(elem_id)])

    return user_traces_dict

def not_in_spike(total_intv, total_traces):

    if total_traces > 200:
        if total_intv/float(total_traces) < 0.25:
            return True
        return False
    elif total_traces < 200 and total_traces > 50:
        if total_intv/float(total_traces) < 0.60:
            return True
        return False
    else:
        return True


def plot_traces(traces_dict, url_list, user_id):
    x = []
    y = []
    sns.set(style='whitegrid')
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    for url in url_list:
        for timst in traces_dict[url]:
            x.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            y.append(timst.date())

    y_label = list(set(y))
    hfmt = dates.DateFormatter('%m-%d')
    ax.yaxis.set_major_formatter(hfmt)

    ax.plot(x,y, '.g')

    ax.set_title('Device usage [user=%s]'%(user_id), fontsize = 30)#, device=%s]'%(username, platform))
    ax.set_ylabel('Date')
    ax.set_yticks(y_label)

    ax.set_xlabel('Device Activity')
    ax.set_xlim(0,24)

    plt.tight_layout()
    fig.savefig('figs_device_constant_usage_filtered/teste_%s.png' % (user_id))
    plt.close(fig)

                        
if __name__ == '__main__':
    get_filtered_traces()
