from blacklist import create_blacklist_dict
from blacklist import is_in_blacklist
from collections import defaultdict
from inter_event_time_by_url_analysis import filter_spikes

import datetime

from Traces import Trace

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import datautils

from IPython.display import display

from model.Base import Base
from model.User import User
from model.Device import Device
from model.HttpReq import HttpReq
from model.DnsReq import DnsReq
from model.user_devices import user_devices;

#output_notebook()

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

ses = Session()
users = ses.query(User)

def main():

    blacklist = create_blacklist_dict()

    for user in users:
        devids = []
        for d in user.devices:
            devids.append(str(d.id))

        devs = {}
        for d in user.devices:
            devs[d.id] = d.platform

        for elem_id in devids:
            sql_userid = """SELECT login FROM devices WHERE id =:d_id"""
            user_id = ses.execute(text(sql_userid).bindparams(d_id = elem_id)).fetchone()
            idt = user_id[0]

            #if idt != 'bowen.laptop' and idt != 'bridgeman.laptop2' and idt != 'bridgeman.stuartlaptop' and idt != 'chrismaley.loungepc' and idt != 'chrismaley.mainpc' and idt != 'clifford.mainlaptop' and idt != 'gluch.laptop' and idt != 'kemianny.mainlaptop' and idt != 'neenagupta.workpc':
            if idt != 'neenagupta.workpc':
                continue

            print idt

            http_traces_list, dns_traces_list = get_test_data(elem_id)
            filter_traces(5*60, http_traces_list, blacklist)
            #filter_traces(10*60, dns_traces_list, blacklist)

def filter_traces(block_length, traces_list, blacklist):

    print datetime.timedelta(0,block_length)
    i = 0
    #filtered_url_traces = defaultdict(list)
    while i < len(traces_list):
        #print i
        block_url_dict = defaultdict(list)
        filtered_block_url_dict = defaultdict(list)
        elem = traces_list[i]
        beg_block =elem.timst
        prev_pos = i
        #creates blocks of url filtered by blacklist
        while elem.timst <= beg_block + datetime.timedelta(0,block_length):
            if i >= len(traces_list):
                break
            if elem.url_domain and not (is_in_blacklist(elem.url_domain, blacklist)):
                block_url_dict[elem.url_domain].append(elem)
            elem = traces_list[i]
            i +=1
        
        #filters by iat < 1
        for key, values in block_url_dict.iteritems():
            print '====='
            print key
            for j in range(0, len(values)-1):
                current_trace = values[j]
                next_trace = values[j+1]
                iat = (next_trace.timst - current_trace.timst).total_seconds()
                if iat > 1:
                    filtered_block_url_dict[key].append(current_trace.timst)
                    if j+1 == len(values) - 1:
                        filtered_block_url_dict[key].append(next_trace.timst)
                    #print iat

            print len(block_url_dict[key])
            print len(filtered_block_url_dict[key])

            #filters by spike
            if filtered_block_url_dict[key] and len(filtered_block_url_dict[key]) > 1:                
                filtered_block_url_dict[key], deleted_url = filter_spikes(filtered_block_url_dict[key], key, [])

            print len(filtered_block_url_dict[key])

        #only one element
        if prev_pos == i:
            i += 1

        


def get_test_data(device_id):

    sql_http = """SELECT req_url_host, ts, lag(ts) OVER (ORDER BY ts) FROM httpreqs2 \
        WHERE devid =:d_id AND matches_urlblacklist = 'f'"""

    sql_dns = """SELECT query, ts, lag(ts) OVER (ORDER BY ts) FROM dnsreqs \
        WHERE devid =:d_id AND matches_blacklist = 'f'"""

    http_traces_list = []
    #add httpreqs
    for row in ses.execute(text(sql_http).bindparams(d_id = device_id)):
        elem = Trace(row[0], row[1])
        http_traces_list.append(elem)

    dns_traces_list = []
    #add dnsreqs
    for row in ses.execute(text(sql_dns).bindparams(d_id = device_id)):
        elem = Trace(row[0], row[1])
        dns_traces_list.append(elem)

    #traces_list.sort(key = lambda x: x.timst)

    return http_traces_list, dns_traces_list


if __name__ == '__main__':
    main()
