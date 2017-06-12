from blacklist import create_blacklist_dict
from blacklist import is_in_blacklist
from collections import defaultdict
from inter_event_time_by_url_analysis import filter_spikes

import datautils
import datetime
from matplotlib import dates
import matplotlib.pyplot as plt
import seaborn as sns

from Traces import Trace

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

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

def final_algorithm_filtered_traces(f_blacklist, f_seconds, f_spikes):

    blacklist = create_blacklist_dict()
    filtered_traces_user_dict = defaultdict(list)

    file_type = get_file_type(f_blacklist, f_seconds, f_spikes)

    bucket_list = [1, 5, 10]

    for bucket in bucket_list:
        traces_bucket = defaultdict(list)
        traces_file = open('traces_bucket_%s_%s'%(str(bucket), file_type), 'w')
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

                if idt != 'bowen.laptop' and idt != 'bridgeman.laptop2' and idt != 'bridgeman.stuartlaptop' and idt != 'chrismaley.loungepc' and idt != 'chrismaley.mainpc' and idt != 'clifford.mainlaptop' and idt != 'gluch.laptop' and idt != 'kemianny.mainlaptop' and idt != 'neenagupta.workpc':
                #if idt != 'neenagupta.workpc':
                    continue

                print idt

                http_traces_list, dns_traces_list = get_test_data(elem_id)

                filtered_http_traces = filter_traces(5*60, http_traces_list, blacklist, f_blacklist, f_seconds, f_spikes)
                filtered_dns_traces = filter_traces(5*60, dns_traces_list, blacklist, f_blacklist, f_seconds, f_spikes)

                for key, timsts in filtered_http_traces.iteritems():
                    for timst in timsts:
                        filtered_traces_user_dict[idt].append(timst)

                for key, timsts in filtered_dns_traces.iteritems():
                    for timst in timsts:
                        filtered_traces_user_dict[idt].append(timst)

                #plot_traces(filtered_http_traces, filtered_dns_traces, idt)
                #plot_traces(filtered_traces_user_dict[idt], idt)

                traces_bucket[idt] = get_block_traces(filtered_traces_user_dict[idt], bucket-1)

                traces_file.write('\n' + idt)
                print len(traces_bucket[idt])
                for timst in traces_bucket[idt]:
                    traces_file.write('\n' +str(timst))

    return filtered_traces_user_dict


def get_file_type(f_blacklist, f_seconds, f_spikes):

    if f_blacklist and f_seconds and f_spikes:
        return 'filtered'

    elif f_blacklist and not f_seconds and not f_spikes:
        return 'blist_filtered'

    elif not f_blacklist and f_seconds and f_spikes:
        return 'interval_filtered'

    elif not f_blacklist and not f_seconds and not f_spikes:
        return 'not_filtered'


def filter_traces(block_length, traces_list, blacklist, filter_blist, filter_iat, filter_spike):

    #print datetime.timedelta(0,block_length)
    i = 0
    filtered_url_traces = defaultdict(list)
    while i < len(traces_list):
        block_url_dict = defaultdict(list)
        filtered_block_url_dict = defaultdict(list)
        elem = traces_list[i]
        beg_block =elem.timst
        prev_pos = i
        #creates blocks of url filtered by blacklist
        while elem.timst <= beg_block + datetime.timedelta(0,block_length):
            if i >= len(traces_list):
                break
            if filter_blist: 
                if elem.url_domain and not (is_in_blacklist(elem.url_domain, blacklist)):
                    block_url_dict[elem.url_domain].append(elem)
            else:
                block_url_dict[elem.url_domain].append(elem)
            elem = traces_list[i]
            i +=1

        #filters by iat < 1
        for key, values in block_url_dict.iteritems():
            if filter_iat:
                for j in range(0, len(values)-1):
                    current_trace = values[j]
                    next_trace = values[j+1]
                    iat = (next_trace.timst - current_trace.timst).total_seconds()
                    if filter_iat:
                        if iat > 1:
                            filtered_block_url_dict[key].append(current_trace.timst)
                            if j+1 == len(values) - 1:
                                filtered_block_url_dict[key].append(next_trace.timst)
            else:
                for elem in values:
                    filtered_block_url_dict[key].append(elem.timst)

            #filters by spike
            if filter_spike and filtered_block_url_dict[key] and len(filtered_block_url_dict[key]) > 1:
                filtered_block_url_dict[key], deleted_url = filter_spikes(filtered_block_url_dict[key], key, [])

            for elem in filtered_block_url_dict[key]:
                filtered_url_traces[key].append(elem)

        #only one element
        if prev_pos == i:
            i += 1
    
    return filtered_url_traces

def get_block_traces(traces, bucket_size):

    print len(traces)
    pre_traces = []
    final_traces = []
    for timst in traces:
        timst = timst.replace(microsecond=0)
        pre_traces.append(timst)

    pre_traces = list(set(pre_traces))
    pre_traces = sorted(pre_traces)
    #print len(pre_traces)
    #for elem in pre_traces:
        #print elem
    first_timst = pre_traces[0]
    first_day = first_timst.replace(hour=00, minute=00, second=00, microsecond=0)
    last_timst = pre_traces[len(pre_traces)-1]
    last_day = last_timst.replace(hour=23, minute=59, second=59, microsecond=0)

    current_bucket_beg = first_day
    current_bucket_end = current_bucket_beg + datetime.timedelta(0,bucket_size)

    daily_traces = get_daily_traces(pre_traces, current_bucket_beg)
    current_date = current_bucket_beg.date()
    previous_date = current_bucket_beg.date()

    while current_bucket_end <= last_day + datetime.timedelta(0,1):
        if current_date != previous_date:
            previous_date = current_date
            print current_date
            daily_traces = get_daily_traces(pre_traces, current_bucket_beg)

        trace_in = trace_in_bucket(daily_traces, current_bucket_beg, current_bucket_end)

        if trace_in:
            timst_insert = current_bucket_beg
            while timst_insert <= current_bucket_end:
                final_traces.append(timst_insert)
                timst_insert = timst_insert + datetime.timedelta(0,1)

        current_bucket_beg = current_bucket_end + datetime.timedelta(0,1)
        current_bucket_end = current_bucket_beg + datetime.timedelta(0,bucket_size)
        current_date = current_bucket_beg.date()

    return final_traces

def trace_in_bucket(traces, bucket_beg, bucket_end):
    #print bucket_beg
    #print bucket_end
    if traces:
        for elem in traces:
            if elem >= bucket_beg and elem <= bucket_end:
                return True

            elif elem > bucket_end:
                #print elem
                return False

    return False


def get_daily_traces(traces, bucket_beg):
    daily_list = []
    for elem in traces:
        if elem.date() == bucket_beg.date():
            daily_list.append(elem)

    return daily_list


def get_test_data(device_id):

    sql_http = """SELECT req_url_host, ts, lag(ts) OVER (ORDER BY ts) FROM httpreqs2 \
        WHERE devid =:d_id AND matches_urlblacklist = 'f' and source = 'hostview'"""

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


def plot_traces(traces_dict, user_id):
    x = []
    y = []
    sns.set(style='whitegrid')
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))

    for timst in traces_dict:
        x.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y.append(timst.date())

    """for key, timsts in http_traces_dict.iteritems():
        for timst in timsts:
            x.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            y.append(timst.date())

    for key, timsts in dns_traces_dict.iteritems():
        for timst in timsts:
            x.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            y.append(timst.date())"""

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
    #fig.savefig('figs_device_constant_usage_filtered/%s.png' % (user_id))
    #plt.close(fig)

    plt.show(fig)

if __name__ == '__main__':
    final_algorithm_filtered_traces(False, False, False)
