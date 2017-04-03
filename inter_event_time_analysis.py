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

def get_traces():
    traces = defaultdict(list)
    for user in users:
        print ('user : ' + user.username)

        devids = []
        for d in user.devices:
            devids.append(str(d.id))

        devs = {}
        for d in user.devices:
            devs[d.id] = d.platform
    
        #sqlq = contains_blacklist(0)
        url = '\'%http://vmp.boldchat.com%\''
        sqlq = get_inter_event_query(url)
        
        sqlq_flow = """SELECT startts, endts FROM flows WHERE devid = :d_id"""
        for elem_id in devids:
            print elem_id
            iat = []
            #traces = []
            for row in ses.execute(text(sqlq).bindparams(d_id = elem_id)):
                if (row[1]==None):
                    continue
                iat.append((row[0]-row[1]).total_seconds())
                traces[user.username+'.'+devs[int(elem_id)]].append(row[0])

                #if (row[0]-row[1]).total_seconds() > 60*60*2:
                    #print 'sleep'
                    #print row[1]
                    #print 'wake up'
                    #print row[0]

            flow_beg = []
            flow_end = []
            for row in ses.execute(text(sqlq_flow).bindparams(d_id = elem_id)):
                flow_beg.append(row[0])
                flow_end.append(row[1])
            
            #if iat:
                #plot_cdf_interval_times(iat, user.username, devs[int(elem_id)], 'figs_CDF', url)
            
            #if flow_beg:
                #plot_traces(traces[user.username+'.'+devs[int(elem_id)]], flow_beg, flow_end, user, devs, elem_id)
            
            #if traces:
                #mix_beg, mix_end = make_block_usage(traces, 60*5)
                #plot_traces([], mix_beg, mix_end, user, devs, elem_id)


    return traces
def make_block_usage(traces, time_itv):

    mix_beg = []
    mix_end = []
    block_beg = traces[0]
    block_end = block_beg
    for i in range(0,len(traces)-1):
        #if points are in the same block
        if (traces[i+1] - block_end).total_seconds() <= time_itv:
            block_end = traces[i+1]
        else:
            mix_beg.append(block_beg)
            mix_end.append(block_end)
            if i + 1 < len(traces):
                block_beg = traces[i+1]
                block_end = block_beg

    return mix_beg, mix_end

def plot_traces(traces, flow_beg, flow_end, user, devs, elem_id): 

    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    x = []
    y = []
    y_label = []
    x_beg = []
    x_end = []
    y_flow = []

    for timst in traces:
        x.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y.append(timst.date())

    cont = 0
    for timst in flow_beg:
        end_timst = flow_end[cont]
        d = timst.date()
        if timst.day == end_timst.day:
            x_beg.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            x_end.append(end_timst.hour+end_timst.minute/60.0+end_timst.second/3600.0)
            y_flow.append(d)
            cont = cont + 1
        else:
            x_beg.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            x_end.append(23.99999)
            y.append(timst.date())

            x_beg.append(00.01)
            x_end.append(end_timst.hour+end_timst.minute/60.0+end_timst.second/3600.0)
            d += timedelta(days=1)
            y_flow.append(d)

            cont = cont + 1
    
    y_label = list(set(y))
    y_flow_label = list(set(y_flow))
    hfmt = dates.DateFormatter('%m-%d')
    ax.yaxis.set_major_formatter(hfmt)
    
    ax.hlines(y_flow, x_beg, x_end, 'g')
    #ax.plot(x,y, '.g')
    
    ax.set_title('Device usage [user=%s, device=%s]'%(user.username, devs[int(elem_id)]))
    ax.set_ylabel('Date')
    ax.set_yticks(list(set(y_flow).union(y_label)))
    
    if y_label and y_flow:
        ax.set_ylim(min(min(y_label),min(y_flow_label)), max(max(y_label),max(y_flow_label)))
    elif y_flow:
        ax.set_ylim(min(y_flow_label), max(y_flow_label))
    elif y_label:
        ax.set_ylim(min(y_label), max(y_label))

    ax.set_xlabel('Device Activity')
    ax.set_xlim(0,24)

    plt.tight_layout()
    fig.savefig('figs_device_constant_usage/%s-%s.png' % (user.username, devs[int(elem_id)]))
    plt.close(fig)



def plot_cdf_interval_times(iat, username, platform, url, folder):

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(12, 4))
    (x,y) = datautils.aecdf(iat)

    ax1.plot(x,y, '-b', lw=2)
    ax1.set_title('Inter-Event Time [user=%s, events=%d]'%(username, len(x)))    
    ax1.set_ylabel('CDF')
    ax1.set_xscale('log')
    ax1.set_xlabel('seconds')
    ax1.set_xticks([0.001,1,60,3600,24*3600])
    ax1.set_xticklabels(['1ms','1s','1min','1h','1day'])    
    ax1.set_xlim(0.001,max(iat))

    
    xp = filter(lambda v : v>=60, x)            
    if xp:
        ax2.plot(xp,y[-len(xp):], '-b', lw=2)
        ax2.set_title('Zoom 1 [values=%d]'%(len(xp)))    
        ax2.set_ylabel('CDF')
        ax2.set_xscale('log')
        ax2.set_xlabel('seconds')
        ax2.set_xticks([60,600,3600,24*3600])
        ax2.set_xticklabels(['1min','10min','1h','1day'])        
        ax2.set_xlim(60,max(iat))

    xp = filter(lambda v : v>=600, x)
    if xp:
        ax3.plot(xp,y[-len(xp):], '-b', lw=2)
        ax3.set_title('Zoom 2 [values=%d]'%(len(xp)))    
        ax3.set_ylabel('CDF')
        ax3.set_xscale('log')
        ax3.set_xlabel('seconds')
        ax3.set_xticks([600,3600,3*3600,12*2600,24*3600,3*24*3600,7*24*3600])
        ax3.set_xticklabels(['10min','1h','3h','12h','1d','3d','1w'])        
        ax3.set_xlim(600,max(iat))
    
    plt.tight_layout()
    fig.savefig('%s/%s-%s.png' % (folder, username, platform))
    plt.close(fig)

def contains_blacklist (var):

    #contains all
    if var == 2:
        return """SELECT ts,lag(ts) OVER (ORDER BY ts) FROM \
        (SELECT ts FROM dnsreqs WHERE devid = :d_id UNION ALL \
        SELECT ts FROM httpreqs2 WHERE devid = :d_id AND user_url = 't') \
        AS events"""

    #contains only blacklist
    elif var == 1:
        return """SELECT ts,lag(ts) OVER (ORDER BY ts) FROM \
        (SELECT ts FROM dnsreqs WHERE devid =  :d_id AND matches_blacklist = 't' UNION ALL \
        SELECT ts FROM httpreqs2 WHERE devid = :d_id AND matches_urlblacklist = 't' AND user_url = 't') \
        AS events;"""

    #doesnt contain blacklist
    return """SELECT ts,lag(ts) OVER (ORDER BY ts) FROM \
    (SELECT ts FROM dnsreqs WHERE devid =  :d_id AND matches_blacklist = 'f' UNION ALL \
    SELECT ts FROM httpreqs2 WHERE devid = :d_id AND matches_urlblacklist = 'f' AND user_url = 't') \
    AS events;"""

def get_inter_event_query(url):

    return ("""SELECT ts, lag(ts) OVER (ORDER BY ts) FROM httpreqs WHERE \
    devid = :d_id AND req_url LIKE %s ORDER BY ts""")%(url)
    

if __name__ == "__main__":
    get_traces()
