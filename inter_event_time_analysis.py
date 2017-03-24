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

def main ():
    for user in users:
        print ('user : ' + user.username)

        devids = []
        for d in user.devices:
            devids.append(str(d.id))

        devs = {}
        for d in user.devices:
            devs[d.id] = d.platform

        iat = []
        traces = []
    
        sqlq = """SELECT ts,lag(ts) OVER (ORDER BY ts) FROM \
        (SELECT ts FROM dnsreqs WHERE devid = :d_id UNION ALL \
        SELECT ts FROM httpreqs2 WHERE devid = :d_id AND user_url = 't' UNION ALL \
        SELECT startts from flows WHERE devid = :d_id) \
        AS events"""
    
        for elem_id in devids:
            for row in ses.execute(text(sqlq).bindparams(d_id = elem_id)):
                if (row[1]==None):
                    continue
                iat.append((row[0]-row[1]).total_seconds())
                traces.append(row[0])   
                #if (row[0]-row[1]).total_seconds() > 60*60*8:
                    #print 'sleep'
                    #print row[1]
                    #print 'wake up'
                    #print row[0]
            
            if iat and user.username != 'desir':
                #plot_cdf_interval_times(iat, user, devs, elem_id)
                plot_traces(traces, user, devs, elem_id)


def plot_traces(traces, user, devs, elem_id): 

    fig, ax = plt.subplots(1, 1, figsize=(12, 4))
    x = []
    y = []
    y_label = []
    cont = 0
    for timst in traces:
        #while cont < 10:
        if timst.date() not in y:
            y_label.append(timst.date())
        x.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y.append(timst.date())
         #   cont = cont + 1

    hfmt = dates.DateFormatter('%m-%d')
    ax.yaxis.set_major_formatter(hfmt)

    ax.plot(x,y, 'og', lw=2)
    ax.set_title('Device usage [user=%s, device=%s]'%(user.username, devs[int(elem_id)]))
    #ax1.set_ylabel('CDF')
    print y_label
    print len(y_label)
    ax.set_yticks(y_label)
    ax.set_ylim(y_label[0], y_label[len(y_label)-1])
    #ax1.set_xscale('log')
    #ax1.set_xlabel('seconds')
    #ax1.set_xticks([0.001,1,60,3600,24*3600])
    #ax1.set_xticklabels(['1ms','1s','1min','1h','1day'])
    ax.set_xlim(0,24)
    plt.show()



def plot_cdf_interval_times(iat, user, devs, elem_id):

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(12, 4))
    (x,y) = datautils.aecdf(iat)
    ax1.plot(x,y, '-b', lw=2)
    ax1.set_title('Inter-Event Time [user=%s, events=%d]'%(user.username, len(x)))    
    ax1.set_ylabel('CDF')
    ax1.set_xscale('log')
    ax1.set_xlabel('seconds')
    ax1.set_xticks([0.001,1,60,3600,24*3600])
    ax1.set_xticklabels(['1ms','1s','1min','1h','1day'])    
    ax1.set_xlim(0.001,max(iat))
    
    xp = filter(lambda v : v>=60, x)            
    ax2.plot(xp,y[-len(xp):], '-b', lw=2)
    ax2.set_title('Zoom 1 [values=%d]'%(len(xp)))    
    ax2.set_ylabel('CDF')
    ax2.set_xscale('log')
    ax2.set_xlabel('seconds')
    ax2.set_xticks([60,600,3600,24*3600])
    ax2.set_xticklabels(['1min','10min','1h','1day'])        
    ax2.set_xlim(60,max(iat))
                
    xp = filter(lambda v : v>=600, x)
    ax3.plot(xp,y[-len(xp):], '-b', lw=2)
    ax3.set_title('Zoom 2 [values=%d]'%(len(xp)))    
    ax3.set_ylabel('CDF')
    ax3.set_xscale('log')
    ax3.set_xlabel('seconds')
    ax3.set_xticks([600,3600,3*3600,12*2600,24*3600,3*24*3600,7*24*3600])
    ax3.set_xticklabels(['10min','1h','3h','12h','1d','3d','1w'])        
    ax3.set_xlim(600,max(iat))
    
    plt.tight_layout()
    fig.savefig('figs_CDF/%s-%s.png' % (user.username, devs[int(elem_id)]))
    plt.close(fig)


if __name__ == "__main__":
    main()
