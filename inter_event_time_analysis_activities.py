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

from model_io.Base import Base
from model_io.Devices import Devices
from model_io.Activities import Activities;

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import datautils

from IPython.display import display

output_notebook()

DB='postgresql+psycopg2:///ucnstudy_hostview_data'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine) 

ses = Session()
devices = ses.query(Devices)

def get_activities_inter_times():
    for device in devices:
        #select only users from ucnstudy
        if device.id == 5 or device.id == 6 or device.id == 8 or device.id == 12 or device.id == 14 or device.id == 18 or device.id == 19 or device.id == 21 or device.id == 22:

            sql = """SELECT logged_at, finished_at \
            FROM activities \
            WHERE session_id = :d_id AND fullscreen = 1"""

            sql_io = """SELECT logged_at, lag(logged_at) OVER (ORDER BY logged_at) \
            FROM io \
            WHERE session_id = :d_id"""

            beg = []
            end = []
            print (device.device_id + '==============')
            for row in ses.execute(text(sql).bindparams(d_id = device.id)):
                beg.append(row[0])
                end.append(row[1])

            io = []
            io_iat = []
            cont = 0
            for row in ses.execute(text(sql_io).bindparams(d_id = device.id)):
                io.append(row[0])
                if (row[1]==None):
                    continue
                io_iat.append((row[0]-row[1]).total_seconds())

            #plot_traces(beg, end, io, device.device_id)
            #plot_cdf_interval_times(io_iat, device.device_id)
            calculate_block_intervals(beg, end, io, 60)
            

def plot_traces(beg, end, io, user): 

    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    x_beg = []
    x_end = []
    y = []
    y_label = []
    x_io = []
    y_io = []

    cont = 0
    for timst in beg:
        #print timst
        x_beg.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        end_timst = end[cont]
        x_end.append(end_timst.hour+end_timst.minute/60.0+end_timst.second/3600.0)
        y.append(timst.date())
        cont = cont + 1

    for timst in io:
        x_io.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y_io.append(timst.date())

    y_label = list(set(y))
    hfmt = dates.DateFormatter('%m-%d')
    ax.yaxis.set_major_formatter(hfmt)
    #ax.plot(x,y, '.g')
    #print 'x_beg'
    #print x_beg
    #print 'x_end'
    #print x_end

    ax.hlines(y, x_beg, x_end, 'g')
    ax.plot(x_io, y_io, '.g')
    ax.set_title('Device usage [user=%s]'%(user))
    ax.set_ylabel('Date')
    ax.set_yticks(list(set(y_io).union(y_label)))
    if y_label and y_io:
        ax.set_ylim(min(min(y_label),min(y_io)), max(max(y_label),max(y_io)))
    elif y_io:
        ax.set_ylim(min(y_io), max(y_io))
    elif y_label:
        ax.set_ylim(min(y_label), max(y_label))

    ax.set_xlabel('Device Activity')
    ax.set_xlim(0,24)

    plt.tight_layout()
    fig.savefig('figs_device_usage_activities/%s.png' % (user))
    plt.close(fig)


def calculate_block_intervals(act_beg, act_end, io, time_itv):

    io_beg = []
    io_end = []
    #create io blocks
    io_beg.append(io[0])
    for i in range(0,len(io)-1):
        if (io[i+1] - io[i]).total_seconds() > time_itv:
            io_end.append(io[i])
            io_beg.append(io[i+1])
    io_end.append(io[len(io)-1])

    #for i in range (0, len(io_beg)):
        #print 'io_beg'
        #print io_beg[i]
        #print 'io_end'
        #print io_end[i]
        
    mix_beg = []
    mix_end = []
    #unite io and act blocks
    if len(io_beg) > len(act_beg):
        io_longer = 1
    else:
        io_longer = 0

    if io_longer:
        j = 0
        #first get everything that overlaps
        for i in range(0, len(io_beg)):
            if j == len(act_beg):
                break
            min_mix = io_beg[i]
            max_mix = io_end[i]

            overlap = (io_beg[i] <= act_end[j] and io_end[i] >= act_beg[j])
            #if doesnt overlap
            while !overlap:
                if io_beg[i+1] < act_beg[j]:
                    continue
                else:
                    j = j+1
                overlap = (io_beg[i] <= act_end[j] and io_end[i] >= act_beg[j])

            #if overlaps
            while overlap:
                #get border values
                min_mix = min(min_mix, act_beg[j])
                max_mix = max(max_mix, act_end[j])
                if act_end[j] < io_beg[i+1]:
                    j = j+1
                else:
                    continue
                overlap = (io_beg[i] <= act_end[j] and io_end[i] >= act_beg[j])
            #merged block
            mix_beg.append(min_mix)
            mix_end.append(max_mix)

            
        j = 0
        #second add non overlaps
        for i in range(0, len(io_beg)):
            if j == len(mix_beg):
                if 
            #if doesnt overlap
            overlap = (io_end[i] < mix_beg[j] and io_beg[i] > mix_end[j])

            while overlap:
                
        
        #in case j is still not complete
        if 










        

def plot_cdf_interval_times(iat, user):

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(12, 4))
    (x,y) = datautils.aecdf(iat)
    ax1.plot(x,y, '-b', lw=2)
    ax1.set_title('Inter-Event Time [user=%s, events=%d]'%(user, len(x)))    
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
    fig.savefig('figs_CDF_io/%s.png' % (user))
    plt.close(fig)

    

if __name__ == "__main__":
    get_activities_inter_times()
