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
    mix_beg = defaultdict(list)
    mix_end = defaultdict(list)

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
            mix_beg[device.device_id], mix_end[device.device_id] = calculate_block_intervals(beg, end, io, 2)
            #plot_traces(mix_beg[device.device_id], mix_end[device.device_id], [], device.device_id)

    return mix_beg, mix_end

def plot_traces(beg, end, io, user): 

    sns.set_style('whitegrid')
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    x_beg = []
    x_end = []
    y = []
    y_label = []
    x_io = []
    y_io = []

    cont = 0
    for timst in beg:
        end_timst = end[cont]
        d = timst.date()
        if timst.day == end_timst.day:
            x_beg.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            x_end.append(end_timst.hour+end_timst.minute/60.0+end_timst.second/3600.0)
            y.append(d)
            cont = cont + 1
        else:
            x_beg.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            x_end.append(23.99999)
            y.append(timst.date())

            x_beg.append(00.01)
            x_end.append(end_timst.hour+end_timst.minute/60.0+end_timst.second/3600.0)
            d += timedelta(days=1)
            y.append(d)
            
            cont = cont + 1

    for timst in io:
        x_io.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y_io.append(timst.date())

    y_label = list(set(y))
    hfmt = dates.DateFormatter('%m-%d')
    ax.yaxis.set_major_formatter(hfmt)
    #ax.plot(x,y, '.g')

    ax.hlines(y, x_beg, x_end, 'g')
    #ax.plot(x_io, y_io, '.g')
    ax.set_title('Device usage [user=%s]'%(user), fontsize = 25)
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
    fig.savefig('figs_device_usage_activities_block/teste_%s.png' % (user))
    plt.close(fig)


def calculate_block_intervals(act_beg, act_end, io, time_itv):

    mix_beg = []
    mix_end = []
    j = 0
    for i in range (0, len(act_beg)):

        if act_beg[i] <= io[j]:

            #if done checking io
            if j == len(io):
                break
            block_beg = act_beg[i]
            block_end = act_end[i]
            #ignore io points that are already inside act interval
            while io[j] > block_beg and io[j] < block_end:
                j = j + 1
                if j == len(io):
                    break
            #merge intervals that are close
            while (io[j] - block_end).total_seconds() <= time_itv:
                block_end = io[j]
                j = j+1
                if j == len(io):
                    break
            mix_beg.append(block_beg)
            mix_end.append(block_end)

        else:
            #if done checking io
            if j == len(io):
                break

            block_beg = io[j]
            block_end = io[j]
            #merge ios
            while io[j] < act_beg[i]:
                block_beg = io[j]
                block_end = io[j]
                mixed = False
                if j + 1 < len(io) - 1:
                    while (io[j+1] - block_end).total_seconds() <= time_itv:
                        block_end = io[j+1]
                        j =j+1
                        #if ios merge with act
                        if block_end > act_beg[i]:
                            block_end = act_end[i]
                            mix_beg.append(block_beg)
                            mix_end.append(block_end)
                            mixed = True
                            break
                        if j == len(io)-1:
                            break
                #block only of ios
                if not(mixed):
                    mix_beg.append(block_beg)
                    mix_end.append(block_end)
                    j = j + 1

                if j == len(io):
                    break


            if not(mixed):
                #if ios dont merge with block
                block_beg = act_beg[i]
                block_end = act_end[i]
                #ignore io points that are already inside act interval
                if j < len(io):
                    while io[j] > block_beg and io[j] < block_end:
                        j = j + 1
                        if j == len(io):
                            break
                mix_beg.append(block_beg)
                mix_end.append(block_end)

    #in case there is still io to check
    while j < len(io)-1:
        loop = False
        block_beg = io[j]
        block_end = io[j]
        if j + 1 < len(io): 
            while (io[j+1] - block_end).total_seconds() <= time_itv:
                block_end = io[j+1]
                j = j + 1
                loop = True
                if j == len(io)-1:
                    break
            if loop == False:
                j = j + 1
        mix_beg.append(block_beg)
        mix_end.append(block_end)        

    mix_beg, mix_end = final_filtering(mix_beg, mix_end)

    return mix_beg, mix_end
        

def final_filtering(mix_beg, mix_end):

    mix_beg_final = []
    mix_end_final = []

    for i in range(0, len(mix_beg)):

        if i > 0:
            if mix_beg[i] == mix_end[i] and mix_end[i-1] == mix_end[i]:
                continue

        mix_beg_final.append(mix_beg[i])
        mix_end_final.append(mix_end[i])

    return mix_beg_final, mix_end_final



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
