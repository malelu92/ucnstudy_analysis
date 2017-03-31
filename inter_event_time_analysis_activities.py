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
            if device.id == 12:
                mix_beg, mix_end = calculate_block_intervals(beg, end, io, 60)
                plot_traces(mix_beg, mix_end, [], device.device_id)

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
        end_timst = end[cont]
        d = timst.date()
        if timst.day == end_timst.day:
            x_beg.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            x_end.append(end_timst.hour+end_timst.minute/60.0+end_timst.second/3600.0)
            y.append(d)
            cont = cont + 1
        else:
            print 'gaidasdhiuasdas'
            print timst.day
            print end_timst.day
            x_beg.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
            x_end.append(23.99999)
            y.append(timst.date())

            x_beg.append(00.00001)
            x_end.append(end_timst.hour+end_timst.minute/60.0+end_timst.second/3600.0)
            d += timedelta(days=1)
            y.append(d)
            
            cont = cont + 1

        #if timst.day == 22:
         #   print '======================'
          #  print 'beg ' + str(beg[cont-1])
           # print 'end ' + str(end[cont -1])
            #print 'beg_t ' + str (x_beg[cont-1])
            #print 'end_t ' + str (x_end[cont-1])

    print 'lala'
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

    cont = 0
    while cont < len(y):
        d = y[cont]
        if d.day == 24 or d.day == 25:
            print '======================='
            print 'beg_t ' + str (x_beg[cont])
            print 'end_t ' + str (x_end[cont])
        cont = cont + 1

    ax.hlines(y, x_beg, x_end, 'g')
    #ax.plot(x_io, y_io, '.g')
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
    fig.savefig('figs_device_usage_activities_block/%s.png' % (user))
    plt.close(fig)


def calculate_block_intervals(act_beg, act_end, io, time_itv):

    mix_beg = []
    mix_end = []
    #print io.sort()
    j = 0
    for i in range (0, len(act_beg)):
        #print i
        #print act_beg[i]
        #print io[j]
        #raw_input('Enter your input:')
        if act_beg[i] <= io[j]:
            print 'caso 1'
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
            print 'caso 2'
            #raw_input('Enter your input:')
            #if done checking io
            if j == len(io):
                break

            block_beg = io[j]
            block_end = io[j]
            #merge ios
            #print io[j]
            #print act_beg[i]
            while io[j] < act_beg[i]:
                #print 'caso 2 - parte 1'
                #print io[j]
                block_beg = io[j]
                block_end = io[j]
                #if i == 8:
                 #   print io[j+1]
                  #  break
                mixed = False
                if j + 1 < len(io) - 1:
                    #print len(io)
                    #print j
                    while (io[j+1] - block_end).total_seconds() <= time_itv:
                        #print 'entrou'
                        #print io[j+1]
                        block_end = io[j+1]
                        j =j+1
                        #if ios merge with act
                        if block_end > act_beg[i]:
                            print 'merge block io'
                            print block_beg
                            print block_end
                            print act_beg[i]
                            print act_end[i]
                            block_end = act_end[i]
                            mix_beg.append(block_beg)
                            mix_end.append(block_end)
                            #print 'vai sair'
                            mixed = True
                            break
                        if j == len(io)-1:
                            break
                   # print 'block_beg'
                    #print block_beg
                    #print 'block_end'
                    #print block_end
                    #raw_input('Enter your input:')
                    #print 'saiu mini loop'
                #block only of ios
                if not(mixed):
                    mix_beg.append(block_beg)
                    mix_end.append(block_end)
                    j = j + 1

                if j == len(io):
                    break


            #print 'saiu while case 2'
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
                #print 'saiu while'
                mix_beg.append(block_beg)
                mix_end.append(block_end)

    print 'caso 3'
    #in case there is still io to check
    while j < len(io)-1:
        #print 'while case 3'
        loop = False
        block_beg = io[j]
        block_end = io[j]
        if j + 1 < len(io): 
            #print 'aqui'
            #print j
            #print len(io)
            while (io[j+1] - block_end).total_seconds() <= time_itv:
                block_end = io[j+1]
                j = j + 1
                loop = True
                #print j
                if j == len(io)-1:
                    #print 'entrou'
                    break
            if loop == False:
                j = j + 1
        mix_beg.append(block_beg)
        mix_end.append(block_end)

    print 'end of function'

    cont = 0
    for elem in mix_beg:
        #print 'beg ' + str (elem)
        #print 'end ' + str (mix_end[cont])
        cont = cont + 1
        

    return mix_beg, mix_end
                    
   # j = 0
   # if io[0] < act_beg[j]:
    """    block_beg = io[0]
        block_end = io[0]
        for i in range(0, len(io)-1):
            if io[i] < act_beg[j]:
                #merge io
                if (io[i+1] - io[i]).total_seconds() > time_itv:
                    mix_beg.append(io[i])
                    mix_end.append(io[i+1])  


    io_beg.append(io[0])
    for i in range(0,len(io)-1):
    if io
        while io[i] < 
        if (io[i+1] - io[i]).total_seconds() > time_itv:
            io_beg.append(io[i])
            io_end.append(io[i+1])
    io_end.append(io[len(io)-1])






    io_beg = []
    io_end = []
    #create io blocks
    io_beg.append(io[0])
    for i in range(0,len(io)-1):
        if (io[i+1] - io[i]).total_seconds() > time_itv:
            io_beg.append(io[i])
            io_end.append(io[i+1])
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

    cont_i
    cont_j
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

        union_beg = []
        union_end = []
        j = 0
        #second add non overlaps
        for i in range(0, len(io_beg)):
            if j == len(mix_beg):
                cont = i
                break

            overlap = (io_end[i] < mix_beg[j] and io_beg[i] > mix_end[j])
            
            #if doesnt overlap
            while !overlap:
                if io_beg[i] < mix_beg[j]:
                    union_beg.append(io_beg[i])
                    union_end.append(io_end[i])
                    union_beg.append(mix_beg[j])
                    union_end.append(mix_end[j])
                else 

        #in case j is still not complete
        if 


"""







        

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
