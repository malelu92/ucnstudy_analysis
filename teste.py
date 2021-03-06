import numpy as np
import scipy as sci
import pandas as pd

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
#%matplotlib inline 

#from bokeh.plotting import figure, show, output_notebook
#from bokeh.charts import *

from collections import defaultdict
from datetime import datetime, timedelta

from model.Base import Base
from model.User import User
from model.Device import Device
from model.DeviceTraffic import DeviceTraffic
from model.DeviceAppTraffic import DeviceAppTraffic
from model.HttpReq import HttpReq
from model.DnsReq import DnsReq
from model.Location import Location
from model.user_devices import user_devices;

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import datautils

from IPython.display import display

#output_notebook()

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine) 


# Traffic distribution
utable = defaultdict(list)
idx = []

ses = Session()
for u in ses.query(User).all():
    idx.append(u.id)
    utable['country'].append(u.country)
    utable['username'].append(u.username)
    notshared = [str(d.id) for d in u.devices if not d.shared]
    utable['devices_perso'].append(len(notshared))
    utable['devices_shared'].append(len(u.devices)-len(notshared))

    # just look at personal devices for now
    devids = ",".join(notshared)
    sqlq = """SELECT count(*),min(ts),max(ts),sum(packets_in),sum(packets_out),sum(bytes_in),sum(bytes_out) FROM devicetraffic where devid IN (%s)"""%(devids)
    
    res = ses.execute(text(sqlq)).fetchone()
    if (res != None and res[0]!=None and res[0]>0):
        (active,firstbyte,lastbyte,totalpkts_in,totalpkts_out,totalbytes_in,totalbytes_out) = res
        utable['totaltime (d)'].append((lastbyte-firstbyte).total_seconds()/(24*3600.0))
        utable['activetime (min)'].append(active/60.0)
        utable['pkts [in/out]'].append((totalpkts_in,totalpkts_out))
        utable['MB [in/out]'].append((totalbytes_in/(1024.0*1024.0),totalbytes_out/(1024.0*1024.0)))
    else:
        utable['totaltime (d)'].append(0.0)
        utable['activetime (min)'].append(0.0)
        utable['pkts [in/out]'].append((0,0))
        utable['MB [in/out]'].append((0,0))

    sqlq = """SELECT count(*) FROM dnsreqs where devid IN (%s)"""%(devids)
    res = ses.execute(text(sqlq)).fetchone()
    if (res != None and res[0]!=None):
        utable['dns'].append(res[0])
    else:
        utable['dns'].append(0) 

    sqlq = """SELECT count(*) FROM httpreqs2 where devid IN (%s)"""%(devids)
    res = ses.execute(text(sqlq)).fetchone()
    if (res != None and res[0]!=None):
        utable['http'].append(res[0])
    else:
        utable['http'].append(0) 
       
        
df = pd.DataFrame(utable, index=idx)

totalbytes = ses.query(func.sum(DeviceAppTraffic.bytes_in)).scalar() + ses.query(func.sum(DeviceAppTraffic.bytes_out)).scalar()
totalpkts = ses.query(func.sum(DeviceAppTraffic.packets_in)).scalar() + ses.query(func.sum(DeviceAppTraffic.packets_out)).scalar()

q = ses.query(DeviceAppTraffic.dstport, 
              DeviceAppTraffic.service, 
              func.sum(DeviceAppTraffic.bytes_in), 
              func.sum(DeviceAppTraffic.bytes_out), 
              func.sum(DeviceAppTraffic.packets_in), 
              func.sum(DeviceAppTraffic.packets_out)).group_by(
                DeviceAppTraffic.dstport, 
                DeviceAppTraffic.service).order_by(DeviceAppTraffic.dstport)
              
traffic = defaultdict(list)
for row in q.all():
    if (row[4]+row[5]<=10):
        continue
        
    traffic['port'].append(row[0])
    traffic['service'].append(str(row[1]))
    traffic['bytes_in'].append(row[2])
    traffic['bytes_out'].append(row[3])
    traffic['pkts_in'].append(row[4])
    traffic['pkts_out'].append(row[5])
    traffic['bytes'].append(row[2]+row[3])
    traffic['pkts'].append(row[4]+row[5])
    traffic['bytes (%)'].append((row[2]+row[3])*100.0/totalbytes)
    traffic['pkts (%)'].append((row[4]+row[5])*100.0/totalpkts)
    
ses.close()

tdf = pd.DataFrame(traffic)
tdf = tdf.sort_values('bytes', ascending=False)
display(tdf.loc[:100,]) 


colors = sns.color_palette('muted')
sns.set_style("whitegrid")
markers = ['x','*','+','s','c']

dffr = df[df['country']=='fr'].sort_values('activetime (min)', ascending=False)
dfuk = df[df['country']=='uk'].sort_values('activetime (min)', ascending=False)

ses = Session()
for uname in list(dffr.loc[:,'username']) + list(dfuk.loc[:,'username']):
    u = ses.query(User).filter(User.username==uname).one()
    
    devids = ",".join([str(d.id) for d in u.devices if not d.shared])    

    devs = {}
    for d in u.devices:
        devs[d.id] = d.platform

    xstart = None
    xend = None
    samples = 0
    
    x = defaultdict(list)
    y = defaultdict(list)
    sqlq = """SELECT ts,devid FROM devicetraffic WHERE devid IN (%s) ORDER BY ts"""%(devids) 
    for row in ses.execute(text(sqlq)):
        ts = datautils.utctocc(row['ts'], u.country)
        
        if (xstart == None):
            xstart = ts
            xend = ts
        xstart = xstart if xstart < ts else ts
        xend = xend if xend > ts else ts
        
        x[row['devid']].append(ts)
        y[row['devid']].append(ts.hour+ts.minute/60.0)
        samples += 1

    # ignore users with less than 5min of observed traffic
    if (xstart == None or samples < 300):
        continue
        
    f, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(12, 8), sharex=True)
    for i,devid in enumerate([d.id for d in u.devices if not d.shared]):
        ax1.scatter(x[devid], y[devid], marker=markers[i], color=colors[i], label='%s [%d]'%(devs[devid],len(x[devid])))
        
    ax1.set_title('Device Traffic [user=%s]'%(uname))    
    ax1.set_ylim((0,24))
    ax1.set_ylabel('Hour of Day')
    ax1.set_xlim(xstart,xend)
    ax1.legend(loc='best')

    x = defaultdict(list)
    y = defaultdict(list)
    sqlq = """SELECT ts,devid FROM dnsreqs WHERE devid IN (%s) ORDER BY ts"""%(devids)
    for row in ses.execute(text(sqlq)):
        ts = datautils.utctocc(row['ts'], u.country)        
        x[row['devid']].append(ts)
        y[row['devid']].append(ts.hour+ts.minute/60.0)
        
    for i,devid in enumerate([d.id for d in u.devices if not d.shared]):
        ax2.scatter(x[devid], y[devid], marker=markers[i], color=colors[i], label='%s [%d]'%(devs[devid],len(x[devid])))
    ax2.set_title('DNS requests')    
    ax2.set_ylabel('Hour of Day')
    ax2.set_ylim((0,24))
    ax2.legend(loc='best')
    
    x = defaultdict(list)
    y = defaultdict(list)
    sqlq = """SELECT ts,devid FROM httpreqs2 WHERE devid IN (%s) ORDER BY ts"""%(devids)
    for row in ses.execute(text(sqlq)):
        ts = datautils.utctocc(row['ts'], u.country)
        x[row['devid']].append(ts)
        y[row['devid']].append(ts.hour+ts.minute/60.0)
        
    for i,devid in enumerate([d.id for d in u.devices if not d.shared]):
        ax3.scatter(x[devid], y[devid], marker=markers[i], color=colors[i], label='%s [%d]'%(devs[devid],len(x[devid])))
    ax3.set_title('HTTP Requests')    
    ax3.set_ylabel('Hour of Day')
    ax3.set_xlim(xstart,xend)
    ax3.set_ylim((0,24))
    ax3.legend(loc='best')

    # filter urls
    x = defaultdict(list)
    y = defaultdict(list)
    sqlq = """SELECT ts,devid FROM httpreqs2 WHERE devid IN (%s) AND user_url = 't' ORDER BY ts"""%(devids)
    for row in ses.execute(text(sqlq)):
        ts = datautils.utctocc(row['ts'], u.country)
        x[row['devid']].append(ts)
        y[row['devid']].append(ts.hour+ts.minute/60.0)
        
    for i,devid in enumerate([d.id for d in u.devices if not d.shared]):
        ax4.scatter(x[devid], y[devid], marker=markers[i], color=colors[i], label='%s [%d]'%(devs[devid],len(x[devid])))
    ax4.set_title('Filtered HTTP Requests')    
    ax4.set_ylabel('Hour of Day')
    ax4.set_xlim(xstart,xend)
    ax4.set_ylim((0,24))
    ax4.legend(loc='best')
    
    plt.tight_layout()
    plt.show()
    
    # inter-event times
    iat = []
    
    sqlq = """SELECT ts,lag(ts) OVER (ORDER BY ts) FROM \
    (SELECT ts FROM dnsreqs WHERE devid IN (%s) UNION ALL \
     SELECT ts FROM httpreqs2 WHERE devid IN (%s) AND user_url = 't') \
    AS events"""%(devids,devids)
    
    for row in ses.execute(text(sqlq)):
        if (row[1]==None):
            continue
        iat.append((row[0]-row[1]).total_seconds())

    f, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(12, 4))
    (x,y) = datautils.aecdf(iat)
    ax1.plot(x,y, '-', lw=2, color=colors[0])
    ax1.set_title('Inter-Event Time [user=%s, events=%d]'%(uname, len(x)))    
    ax1.set_ylabel('CDF')
    ax1.set_xscale('log')
    ax1.set_xlabel('seconds')
    ax1.set_xticks([0.001,1,60,3600,24*3600])
    ax1.set_xticklabels(['1ms','1s','1min','1h','1day'])    
    ax2.set_xlim(0.001,max(iat))
    
    xp = filter(lambda v : v>=60, x)
    ax2.plot(xp,y[-len(xp):], '-', lw=2, color=colors[0])
    ax2.set_title('Zoom 1 [values=%d]'%(len(xp)))    
    ax2.set_ylabel('CDF')
    ax2.set_xscale('log')
    ax2.set_xlabel('seconds')
    ax2.set_xticks([60,600,3600,24*3600])
    ax2.set_xticklabels(['1min','10min','1h','1day'])        
    ax2.set_xlim(60,max(iat))

    xp = filter(lambda v : v>=600, x)
    ax3.plot(xp,y[-len(xp):], '-', lw=2, color=colors[0])
    ax3.set_title('Zoom 2 [values=%d]'%(len(xp)))    
    ax3.set_ylabel('CDF')
    ax3.set_xscale('log')
    ax3.set_xlabel('seconds')
    ax3.set_xticks([600,3600,3*3600,12*2600,24*3600,3*24*3600,7*24*3600])
    ax3.set_xticklabels(['10min','1h','3h','12h','1d','3d','1w'])        
    ax3.set_xlim(600,max(iat))
    
    plt.tight_layout()
    plt.show()            
    
ses.close()
