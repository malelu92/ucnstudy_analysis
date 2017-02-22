import numpy as np
import scipy as sci
import pandas as pd
import ipaddress
import re

from collections import defaultdict, Counter
from datetime import datetime, timedelta

import seaborn as sns
import matplotlib.pyplot as plt
#%matplotlib inline 

from bokeh.plotting import figure, show, output_notebook
from bokeh.charts import *
output_notebook()

from sqlalchemy import create_engine, text, func, or_, and_, not_, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from model.Base import Base
from model.User import User
from model.Device import Device
from model.DeviceTraffic import DeviceTraffic
from model.DeviceAppTraffic import DeviceAppTraffic
from model.HttpReq import HttpReq
from model.DnsReq import DnsReq
from model.Location import Location
from model.user_devices import user_devices;

import datautils

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

sns.set(style="whitegrid", context="paper", font_scale=1.5)
palette = sns.light_palette("grey", n_colors=8, reverse=True)
revpalette = sns.light_palette("grey", n_colors=8, reverse=False)
sns.set_palette(palette)
sns.set_color_codes()

cmap = sns.light_palette("grey", as_cmap=True)

# For section2: basic data set
ses = Session()

data = defaultdict(list)
for u in ses.query(User).all():    
    devs = [d.id for d in u.devices if not d.shared]
    
    data['uid'].append(u.id)
    data['devsmobile'].append(len([d.id for d in u.devices if not d.shared and (d.devtype == 'phone')]))
    data['devstablet'].append(len([d.id for d in u.devices if not d.shared and (d.devtype == 'tablet')]))
    data['devspc'].append(len([d.id for d in u.devices if not d.shared and (d.devtype == 'pc')]))
    data['devslaptop'].append(len([d.id for d in u.devices if not d.shared and (d.devtype == 'laptop')]))
    data['devs'].append(len(devs))
    data['cc'].append(u.country)
            
    bytes = ses.query(func.sum(DeviceAppTraffic.bytes_in)+func.sum(DeviceAppTraffic.bytes_out)).filter(
                DeviceAppTraffic.devid.in_(devs)).scalar()
    data['bytes'].append(bytes)
    
    dns = ses.query(func.count(DnsReq.ts)).filter(DnsReq.devid.in_(devs)).scalar()
    data['dns'].append(dns)

    http = ses.query(func.count(HttpReq.ts)).filter(HttpReq.devid.in_(devs)).scalar()
    data['http'].append(http)

    data['app'].append(http+dns)
        
ses.close()

df = pd.DataFrame(data, index=range(0,len(data['uid']),1))
df = df.sort_values('bytes', ascending=False)
df['rank'] = list(range(1, len(df['uid'])+1, 1))

# FRANCE
df1 = df[df['cc']=='fr'].sort_values('bytes', ascending=False)
df1['x'] = list(range(1, len(df1['uid'])+1, 1))

# UK
df2 = df[df['cc']=='uk'].sort_values('bytes', ascending=False)
df2['x'] = list(range(1, len(df2['uid'])+1, 1))

f, ((ax1),(ax2)) = plt.subplots(2, 1, figsize=(6, 4), sharex=True)

sns.barplot(data=df1, x='x', y='bytes', ax=ax1, color=palette[0])
ax1.set_ylabel("Bytes")
#ax1.set_yscale('log')
ax1.set_yticks([y*1e8 for y in np.arange(0.0,10.0,2.0)])
#ax1.set_yticklabels([y*1e9 for y in np.arange(0.0,2.0,0.25)])
ax1.set_xlabel("")
ax1.set_title("France")

sns.barplot(data=df2, x='x', y='bytes', ax=ax2, color=palette[0])
ax2.set_ylabel("Bytes")
#ax2.set_yscale('log')
ax2.set_yticks([y*1e9 for y in np.arange(0.0,20.0,5.0)])
#ax2.set_yticklabels([y*1e1 for y in np.arange(0.0,2.0,0.25)])
ax2.set_xlabel("Participant")
ax2.set_title("UK")

plt.tight_layout()
f.savefig("figs/dataset_traffic.eps")
