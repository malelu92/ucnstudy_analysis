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

from inter_event_time_analysis_activities import get_activities_inter_times
from inter_event_time_analysis import get_traces
from inter_event_time_analysis import plot_cdf_interval_times


#from model_io.Base import Base
#from model_io.Devices import Devices
#from model_io.Activities import Activities;

#from sqlalchemy import create_engine, text, func
#from sqlalchemy.orm import sessionmaker
#from sqlalchemy.pool import NullPool

import datautils

output_notebook()

#DB='postgresql+psycopg2:///ucnstudy_hostview_data'

#engine = create_engine(DB, echo=False, poolclass=NullPool)
#Base.metadata.bind = engine
#Session = sessionmaker(bind=engine)

#ses = Session()
#devices = ses.query(Devices)

def main():

    mix_beg, mix_end = get_activities_inter_times()
    traces = get_traces()

    for user_mix, blocks_beg in mix_beg.items():
        for user_traces, row in traces.items():
            #get same user
            dev = user_traces.rsplit('.')[-1]
            if user_mix.rsplit('.')[0] == user_traces.rsplit('.')[0] and dev != 'iphone' and dev != 'ipad' and dev != 'macbook':
                #raw_input('Enter your input:')
                iat_block = []
                iat_out_block = []
                print user_mix.rsplit('.')[0]
                blocks_end = mix_end[user_mix]
                for i in range (0,len(row)-1):
                    iat = (row[i+1] - row[i]).total_seconds()
                    if trace_in_interval(row[i], row[i+1], blocks_beg, blocks_end):
                        iat_block.append(iat)
                    else:
                        iat_out_block.append(iat)
                if iat_block:
                    plot_cdf_interval_times(iat_block, user_traces.rsplit('.', 1)[0], str(user_traces.rsplit('.')[-1]+'-boldchat-inBlock'), '', 'figs_CDF_mix')
                if iat_out_block:
                    plot_cdf_interval_times(iat_out_block, user_traces.rsplit('.', 1)[0], str(user_traces.rsplit('.')[-1]+'-boldchat-outBlock'), '', 'figs_CDF_mix')
                break
            else:
                continue


def trace_in_interval(trace_beg, trace_end, mix_beg, mix_end):
    for i in range(0, len(mix_beg)):
        if trace_beg <= mix_end[i] and trace_end >= mix_beg[i]:
            return 1
    return 0
            
    

if __name__ == "__main__":
    main()
