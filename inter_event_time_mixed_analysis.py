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
from inter_event_time_by_url_analysis import get_filtered_traces

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

    traces = get_filtered_traces()
    mix_beg, mix_end = get_activities_inter_times()

    print 'ajdlajsdlsajdas'
    for key, values in traces.iteritems():
        print key

    for key, values in mix_beg.iteritems():
        print key

    file_precision = open('activity_precision.txt','w')

    for user_mix, blocks_beg in mix_beg.items():
        for user_traces, row in traces.items():
            #get same user
            #dev = user_traces.rsplit('.')[-1]
            #if user_mix.rsplit('.')[0] == user_traces.rsplit('.')[0] and dev != 'iphone' and dev != 'ipad' and dev != 'macbook':
            if user_mix == user_traces:
                sorted_traces = sorted(row)
                sorted_blocks_beg = sorted(blocks_beg)
                sorted_blocks_end = sorted(mix_end[user_mix], reverse = True)

                perc_hour, perc_15min, perc_min = get_precision(sorted_traces, sorted_blocks_beg)
                file_precision.write('\n\n\n' + user_traces + '========\n')
                file_precision.write('\nbeginning of day:')
                file_precision.write('\n1 hour range - precision: ' + str(perc_hour))
                file_precision.write('\n15 min range - precision: ' + str(perc_15min))
                file_precision.write('\n1 min range - precision: ' + str(perc_min))
                
                perc_hour, perc_15min, perc_min = get_precision(sorted_traces, sorted_blocks_end)
                file_precision.write('\n\nend of day:')
                file_precision.write('\n1 hour range - precision: ' + str(perc_hour))
                file_precision.write('\n15 min range - precision: ' + str(perc_15min))
                file_precision.write('\n1 min range - precision: ' + str(perc_min))
                break

def get_precision(traces, blocks):
    dt = 0
    total_days = 0
    diff_hour = 0
    diff_15min = 0
    diff_min = 0
    for timst in traces:
        if dt != timst.day:
            dt = timst.day
            for timst_block in blocks:
                if timst_block.date() == timst.date():
                    total_days += 1
                    #print timst
                    #print timst_beg
                    time_diff =  abs(timst_block-timst).total_seconds()
                    if time_diff <= 60*60:
                        diff_hour +=1
                    if time_diff <= 60*15:
                        diff_15min +=1
                    if time_diff <= 60:
                        diff_min +=1
                    break

    return diff_hour/float(total_days), diff_15min/float(total_days), diff_min/float(total_days)



def get_intersection():

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
