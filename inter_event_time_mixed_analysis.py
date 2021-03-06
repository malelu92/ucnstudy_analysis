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

import datautils

#output_notebook()

time_intervals = [1, 2, 3, 4, 5]

def main():

    traces = get_filtered_traces()
    mix_beg, mix_end = get_activities_inter_times()

    file_precision = open('activity_precision.txt','w')

    tp_list = defaultdict(int)
    fp_list = defaultdict(int)
    tn_list = defaultdict(int)
    fn_list = defaultdict(int)
    initialize_list(tp_list, fp_list, tn_list, fn_list)

    for user_mix, blocks_beg in mix_beg.items():
        for user_traces, row in traces.items():
            #get same user
            if user_mix == user_traces:
                print user_mix
                sorted_traces = sorted(row)
                sorted_blocks_beg = sorted(blocks_beg)
                sorted_blocks_end = sorted(mix_end[user_mix], reverse = True)

                for i in range(0, len(time_intervals)):
                    tp, fp, tn, fn, total_days = get_precision(sorted_traces, sorted_blocks_beg, time_intervals[i])
                    print 'tp ' + str(tp)
                    print 'fp ' + str(fp)
                    print 'fn ' + str(fn)
                    tp_list['beg'+str(time_intervals[i])] += tp
                    fp_list['beg'+str(time_intervals[i])] += fp
                    tn_list['beg'+str(time_intervals[i])] += tn
                    fn_list['beg'+str(time_intervals[i])] += fn
                #edit_precision_file(file_precision, user_traces, 'beginning', tp_list['beg'][5], tp_list['beg'][3], tp_list['beg'][0], total_days)
               
                for i in range(0, len(time_intervals)):
                    tp, fp, tn, fn, total_days = get_precision(sorted(sorted_traces, reverse = True), sorted_blocks_end, time_intervals[i])
                    tp_list['end'+str(time_intervals[i])] += tp
                    fp_list['end'+str(time_intervals[i])] += fp
                    tn_list['end'+str(time_intervals[i])] += tn
                    fn_list['end'+str(time_intervals[i])] += fn
                #edit_precision_file(file_precision, user_traces, 'end', tp_list['end'][5], tp_list['end'][3] tp_list['end'][0], total_days)
                break

    plot_ROC_curve(tp_list, fp_list, tn_list, fn_list, user_traces, 'beg')
    plot_ROC_curve(tp_list, fp_list, tn_list, fn_list, user_traces, 'end')

    file_precision.close()


def edit_precision_file (file_precision, username, beg_end, tp_hour, tp_15min, tp_min, total_days):  

    file_precision.write('\n\n\n' + username + '========\n')
    file_precision.write('\n%s of day:'%(beg_end))
    file_precision.write('\n1 hour range - precision: ' + str(tp_hour/float(total_days)))
    file_precision.write('\n15 min range - precision: ' + str(tp_15min/float(total_days)))
    file_precision.write('\n1 min range - precision: ' + str(tp_min/float(total_days)))

def initialize_list(tp_list, fp_list, tn_list, fn_list):

    for i in range(0, len(time_intervals)):
        tp_list['beg'+str(time_intervals[i])] = 0
        tp_list['end'+str(time_intervals[i])] = 0
        fp_list['beg'+str(time_intervals[i])] = 0
        fp_list['end'+str(time_intervals[i])] = 0
        tn_list['beg'+str(time_intervals[i])] = 0
        tn_list['end'+str(time_intervals[i])] = 0
        fn_list['beg'+str(time_intervals[i])] = 0
        fn_list['end'+str(time_intervals[i])] = 0

def get_precision(traces, blocks, interval):
    dt, tp, tn, fp, fn, traces_days, matched_days = 0, 0, 0, 0, 0, 0, 0
    first_date = min(traces[0], blocks[0])
    last_date = max(traces[len(traces)-1], blocks[len(blocks)-1])
    #print 'first_day :' + str(first_date.day)
    #print 'last_day : ' + str(last_date.day)
    for timst in traces:
        if dt != timst.day:
            traces_days += 1
            dt = timst.day
            check = False
            for timst_block in blocks:
                if timst_block.date() == timst.date() and check == False:
                    matched_days += 1
                    check = True
                    time_diff =  abs(timst_block-timst).total_seconds()
                    if time_diff <= 60*interval:
                        tp +=1
                        #print timst_block
                        #print timst
                        #print '====='
                    else:
                        #person not awaken yet
                        if timst < timst_block:
                            fn += 1
                        #person already awaken
                        else:
                            fp += 1
                    #break

    total_days = abs(first_date.day - last_date.day)
    if total_days > matched_days:
        #more info on dns and http reqs than act
        if traces_days > matched_days:
            fp += total_days - matched_days
        #days without info on both
        elif traces_days == matched_days:
            tn += total_days - matched_days
        #more info on act than dns and http
        else:
            fn += total_days - matched_days

    #return diff_hour, diff_15min, diff_min, total_days
    return tp, fp, tn, fn, total_days


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
                    plot_cdf_interval_times(iat_block, user_traces.rsplit('.', 1)[0], str(user_traces.rsplit('.')[-1]+'-avast-white-inBlock'), '', 'figs_CDF_mix')
                if iat_out_block:
                    plot_cdf_interval_times(iat_out_block, user_traces.rsplit('.', 1)[0], str(user_traces.rsplit('.')[-1]+'-avast-white-outBlock'), '', 'figs_CDF_mix')
                break
            else:
                continue


def trace_in_interval(trace_beg, trace_end, mix_beg, mix_end):
    for i in range(0, len(mix_beg)):
        if trace_beg <= mix_end[i] and trace_end >= mix_beg[i]:
            return 1
    return 0
            

def plot_ROC_curve(tp_list, fp_list, tn_list, fn_list, username, beg_end):

    x = []
    y = []
    
    print username
    print beg_end
    print 'tp_list: ' + str(tp_list)
    print 'fp_list: ' + str(fp_list)
    print 'fn_list: ' + str(fn_list)
    print 'tn_list: ' + str(tn_list)
    #print 'total: ' + str(total)

    #for i in range (0, len(tp_list)):
    for i in range (0, len(time_intervals)):
        key = str(beg_end)+str(time_intervals[i])
        if tp_list[key]+fp_list[key] != 0 and tp_list[key]+fn_list[key] != 0:
            #sensitivity = tp_list[i]/float(tp_list[i]+fn_list[i])
            #specificity = tn_list[i]/float(fp_list[i]+tn_list[i])
            precision = tp_list[key]/float(tp_list[key]+fp_list[key])
            recall = tp_list[key]/float(tp_list[key]+fn_list[key])
            x.append(precision)# false_positive_rate
            y.append(recall)# true_positive_rate 

    print 'recall ' + str(y)
    print 'precision ' + str(x)
    print '======'

    # This is the ROC curve
    sns.set(style='whitegrid')
    plt.title('ROC curve [interval - filtered]')
    plt.ylabel('Recall')
    plt.ylim((0.0, 1.0))
    plt.xlabel('Precision')
    plt.xlim((0.0, 1.0))
    plt.plot(x,y, 'orange')

    #time text
    """pos = defaultdict(int)
    for i in range(0, len(x)):
        label = str(time_intervals[i]) + ' min'
        if not pos[x[i]+y[i]]:
            pos[x[i]+y[i]] = 0
        else:
            pos[x[i]+y[i]] += 0.1
            print pos[x[i]+y[i]]

        plt.text (pos[x[i]+y[i]] + x[i], y[i], label, bbox=dict(facecolor='blue', alpha=0.5))"""

    plt.savefig('figs_ROC_curves/filtered-%s.png' % (beg_end))
    plt.close()
    

if __name__ == "__main__":
    main()
