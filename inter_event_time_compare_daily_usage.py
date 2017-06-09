import datetime
import datautils
from matplotlib import dates
import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict

from inter_event_time_analysis_activities import get_activities_inter_times
from inter_event_time_analysis import make_block_usage
from inter_event_time_by_url_analysis import get_filtered_traces
from inter_event_time_theoretical_count import get_interval_list_predefined_gap

from final_algorithm import final_algorithm_filtered_traces

def compare_daily_activity():

    results_file = open('results_9_june.txt','w')

    f_blacklist = [False, True, True, False]
    f_seconds = [False, True, False, True]
    f_spike = [False, True, False, True]
    filter_type = ['not filtered', 'totally_filtered', 'only blacklist', 'only interval']

    sliding_window = [0, 3, 5, 7, 10, 15]

    for f_type in range (0, 4):
        act_beg, act_end = get_activities_inter_times()
        traces_dict = final_algorithm_filtered_traces(f_blacklist[f_type], f_seconds[f_type], f_spike[f_type])

        for cont_sliding_window in range (0, 6):
            results_file.write('\n=========' + filter_type[f_type] + '============')
            results_file.write('\n***sliding window size ' + str(sliding_window[cont_sliding_window]) + ' seconds***')

            #traces_dict = get_filtered_traces()

            tp_dict = defaultdict(int)
            tn_dict = defaultdict(int)
            fp_dict = defaultdict(int)
            fn_dict = defaultdict(int)
            error_window = [0, 15, 30, 45, 60, 60*2, 60*3, 60*4, 60*5]

            initialize_dict(tp_dict, error_window)
            initialize_dict(tn_dict, error_window)
            initialize_dict(fp_dict, error_window)
            initialize_dict(fn_dict, error_window)

            for user, timsts in traces_dict.iteritems():
                traces = []
                print user
        
                #print '----------'
                #print len(timsts)
                #get traces in seconds
                for timst in timsts:
                    timst = timst.replace(microsecond=0)
                    traces.append(timst)

                traces = list(set(traces))
                traces = sorted(traces)

                #print 'TOTAL TRACES'
                #print len(traces)

                #interval_list = get_interval_list_predefined_gap(sorted(traces), 1)
                #traces = get_seconds_interval_list(interval_list)

                #print 'POST INterval TOTAL TRACES'
                #print len(traces)

                act_beg_final, act_end_final = activities_in_seconds(act_beg[user], act_end[user])
        
                first_day = act_beg_final[0]
                first_day = first_day.replace(hour=00, minute=00, second=00, microsecond=0)
                last_day = act_end_final[len(act_end_final)-1]
                last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=0)

                for i in error_window:
                    #tp, fn, fp, tn = get_tp_fn_fp_tn(traces, act_beg_final, act_end_final, first_day, last_day, i, filter_type[f_type], gap_int[gap_size], user)
                    tp, fn, fp, tn = get_tp_fn_fp_tn_sliding_window(traces, act_beg_final, act_end_final, first_day, last_day, i, sliding_window[cont_sliding_window])

                    tp_dict[i] += tp
                    tn_dict[i] += tn
                    fp_dict[i] += fp
                    fn_dict[i] += fn

            precision_dict = defaultdict(int)
            recall_dict = defaultdict(int)
            initialize_dict(precision_dict, error_window)
            initialize_dict(recall_dict, error_window)

            for i in error_window:
                precision = float(tp_dict[i])/(tp_dict[i]+fp_dict[i])
                recall = float(tp_dict[i])/(tp_dict[i] + fn_dict[i])

                print '======error window' + str(i)
                print 'sum of tp + fp ... ' + str(fn_dict[i]+fp_dict[i]+tn_dict[i]+tp_dict[i])
                print 'finalfn ' + str(fn_dict[i])
                print 'finalfp ' + str(fp_dict[i])
                print 'finaltp ' + str(tp_dict[i])
                print 'precision ' + str(precision*100) + '%'
                print 'recall ' + str(recall*100) + '%'

                results_file.write('\nerror window' + str(i) + '\nprecision ' + str(precision*100) + '\nrecall ' + str(recall*100))
                results_file.write('\nfinalfn ' + str(fn_dict[i]) + ' finalfp ' + str(fp_dict[i]) + ' finaltp ' + str(tp_dict[i]) + ' final tn ' + str(tn_dict[i]))


def get_daily_traces(traces, bucket_beg):
    daily_list = []
    for elem in traces:
        if elem.date() == bucket_beg.date():
            daily_list.append(elem)

    return daily_list

def get_daily_activities(act_beg, act_end, bucket_beg):
    daily_list_beg = []
    daily_list_end = []
    cont = 0
    for elem in act_beg:
        if elem.date() == bucket_beg.date():
            daily_list_beg.append(elem)
            daily_list_end.append(act_end[cont])
        cont += 1

    return daily_list_beg, daily_list_end
    
def get_tp_fn_fp_tn_sliding_window(traces, act_beg, act_end, first_day, last_day, error_window, sliding_window_size):
    tp, fn, fp, tn = 0, 0, 0, 0
    current_bucket_beg = first_day
    current_bucket_end = current_bucket_beg + datetime.timedelta(0,sliding_window_size)

    cont = 0

    daily_traces = get_daily_traces(traces, current_bucket_beg)
    daily_act_beg, daily_act_end = get_daily_activities(act_beg, act_end, current_bucket_beg)
    current_date = current_bucket_beg.date()
    previous_date = current_bucket_beg.date()

    while current_bucket_end <= last_day + datetime.timedelta(0,1):
        if current_date != previous_date:
            previous_date = current_date
            print current_date
            daily_traces = get_daily_traces(traces, current_bucket_beg)
            daily_act_beg, daily_act_end = get_daily_activities(act_beg, act_end, current_bucket_beg)

        trace_in = trace_in_bucket(daily_traces, current_bucket_beg, current_bucket_end, error_window)
        act_in = act_in_bucket(daily_act_beg, daily_act_end, current_bucket_beg, current_bucket_end, error_window)

        if trace_in and act_in:
            tp += 1

        elif trace_in and not act_in:
            fp += 1

        elif not trace_in and act_in:
            fn += 1

        elif not trace_in and not act_in:
            tn += 1
            
        current_bucket_beg = current_bucket_beg + datetime.timedelta(0,1)
        current_bucket_end = current_bucket_end + datetime.timedelta(0,1)
        current_date = current_bucket_beg.date()

    print 'error window' + str(error_window)
    print 'cont ' + str(cont)
    print 'number of traces ' + str(len(traces))
    print 'tp ' + str(tp)
    print 'fn ' + str(fn)
    print 'tn ' + str(int(tn))
    print 'fp ' + str(int(fp))
    print 'tp + fn + tn + fp = ' + str(tp + fn + tn + fp)
    return tp, fn, fp, tn

def trace_in_bucket(traces, bucket_beg, bucket_end, error_window):
    beg_limit = bucket_beg - datetime.timedelta(0,error_window)
    end_limit = bucket_end + datetime.timedelta(0,error_window)

    in_bucket = False

    if traces:
        for elem in traces:
            if elem >= beg_limit and elem <= end_limit:
                return True

            elif elem > end_limit:
                return False

    return False

    """while cont < len(traces):
        if traces[cont] > end_limit and not in_bucket:
            return False, bucket_cont

        elif traces[cont] > end_limit and in_bucket:
            return True, bucket_cont

        if traces[cont] >= beg_limit and traces[cont] <= end_limit:
            cont += 1
            in_bucket = True
            if traces[bucket_cont] >= bucket_beg and traces[bucket_cont] <= bucket_end:
                bucket_cont += 1
            #return True, cont


        #cont += 1
    return False, bucket_cont"""


def act_in_bucket(act_beg, act_end, bucket_beg, bucket_end, error_window):
    
    beg_limit = bucket_beg - datetime.timedelta(0,error_window)
    end_limit = bucket_end + datetime.timedelta(0,error_window)

    for j in range(0, len(act_beg)):
        date_ranges_overlap = max(act_beg[j], beg_limit) <= min(act_end[j], end_limit)
        if date_ranges_overlap:
            return True

        if act_beg[j] > end_limit:
            return False

    return False

    """if (act_beg[cont] <= bucket_beg and  #+ datetime.timedelta(0,error_window) and
            act_end[cont] >= bucket_beg) or \
            #- datetime.timedelta(0,error_window)) or \
        (act_beg[cont] <= bucket_end and # + datetime.timedelta(0,error_window) and
         act_end[cont] >= bucket_end): # - datetime.timedelta(0,error_window)):
    #original:
    in_bucket = False
    cont2 = 0

    if cont >= len(act_beg):
        return False, cont
    #while cont < len(act_beg):

    #activity is bigger than bucket
    if (act_beg[cont] <= bucket_beg and act_end[cont] >= bucket_end):
        in_bucket = True
        el = act_beg[cont]
        if el.day == 19 and el.hour == 9 and el.minute == 46:
            print '**act_beg ' + str(el)
            print '**act_end ' + str(act_end[cont])
            print '**beg and end of bucket: ' + str(bucket_beg) + ' ' + str(bucket_end)

        if (cont+1) < len(act_beg):
            if act_end[cont] <= bucket_end:
                cont += 1
    else:
        #activity intersects or is contained in bucket
        while (act_beg[cont] <= bucket_beg and act_end[cont] > bucket_beg and act_end[cont] <= bucket_end) or (act_beg[cont] >= bucket_beg and act_beg[cont] < bucket_end and act_end[cont] >= bucket_end) or (act_beg[cont] >= bucket_beg and act_end[cont] <= bucket_end):# or (act_beg[cont] <= bucket_beg and act_end[cont] >= bucket_end):
            in_bucket = True
            el = act_beg[cont]
            if el.day == 19 and el.hour == 9 and el.minute == 46:
                print 'act_beg ' + str(el)
                print 'act_end ' + str(act_end[cont])
                print 'beg and end of bucket: ' + str(bucket_beg) + ' ' + str(bucket_end)

            if cont+1 >= len(act_beg):
                return True, cont

            cont+=1

        el = act_beg[cont]
        if el.day == 19 and el.hour == 9 and el.minute == 46 and in_bucket == False:
            print '==act_beg ' + str(el)
            print '==act_end ' + str(act_end[cont])
            print '==beg and end of bucket: ' + str(bucket_beg) + ' ' + str(bucket_end)

    #activity is bigger than bucket
    if (act_beg[cont] <= bucket_beg and act_end[cont] >= bucket_end):
        in_bucket = True

    if in_bucket:
        return True, cont
        #cont2+=1

    return False, cont
    #fim do original
    while cont < len(act_beg):
        if (act_beg[cont] <= bucket_beg and act_end[cont] >= bucket_beg) or (act_beg[cont] <= bucket_end and act_end[cont] >= bucket_end):
            return True, cont
        if act_beg[cont] > bucket_end:
            return False, cont
        cont += 1

    return False, cont"""

def initialize_dict(var_dict, error_window):
    
    for i in error_window:
        var_dict[i] = 0


def get_tp_fn_fp_tn(traces, act_beg, act_end, first_day, last_day, error_window, filtering, gap_size, user_id):

    j = 0
    tp, fn, fp, tn = 0, 0, 0, 0
    tp_timsts = []
    fp_timsts = []
    tn_timsts = []
    fn_timsts = []
    act_duration = 0

    for i in range(0, len(act_beg)):
        if j == len(traces):
            break
        current_trace = traces[j]
        current_beg = act_beg[i]
        current_end = act_end[i]

        while current_trace <= current_end and j < len(traces):
            while current_trace >= (current_beg - datetime.timedelta(0,error_window)) and current_trace <= (current_end + datetime.timedelta(0,error_window)):
                tp_timsts.append(current_trace)
                tp += 1
                j += 1
                if j == len(traces):
                    break
                current_trace = traces[j]

            while current_trace < (current_beg - datetime.timedelta(0,error_window)):
                fp_timsts.append(current_trace)
                fp += 1
                j += 1
                if j == len(traces):
                    break
                current_trace = traces[j]

    while i == len(act_end) and j < len(traces):
        fp_timsts.append(current_trace)
        fp += 1
        j += 1
         
    for i in range(0, len(act_beg)):    
        act_duration += (act_end[i] - act_beg[i]).total_seconds()
 
    if int(act_duration) - tp > 0:
        fn = int(act_duration) - tp
    else:
        fn = 0

    #fp = len(traces) - tp
    non_act_duration = (last_day - first_day).total_seconds() - act_duration
    tn = int(non_act_duration) - fp
 
    precision = float(tp)/(tp+fp)
    recall = float(tp)/(tp + fn)
 

    plot_traces(tp_timsts, fn_timsts, fp_timsts, tn_timsts, user_id, error_window, filtering, gap_size)

    #print 'first_day ' + str(first_day)
    #print 'last day ' + str(last_day)
    print 'act_duration ' + str(act_duration)
    print 'number of traces ' + str(len(traces))
    print 'non act duration ' + str(non_act_duration)
    print 'tp ' + str(tp)
    print 'fn ' + str(fn)
    print 'tn ' + str(int(tn))
    print 'fp ' + str(int(fp))
    return tp, fn, fp, tn

"""def get_precision_slow(traces, act_timsts, first_day, last_day):

    total_time = (last_day-first_day).total_seconds()
    current_time = first_day
    tp, fn, fp, tn = 0, 0, 0, 0

    print total_time
    for i in range(0, int(total_time)):
        print i
        if (current_time in traces):
            traces_ok = True
        else:
            traces_ok = False

        if (current_time in act_timsts):
            act_timsts_ok = True
        else:
            act_timsts_ok = False

        if traces_ok and act_timsts_ok:
            tp += 1
        elif traces_ok and not(act_timsts_ok):
            fn += 1
        elif not(traces_ok) and act_timsts_ok:
            fp += 1
        else:
            tn += 1

        current_time += datetime.timedelta(0,1)            

    print tp
    print tn
    print fn
    print fp

"""

def get_seconds_interval_list(interval_list):

    traces = []
    for key, values in interval_list.iteritems():
        beg = values[0]
        end = values[len(values)-1]
        #print '+++++'
        #print beg
        #print end
        timst = beg.replace(microsecond=0)
        end = end.replace(microsecond=0)
        interval = (end-timst).total_seconds()
    
        while timst < end:
            traces.append(timst)
            timst += datetime.timedelta(0,1)

    return traces

def activities_in_seconds(act_beg, act_end):
    
    act_beg_final = []
    act_end_final = []

    for i in range(0, len(act_beg)):

        timst_beg = act_beg[i]
        timst_beg = timst_beg.replace(microsecond=0)
        act_beg_final.append(timst_beg)

        timst_end = act_end[i]
        timst_end = timst_end.replace(microsecond=0)
        act_end_final.append(timst_end)

    return act_beg_final, act_end_final

def get_seconds_activities(act_beg_user, act_end_user):

    act_list = []
    
    for i in range(0, len(act_beg_user)):
        timst = act_beg_user[i]
        timst = timst.replace(microsecond=0)

        while timst < act_end_user[i]:
            act_list.append(timst)
            timst += datetime.timedelta(0,1)

    return sorted(act_list)

def plot_traces(traces_tp, traces_fn, traces_fp, traces_tn, user_id, error_window, filtering, gap_size):
    x_tp = []
    y_tp = []
    x_fp = []
    y_fp = []
    x_tn = []
    y_tn = []
    x_fn = []
    y_fn = []
    sns.set(style='whitegrid')
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))

    for timst in traces_tp:
        x_tp.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y_tp.append(timst.date())

    for timst in traces_tn:
        x_tn.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y_tn.append(timst.date())

    for timst in traces_fp:
        x_fp.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y_fp.append(timst.date())

    for timst in traces_fn:
        x_fn.append(timst.hour+timst.minute/60.0+timst.second/3600.0)
        y_fn.append(timst.date())

    #y_label = list(set(y))
    hfmt = dates.DateFormatter('%m-%d')
    ax.yaxis.set_major_formatter(hfmt)

    ax.plot(x_tp,y_tp, '.g')
    ax.plot(x_tn,y_tn, '.b')
    ax.plot(x_fp,y_fp, '.r')
    ax.plot(x_fn,y_fn, '.black')

    ax.set_title('TP + TN + FN + FP [user=%s]'%(user_id), fontsize = 30)#, device=%s]'%(username, platform))
    ax.set_ylabel('Date')
    #ax.set_yticks(y_label)

    ax.set_xlabel('Matches during day')
    ax.set_xlim(0,24)

    plt.tight_layout()
    fig.savefig('figs_device_tps_along_day/%s-%s-gap:%d-error:%d.png' % (user_id, filtering, gap_size, error_window))
    plt.close(fig)

if __name__ == "__main__":
    compare_daily_activity()
