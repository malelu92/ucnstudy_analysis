import datetime
import datautils
from matplotlib import dates
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.markers import TICKDOWN

from collections import defaultdict

#from inter_event_time_analysis_activities import get_activities_inter_times
from inter_event_time_analysis import make_block_usage
#from inter_event_time_by_url_analysis import get_filtered_traces
from inter_event_time_theoretical_count import get_interval_list_predefined_gap

from final_algorithm import final_algorithm_filtered_traces

def compare_daily_activity():

    results_file = open('results_14_june.txt','w')

    #f_blacklist = [False, True, True, False]
    #f_seconds = [False, True, False, True]
    #f_spike = [False, True, False, True]
    filter_type = ['interval_filtered', 'filtered', 'not_filtered', 'blist_filtered']

    sliding_window = [0, 5, 10]

    #act_file = open('user_online_activities_devtraffic.txt', 'r')
    act_file = open('user_activities_devtraffic.txt', 'r')
    act_dict = get_timst_from_file(act_file)

    for f_type in range (0, 4):
        #act_beg, act_end = get_activities_inter_times()
        #traces_dict = final_algorithm_filtered_traces(f_blacklist[f_type], f_seconds[f_type], f_spike[f_type])
        #traces_dict = get_filtered_traces()
        for cont_sliding_window in range (0, 3):
            tp, tn, fn, fp = 0, 0, 0, 0
            traces_file = open('traces_bucket_%d_%s'%(sliding_window[cont_sliding_window], filter_type[f_type]), 'r')
            traces_dict = get_timst_from_file(traces_file)

            results_file.write('\n=========' + filter_type[f_type] + '============')
            results_file.write('\n***sliding window size ' + str(sliding_window[cont_sliding_window]+1) + ' seconds***')

            #tp_dict = defaultdict(int)
            #tn_dict = defaultdict(int)
            #fp_dict = defaultdict(int)
            #fn_dict = defaultdict(int)
            #error_window = [0]#, 15, 30, 45, 60, 60*2, 60*3, 60*4, 60*5]

            #initialize_dict(tp_dict, error_window)
            #initialize_dict(tn_dict, error_window)
            #initialize_dict(fp_dict, error_window)
            #initialize_dict(fn_dict, error_window)

            for user, timsts in traces_dict.iteritems():
                traces = []
                print user
        
                #get traces in seconds
                for timst in timsts:
                    #timst = timst.replace(microsecond=0)
                    traces.append(timst)

                traces = list(set(traces))
                traces = sorted(traces)

                #interval_list = get_interval_list_predefined_gap(sorted(traces), 1)
                #traces = get_seconds_interval_list(interval_list)

                #act_beg_final, act_end_final = activities_in_seconds(act_beg[user], act_end[user])

                #first_day = act_beg_final[0]
                #first_day = first_day.replace(hour=00, minute=00, second=00, microsecond=0)
                #last_day = act_end_final[len(act_end_final)-1]
                #last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=0)

                #for i in error_window:
                    #tp, fn, fp, tn = get_tp_fn_fp_tn(traces, act_beg_final, act_end_final, first_day, last_day, i, filter_type[f_type], gap_int[gap_size], user)
                    #tp, fn, fp, tn = get_tp_fn_fp_tn_sliding_window(traces, act_beg_final, act_end_final, first_day, last_day, i, sliding_window[cont_sliding_window], user, filter_type[f_type])

                #for i in error_window:
                tp_user, fn_user, fp_user, tn_user = get_tp_fn_fp_tn_second(traces, act_dict[user])
                tp += tp_user
                fn += fn_user
                fp += fp_user
                tn += tn_user
                #tp_dict[i] += tp
                #tn_dict[i] += tn
                #fp_dict[i] += fp
                #fn_dict[i] += fn

            #precision_dict = defaultdict(int)
            #recall_dict = defaultdict(int)
            #initialize_dict(precision_dict, error_window)
            #initialize_dict(recall_dict, error_window)

            if tp+fp > 0:
                precision = float(tp)/(tp+fp)
            else: 
                precision = -1
            if tp+fn > 0:
                recall = float(tp)/(tp+fn)
            else:
                recall = -1
            
            #for i in error_window:
                #precision = float(tp_dict[i])/(tp_dict[i]+fp_dict[i])
                #recall = float(tp_dict[i])/(tp_dict[i] + fn_dict[i])

            #print '======error window' + str(i)
            print 'sum of tp + fp ... ' + str(fn+fp+tn+tp)
            print 'finalfn ' + str(fn)
            print 'finalfp ' + str(fp)
            print 'finaltp ' + str(tp)
            print 'precision ' + str(precision*100) + '%'
            print 'recall ' + str(recall*100) + '%'

            results_file.write('\nprecision ' + str(precision*100) + '\nrecall ' + str(recall*100))
            results_file.write('\nfinalfn ' + str(fn) + ' finalfp ' + str(fp) + ' finaltp ' + str(tp) + ' final tn ' + str(tn))



def get_tp_fn_fp_tn_second(traces, acts):

    tp, fn, fp, tn = 0, 0, 0, 0

    if acts and traces:
        first_day = min(acts[0], traces[0])
        last_day = max(acts[len(acts)-1], traces[len(traces)-1])
    elif acts and not traces:
        first_day = acts[0]
        last_day = acts[len(acts)-1]
    else:
        first_day = traces[0]
        last_day = traces[len(traces)-1]

    first_day = first_day.replace(hour=00, minute=00, second=00, microsecond=0)
    last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=0)

    current_second = first_day

    daily_traces = get_daily_traces(traces, current_second)
    daily_act = get_daily_traces(acts, current_second)
    current_date = current_second.date()
    previous_date = current_second.date()

    while current_second <= last_day + datetime.timedelta(0,1):
        if current_date != previous_date:
            previous_date = current_date
            print current_date
            daily_traces = get_daily_traces(traces, current_second)
            daily_act = get_daily_traces(acts, current_second)

        trace_in = timst_in_second(daily_traces, current_second)
        act_in = timst_in_second(daily_act, current_second)

        if trace_in and act_in:
            tp += 1

        elif trace_in and not act_in:
            fp += 1

        elif not trace_in and act_in:
            fn += 1

        elif not trace_in and not act_in:
            tn += 1

        current_second = current_second + datetime.timedelta(0,1)
        current_date = current_second.date()

    print 'number of traces ' + str(len(traces))
    print 'tp ' + str(tp)
    print 'fn ' + str(fn)
    print 'tn ' + str(int(tn))
    print 'fp ' + str(int(fp))
    print 'tp + fn + tn + fp = ' + str(tp + fn + tn + fp)
    return tp, fn, fp, tn


def get_timst_from_file(data_file):

    content = data_file.read().splitlines()
    traces = defaultdict(list)
    cont = 0
    user = None

    for line in content:
        if cont == 0:
            cont += 1
            continue

        if line == 'bowen.laptop' or line == 'bridgeman.laptop2' or line == 'bridgeman.stuartlaptop' or line == 'chrismaley.loungepc' or line == 'chrismaley.mainpc' or line == 'clifford.mainlaptop' or line == 'gluch.laptop' or line == 'kemianny.mainlaptop' or line == 'neenagupta.workpc':
            user = line
        else:
            timst = datetime.datetime.strptime(line, "%Y-%m-%d %H:%M:%S")
            traces[user].append(timst)

    return traces


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
    
def get_tp_fn_fp_tn_sliding_window(traces, act_beg, act_end, first_day, last_day, error_window, sliding_window_size, user_id, filter_type):
    tp, fn, fp, tn = 0, 0, 0, 0
    current_bucket_beg = first_day
    current_bucket_end = current_bucket_beg + datetime.timedelta(0,sliding_window_size)
    tp_traces = []
    fn_traces = []
    fp_traces = []
    tn_traces = []

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
            tp_traces.append(current_bucket_beg)
            tp += 1

        elif trace_in and not act_in:
            fp += 1
            fp_traces.append(current_bucket_beg)

        elif not trace_in and act_in:
            fn += 1
            fn_traces.append(current_bucket_beg)

        elif not trace_in and not act_in:
            tn += 1
            tn_traces.append(current_bucket_beg)

        current_bucket_beg = current_bucket_beg + datetime.timedelta(0,1)
        current_bucket_end = current_bucket_end + datetime.timedelta(0,1)
        current_date = current_bucket_beg.date()

    plot_traces(tp_traces, fn_traces, fp_traces, tn_traces, user_id, error_window, filter_type, sliding_window_size)
    print 'error window' + str(error_window)
    print 'cont ' + str(cont)
    print 'number of traces ' + str(len(traces))
    print 'tp ' + str(tp)
    print 'fn ' + str(fn)
    print 'tn ' + str(int(tn))
    print 'fp ' + str(int(fp))
    print 'tp + fn + tn + fp = ' + str(tp + fn + tn + fp)
    return tp, fn, fp, tn


def timst_in_second(timsts, second_timst):

    if timsts:
        for elem in timsts:
            if elem == second_timst:
                return True

            elif elem > second_timst:
                return False

    return False

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

def plot_traces(traces_tp, traces_fn, traces_fp, traces_tn, user_id, error_window, filtering, sli_window):
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

    ax.plot(x_tn,y_tn, '3b', label = 'true negative')
    ax.plot(x_fp,y_fp, '3r', label = 'false positive')
    ax.plot(x_fn,y_fn, '3k', label = 'false negative')
    ax.plot(x_tp,y_tp, '3g', label = 'true positive')

    ax.set_title('TP + TN + FN + FP [user=%s]'%(user_id), fontsize = 30)#, device=%s]'%(username, platform))
    ax.set_ylabel('Date')
    #ax.set_yticks(y_label)

    ax.set_xlabel('Matches during day')
    ax.set_xlim(0,24)

    plt.legend (loc=2, borderaxespad=0.)
    plt.tight_layout()
    fig.savefig('figs_device_tps_along_day/%s-%s-swindow-%d-error-%d.png' % (user_id, filtering, sli_window, error_window))
    plt.close(fig)

if __name__ == "__main__":
    compare_daily_activity()
