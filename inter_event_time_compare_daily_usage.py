import datetime

from inter_event_time_analysis_activities import get_activities_inter_times
from inter_event_time_analysis import make_block_usage
from inter_event_time_by_url_analysis import get_filtered_traces
from inter_event_time_theoretical_count import get_interval_list

from collections import defaultdict

def compare_daily_activity():

    act_beg, act_end = get_activities_inter_times()
    traces_dict = get_filtered_traces()

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
        
        #get traces in seconds
        for timst in timsts:
            timst = timst.replace(microsecond=0)
            traces.append(timst)

        traces = list(set(traces))
        traces = sorted(traces)

        interval_list = get_interval_list(sorted(traces))
        traces = get_seconds_interval_list(interval_list)

        act_beg_final, act_end_final = activities_in_seconds(act_beg[user], act_end[user])
        
        first_day = act_beg_final[0]
        first_day = first_day.replace(hour=00, minute=00, second=00, microsecond=0)
        last_day = act_end_final[len(act_end_final)-1]
        last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=0)

        for i in error_window:
            tp, fn, fp, tn = get_tp_fn_fp_tn(traces, act_beg_final, act_end_final, first_day, last_day, i)

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

        print 'error window' + str(i)
        print 'precision ' + str(precision*100) + '%'
        print 'recall ' + str(recall*100) + '%'

def initialize_dict(var_dict, error_window):
    
    for i in error_window:
        var_dict[i] = 0


def get_tp_fn_fp_tn(traces, act_beg, act_end, first_day, last_day, error_window):

    j = 0
    tp, fn, fp, tn = 0, 0, 0, 0
    act_duration = 0

    for i in range(0, len(act_beg)):
        if j == len(traces):
            break
        current_trace = traces[j]
        current_beg = act_beg[i]
        current_end = act_end[i]

        #print '-------'
        #print current_beg
        #print current_end

        while current_trace <= current_end and j < len(traces):
            while current_trace >= (current_beg - datetime.timedelta(0,error_window)) and current_trace <= (current_end + datetime.timedelta(0,error_window)):
                #print '======'
                #print 'beg ' + str(current_beg)
                #print 'end ' + str(current_end)
                #print 'curr ' + str(current_trace)
                tp += 1
                j += 1
                if j == len(traces):
                    break
                current_trace = traces[j]
            
            while current_trace < (current_beg - datetime.timedelta(0,error_window)):
                #print '*********'
                #print 'beg ' + str(current_beg)
                #print 'end ' + str(current_end)
                #print 'curr ' + str(current_trace)
                fp += 1
                j += 1
                if j == len(traces):
                    break
                current_trace = traces[j]

        while i == len(act_end) and j < len(traces):
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


if __name__ == "__main__":
    compare_daily_activity()
