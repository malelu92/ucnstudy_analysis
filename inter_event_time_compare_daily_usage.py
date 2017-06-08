import datetime

from collections import defaultdict

from inter_event_time_analysis_activities import get_activities_inter_times
from inter_event_time_analysis import make_block_usage
from inter_event_time_by_url_analysis import get_filtered_traces
from inter_event_time_theoretical_count import get_interval_list

from final_algorithm import final_algorithm_filtered_traces

def compare_daily_activity():

    act_beg, act_end = get_activities_inter_times()
    #traces_dict = get_filtered_traces()
    traces_dict = final_algorithm_filtered_traces()

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

        #interval_list = get_interval_list(sorted(traces))
        #traces = get_seconds_interval_list(interval_list)

        #print 'POST INterval TOTAL TRACES'
        #print len(traces)

        act_beg_final, act_end_final = activities_in_seconds(act_beg[user], act_end[user])
        
        first_day = act_beg_final[0]
        first_day = first_day.replace(hour=00, minute=00, second=00, microsecond=0)
        last_day = act_end_final[len(act_end_final)-1]
        last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=0)

        for i in error_window:
            #tp, fn, fp, tn = get_tp_fn_fp_tn(traces, act_beg_final, act_end_final, first_day, last_day, i, 1)
            tp, fn, fp, tn = get_tp_fn_fp_tn_sliding_window(traces, act_beg_final, act_end_final, first_day, last_day, i, 10)

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

        print 'finaltp ' + str(tp_dict[i])
        print 'error window' + str(i)
        print 'precision ' + str(precision*100) + '%'
        print 'recall ' + str(recall*100) + '%'


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
    if sliding_window_size == 1:
        current_bucket_end = current_bucket_beg
    else:
        current_bucket_end = current_bucket_beg + datetime.timedelta(0,sliding_window_size)

    cont_trace = 0
    cont_act = 0
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

        trace_in, cont_trace = trace_in_bucket(daily_traces, current_bucket_beg, current_bucket_end, error_window, cont_trace)
        act_in, cont_act = act_in_bucket(daily_act_beg, daily_act_end, current_bucket_beg, current_bucket_end, error_window, cont_act)

        if trace_in and act_in:
            tp += 1

        elif trace_in and not act_in:
            #print 'beg and end of bucket: ' + str(current_bucket_beg) + ' ' + str(current_bucket_end)
            #if cont_act < len(act_beg):
                #print act_beg[cont_act]
                #print act_end[cont_act]
            fp += 1

        elif not trace_in and act_in:
            fn += 1

        elif not trace_in and not act_in:
            tn += 1
            
        if sliding_window_size == 1:
            current_bucket_beg = current_bucket_beg + datetime.timedelta(0,sliding_window_size)
        else:
            current_bucket_beg = current_bucket_end
        current_bucket_end = current_bucket_end + datetime.timedelta(0,sliding_window_size)
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

def trace_in_bucket(traces, bucket_beg, bucket_end, error_window, cont):
    beg_limit = bucket_beg - datetime.timedelta(0,error_window)
    end_limit = bucket_end + datetime.timedelta(0,error_window)

    bucket_cont = cont
    in_bucket = False

    #if cont >= len(traces):
        #return False, cont

    if traces:
        for elem in traces:
            if elem >= beg_limit and elem <= end_limit:
                return True, cont

            elif elem > end_limit:
                return False, cont

    return False, cont

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


def act_in_bucket(act_beg, act_end, bucket_beg, bucket_end, error_window, cont):
    
    beg_limit = bucket_beg - datetime.timedelta(0,error_window)
    end_limit = bucket_end + datetime.timedelta(0,error_window)

    for j in range(0, len(act_beg)):
        date_ranges_overlap = max(act_beg[j], beg_limit) <= min(act_end[j], end_limit)
        if date_ranges_overlap:
            return True, cont

        if act_beg[j] > end_limit:
            return False, cont

    return False, cont

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


def get_tp_fn_fp_tn(traces, act_beg, act_end, first_day, last_day, error_window, sliding_window):

    j = 0
    tp, fn, fp, tn = 0, 0, 0, 0
    act_duration = 0
    end_fp = 0

    for i in range(0, len(act_beg)):
        if j >= len(traces):
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

                previous_trace = current_trace
                j += 1
                if j >= len(traces):
                    break
                #print '======='
                #print 'j ' + str(j)
                #print 'traces ' + str(len(traces))
                current_trace = traces[j]

                while (current_trace - previous_trace).total_seconds() < sliding_window:
                    j += 1
                    if j >= len(traces):
                        break
                    current_trace = traces[j]
            
            while current_trace < (current_beg - datetime.timedelta(0,error_window)):
                #print '*********'
                #print 'beg ' + str(current_beg)
                #print 'end ' + str(current_end)
                #print 'curr ' + str(current_trace)
                previous_trace = current_trace
                fp += 1
                j += 1
                if j >= len(traces):
                    break
                current_trace = traces[j]

                while (current_trace - previous_trace).total_seconds() < sliding_window:
                    j += 1
                    if j >= len(traces):
                        break
                    current_trace = traces[j]

        while i == len(act_end) and j < len(traces):
            end_fp += 1
            j += 1
        fp = fp + int(end_fp)/sliding_window

        
    for i in range(0, len(act_beg)):    
        act_duration += (act_end[i] - act_beg[i]).total_seconds()

    if int(act_duration) - tp > 0:
        fn = int(int(act_duration)/sliding_window) - tp
    else:
        fn = 0

    #fp = len(traces) - tp
    non_act_duration = (last_day - first_day).total_seconds() - act_duration
    tn = int(int(non_act_duration)/sliding_window) - fp

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
