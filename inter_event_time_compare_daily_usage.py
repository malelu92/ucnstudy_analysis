import datetime

from inter_event_time_analysis_activities import get_activities_inter_times
from inter_event_time_analysis import make_block_usage
from inter_event_time_by_url_analysis import get_filtered_traces


def compare_daily_activity():

    traces_dict = get_filtered_traces()
    act_beg, act_end = get_activities_inter_times()

    for user, timsts in traces_dict.iteritems():
        traces = []
        print user

        #get traces in seconds
        for timst in timsts:
            timst = timst.replace(microsecond=0)
            traces.append(timst)
        traces = list(set(traces))
        traces = sorted(traces)
        #for elem in traces:
            #print elem

        #get act timsts in seconds
        #act_timsts = get_seconds_activities(act_beg[user], act_end[user])
        #for elem in act_timsts:
            #print elem
        
        first_day = act_beg[user][0]
        first_day = first_day.replace(hour=00, minute=00, second=00, microsecond=0)
        last_day = act_end[user][len(act_end[user])-1]
        last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=0)
        #print first_day
        #print last_day

        precision, recall = get_precision_and_recall(traces, act_beg[user], act_end[user], first_day, last_day)

        print 'precision ' + str(precision*100) + '%'
        print 'recall ' + str(recall*100) + '%'
    """for user_mix, blocks_beg in act_beg.items():
        for user_traces, row in traces.items():
            #get same user
            if user_mix == user_traces:
                print user_mix
                traces_beg, traces_end = make_block_usage(sorted(traces[user_traces]), 60*10)"""

def get_precision_and_recall(traces, act_beg, act_end, first_day, last_day):

    j = 0
    tp, fn = 0, 0
    act_duration = 0

    for i in range(0, len(act_beg)):
        print i
        if j == len(traces):
            break

        current_trace = traces[j]
        current_beg = act_beg[i]
        current_end = act_end[i]

        while current_trace < current_beg:
            print '*********'
            print 'beg ' + str(current_beg)
            print 'end ' + str(current_end)
            print 'curr ' + str(current_trace)
            fn += 1
            j += 1
            if j == len(traces):
                break
            current_trace = traces[j]
            
        while current_trace > current_beg and current_trace < current_end:
            #print '======'
            #print 'beg ' + str(current_beg)
            #print 'end ' + str(current_end)
            #print 'curr ' + str(current_trace)
            tp += 1
            j += 1
            if j == len(traces):
                break
            current_trace = traces[j]

        while i == len(act_end) and j < len(traces):
            fn += 1
            j += 1
        
        act_duration += (current_end - current_beg).total_seconds()

    fp = int(act_duration) - tp
    non_act_duration = (last_day - first_day).total_seconds() - act_duration
    tn = int(non_act_duration) - fn

    precision = float(tp)/(tp+fp)
    recall = float(tp)/(tp + fn)
    print 'first_day ' + str(first_day)
    print 'last day ' + str(last_day)
    print 'act_duration ' + str(act_duration)
    print 'number of traces ' + str(len(traces))
    print 'non act duration ' + str(non_act_duration)
    print 'tp ' + str(tp)
    print 'fn ' + str(fn)
    print 'tn ' + str(int(tn))
    print 'fp ' + str(int(fp))
    return precision, recall


def get_precision_slow(traces, act_timsts, first_day, last_day):

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


def get_seconds_activities(act_beg_user, act_end_user):

    act_list = []
    
    for i in range(0, len(act_beg_user)):
        timst = act_beg_user[i]
        timst = timst.replace(microsecond=0)

        while timst < act_end_user[i]:
            act_list.append(timst)
            timst += datetime.timedelta(0,1)

    return sorted(act_list)
        



"""def get_matching_nonmatching_time(traces_beg, traces_end, act_beg, act_end):

    cont_traces = 0
    cont_act = 0
    match_time = 0
    non_match_time = 0
    first_daytime = min(act_beg[0], traces_beg[0])
    last_daytime = max(act_end[len(act_end)-1], traces_end[len(traces_end)-1])
    for i in range(0,len(act_beg)):
        a_beg = act_beg[i]
        current_date = a_beg.date()
        #t_beg = traces_beg[cont_traces]
        t_beg_curr_day = get_beginning_of_day_time(current_date, t_beg)
        a_beg_curr_day = get_beginning_of_day_time(current_date, a_beg)
        match_time += min(a_beg_curr_day.time().total_seconds(), t_beg_curr_day.time().total_seconds())
        if a_beg.date() <= t_beg.date():


def get_beginning_of_day_time(date, timsts):

    for elem in timsts:
        if elem.date() == date:
            return elem
    return datetime.datetime.now()"""

if __name__ == "__main__":
    compare_daily_activity()
