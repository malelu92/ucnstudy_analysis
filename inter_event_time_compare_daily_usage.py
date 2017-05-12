#from inter_event_time_analysis import get_traces
from inter_event_time_analysis_activities import get_activities_inter_times
from inter_event_time_analysis import make_block_usage
from inter_event_time_by_url_analysis import get_filtered_traces


def compare_daily_activity():

    traces_dict = get_filtered_traces()
    act_beg, act_end = get_activities_inter_times()

    for user, timsts in traces_dict.iteritems():
        traces = []
        print user
        #timsts = sorted(timsts)
        for timst in timsts:
            timst = timst.replace(microsecond=0)
            traces.append(timst)

        traces = list(set(traces))
        traces = sorted(traces)
        for elem in traces:
            print elem
    """for user_mix, blocks_beg in act_beg.items():
        for user_traces, row in traces.items():
            #get same user
            if user_mix == user_traces:
                print user_mix
                traces_beg, traces_end = make_block_usage(sorted(traces[user_traces]), 60*10)"""







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
