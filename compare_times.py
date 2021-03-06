import seaborn as sns

from model.Base import Base
from model.Device import Device
from model.DnsReq import DnsReq
from model.User import User
from model.user_devices import user_devices

from collections import defaultdict
from datetime import timedelta
from datetime import datetime, date

import matplotlib.pyplot as plt
from matplotlib import dates

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from activities_analysis import get_activities_data
from devicetraffic_analysis import get_devtraffic_data
from dnsreqs_analysis import get_dns_data
from flow_analysis import get_flow_data
from httpreqs_analysis import get_http_data
from io_analysis import get_io_data
from location_analysis import get_locations_data

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def main():

    ses = Session()
    users = ses.query(User)

    devtfc_week_beg, devtfc_week_end = get_devtraffic_data()
    dns_week_beg, dns_week_end = get_dns_data()
    flow_week_beg, flow_week_end = get_flow_data()
    http_week_beg, http_week_end = get_http_data()
    loc_week_beg, loc_week_end = get_locations_data()
    io_week_beg, io_week_end = get_io_data()
    #act_week_beg, act_week_end = get_activities_data()

    io_beg = defaultdict(list)
    for key, value in io_week_beg.iteritems():
        io_beg[key.rpartition('.')[0]] = io_week_beg[key]
    #print(io_beg)
    io_end = defaultdict(list)
    for key, value in io_week_end.iteritems():
        io_end[key.rpartition('.')[0]] = io_week_end[key]

    devtfc_cmp_file = open('devtfc_comp.txt','w')
    dns_cmp_file = open('dns_comp.txt','w')    
    flow_cmp_file = open('flow_comp.txt','w')
    http_cmp_file = open('http_comp.txt','w')
    loc_cmp_file = open('loc_comp.txt','w')

    for user in users:
        name = user.username
        #only compares users with information on io
        cmp_results = defaultdict(list)
        if io_beg.has_key(name):
            print user.username
            #print devtfc_week_beg[user.username]
            # devtfc, dns, flow, http, loc === 0 -> no file === 1 -> file
            #generate_comparison_file(1, 1, 1, 1, 1, name)
            cmp_results['devtfc_beg'] = analize_timst_difference(devtfc_cmp_file, devtfc_week_beg[user.username], io_beg[name], 'devtfc', 'beg', user)
            #print cmp_results['devtfc_beg']
            cmp_results['dns_beg'] = analize_timst_difference(dns_cmp_file, dns_week_beg[user.username], io_beg[name], 'dns', 'beg', user)
            cmp_results['flow_beg'] = analize_timst_difference(flow_cmp_file, flow_week_beg[user.username], io_beg[name], 'flow', 'beg', user)
            cmp_results['http_beg'] = analize_timst_difference(http_cmp_file, http_week_beg[user.username], io_beg[name], 'http', 'beg', user)
            cmp_results['loc_beg'] = analize_timst_difference(loc_cmp_file, loc_week_beg[user.username], io_beg[name], 'loc', 'beg', user)
            cmp_results['devtfc_end'] =analize_timst_difference(devtfc_cmp_file, devtfc_week_end[user.username], io_end[name], 'devtfc', 'end', user)
            cmp_results['dns_end'] =analize_timst_difference(dns_cmp_file, dns_week_end[user.username], io_end[name], 'dns', 'end', user)
            cmp_results['flow_end'] =analize_timst_difference(flow_cmp_file, flow_week_end[user.username], io_end[name], 'flow', 'end', user)
            cmp_results['http_beg'] = analize_timst_difference(http_cmp_file, http_week_end[user.username], io_end[name], 'http', 'end', user)
            cmp_results['loc_beg'] = analize_timst_difference(loc_cmp_file, loc_week_end[user.username], io_end[name], 'loc', 'end', user)

            plot_comparison(cmp_results, user)

        elif io_beg.has_key(name.rpartition('.')[0]):
            print(user.username)
            cmp_results['devtfc_beg'] = analize_timst_difference(devtfc_cmp_file, devtfc_week_beg[user.username], io_beg[name.rpartition('.')[0]], 'devtfc', 'beg', user)
            #print cmp_results['devtfc_beg']
            cmp_results['dns_beg'] = analize_timst_difference(dns_cmp_file, dns_week_beg[user.username], io_beg[name.rpartition('.')[0]], 'dns', 'beg', user)
            cmp_results['flow_beg'] = analize_timst_difference(flow_cmp_file, flow_week_beg[user.username], io_beg[name.rpartition('.')[0]], 'flow', 'beg', user)
            cmp_results['http_beg'] = analize_timst_difference(http_cmp_file, http_week_beg[user.username], io_beg[name.rpartition('.')[0]], 'http', 'beg', user)
            cmp_results['loc_beg'] = analize_timst_difference(loc_cmp_file, loc_week_beg[user.username], io_beg[name.rpartition('.')[0]], 'loc', 'beg', user)
            cmp_results['devtfc_end'] = analize_timst_difference(devtfc_cmp_file, devtfc_week_end[user.username], io_end[name.rpartition('.')[0]], 'devtfc', 'end', user)
            cmp_results['dns_end'] = analize_timst_difference(dns_cmp_file, dns_week_end[user.username], io_end[name.rpartition('.')[0]], 'dns', 'end', user)
            cmp_results['flow_end'] = analize_timst_difference(flow_cmp_file, flow_week_end[user.username], io_end[name.rpartition('.')[0]], 'flow', 'end', user)
            cmp_results['http_end'] = analize_timst_difference(http_cmp_file, http_week_end[user.username], io_end[name.rpartition('.')[0]], 'http', 'end', user)
            cmp_results['loc_end'] = analize_timst_difference(loc_cmp_file, loc_week_end[user.username], io_end[name.rpartition('.')[0]], 'loc', 'end', user)


            plot_comparison(cmp_results, user)

        #analize_timst(dns_week_beg[user.username], http_week_beg[user.username], 'dns', 'Beg', user, len(dns_week_beg[user.username]), 1000)
        #analize_timst(dns_week_end[user.username], http_week_end[user.username], 'dns', 'End', user, len(dns_week_end[user.username]), 1000)
        #analize_timst_percentage(cmp_file, devtfc_week_beg[user.username], flow_week_beg[user.username], 'dev', 'Beg', user, len(devtfc_week_beg[user.username]), 1000)
        #analize_timst_percentage(cmp_file, devtfc_week_end[user.username], flow_week_end[user.username], 'dev', 'End', user, len(devtfc_week_end[user.username]), 1000)
        #cmp_file.write('\n')

    devtfc_cmp_file.close()



def analize_timst_percentage(cmp_file,info_week, activity_week, data_comp, key_beg_end, user, quantity_dev, time_interval):
    days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}
    #for each user device analize usage times
    for dev in range (0, quantity_dev):
        if info_week[dev][0]['platform']:
            platform = info_week[dev][0]['platform'][0]
            contained = 0
            total_elems = 0
            for weekday in days_str:
                #get times to be compared to activity
                timst_list  = info_week[dev][0][weekday]
                timst_list_act = activity_week[dev][0][weekday]
                total_elems = total_elems + len(timst_list)
                for timst in timst_list:
                    elem_found = False
                    timst_sec = timst.time().second + timst.time().minute*60 + timst.hour*60*60
                    for timst_act in timst_list_act:
                        timst_act_sec = timst_act.time().second + timst_act.time().minute*60 + timst_act.hour*60*60
                        
                        #check if timst is contained on interval
                        if (timst_sec > timst_act_sec - time_interval) \
                        and (timst_sec < timst_act_sec + time_interval) and elem_found == False:
                            contained = contained + 1 
                            elem_found = True
                            #print timst.time()
                            #print timst_act.time()
            #print(contained)
            #print(total_elems)
            if total_elems != 0:
                equivalence = contained/float(total_elems)
                cmp_file.write(key_beg_end + ' ' + data_comp + ' compared to flow \t user: ' + user.username + '\t\t device: ' + platform + '\t equivalence ' + str(equivalence*100) + '%\n')
                        

def analize_timst_difference(cmp_file, info_week, io_info_week, data_comp, key_beg_end, user):

    days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}
    cmp_file.write('\n' + key_beg_end + ' ' + user.username + '======\n')

    #for each user device analize usage times
    quantity_dev = len(info_week[0])
    cmp_timst_list = defaultdict(list)
    for dev in range (0, quantity_dev):
        if info_week[0][dev]['platform']:
            platform = info_week[0][dev]['platform'][0]
            cmp_file.write('\nplatform: ' + platform + '****\n')
            max_dif = timedelta(microseconds=-1)
            non_similar_days = 0
            checked_days = []
            for weekday in days_str:
                #compares equivalent days
                timst_list  = info_week[0][dev][weekday]
                timst_list_io = io_info_week[0][weekday]
                cmp_file.write('\n' + weekday+'\n')
                if timst_list and timst_list_io:
                    #non_similar_days = 0
                    for timst_io in timst_list_io:
                        if timst_io.date() not in checked_days:
                            checked_days.append(timst_io.date())
                            same_day = False
                            for timst in timst_list:
                                #get the same day
                                if timst.date() == timst_io.date() and same_day == False:
                                    same_day = True
                                    if timst.time() > timst_io.time():
                                        diff = datetime.combine(date.today(), timst.time()) - datetime.combine(date.today(), timst_io.time())
                                        cmp_file.write(str(timst.date()) + ' time diff: ' + str(diff) + '\n')
                                    else:
                                        diff = datetime.combine(date.today(), timst_io.time()) - datetime.combine(date.today(), timst.time())
                                        cmp_file.write(str(timst.date()) + ' time diff: ' + str(diff) + '\n')
                                    if max_dif < diff:
                                        max_dif = diff
                                    #creates new timestamp
                                    days, seconds = diff.days, diff.seconds
                                    hours = days * 24 + seconds // 3600
                                    minutes = (seconds % 3600) // 60
                                    seconds = seconds % 60
                                    timst_cmp = datetime(timst.year, timst.month, timst.day, hours, minutes, seconds)
                                    cmp_timst_list[platform].append(timst_cmp)
                                    #cmp_file.write('\n cmp: ' + str(timst_cmp) + '\n')

                            if same_day == False:
                                non_similar_days = non_similar_days + 1
            if max_dif != timedelta(microseconds=-1):
                cmp_file.write('\nmax difference between days: ' + str(max_dif) + '\n')
                cmp_file.write('number of io days not covered: ' + str(non_similar_days) + '\n')
              
    #tratar para diferentes devices
    return cmp_timst_list



def generate_comparison_file(devtfc, dns, flow, http, loc, user_io):

    if devtfc:
        analize_timst_difference(devtfc_cmp_file, devtfc_week_beg[user.username], io_beg[name], 'devtfc', 'beg', user, len(devtfc_week_beg[user.username]))
        analize_timst_difference(devtfc_cmp_file, devtfc_week_end[user.username], io_end[name], 'devtfc', 'end', user, len(devtfc_week_end[user.username]))
    
    if dns:
        analize_timst_difference(dns_cmp_file, dns_week_beg[user.username], io_beg[name], 'dns', 'beg', user, len(dns_week_beg[user.username]))
        analize_timst_difference(dns_cmp_file, dns_week_end[user.username], io_end[name], 'dns', 'end', user, len(dns_week_end[user.username]))
    
    if flow:
        analize_timst_difference(flow_cmp_file, flow_week_beg[user.username], io_beg[name], 'flow', 'beg', user, len(flow_week_beg[user.username]))
        analize_timst_difference(flow_cmp_file, flow_week_end[user.username], io_end[name], 'flow', 'end', user, len(flow_week_end[user.username]))
    
    if http:
        analize_timst_difference(http_cmp_file, http_week_beg[user.username], io_beg[name], 'http', 'beg', user, len(http_week_beg[user.username]))
        analize_timst_difference(http_cmp_file, http_week_end[user.username], io_end[name], 'http', 'end', user, len(http_week_end[user.username]))
    
    if loc:
        analize_timst_difference(loc_cmp_file, loc_week_beg[user.username], io_beg[name], 'loc', 'beg', user, len(loc_week_beg[user.username]))
        analize_timst_difference(loc_cmp_file, loc_week_end[user.username], io_end[name], 'loc', 'end', user, len(loc_week_end[user.username]))


def plot_comparison (cmp_results, user):


    #fig, ((ax1, ax2)) = plt.subplots(nrows = 1, ncols = 2, figsize=(15, 5))#(20, 25))

    user_platforms_list = []
    #memlhorar, fazer para so um key_beg_end
    for key_beg_end, platform_data in cmp_results.iteritems():
        for platform, dates in platform_data.iteritems():
            if platform not in user_platforms_list:
                user_platforms_list.append(platform)

    print user_platforms_list
    for platform in user_platforms_list:
        fig, ((ax2, ax3, ax5, ax7, ax9),(ax1, ax4, ax6, ax8, ax10)) = plt.subplots(nrows = 2, ncols = 5, figsize=(30, 10))
        subaxes = [ax1,ax2,ax3,ax4,ax5,ax6,ax7,ax8,ax9,ax10]
        cont = 0
        #print('PLATAFORMMMMMMMMM: ' + platform)
        for key_beg_end, platform_data in cmp_results.iteritems():
            #print (platform + ' ' + key_beg_end)
            timsts = cmp_results[key_beg_end][platform]
            timsts.sort()
            #print timsts
     
            #fig, ((ax1, ax2)) = plt.subplots(nrows = 1, ncols = 2, figsize=(15, 5))#(20, 25))
            create_subplot(subaxes[cont], key_beg_end, platform, timsts, user)
            #create_subplot(ax2, key_beg_end, platform, timsts, user)
            #print ('plotou ****')
            cont = cont + 1
        fig.subplots_adjust(hspace = .8)
        fig.savefig('figs_comparison/' + user.username + '-' + platform + '.png')
        plt.close(fig)

    #print cmp_results


def create_subplot(ax, key_beg_end, platform, date_time_list, user):
    x = []
    y = []
    sns.set_style('whitegrid')
    for timst in date_time_list:
        x.append(timst.date())
        y.append(timst.hour+timst.minute/60.0)
    
    #ax.set_xticklabels(x, rotation=45, fontsize = 8, minor=False)
    #ax.set_xticklabels(xlabels, fontsize = 7)
    if len(x) > 1:
        hfmt = dates.DateFormatter('%m-%d')
        ax.xaxis.set_major_formatter(hfmt)
    ax.set_title(key_beg_end + ' ' + platform + ' - User: ' +  user.username)
    ax.set_ylim([0,24])
    ax.set_ylabel('Hour of Day')
    ax.grid(True)
    ax.plot(x, y, 'o')

if __name__ == '__main__':
    main()
