from model.Base import Base
from model.Device import Device
from model.DnsReq import DnsReq
from model.User import User
from model.user_devices import user_devices

from collections import defaultdict
from datetime import timedelta
from datetime import datetime, date

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
    #dns_week_beg, dns_week_end = get_dns_data()
    #flow_week_beg, flow_week_end = get_flow_data()
    #http_week_beg, http_week_end = get_http_data()
    #loc_week_beg, loc_week_end = get_locations_data()
    io_week_beg, io_week_end = get_io_data()
    #act_week_beg, act_week_end = get_activities_data()

    #cmp_file = open('cmp_devtfc_flow.txtio_users = []

    io_beg = defaultdict(list)
    for key, value in io_week_beg.iteritems():
        io_beg[key.rpartition('.')[0]] = io_week_beg[key]
    #print(io_beg)
    io_end = defaultdict(list)
    for key, value in io_week_end.iteritems():
        io_end[key.rpartition('.')[0]] = io_week_end[key]

    
    for user in users:
        name = user.username
        #only compares users with information on io
        if io_beg.has_key(name):
            print user.username
            analize_timst_difference(devtfc_week_beg[user.username], io_beg[name], 'devtfc', 'beg', user, len(devtfc_week_beg[user.username]))

        elif io_beg.has_key(name.rpartition('.')[0]):
            print(user.username)
            analize_timst_difference(devtfc_week_beg[user.username], io_beg[name.rpartition('.')[0]], 'devtfc', 'beg', user, len(devtfc_week_beg[user.username]))


        #analize_timst(dns_week_beg[user.username], http_week_beg[user.username], 'dns', 'Beg', user, len(dns_week_beg[user.username]), 1000)
        #analize_timst(dns_week_end[user.username], http_week_end[user.username], 'dns', 'End', user, len(dns_week_end[user.username]), 1000)
        #analize_timst_percentage(cmp_file, devtfc_week_beg[user.username], flow_week_beg[user.username], 'dev', 'Beg', user, len(devtfc_week_beg[user.username]), 1000)
        #analize_timst_percentage(cmp_file, devtfc_week_end[user.username], flow_week_end[user.username], 'dev', 'End', user, len(devtfc_week_end[user.username]), 1000)
        #cmp_file.write('\n')

    #cmp_file.close()

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
                        

def analize_timst_difference(info_week, io_info_week, data_comp, key_beg_end, user, quantity_dev):

    days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}
   
    #for each user device analize usage times
    for dev in range (0, quantity_dev):
        if info_week[dev][0]['platform']:
            platform = info_week[dev][0]['platform'][0]
            max_dif = timedelta(microseconds=-1)
    #        total_elems = 0
            for weekday in days_str:
                #compares equivalent days
                timst_list  = info_week[dev][0][weekday]
                timst_list_io = io_info_week[0][weekday]
                print(weekday)
                if timst_list and timst_list_io:
                    for timst in timst_list:
                        for timst_io in timst_list_io:
                            #get the same day
                            if timst.date() == timst_io.date():
                                #print(timst.time()-timst_io.time())
                                if timst.time() > timst_io.time():
                                    diff = datetime.combine(date.today(), timst.time()) - datetime.combine(date.today(), timst_io.time())
                                    print(diff)
                                else:
                                    diff = datetime.combine(date.today(), timst_io.time()) - datetime.combine(date.today(), timst.time())
                                    print(diff)
                                if max_dif < diff:
                                    max_dif = diff
        print ('max difference between days: ' + str(max_dif))
                            
               
               
               
               
    #            timst_list_act = activity_week[dev][0][weekday]
    #            total_elems = total_elems + len(timst_list)
    #            for timst in timst_list:
    #                elem_found = False

if __name__ == '__main__':
    main()
