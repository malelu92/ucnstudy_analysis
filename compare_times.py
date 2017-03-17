#from __future__ import division

from model.Base import Base
from model.Device import Device
from model.DnsReq import DnsReq
from model.User import User
from model.user_devices import user_devices

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from devicetraffic_analysis import get_devtraffic_data
from dnsreqs_analysis import get_dns_data
from flow_analysis import get_flow_data
from httpreqs_analysis import get_http_data
from location_analysis import get_locations_data

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def main():

    ses = Session()
    users = ses.query(User)

    #devtfc_week_beg, devtfc_week_end = get_devtraffic_data()
    dns_week_beg, dns_week_end = get_dns_data()
    #flow_week_beg, flow_week_end = get_flow_data()
    http_week_beg, http_week_end = get_http_data()
    #loc_week_beg, loc_week_end = get_locations_data()

    for user in users:
        analize_timst(dns_week_beg[user.username], http_week_beg[user.username], 'dns', 'Beg', user, len(dns_week_beg[user.username]), 1000)
        analize_timst(dns_week_end[user.username], http_week_end[user.username], 'dns', 'End', user, len(dns_week_end[user.username]), 1000)


def analize_timst(info_week, activity_week, data_comp, key_beg_end, user, quantity_dev, time_interval):
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
                print (key_beg_end + ' ' + data_comp + ' compared to activity -  user: ' + user.username + ' device: ' + platform + ' equivalence ' + str(equivalence*100) + '%')
                        

if __name__ == '__main__':
    main()
