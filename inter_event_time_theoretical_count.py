from collections import defaultdict

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from model.Base import Base
from model.User import User
from model.Device import Device
from model.HttpReq import HttpReq

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

ses = Session()
users = ses.query(User)

gap_interval = 10*60

def main():
    for user in users:
        print ('user : ' + user.username)

        devids = []
        for d in user.devices:
            devids.append(str(d.id))

        devs = {}
        for d in user.devices:
            devs[d.id] = d.platform

        sql_url = """SELECT DISTINCT req_url_host FROM \
        httpreqs2 WHERE devid =:d_id AND matches_urlblacklist = 'f';"""

        sqlq = """SELECT ts, lag(ts) OVER (ORDER BY ts) FROM httpreqs2 \
        WHERE devid =:d_id AND req_url_host =:url_name AND matches_urlblacklist = 'f'"""

        sql_userid = """SELECT login FROM devices WHERE id =:d_id"""

        for elem_id in devids:
            url_list = []
            #interval_list = defaultdict(list)

            user_id = ses.execute(text(sql_userid).bindparams(d_id = elem_id)).fetchone()
            idt = user_id[0]

            if not analyze_user_device(idt):
                continue

            print idt
            for row in ses.execute(text(sql_url).bindparams(d_id = elem_id)):
                if row[0]:
                    url_list.append(row[0])

            for url in url_list:
                intv = 0
                interval_list = defaultdict(list)
                for row in ses.execute(text(sqlq).bindparams(d_id = elem_id, url_name = url)):
                    if row[1] == None:
                        interval_list[intv].append(row[0])
                        continue

                    iat = (row[0]-row[1]).total_seconds()
                    if iat > gap_interval:
                        intv += 1
                    interval_list[intv].append(row[0])

                if url == 'google.com':
                    print url
                    for key, values in interval_list.iteritems():
                        if key >= 0 and key <= 5:
                            print key
                            for item in values:
                                print item

                    calculate_average_periodicity(interval_list)



def calculate_average_periodicity(interval_list):

    #calculate theoretic periodicity per interval
    for key in interval_list.keys():
        timsts = interval_list[key]
        print '==='
        print timsts[0]
        print timsts[len(timsts)-1]


    """key_check = -1
    for key, value in interval_list.iteritems():
        if key != key_check:
            print(key, len(interval_list[key]))
            key_check = key"""
                    
                    
def analyze_user_device(user_dev):

    if user_dev != 'neenagupta.macair':#'clifford.mainlaptop':
        return False
    return True

if __name__ == '__main__':
    main()
