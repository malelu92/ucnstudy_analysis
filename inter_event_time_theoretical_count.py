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



def calculate_average_periodicity(interval_dict):

    #calculate theoretic periodicity per interval
    for key in interval_dict.keys():
        #if key != 5:
            #continue

        distrib_dict = get_interval_distribution(key, interval_dict)
        #print distrib_dict

        for iat_total in distrib_dict.keys():
            if iat_total != 'total' and \
               distrib_dict['total'] > 10 and \
               (distrib_dict[iat_total]/float(distrib_dict['total'])) > 0.5:

                if iat_total != 0:

                    timsts = interval_dict[key]
                    beg_block = timsts[0]
                    end_block = timsts[len(timsts)-1]

                    block_time = (end_block - beg_block).total_seconds()
                    print '***'
                    print 'block time ' + str(block_time)
                    print 'iat ' + str(iat_total)
                    print 'theoretic count ' + str(block_time/float(iat_total))
                    print 'real count ' + str(distrib_dict[iat_total])
                    print 'total ' + str(distrib_dict['total'])


def get_interval_distribution(key, interval_list):

    interval_dist = defaultdict(int)
    interval_dist['total'] = 0
    timsts = interval_list[key]

    if len(timsts) > 1:
        for i in range (0, len(timsts)-1):
            iat = (timsts[i+1]-timsts[i]).total_seconds()

            #round interval
            if iat - int(iat) >= 0.5:
                iat = int(iat)+1
            else:
                iat = int(iat)

            if iat not in interval_dist.keys():
                interval_dist[iat] = 1
            else:
                interval_dist[iat] += 1

            interval_dist['total'] +=1

    return interval_dist
                    
                    
def analyze_user_device(user_dev):

    if user_dev != 'neenagupta.macair':#'clifford.mainlaptop':
        return False
    return True

if __name__ == '__main__':
    main()
