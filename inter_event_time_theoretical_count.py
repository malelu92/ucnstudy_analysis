import datautils
import itertools
import matplotlib.pyplot as plt
import seaborn as sns

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

        theoretic_count = defaultdict(list)
        real_count = defaultdict(list)

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

                """if url == 'google.com':
                    print url
                    for key, values in interval_list.iteritems():
                        if key >= 0 and key <= 5:
                            print key
                            for item in values:
                                print item"""

                theoretic_count_per_url_list, real_count_per_url_list = calculate_average_periodicity(interval_list)

                if theoretic_count_per_url_list:
                    theoretic_count[idt].append(theoretic_count_per_url_list)
                if real_count_per_url_list:
                    real_count[idt].append(real_count_per_url_list)

            plot_counts_ditr(theoretic_count[idt], real_count[idt], idt)


def calculate_average_periodicity(interval_dict):

    #calculate theoretic periodicity per interval
    theoretic_count = []
    real_count = []

    for key in interval_dict.keys():
        #if key != 5:
            #continue

        distrib_dict = get_interval_distribution(key, interval_dict)

        for iat_total in distrib_dict.keys():
            if iat_total != 'total' and \
               distrib_dict['total'] > 5 and \
               (distrib_dict[iat_total]/float(distrib_dict['total'])) > 0.5:

                if iat_total != 0:

                    timsts = interval_dict[key]
                    beg_block = timsts[0]
                    end_block = timsts[len(timsts)-1]

                    block_time = (end_block - beg_block).total_seconds()
                    theo_count = round(block_time/iat_total)
                    theoretic_count.append(theo_count)
                    real_count.append(distrib_dict[iat_total])

                    print '***'
                    print 'block time ' + str(block_time)
                    print 'interval that appears more than 50% of times ' + str(iat_total)
                    print 'theoretic periodicity ' + str(theo_count)
                    print 'real periodicity ' + str(distrib_dict[iat_total])
                    print 'total number of intervals ' + str(distrib_dict['total'])

    #contains values for a certain url
    return theoretic_count, real_count
                    
def get_interval_distribution(key, interval_list):

    interval_dist = defaultdict(int)
    interval_dist['total'] = 0
    timsts = interval_list[key]

    if len(timsts) > 1:
        for i in range (0, len(timsts)-1):
            iat = (timsts[i+1]-timsts[i]).total_seconds()

            #round interval
            iat = round(iat)

            if iat not in interval_dist.keys():
                interval_dist[iat] = 1
            else:
                interval_dist[iat] += 1

            interval_dist['total'] +=1

    return interval_dist
                    
                    
def round(value):

    if value - int(value) >= 0.5:
        value = int(value)+1
    else:
        value = int(value)

    return value

def analyze_user_device(user_dev):

    if user_dev != 'neenagupta.macair':#'clifford.mainlaptop':
        return False
    return True

def plot_counts_ditr(theoretic_counts, real_counts, username):

    sns.set_style('whitegrid')

    merged_theoretic_counts = list(itertools.chain(*theoretic_counts))

    #print merged_theoretic_counts
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    (x,y) = datautils.aecdf(merged_theoretic_counts)

    ax1.plot(x,y, '-b', lw=2)
    ax1.set_title('Interval sizes per block [user=%s, events=%d]'%(username, len(x)))  \

    ax1.set_ylabel('CDF')
    ax1.set_xscale('log')
    ax1.set_xlabel('seconds')
    ax1.set_xticks([0.001,1,60,3600,24*3600])
    ax1.set_xticklabels(['1ms','1s','1min','1h','1day'])
    ax1.set_xlim(0.001,max(merged_theoretic_counts))

    xp = filter(lambda v : v>=60, x)
    if xp:
        ax2.plot(xp,y[-len(xp):], '-b', lw=2)
        ax2.set_title('Zoom 1 [values=%d]'%(len(xp)))
        ax2.set_ylabel('CDF')
        ax2.set_xscale('log')
        ax2.set_xlabel('seconds')
        ax2.set_xticks([60,600,3600,24*3600])
        ax2.set_xticklabels(['1min','10min','1h','1day'])
        ax2.set_xlim(60,max(merged_theoretic_counts))

    plt.tight_layout()
    fig.savefig('figs_CDF_theoretic_counts/%s.png' % (username))
    plt.close(fig)

if __name__ == '__main__':
    main()
