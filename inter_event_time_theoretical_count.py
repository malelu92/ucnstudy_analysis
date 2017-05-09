import datautils
import itertools
import matplotlib.pyplot as plt
import seaborn as sns

from collections import defaultdict

#just for ploting on test
from inter_event_time_analysis import plot_cdf_interval_times

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


def get_interval_list(traces_list):

    intv = 0
    interval_list = defaultdict(list)

    interval_list[intv].append(traces_list[0])
    for i in range(0, len(traces_list)-1):
        iat = (traces_list[i+1]-traces_list[i]).total_seconds()
        if iat > gap_interval:
            intv += 1
        interval_list[intv].append(traces_list[i+1])

    return interval_list

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
               (distrib_dict[iat_total]/float(distrib_dict['total'])) > 0.40:

                if iat_total != 0:

                    timsts = interval_dict[key]
                    beg_block = timsts[0]
                    end_block = timsts[len(timsts)-1]

                    block_time = (end_block - beg_block).total_seconds()
                    theo_count = round(block_time/iat_total)
                    theoretic_count.append(theo_count)
                    real_count.append(distrib_dict[iat_total])

                    #print '***'
                    #print 'block time ' + str(block_time)
                    #print 'interval that appears more than 40% of times ' + str(iat_total)
                    #print 'theoretic periodicity ' + str(theo_count)
                    #print 'real periodicity ' + str(distrib_dict[iat_total])
                    #print 'total number of intervals ' + str(distrib_dict['total'])

    #contains values for a certain url
    return theoretic_count, real_count

#obs: url_domain just for printing, can take it off
def get_free_spikes_traces(interval_dict, url_domain):

    #calculate theoretic periodicity per interval
    theoretic_count = []
    real_count = []
    filtered_traces = []

    for key in interval_dict.keys():

        if url_domain == 'content.very.co.uk':
            iat_list = []
            int_list = interval_dict[key]
            for i in range(0, len(int_list)-1):
                iat = (int_list[i+1]-int_list[i]).total_seconds()
                iat_list.append(iat)
            plot_cdf_interval_times(iat_list, 'kemianny'+str(key), 'main_laptop', 'figs_CDF_theoretic_counts', 'content.very.co.uk')


        distrib_dict = get_interval_distribution(key, interval_dict)
        filtered_interval_list = interval_dict[key]

        for iat_total in distrib_dict.keys():
            #if potential spike

            if url_domain == 'content.very.co.uk':
                print '===='
                print url_domain
                print 'interval size: ' + str(iat_total)
                print 'appears this many times: ' + str(distrib_dict[iat_total])
                print 'total number of intervals: ' + str(distrib_dict['total'])


            if iat_total != 'total' and \
               distrib_dict['total'] > 5 and \
               (distrib_dict[iat_total]/float(distrib_dict['total'])) > 0.40:

                if iat_total != 0:
                    timsts = interval_dict[key]
                    beg_block = timsts[0]
                    end_block = timsts[len(timsts)-1]

                    block_time = (end_block - beg_block).total_seconds()
                    theo_count = round(block_time/iat_total)
                    re_count = distrib_dict[iat_total]
                    theoretic_count.append(theo_count)
                    real_count.append(re_count)

                    if url_domain == 'content.very.co.uk':
                        print '===='
                        print url_domain
                        print 'interval size: ' + str(iat_total)
                        print 'theoretic counts: ' + str(theo_count)
                        print 'real counts: ' + str(re_count)

                    #if number of intervals is close enough to theoretical number of intervals, eliminate traces
                    if theo_count <= 10:
                        error_margin = 0.2
                    elif theo_count > 10 and theo_count <= 100:
                        error_margin = 0.1
                    else:
                        error_margin = 0.05
                    if re_count > (theo_count - theo_count*error_margin):
                        #print 'lele'
                        filtered_interval_list = eliminate_spikes(filtered_interval_list, iat_total)

        filtered_traces.append(filtered_interval_list)        

    filtered_traces = list(itertools.chain(*filtered_traces))

    #print filtered_traces
    return filtered_traces


def eliminate_spikes(interval_list, iat_to_eliminate):

    timsts = interval_list
    filtered_interval_list = []

    filtered_interval_list.append(timsts[0])
    if len(timsts) > 1:
        for i in range (0, len(timsts)-1):
            iat = (timsts[i+1]-timsts[i]).total_seconds()

            #round interval
            iat = get_approximation(iat)

            if iat != iat_to_eliminate:
                filtered_interval_list.append(timsts[i+1])

    return filtered_interval_list
                    


def get_interval_distribution(key, interval_list):

    interval_dist = defaultdict(int)
    interval_dist['total'] = 0
    timsts = interval_list[key]

    if len(timsts) > 1:
        for i in range (0, len(timsts)-1):
            iat = (timsts[i+1]-timsts[i]).total_seconds()

            #round interval
            #iat = round(iat)
            iat = get_approximation(iat)

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

def get_approximation(value):

    if value <= 5:
        return round(value)
    #10 seconds gap
    elif value > 5 and value <= 15:
        return 10
    elif value > 15 and value <= 25:
        return 20
    elif value >25 and value <= 35:
        return 30
    elif value > 35 and value <= 45:
        return 40
    #30 seconds gap
    elif value > 45 and value <= 75:
        return 60
    elif value > 75 and value <= 105:
        return 90
    elif value > 105 and value <=135:
        return 120
    elif value > 135 and value <= 165:
        return 150
    elif value > 165 and value <= 195:
        return 180
    elif value > 195 and value <= 225:
        return 210
    elif value > 225 and value <= 270:
        return 240
    #60 seconds gap
    elif value > 270 and value <= 330:
        return 300
    elif value > 330 and value <= 390:
        return 360
    elif value > 390 and value <= 450:
        return 420
    elif value > 450 and value <= 510:
        return 480
    elif value > 510 and value <= 570:
        return 540
    elif value > 570 and value <= 675:
        return 600
    #150 seconds gaps -> 2.5 min
    elif value > 675 and value <= 825:
        return 750
    elif value > 825 and value <= 975:
        return 900
    elif value > 975 and value <= 1125:
        return 1050
    elif value > 1125 and value <= 1350:
        return 1200 #20 min
    #300 seconds gaps -> 5 min
    elif value > 1350 and value <= 1650:
        return 1500
    elif value > 1650 and value <= 1950:
        return 1800
    elif value > 1950 and value <= 2250:
        return 2100
    elif value > 2250 and value <= 2550:
        return 2400 # 40 min
    elif value > 2550 and value <= 2850:
        return 2700
    elif value > 2850 and value <= 3150:
        return 3000
    #900 seconds gaps -> 15 min gaps
    elif value > 3150 and value <= 4050:
        return 3600 # 1 hr
    elif value > 4050 and value <= 4950:
        return 4500
    elif value > 4950 and value <= 5850:
        return 5400
    elif value > 5850 and value <= 6750:
        return 6300
    elif value > 6750 and value <= 8100:
        return 7200 #2 hrs
    #1800 seconds gaps -> 30 min
    elif value > 8100 and value <= 9900:
        return 9000
    elif value > 9900 and value <= 11700:
        return 10800 #3 hrs
    elif value > 11700 and value <= 13500:
        return 12600
    elif value > 13500 and value <= 15300:
        return 14400 #4 hrs
    elif value > 15300 and value <= 17100:
        return 16200
    elif value > 17100 and value <= 18900:
        return 18000 #5 hrs
    #3600 seconds intervals
    elif value > 18900 and value <= 22500:
        return 21600
    elif value > 22500 and value <= 26100:
        return 25200
    elif value > 26100 and value <= 29700:
        return 28800
    elif value > 29700 and value <= 33300:
        return 32400
    elif value > 33300 and value <= 36900:
        return 36000 #10 hrs
    else:
        return 43200 #12 hrs
    

def analyze_user_device(user_dev):

    if user_dev != 'bowen.laptop' and user_dev != 'bridgeman.laptop2' and \
       user_dev != 'bridgeman.stuartlaptop' and user_dev != 'chrismaley.loungepc' and \
        user_dev != 'chrismaley.mainpc' and user_dev != 'clifford.mainlaptop' and \
        user_dev != 'gluch.laptop' and user_dev != 'kemianny.mainlaptop' and user_dev != 'neenagupta.workpc':
        return False
    return True

def plot_counts_ditr(theoretic_counts, real_counts, username):

    sns.set_style('whitegrid')

    merged_theoretic_counts = list(itertools.chain(*theoretic_counts))

    #print merged_theoretic_counts
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    (x,y) = datautils.aecdf(merged_theoretic_counts)

    ax1.plot(x,y, '-b', lw=2)
    ax1.set_title('Theoretic interval sizes per block [user=%s, events=%d]'%(username, len(x)), fontsize = 15)
    ax1.set_ylabel('CDF', fontsize = 10)
    ax1.set_xscale('log')
    #ax1.set_xlabel('seconds', fontsize = 10)
    #ax1.set_xticks([0.001,1,60,3600,24*3600])
    #ax1.set_xticklabels(['1ms','1s','1min','1h','1day'], fontsize = 10)
    ax1.set_xlabel('conts', fontsize = 10)
    ax1.set_xticks([1,10,100,1000])
    ax1.set_xticklabels(['1','10','100','1000'], fontsize = 10)

    #ax1.set_xlim(0.001,max(merged_theoretic_counts))
    ax1.set_xlim(1,max(merged_theoretic_counts))


    xp = filter(lambda v : v>=100, x)
    if xp:
        ax2.plot(xp,y[-len(xp):], '-b', lw=2)
        ax2.set_title('Zoom 1 [values=%d]'%(len(xp)))
        ax2.set_ylabel('CDF')
        ax2.set_xscale('log')
        #ax2.set_xlabel('seconds')
        #ax2.set_xticks([60,600,3600,24*3600])
        #ax2.set_xticklabels(['1min','10min','1h','1day'])
        ax2.set_xlabel('conts')
        ax2.set_xticks([100,1000])
        ax2.set_xticklabels(['100','1000'])

        ax2.set_xlim(100,max(merged_theoretic_counts))

    plt.tight_layout()
    fig.savefig('figs_CDF_theoretic_counts/%s.png' % (username))
    plt.close(fig)

if __name__ == '__main__':
    main()
