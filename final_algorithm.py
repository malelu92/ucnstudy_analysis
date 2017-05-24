from Traces import Trace

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import datautils

from IPython.display import display

from model.Base import Base
from model.User import User
from model.Device import Device
from model.HttpReq import HttpReq
from model.DnsReq import DnsReq
from model.user_devices import user_devices;

#output_notebook()

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

ses = Session()
users = ses.query(User)

def main():

    for user in users:
        devids = []
        for d in user.devices:
            devids.append(str(d.id))

        devs = {}
        for d in user.devices:
            devs[d.id] = d.platform

        for elem_id in devids:
            sql_userid = """SELECT login FROM devices WHERE id =:d_id"""
            user_id = ses.execute(text(sql_userid).bindparams(d_id = elem_id)).fetchone()
            idt = user_id[0]

            #if idt != 'bowen.laptop' and idt != 'bridgeman.laptop2' and idt != 'bridgeman.stuartlaptop' and idt != 'chrismaley.loungepc' and idt != 'chrismaley.mainpc' and idt != 'clifford.mainlaptop' and idt != 'gluch.laptop' and idt != 'kemianny.mainlaptop' and idt != 'neenagupta.workpc':
            if idt != 'neenagupta.workpc':
                continue

            print idt

            get_test_data(elem_id)


def get_test_data(device_id):

    sql_http = """SELECT req_url_host, ts, lag(ts) OVER (ORDER BY ts) FROM httpreqs2 \
        WHERE devid =:d_id AND matches_urlblacklist = 'f'"""

    sql_dns = """SELECT query, ts, lag(ts) OVER (ORDER BY ts) FROM dnsreqs \
        WHERE devid =:d_id AND matches_blacklist = 'f'"""

    traces_list = []

    #add httpreqs
    for row in ses.execute(text(sql_http).bindparams(d_id = device_id)):
        elem = Trace(row[0], row[1])
        traces_list.append(elem)

    #add dnsreqs
    for row in ses.execute(text(sql_dns).bindparams(d_id = device_id)):
        elem = Trace(row[0], row[1])
        traces_list.append(elem)

    traces_list.sort(key = lambda x: x.timst)

    for elem in traces_list:
        print elem.timst

    return traces_list


if __name__ == '__main__':
    main()
