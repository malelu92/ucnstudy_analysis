from model_io.Base import Base
from model_io.Devices import Devices
from model_io.Activities import Activities;

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import datautils

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy_hostview_data'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

ses = Session()
devices = ses.query(Devices)

def main():

    sql = """SELECT distinct name \
    FROM activities \
    WHERE session_id = :d_id AND fullscreen = 1"""

    sql_io = """SELECT distinct name \
    FROM io \
    WHERE session_id = :d_id"""

    for device in devices:
        #select only users from ucnstudy
        if device.id == 5 or device.id == 6 or device.id == 8 or device.id == 12 or device.id == 14 or device.id == 18 or device.id == 19 or device.id == 21 or device.id == 22:

            app_list = []

            print (device.device_id + '==============')
            for row in ses.execute(text(sql).bindparams(d_id = device.id)):
                if row[0] not in app_list:
                    app_list.append(row[0])

            for row in ses.execute(text(sql_io).bindparams(d_id = device.id)):
                if row[0] not in app_list:
                    app_list.append(row[0])

            for elem in app_list:
                print elem

if __name__ == "__main__":
    main()
