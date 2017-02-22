#!/usr/bin/python
import sys
import os

from model.Base import Base
from model.User import User
from model.Device import Device

from sqlalchemy import create_engine    
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy import Table, Column, ForeignKey, Integer

# In the french experiment, the user to device mapping is 1-to-1.
# We can find the mappings by taking the username from 
# the Device.login (<username.devicename>), and searching for 
# matching user in the db.

# In the UK experiment, username was mapped to several people in 
# same households in some cases. Based on input from Tom & Murray (Nottingham) 
# the missing users were added manually to the 'users' table. Below 
# the mapping from the Device.login (<origusername.devicename>), 
# to the new username(s). Note it's a list, lot of the devices were shared.
# For the ones not in the list, we assume 1-to-1 mapping from Device.login
# to username as for the french users.

ukdevices = {
    'clifford.mainlaptop' : ['clifford.wife'],
    'clifford.rachelphone' : ['clifford.wife'],
    'clifford.rachelipad' : ['clifford.wife'],
    'clifford.aidanlaptop' : ['clifford.husband'],
    'clifford.aidanphone' : ['clifford.husband'],
    'barnesldavid.tablet' : ['barnesldavid.wife'],
    'barnesldavid.phone' : ['barnesldavid.husband'],
    'barnesldavid.phone2' : ['barnesldavid.wife','barnesldavid.husband'],
    'bowen.ipad' : ['bowen.wife','bowen.husband'],
    'bowen.laptop' : ['bowen.wife','bowen.husband'],
    'bowen.iphone' : ['bowen.wife'],
    'bowen.iphonedominic' : ['bowen.husband'],
    'bridgeman.stuartlaptop' : ['bridgeman.husband'],
    'bridgeman.laptop2' : ['bridgeman.wife'],
    'bridgeman.iphonejan' : ['bridgeman.wife']
}

if __name__ == "__main__":
    """
    Create and populate the users to devices many-to-many relation.
    """
    db = sys.argv[1]

    engine = create_engine(db, poolclass=NullPool, echo=True)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine) 

    # simple many to many mapping from users to devices
    user_devices = Table('user_devices', 
                         Base.metadata,
                         Column('user_id', 
                                ForeignKey('users.id'), primary_key=True),
                         Column('device_id', 
                                ForeignKey('devices.id'), primary_key=True))

    user_devices.drop(engine, checkfirst=True)
    user_devices.create(engine)

    # transaction
    dbs = Session()    
    for d in dbs.query(Device).all():
        print 'handle',str(d)

        # assume 1-to-1 mapping from device to user
        (username,devicename) = d.login.split('.')
        unames = [username]

        # fix the UK devices
        if (d.login in ukdevices):
            unames = ukdevices[d.login]
            
        d.shared = (len(unames)>1)            
        for uname in unames:
            u = dbs.query(User).filter(User.username==uname).one()
            print 'used by',str(u)
            d.users.append(u)
        dbs.add(u)
    dbs.commit()

