import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Device import Device
from model.DnsReq import DnsReq
from model.User import User
from model.user_devices import user_devices

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from dnsreqs_analysis import get_dns_data
#from httpreqs_analysis import get_http_data
#from locations_analysis import get_locations_data

from IPython.display import display


def main():
  
  dns_info_week_beg, dns_info_week_end = get_dns_data()
  print (dns_info_week_beg[0])
  print (dns_info_week_beg[1])
 
if __name__ == '__main__':
  main()





