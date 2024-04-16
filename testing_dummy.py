"""
For testing code snippets
"""

import pandas as pd
from setup import config
import requests
import utils
import os
import urllib.request
import zipfile
import csv
import time
import json
import sqlite3
from io import StringIO
import numpy as np
from matplotlib import pyplot as pp
from datetime import datetime

df = pd.DataFrame(index=pd.date_range("2020-01-01 00:00:00","2020-12-31 23:00:00",freq='1h'))
df['time'] = pd.date_range("2020-01-01 00:00:00","2020-12-31 23:00:00",freq='1h')
df['n'] = range(8784)

print(df)

df = utils.realign_timezone(df, from_utc_offset=-4, time_col='time')

print(df)

print(config.gdp_index['ON'])