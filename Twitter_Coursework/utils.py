# Imports
import os
import zipfile
import json
import ndjson
import pandas as pd
import sys
import numpy as np
import pickle
from multiprocessing import Pool
import time
from concurrent import futures
import csv
import pickle
import ast
import datetime
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import scipy.stats
from dask.dataframe import from_pandas


# Define static variables
in_folder = "Twitter Data"
out_file = "june_tweets_country.txt"