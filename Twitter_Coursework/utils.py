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
# from concurrent import futures
import csv
import pickle
import ast
import datetime
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as PolygonPatch
from matplotlib.collections import PatchCollection
from matplotlib.colors import LinearSegmentedColormap
import mplcursors as mpc

import matplotlib
import seaborn as sns
import scipy.stats
from dask.dataframe import from_pandas
from shapely.geometry import shape
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import fiona
from pprint import pprint


# Define static variables
in_folder = "Twitter Data"
out_file = "all_june_tweets.txt"

# Some Functions
def to_coord(df_original , coord_cols):
    '''
    takes coordinate column [x.xxx , y.yyy] and returns df with latitude and longitude separated
    params : df_original -> dataframe on which conversion is to be carried out
    params : columns with coordinates
    returns: df with converted columns
    '''
    # Make copy
    df = df_original.copy()

    for coord_col in coord_cols:
        long_col = coord_col + '_long'
        lat_col = coord_col + '_lat'
        # Long and Lat - clean up
        df[[long_col , lat_col]] = df[coord_col].str.split(',' , expand = True)
        # Long
        df[long_col] = df[long_col].apply(lambda x : x[1:])#.astype('float')
        df[long_col] = df[long_col].astype('float')
        # Lat
        df[lat_col] = df[lat_col].apply(lambda x : x[:-1])#.astype('float')
        df[lat_col] = df[lat_col].astype('float')

        # remove coord col
        df.drop(coord_col , axis = 1 , inplace = True)
    
    # return modified df
    return df