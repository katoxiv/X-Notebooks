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

import csv
import pickle
import ast
import datetime
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as PolygonPatch
from matplotlib.collections import PatchCollection
from matplotlib.colors import LinearSegmentedColormap
from bng_latlon import OSGB36toWGS84

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

from wordcloud import WordCloud
from wordcloud import STOPWORDS
import spacy

from collections import Counter

# Define static variables
in_folder = "Twitter Data"
out_file = "all_june_tweets.txt"
out_file_for_text = "all_june_tweets_text.txt"
out_file_for_hashtag = "all_june_tweets_hastags.txt"

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

def layout(shape_file):
    # Euro Road Layout - 2min31.7sec - https://osdatahub.os.uk/downloads/open/OpenRoads
    '''
    Helper function to read and store info from .shp files
    params: shape_file: path to a specific shape_file
    returns: patches: list with shapely objects from shape_file
    '''
    patches = []

    with fiona.open(shape_file) as layers:
        for feature in layers:
            p = shape(feature['geometry'])
            try:
                # idk..something weird is happening with the lats and longs - some sort of switch. swapped them for now
                lons , lats = np.array(p.xy)
                lons_and_lats = list(zip(lons, lats))
                wgs84_coord = []
                for lon_and_lat in lons_and_lats:
                    wgs84_coord.append(OSGB36toWGS84(lon_and_lat[0] , lon_and_lat[1])[::-1])
                patches.append(PolygonPatch(wgs84_coord , closed = False))
            except AttributeError:
                for poly in p.geoms:
                    # idk..something weird is happening with the lats and longs - some sort of switch. swapped them for now
                    lons , lats = np.array(poly.xy)
                    lons_and_lats = list(zip(lons, lats))
                    wgs84_coord = []
                    for lon_and_lat in lons_and_lats:
                        wgs84_coord.append(OSGB36toWGS84(lon_and_lat[0] , lon_and_lat[1])[::-1])
                    patches.append(PolygonPatch(wgs84_coord , closed = False))
    
    return patches

def get_patches(path_to_shape_files):
    '''
    params: apply_layout: layout func
    params: path_to_shape_files: path to various shape files that need to be plotted

    returns: patches: list with all shape file geometries attached
    '''
    patches = [] # storage container
    with Pool() as pool_exec:
        results = pool_exec.map(layout  , path_to_shape_files)
        # Iterating through Generator Object
        for result in results:
            patches.append(result)
    return patches

def test_func(x):
    return x**2

def test_pool(test_func , L):
    '''
    just a func to test if pool is working
    '''
    l_0 = []
    with Pool() as pool_exec:
        results = pool_exec.map(test_func  , L)
        # Iterating through Generator Object
        for result in results:
            l_0.append(result)
    return l_0