from geopy import *
from geopy.geocoders import Nominatim
import os
from dotenv import load_dotenv
import pandas as pd

import psycopg
load_dotenv()

def connect(): 
    db_host = os.getenv('POSTGRES_HOST')
    db_port = os.getenv('POSTGRES_PORT')
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')

    conn = psycopg.connect(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}") 
    
    return conn

def fetch_track_list():
    conn = connect()
    cur = conn.cursor()

    # query = "select distinct des_track from dim_tracks where des_track = 'Circuit de Catalunya'"
    # query = "select distinct des_track, loc_track from dim_tracks where des_track = 'Petronas Sepang International Circuit'"
    query = "select distinct des_track, loc_track from dim_tracks where latitude is null"

    cur.execute(query)
    result_args = cur.fetchall()
    df_tracks = pd.DataFrame(result_args, columns=['des_track', 'loc_track'])
    return df_tracks

def fetch_rider_list():
    conn = connect()
    cur = conn.cursor()

    # query = "select distinct des_track from dim_tracks where des_track = 'Circuit de Catalunya'"
    # query = "select distinct des_track, loc_track from dim_tracks where des_track = 'Petronas Sepang International Circuit'"
    query = "select distinct rider_full_name, loc_birth from dim_riders where loc_birth is not null"

    cur.execute(query)
    result_args = cur.fetchall()
    df_tracks = pd.DataFrame(result_args, columns=['des_rider', 'loc_birth'])
    return df_tracks


def update_location_track(latitude,longitude, track):
    
    conn = connect()
    
    cur = conn.cursor()

    query = f"Update dim_tracks set latitude = {latitude}, longitude = {longitude} where des_track='{track}'"
    cur.execute(query)
    conn.commit()

def update_location_track(latitude,longitude, des_rider):
    
    conn = connect()
    
    cur = conn.cursor()

    query = f"Update dim_riders set birth_latitude = {latitude}, birth_longitude = {longitude} where rider_full_name='{des_rider}'"
    cur.execute(query)
    conn.commit()

def geodata_tracks():
    # gn = geocoders.GeoNames("Cleveland, OH 44106")
    geolocator = Nominatim(user_agent="skllg")
    tracks_list = fetch_track_list()
    # print(tracks_list)
    result=[]
    for i in range(len(tracks_list)):
        track_name = tracks_list.loc[i,"des_track"]
        track_location = tracks_list.loc[i,"loc_track"]

        location = geolocator.geocode(track_name)
        if location is not None: 
            latitude = location.latitude
            longitude = location.longitude
            print(str(location) + str(latitude) +' '+ str(longitude))
            # track_name = 'Circuit de Catalunya'
            # latitude = 41.569468549999996
            # longitude = 2.258063106666664
            
            update_location_track(latitude, longitude, track_name)
        else:
            location2 = geolocator.geocode(track_location)
            if location2 is not None:
                latitude = location2.latitude
                longitude = location2.longitude
                print(str(location2) + str(latitude) +' '+ str(longitude))

                update_location_track(latitude, longitude, track_name)


def geodata_riders():
    # gn = geocoders.GeoNames("Cleveland, OH 44106")
    geolocator = Nominatim(user_agent="skllg")
    tracks_list = fetch_rider_list()
    # print(tracks_list)
    result=[]
    for i in range(len(tracks_list)):
        des_rider = tracks_list.loc[i,"des_rider"]
        loc_birth = tracks_list.loc[i,"loc_birth"]

        location = geolocator.geocode(loc_birth)
        if location is not None: 
            latitude = location.latitude
            longitude = location.longitude
            print(str(location) + str(latitude) +' '+ str(longitude))
            
            
            update_location_track(latitude, longitude, des_rider)
         
# with main:
if __name__ == "__main__": 
    # geodata_tracks()
    geodata_riders()