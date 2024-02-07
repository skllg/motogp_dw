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
    df_tracks['longitude'] = 0
    df_tracks['latitude'] = 0
    return df_tracks

def fetch_rider_list():
    conn = connect()
    cur = conn.cursor()

    
    query = f"select distinct rider_full_name, loc_birth from dim_riders where loc_birth is not null "

    cur.execute(query)
    result_args = cur.fetchall()
    df_riders = pd.DataFrame(result_args, columns=['des_rider', 'loc_birth'])
    df_riders['longitude'] = 0
    df_riders['latitude'] = 0
    
    return df_riders


def update_location_track(latitude,longitude, track):
    
    conn = connect()
    
    cur = conn.cursor()

    query = f"Update dim_tracks set latitude = {latitude}, longitude = {longitude} where des_track='{track}'"
    cur.execute(query)
    conn.commit()

def update_location_rider(latitude,longitude, des_rider):
    
    conn = connect()
    
    cur = conn.cursor()

    query = f"Update dim_riders set birth_latitude = {latitude}, birth_longitude = {longitude} where rider_full_name='{des_rider}'"
    cur.execute(query)
    conn.commit()

def geodata_tracks():
    gn = geocoders.GeoNames("Cleveland, OH 44106")
    geolocator = Nominatim(user_agent="skllg")
    tracks_list = fetch_track_list()
    # print(tracks_list)
    result=[]
    f = open("tracks.txt", "a")
    for i in range(len(tracks_list)):
        track_name = tracks_list.loc[i,"des_track"]
        track_location = tracks_list.loc[i,"loc_track"]

        location = geolocator.geocode(track_name)
        if location is not None: 
            latitude = location.latitude
            longitude = location.longitude
            print(str(location) + str(latitude) +' '+ str(longitude))
            f.write(f"{track_name};{latitude};{longitude} \n")
            update_location_track(latitude, longitude, track_name)
        else:
            location2 = geolocator.geocode(track_location)
            if location2 is not None:
                latitude = location2.latitude
                longitude = location2.longitude
                print(str(location2) + str(latitude) +' '+ str(longitude))
                
                f.write(f"{track_name};{latitude};{longitude} \n")

                update_location_track(latitude, longitude, track_name)
    f.close()    

def geodata_riders():
    geolocator = Nominatim(user_agent="skllg")
    rider_list = fetch_rider_list()
    # print(tracks_list)
    result=[]
    for i in range(len(rider_list)):
        des_rider = rider_list.loc[i,"des_rider"]
        loc_birth = rider_list.loc[i,"loc_birth"]

        
        location = geolocator.geocode(loc_birth)
        if location is not None: 
            latitude = location.latitude
            longitude = location.longitude
            # print(str(des_rider)+' '+str(location) +' '+ str(latitude) +' '+ str(longitude))
            
            f = open("riders.txt", "w", encoding="utf-8")
            string = f"{des_rider};{latitude};{longitude}\n"
            f.write(string)
            f.close()
            
            update_location_rider(latitude, longitude, des_rider)
         
# with main:
if __name__ == "__main__": 
    # geodata_tracks()
    geodata_riders()
    