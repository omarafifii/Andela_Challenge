import numpy as np
import pandas as pd
import requests
import datetime
import json
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base

engine = sqlalchemy.create_engine(
    'mysql+mysqlconnector://omar:******@localhost:3306/sqlalchemy',
    echo=True)

Base = declarative_base()
 
class Channels(Base):
    __tablename__ = 'channels'
 
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    channel = sqlalchemy.Column(sqlalchemy.String(length=20))
    title = sqlalchemy.Column(sqlalchemy.String(length=50))
    releaseYear = sqlalchemy.Column(sqlalchemy.String(length=10))
    genres = sqlalchemy.Column(sqlalchemy.String(length=100))
    shortDescription = sqlalchemy.Column(sqlalchemy.String(length=100))
 
class Theatres(Base):
    __tablename__ = 'theatres'
 
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    theatre = sqlalchemy.Column(sqlalchemy.String(length=20))
    title = sqlalchemy.Column(sqlalchemy.String(length=50))
    releaseYear = sqlalchemy.Column(sqlalchemy.String(length=10))
    genres = sqlalchemy.Column(sqlalchemy.String(length=100))
    shortDescription = sqlalchemy.Column(sqlalchemy.String(length=100)) 
    datetime = sqlalchemy.Column(sqlalchemy.String(length=50)) 

Base.metadata.create_all(engine)

def get_data_from_api(api_secret,zip_code,start_date,line_up_id,date_time):
    
    # Get data from first link
    r1 = requests.get('http://data.tmsapi.com/v1.1/movies/showings?startDate={}&zip={}&api_key={}'.format(start_date,zip_code,api_secret))

    #  Some data wrangling to obtain the shape we need
    r1_json = json.loads(r1.text)
    r1_df = pd.json_normalize(r1_json, record_path=['showtimes'] ,meta=['title','shortDescription','genres','releaseYear'], errors='ignore')
    r1_df_final = r1_df[['theatre.name','title', 'releaseYear','genres', 'shortDescription','dateTime']]
    r1_df_final.columns = ['theatre','title', 'releaseYear','genres', 'shortDescription','dateTime']
    r1_df_final.dropna(inplace=True)
    r1_df_final['genres'] = r1_df_final['genres'].apply(','.join)

    # Store to database
    r1_df_final.to_sql("theatres", con=engine, index=False, if_exists="replace")

    # Get data from second link
    r2 = requests.get('http://data.tmsapi.com/v1.1/movies/airings?lineupId={}&startDateTime={}Z&api_key={}'.format(line_up_id,date_time,api_secret))

    #  Some data wrangling to obtain the shape we need
    r2_json = json.loads(r2.text)
    r2_df = pd.json_normalize(r2_json)
    r2_df_final = r2_df[['channels','program.title', 'program.releaseYear','program.genres', 'program.shortDescription']]
    r2_df_final.columns = ['channel','title', 'releaseYear','genres', 'shortDescription']
    r2_df_final.dropna(inplace=True)
    r2_df_final['genres'] = r2_df_final['genres'].apply(','.join)
    r2_df_final['channel'] = r2_df_final['channel'].apply(','.join)

    # Store to database
    r2_df_final.to_sql("channels", con=engine, index=False, if_exists="replace")

def get_top_five_genres():
    # Get data from database
    r1_new = pd.read_sql_table('theatres', con=engine)
    r2_new = pd.read_sql_table('channels', con=engine)

    # Merge data into one table
    df_total = pd.concat([r1_new, r2_new])

    # Process the data to get the top five genres and return their movie details
    df_total['genres'] = df_total['genres'].apply(lambda x: x.split(','))
    df_total = df_total.explode('genres')
    top_5 = df_total.groupby('genres')['title'].count().sort_values(ascending=False)[:5]
    top_5_list = list(top_5.index.values)
    df_top_5_final = df_total[df_total['genres'].isin(top_5_list)]
    df_top_5_final = df_top_5_final.reset_index(drop=True)
    return top_5, df_top_5_final


api_secret = '*************'
zip_code = '78701'
start_date = datetime.datetime.now().strftime('%Y-%m-%d')
line_up_id = 'USA-TX42500-X'
date_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M')

get_data_from_api(api_secret,zip_code,start_date,line_up_id,date_time)
result = get_top_five_genres()
print('\nThe top 5 genres are:\n')
print(result[0])
print('\nMovie details for the top 5 genres:\n')
print(result[1])
