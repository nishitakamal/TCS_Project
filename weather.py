import pandas as pd
import requests
import json
from pymongo import MongoClient
import logging
import config as cfg

logging.basicConfig(filename='weather.log',encoding='utf-8', level=logging.DEBUG )

# Fetch weather data- http://api.weatherapi.com/v1/  
def weatherAPI(url,key,cities):
    main_df=pd.DataFrame()
    # cities=['Boston','New York','Chicago']
    for city in cities:
        api={'url':url,'key':key,'city':f'{city}','date':'2022-12-16'}
        for i in range(0,24):
            try:
                logging.info(f'Fetching Data for {i}')
                r=requests.get(api['url'], params={'key':api['key'],'q':api['city'],'dt': api['date'],'hour':f'{str(i)}'})
                data=r.json()
                details={'city':[data['location']['name']], 'state':[data['location']['region']], 'country':[data['location']['country']],'lat':[data['location']['lat']],'lon':[data['location']['lon']],
                'time':[data['forecast']['forecastday'][0]['hour'][0]['time']],'temp_c':[data['forecast']['forecastday'][0]['hour'][0]['temp_c']],'temp_f':[data['forecast']['forecastday'][0]['hour'][0]['temp_f']],
                'feelslike_c':[data['forecast']['forecastday'][0]['hour'][0]['feelslike_c']],'feelslike_f':[data['forecast']['forecastday'][0]['hour'][0]['feelslike_f']]}
                df = pd.DataFrame(details)
                main_df=pd.concat([main_df,df],axis=0)
                main_df=main_df.reset_index().drop(['index'],axis=1)
                logging.info(f'DataFrame for {i}  Created')
            except Exception as e:
                logging.info(f"Exception occured for hour {i}: {e}")
                raise Exception(e)

    logging.info("Data Fetching Completed! ")
    main_df.head() # Weather information hourly for the day 2022-12-16
    return main_df

# Connect and Store data in MongoDB
def dbConnection(user,password,main_df):
    client = MongoClient(f"mongodb+srv://{user}:{password}@weathercluster.66kio56.mongodb.net/?retryWrites=true&w=majority")
    logging.info("Mongo DB connection Established")
    db = client['WeatherDB']
    logging.info(db)
    collection = db['Boston']
    logging.info(collection)
    data_dict = main_df.to_dict("records")
    collection.insert_many(data_dict)
    logging.info(f"Data successfully inserted into weatherDB Boston collection ")
# Data exposed using MongoDB Data API
def apiResponse(url,key):
    # url = "https://data.mongodb-api.com/app/data-zryyz/endpoint/data/v1/action/findOne"

    payload = json.dumps({
        "collection": "Boston",
        "database": "WeatherDB",
        "dataSource": "WeatherCluster",
        "filter": {
            "time":  '2022-12-16 02:00'
        }
    })
    headers = {
    'Content-Type': 'application/json',
    'Access-Control-Request-Headers': '*',
    'api-key': key,
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)

    response.json()
    logging.info(f"{response.status_code}\n {response.json()}")

cities=['Boston','New York','Chicago']

df= weatherAPI(cfg.weather_api['url'],cfg.weather_api['key'],cities)
dbConnection(cfg.mongoDB['user'],cfg.mongoDB['password'],df)
apiResponse(cfg.data_api['url'],cfg.data_api['key'])