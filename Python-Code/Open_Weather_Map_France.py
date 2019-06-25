import urllib.request
import requests
import json
import pandas as pd
from pandas.io import gbq
from google.oauth2 import service_account
import pandas_gbq


credentials = service_account.Credentials.from_service_account_file(
    'C:\MyProjectKey.json',
)


url = "http://api.openweathermap.org/data/2.5/weather?q=Paris&APPID=4de7f065b9182d75ac6d2c197f7b0eac"

with urllib.request.urlopen(url) as url:
    data = json.loads(url.read().decode())

from pandas.io.json import json_normalize   
 
df = json_normalize(data)

df1 = pd.DataFrame(df['weather'].str[0].values.tolist()).add_prefix('weather.')

df2 = pd.concat([df.drop('weather', 1), df1], axis=1)


df2.columns = df2.columns.str.replace(r"[.]", "_")

pandas_gbq.to_gbq(
    df2, 'Demo.OpenWeatherMap', project_id='my-project-1-244517', if_exists='append',
)