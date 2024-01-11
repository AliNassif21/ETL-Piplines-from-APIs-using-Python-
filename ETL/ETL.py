import pandas as pd
import requests
from sqlalchemy import create_engine


def extract() -> dict:
    API_URL=AN_API
    data=requests.get(API_URL).json()
    return data


def Transform(data:dict) ->pd.DataFrame:
    df=pd.DataFrame(data)
    filt=df["name"].str.contains("New York")
    df=df[filt]
    print(f"number of universties in new york is {len(df)}")
    df["domains"]=[','.join(map(str,i))for i in df["domains"]]
    df["web pages"] = [','.join(map(str, i)) for i in df["web_pages"]]
    df.reset_index(inplace=True,drop=True)
    df=df[["domains","country","name","web pages"]]
    return df


def load(df:pd.DataFrame) ->None:
    disk_engine=create_engine("sqlite:///my_lite_stor.db")
    df.to_sql("cal_uni",disk_engine,if_exists="replace")
#%%
data=extract()
df=Transform(data)
load(df)
