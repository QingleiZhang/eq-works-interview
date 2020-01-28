import pandas as pd
import math

data = pd.read_csv("data/DataSample.csv")
poi_list = pd.read_csv("data/POIList.csv")
poi_list = poi_list.rename(columns={
    "Latitude": "POI_Latitude",
    "Longitude": "POI_Longitude"
})

''' 1. Cleanup '''
clean_data = data.drop_duplicates(subset=["TimeSt", "Latitude", "Longitude"])

''' 2. Label '''
def point_diff(x1,y1, x2,y2):
    return math.sqrt((x1-x2)**2 + (y1-y2)**2)
cartesian = clean_data.assign(key=1).merge(poi_list.assign(key=1), on='key').drop('key',1)
cartesian['distance'] = cartesian.apply(
    lambda row: point_diff(
        row["Latitude"], row["Longitude"], 
        row["POI_Latitude"], row["POI_Longitude"]
        ),
    axis=1
    )
labeled = cartesian.loc[cartesian.groupby("_ID")["distance"].idxmin()]

''' 3. Analysis '''
group_by_distance = labeled.groupby("POIID")["distance"]
avg = group_by_distance.mean()
stddv = group_by_distance.std()
max_distance = group_by_distance.max()
areas = (22/7)*(max_distance**2) # pi*r^2
requests = group_by_distance.count()
density = requests/areas
