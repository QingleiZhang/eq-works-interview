import pandas as pd
import math

data = pd.read_csv("data/DataSample.csv")
poi_list = pd.read_csv("data/POIList.csv")
poi_list = poi_list.rename(columns={
    "Latitude": "POI_Latitude",
    "Longitude": "POI_Longitude"
})

''' 1. Cleanup '''
clean_data = data.drop_duplicates(
    subset=["TimeSt", "Latitude", "Longitude"]
    )
print("Cleaned up data:")
print(clean_data)

''' 2. Label '''
def euclidean_distance(x1,y1, x2,y2):
    # Square root of the sum of differences between coordinates
    return math.sqrt((x1-x2)**2 + (y1-y2)**2)

cartesian = clean_data.assign(key=1).merge(poi_list.assign(key=1), on='key').drop('key',1)
cartesian['distance'] = cartesian.apply(
    lambda row: euclidean_distance(
        row["Latitude"], row["Longitude"], 
        row["POI_Latitude"], row["POI_Longitude"]
        ),
    axis=1
    )
labeled = cartesian.loc[cartesian.groupby("_ID")["distance"].idxmin()]
print("Labeled data:")
print(labeled)

''' 3. Analysis '''
# 1. Grouping by each POI ID
group_by_distance = labeled.groupby("POIID")["distance"]
avg = group_by_distance.mean()
stddv = group_by_distance.std()
print(f"Average of each POI is {avg}")
print(f"Standard Deviation: {stddv}")

# 2. Radius of the circle is the maximum point away from the POI
max_distance = group_by_distance.max()
areas = (22/7)*(max_distance**2) # pi*r^2
requests = group_by_distance.count()
density = requests/areas
print(f"Density of requests is {density}")


''' 4b. Pipeline Dependency '''
f = open("question.txt", "r")
lines = f.readlines()
starting_task = lines[0].split(":")[1].strip()
goal_task = lines[1].split(":")[1].strip()

f = open("relations.txt", "r")
lines = f.readlines()
relations = list(map(lambda x: x.strip().split("->"), lines))

f = open("task_ids.txt")
line = f.read()
task_ids = line.split(",")

import networkx as nx
import matplotlib.pyplot as plt 

# To build a DAG pipeline
def build_pipeline(tasks):
    # Instantiate a Directed graph
    pipeline = nx.DiGraph()
    for task in tasks:
        pipeline.add_edge(task[0],task[1])

    # Generated pipeline
    nx.draw_networkx(pipeline, arrows=True)
    return pipeline

# Find path between two tasks
def find_path(pipeline, sources, destination):
    final_path = []
    for source in sources:
        if nx.has_path(pipeline, source, destination):
            print(f"Path found between {source}->{destination}")
            # Shortest path does not mean it will cover all dependencies
            path = nx.shortest_path(pipeline,source,destination)
            for node in path:
                # Find the ancestors of each node
                ancestors = pipeline.predecessors(node)
                # If the node has an ancestor
                # and task not already completed.
                if ancestors and node not in sources:                
                    for ancestor in ancestors:
                        # Add the ancestor to the final_path 
                        # if it doesn't exist already
                        if ancestor not in final_path:
                            final_path.append(ancestor)
                    final_path.append(node)
            return final_path
        else:
            print(f"No path found between {source}->{destination}")

pipeline = build_pipeline(relations)
path = find_path(pipeline, [starting_task], goal_task)
print(f"Path found: {path}")