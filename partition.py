import geopandas as gp
import numpy as np
import pandas as pd
import dask_geopandas
from shapely.ops import unary_union, split
from shapely.geometry import LineString, Point

def partition_data_by_row_shape(df_shape_file, number_of_partitions):
    intervals = np.linspace(
        df_shape_file.geometry.bounds.miny.min(),
        df_shape_file.geometry.bounds.maxy.max(),
        number_of_partitions + 1,
    )
    min_x = df_shape_file.geometry.bounds.minx.min()
    max_x = df_shape_file.geometry.bounds.maxx.max()
    lines = [
        LineString([Point(min_x, y), Point(max_x, y)]) 
        for y in intervals[1:number_of_partitions][::-1]
    ]
    df_shape_file_partitioned = gp.GeoDataFrame(columns=["geometry", "partition"])
    polygon = unary_union(df_shape_file.geometry)

    partitioned_data = []
    for partition, line in enumerate(lines):
        new_polygons = split(polygon, line)
        if len(new_polygons.geoms)>1:
            upper, lower = ((new_polygons.geoms[0], unary_union(new_polygons.geoms[1:])) 
                           if new_polygons.geoms[0].bounds[1] > line.bounds[1]
                           else (unary_union(new_polygons.geoms[1:]), new_polygons.geoms[0]))
        
        partitioned_data.append({
            'geometry': upper,
            'partition': partition+1
        })
        polygon =lower
        if partition+1 == len(lines):
            partitioned_data.append({
                'geometry': polygon,
                'partition': number_of_partitions
            })
            break
            
    
    df_shape_file_partitioned = gp.GeoDataFrame(partitioned_data)
    
    return df_shape_file_partitioned

def partition_data_by_row_agents(df_agents, number_of_partitions):
    intervals = np.linspace(
        df_agents.geometry.bounds.miny.min(),
        df_agents.geometry.bounds.maxy.max(),
        number_of_partitions + 1,
    )
    dfs = []


    for i in range(number_of_partitions):
        filtered_data = df_agents[(df_agents.geometry.y>=intervals[i]) &
        (df_agents.geometry.y<intervals[i+1])]
        filtered_data['partition'] =i+1
        
        dfs.append(filtered_data)
    df_agents_partitioned = pd.concat(dfs)
    df_agents_partitioned = gp.GeoDataFrame(df_agents_partitioned)
    return df_agents_partitioned    


def partition_data_by_col_shape(df_shape_file, number_of_partitions):
    intervals = np.linspace(
        df_shape_file.geometry.bounds.minx.min(),
        df_shape_file.geometry.bounds.maxx.max(),
        number_of_partitions + 1,
    )
    min_y = df_shape_file.geometry.bounds.miny.min()
    max_y = df_shape_file.geometry.bounds.maxy.max()
    lines = [
        LineString([Point(x, min_y), Point(x, max_y)]) 
        for x in intervals[1:number_of_partitions][::-1]
    ]
    df_shape_file_partitioned = gp.GeoDataFrame(columns=["geometry", "partition"])
    polygon = unary_union(df_shape_file.geometry)

    partitioned_data = []
    for partition, line in enumerate(lines):
        new_polygons = split(polygon, line)
        if len(new_polygons.geoms)>1:
            left, right = ((new_polygons.geoms[0], unary_union(new_polygons.geoms[1:])) 
                           if new_polygons.geoms[0].bounds[0] < line.bounds[0]
                           else (unary_union(new_polygons.geoms[1:]), new_polygons.geoms[0]))
        
        partitioned_data.append({
            'geometry': right,
            'partition': partition+1
        })
        polygon =left
        if partition+1 == len(lines):
            partitioned_data.append({
                'geometry': polygon,
                'partition': number_of_partitions
            })
            break
            
    
    df_shape_file_partitioned = gp.GeoDataFrame(partitioned_data)
    
    return df_shape_file_partitioned


def partition_data_by_col_agents(df_agents, number_of_partitions):
    intervals = np.linspace(
        df_agents.geometry.bounds.minx.min(),
        df_agents.geometry.bounds.maxx.max(),
        number_of_partitions + 1,
    )
    dfs = []


    for i in range(number_of_partitions):
        filtered_data = df_agents[(df_agents.geometry.x>=intervals[i]) &
        (df_agents.geometry.x<intervals[i+1])]
        filtered_data['partition'] =i+1
        
        dfs.append(filtered_data)
    df_agents_partitioned = pd.concat(dfs)
    df_agents_partitioned = gp.GeoDataFrame(df_agents_partitioned)
    return df_agents_partitioned    

def partition_data_by_hilbert(df_shape_file, number_of_partitions):
    d_gdf = dask_geopandas.from_geopandas(
        df_shape_file, npartitions=number_of_partitions
    )
    d_gdf = d_gdf.spatial_shuffle(by="hilbert")
    df_shape_file_partitioned = gp.GeoDataFrame(
        columns=["geometry", "partition"]
    )
    for i in range(number_of_partitions):
        d_gdf_temp = d_gdf.partitions[i].compute().reset_index(drop=True)
        d_gdf_temp["partition"] = i + 1
        df_shape_file_partitioned = pd.concat([df_shape_file_partitioned, d_gdf_temp])
    return df_shape_file_partitioned


def partition_data_by_morton(df_shape_file, number_of_partitions):
    d_gdf = dask_geopandas.from_geopandas(
        df_shape_file, npartitions=number_of_partitions
    )
    d_gdf = d_gdf.spatial_shuffle(by="morton")
    df_shape_file_partitioned = gp.GeoDataFrame(
        columns=["geometry", "partition"]
    )
    for i in range(number_of_partitions):
        d_gdf_temp = d_gdf.partitions[i].compute().reset_index(drop=True)
        d_gdf_temp["partition"] = i + 1
        df_shape_file_partitioned = pd.concat([df_shape_file_partitioned, d_gdf_temp])
    return df_shape_file_partitioned


def partition_data_by_geohash(df_shape_file, number_of_partitions):
    d_gdf = dask_geopandas.from_geopandas(
        df_shape_file, npartitions=number_of_partitions
    )
    d_gdf = d_gdf.spatial_shuffle(by="geohash")
    df_shape_file_partitioned = gp.GeoDataFrame(
        columns=["geometry", "partition"]
    )
    for i in range(number_of_partitions):
        d_gdf_temp = d_gdf.partitions[i].compute().reset_index(drop=True)
        d_gdf_temp["partition"] = i + 1
        df_shape_file_partitioned = pd.concat([df_shape_file_partitioned, d_gdf_temp])
    return df_shape_file_partitioned


def partition_data(df, number_of_partitions=4, kind="row", for_="shape"):
    if kind == "row" and for_=="shape":
        return partition_data_by_row_shape(df, number_of_partitions)
    if kind == "row" and for_=="agents":
        return partition_data_by_row_agents(df, number_of_partitions)
    if kind == "col" and for_=="shape":
        return partition_data_by_col_shape(df, number_of_partitions)
    if kind == "col" and for_=="agents":
        return partition_data_by_col_agents(df, number_of_partitions)
    if kind == "hilbert":
        return partition_data_by_hilbert(df, number_of_partitions)
    if kind == "morton":
        return partition_data_by_morton(df, number_of_partitions)
    if kind == "geohash":
        return partition_data_by_geohash(df, number_of_partitions)