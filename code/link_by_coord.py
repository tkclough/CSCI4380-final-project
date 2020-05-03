import csv
import gzip
import math
import time

def link_by_coord():
    stations = list()
    with gzip.open('code/datasets/HPD_v02r02_stationinv_c20200429.csv.gz', 'rt') as station_file:
        csv_reader = csv.reader(station_file, delimiter=',')
        next(csv_reader)
        for row in csv_reader:
            stations.append((str(row[0]), float(row[1]), float(row[2])))
    stations.sort(key=lambda x: x[1])
    zipcodes = list()
    with open('code/datasets/raw', 'r') as zipcode_file:
        csv_reader = csv.reader(zipcode_file, delimiter=',')
        next(csv_reader)
        for row in csv_reader:
            zipcode = str(row[0])
            lat = float(row[1])
            lng = float(row[2])
            closest = ""
            dist = -1
            for station in stations:
                st_name = station[0]
                st_lat = station[1]
                st_lng = station[2]
                if dist >= 0 and (st_lat - lat) > dist:
                    break
                st_dist = math.sqrt((st_lat-lat)**2 + (st_lng-lng)**2)
                if st_dist < dist or dist == -1:
                    closest = st_name
                    dist = st_dist
            zipcodes.append((zipcode, closest))
    return zipcodes

if __name__ == "__main__":
    link_by_coord()