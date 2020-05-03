import psycopg2
import tarfile
import io
import re
import tqdm
import xlrd
from datetime import datetime
import gzip
import csv
import math
import bisect
connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

# TODO add your code here (or in other files, at your discretion) to load the data


def main():
    # TODO invoke your code to load the data into the database
    print("Loading data")

    with psycopg2.connect(connection_string) as conn:
        # write to a new csv that only includes rows from 2008
        print('Loading measurement data')
        load_measurements(conn, 'datasets/HPD_v02r02_POR_s19400101_e20200422_c20200429.tar.gz')

        # read incident data
        print('Loading 2008 fire incident data')
        load_incidents(conn, 'datasets/2008incidentaddress.xlsx')

        # compute closest stations
        print('Computing closest stations')
        compute_closest_stations(conn)


def load_incidents(conn, incident_filename):
    print('Opening excel workbook, give me a minute...')
    with conn.cursor() as cur, \
         xlrd.open_workbook(incident_filename) as f:
        sheet = f.sheets()[0] # for some reason get_sheet(0) fails
        rows = sheet.get_rows()

        # skip header
        next(rows)
        for row in tqdm.tqdm(rows, total=sheet.nrows):
            state = row[0].value
            city = row[12].value
            zipcode = row[14].value

            if state == '':
                state = None
            if city == '':
                city = None
            if zipcode == '':
                zipcode = None

            # dates have a weird format '%m%d%Y' but month is not zero padded so strptime without some help
            datestr = row[2].value
            date = datetime.strptime('%08d' % (datestr,), '%m%d%Y')
            date = datetime.strftime(date, '%Y-%m-%d')

            cur.execute('INSERT INTO incident VALUES (%s, %s, %s, %s);', (city, state, zipcode, date))


def load_measurements(conn, tarfilename, year=2008):
    pat = r'(\d{4})-(\d{2})-(\d{2})'

    headers = [
        'StnID', 'Lat', 'Lon', 'Elev', 'Year-Month-Day', 'Element', 'HR00Val', 'HR00MF', 'HR00QF', 'HR00S1', 'HR00S2',
        'HR01Val', 'HR01MF', 'HR01QF', 'HR01S1', 'HR01S2', 'HR02Val', 'HR02MF', 'HR02QF', 'HR02S1', 'HR02S2', 'HR03Val',
        'HR03MF'    'HR03QF', 'HR03S1', 'HR03S2', 'HR04Val', 'HR04MF', 'HR04QF', 'HR04S1', 'HR04S2', 'HR05Val',
        'HR05MF', 'HR05QF', 'HR05S1', 'HR05S2', 'HR06Val', 'HR06MF', 'HR06QF', 'HR06S1', 'HR06S2', 'HR07Val', 'HR07MF',
        'HR07QF', 'HR07S1', 'HR07S2', 'HR08Val', 'HR08MF', 'HR08QF', 'HR08S1', 'HR08S2', 'HR09Val', 'HR09MF', 'HR09QF',
        'HR09S1', 'HR09S2', 'HR10Val', 'HR10MF', 'HR10QF', 'HR10S1', 'HR10S2', 'HR11Val', 'HR11MF', 'HR11QF', 'HR11S1',
        'HR11S2', 'HR12Val', 'HR12MF', 'HR12QF', 'HR12S1', 'HR12S2', 'HR13Val', 'HR13MF', 'HR13QF', 'HR13S1', 'HR13S2',
        'HR14Val', 'HR14MF', 'HR14QF', 'HR14S1', 'HR14S2', 'HR15Val', 'HR15MF', 'HR15QF', 'HR15S1', 'HR15S2', 'HR16Val',
        'HR16MF', 'HR16QF', 'HR16S1', 'HR16S2', 'HR17Val', 'HR17MF', 'HR17QF', 'HR17S1', 'HR17S2', 'HR18Val', 'HR18MF',
        'HR18QF', 'HR18S1', 'HR18S2', 'HR19Val', 'HR19MF', 'HR19QF', 'HR19S1', 'HR19S2', 'HR20Val', 'HR20MF', 'HR20QF',
        'HR20S1', 'HR20S2', 'HR21Val', 'HR21MF', 'HR21QF', 'HR21S1', 'HR21S2', 'HR22Val', 'HR22MF', 'HR22QF', 'HR22S1',
        'HR22S2', 'HR23Val', 'HR23MF', 'HR23QF', 'HR23S1', 'HR23S2', 'DlySum', 'DlySumMF', 'DlySumQF', 'DlySumS1',
        'DlySumS2'
    ]
    nameToCol = {name: i for i, name in enumerate(headers)}

    # open the output file for writing and the .tar.gz archive for reading
    print('Opening tarfile, give me a minute...')
    with conn.cursor() as cur, \
         tarfile.open(tarfilename) as t:
        # for each csv file in the archive
        for member in tqdm.tqdm(t.getmembers()):
            # open the csv file within the archive (binary), and wrap in line based layer
            found = False
            with t.extractfile(member) as f2, io.TextIOWrapper(f2, encoding='utf-8', newline='') as csvfile:
                next(csvfile)
                # for each line in the file
                for line in csvfile:
                    # the date is the 4th column
                    parts = line.split(',')
                    date = parts[nameToCol['Year-Month-Day']].strip()
                    match = re.match(pat, date)

                    # determine if the year is correct, and if so, write
                    if match and int(match.group(1)) == year:
                        found = True
                        for hr in range(24):
                            colname = 'HR%02dVal' % (hr,)
                            val = parts[nameToCol[colname]]
                            dt = '%s %02d:00:00' % (date, hr)
                            cur.execute("INSERT INTO measurement VALUES (%s, %s, %s)",
                                        (parts[nameToCol['StnID']], dt, val))
                    elif found:
                        break
                conn.commit()


def compute_closest_stations(conn):
    with conn.cursor() as cur:
        stations = list()
        with gzip.open('datasets/HPD_v02r02_stationinv_c20200429.csv.gz', 'rt') as station_file:
            csv_reader = csv.reader(station_file, delimiter=',')
            next(csv_reader)
            for row in csv_reader:
                stations.append((str(row[0]), float(row[1]), float(row[2])))
        stations.sort(key=lambda x: x[1])
        with open('datasets/raw', 'r') as zipcode_file:
            csv_reader = csv.reader(zipcode_file, delimiter=',')
            next(csv_reader)
            for row in tqdm.tqdm(csv_reader):
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

                cur.execute("INSERT INTO closest_station VALUES (%s, %s);", (zipcode, closest))
            conn.commit()


if __name__ == "__main__":
    main()
