import psycopg2
import tarfile
import io
import re
import tqdm
connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

# TODO add your code here (or in other files, at your discretion) to load the data


def main():
    # TODO invoke your code to load the data into the database
    print("Loading data")

    # write to a new csv that only includes rows from 2008
    write_relevant_rows('datasets/HPD_v02r02_POR_s19400101_e20200422_c20200429.tar.gz', 'datasets/2008-all.csv')


def write_relevant_rows(tarfilename, outfilename, year=2008):
    pat = r'(\d{4})-\d{2}-\d{2}'

    # open the output file for writing and the .tar.gz archive for reading
    with open(outfilename, 'w') as f, tarfile.open(tarfilename) as t:
        # for each csv file in the archive
        for member in tqdm.tqdm(t.getmembers()):
            # open the csv file within the archive (binary), and wrap in line based layer
            with t.extractfile(member) as f2, io.TextIOWrapper(f2, encoding='utf-8', newline='') as csvfile:
                # for each line in the file
                for line in csvfile:
                    # the date is the 4th column
                    date = line.split(',')[4].strip()
                    match = re.match(pat, date)

                    # determine if the year is correct, and if so, write
                    if match and int(match.group(1)) == year:
                        f.write(line)


if __name__ == "__main__":
    main()
