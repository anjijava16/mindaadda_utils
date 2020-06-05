import csv
import traceback
import sys
import pymysql
import logging
from datetime import datetime

## Logger settings
logger = logging.getLogger()
logger.setLevel(logging.INFO)

## static configuration
#query=""" INSERT INTO analytics_jobs_tbl (source_engine,job_location,job_link,company_name,job_title,job_description,date_of_post,job_duration,job_type,job_skills,job_status,created_by,updated_by) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
query=""" INSERT INTO analytics_jobs_tbl (source_engine,job_location,job_link,company_name,job_title,job_description,job_start_dt,job_type,job_skills,job_end_dt,no_of_openings,job_duration_months,max_sal,job_status,created_by,created_ts,updated_by,updated_ts) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)  """

#INPUT_FILE_PATH='C:/Tech_Learn_2/anlaytics/data_processed_sample1_dB_format.csv'
INPUT_FILE_PATH='C:/Tech_Learn_2/anlaytics/no_null_skill.csv'
DELIMITER='|'
DB_HOST_IP='localhost'
DB_NAME='mmtechso_mindadda-analytics-db'
DB_PORT=3306
DB_USER_NAME='root'
DB_USER_PASSWORD='root'
YEAR=2020
YEAR_21=2021
current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
created_by="intershala_user"
updated_by="intershala_user"
job_status=1

class Utils(object):

    def __init__(self):
        print("Utils Initilize method")

    def monthname_to_number(self, short_month):
        return {
            'Jan': 1,
            'Feb': 2,
            'Mar': 3,
            'Apr': 4,
            'May': 5,
            'Jun': 6,
            'Jul': 7,
            'Aug': 8,
            'Sep': 9,
            'Oct': 10,
            'Nov': 11,
            'Dec': 12
        }[short_month]

    def date_conversion(self, last_date):
        string_array = last_date.split(" ")
        day = string_array[0]
        month = string_array[1]
        day = day.zfill(2)
        month = self.monthname_to_number(month)
        year=string_array[2];
        if year=='20':
            year=YEAR
        elif year=='21':
            year=YEAR_21
        else:
            year=YEAR_21
        final_date = str(year) + "-" + str(month).zfill(2) + "-" + str(day)
        return final_date;

class DBConnection(object):
    def __init__(self):
        self.connection=pymysql.connect(host=DB_HOST_IP, port=3306, user=DB_USER_NAME, passwd=DB_USER_PASSWORD, db=DB_NAME, connect_timeout=30)
        self.utils=Utils();

    def  write_into_db(self,records):
        print(records)
        print("Start time :"+str(datetime.now()))
        for row in records:
            try:
                cursor = self.connection.cursor()
                #cursor.executemany(query, [(row[1], row[2], row[3], row[5], row[6], row[7], row[11], row[9], row[12],row[13], 1, 'intershall_user', 'intershall_user')])
                cursor.executemany(query,[(row[1], row[2], row[3], row[5], row[6], row[7], row[9], row[12],row[13],str(self.utils.date_conversion(row[11])),str(row[14]),str(row[15]),str(row[16]), job_status, created_by, current_timestamp,updated_by,current_timestamp)])
            except Exception as e:
                print("Error Message into write_into_db:  "+e)
                traceback.print_exc()
        self.connection.commit()
        print("End time :"+str(datetime.now()))


class FileReader(object):
    def __init__(self):
        print("Inilize the constrcuores")

    def read_csv(self):
        with open(INPUT_FILE_PATH) as input_file:
            reader = csv.reader(input_file, delimiter=DELIMITER)
            batch = []
            line_count=0
            for row in reader:
                if line_count == 0:
                    #Skip Header here
                    print(f'Column names are {", ".join(row)}')
                    line_count += 1
                else:
                    batch.append(row)
                # if len(row) == size:
                #     yield batch
                #     del batch[:]
            return batch

#
# if __name__ == '__main2__':
#     try:
#         date="10 Jun 20";
#         ops=FileReader();
#         res= ops.date_conversion(date)
#         print(res);
#     except Exception as e:
#         traceback.print_exc()
#         print("Error messages : "+e)
#         sys.exit(-1)

if __name__ == '__main__':
    try:
        logger.info("File Read Operation started here ")
        file_read = FileReader()
        read_list=file_read.read_csv()
        logger.info("File Read Operation Ended here ")

        #
        # for row in read_list:
        #      print(str(row[16]))

        logger.info("DBConnection started here ")
        db_write=DBConnection();
        db_write.write_into_db(read_list)
        logger.info("DBConnection ended here ")

    except Exception as e:
        traceback.print_exc()
        print("Error messages : "+e)
        sys.exit(-1)
