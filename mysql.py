 import logging
import pymysql
import json
import os
import boto3;
import csv;

# Logger settings - CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Connect to the RDS instance
# logger.info("Connecting to MySQL database")conn = pymysql.connect(host="us-east-1.rds.amazonaws.com", port=3306, user="root", passwd="hack2018", db="hack2018",connect_timeout=30)
conn = pymysql.connect(host="101.53.145.200", port=3306, user="webuser", passwd="ybutymeze", db="mmtechso_webdev",
                       connect_timeout=30)

logger.info("SUCCESS: Connection to MySQL database succeeded")


def createTable(createString):
    try:
        cursorCrate = conn.cursor()
        cursorCrate.execute(createString);
    except Exception:
        print("Exception In Catch Block");
        return False;
    return True;


def showTables(databaseName):
    data = [];
    print("show tables")
    try:
        cursorCrate = conn.cursor()
        cursorCrate.execute("SHOW TABLES");
        for (databases) in cursorCrate:
            data.append(databases[0]);
    except Exception:
        print("Show tables In Catch Block");
        return False;
    return data;


def deleteEntireTableData(tablename):
    try:
        cursorCrate = conn.cursor()
        deleteQuery = "DELETE FROM " + tablename;
        cursorCrate.execute(deleteQuery);
    except Exception:
        print("Exception In Delete Query");
        return False;
    return True;


def insertQueryTable(insertQuery):
    try:
        cursorCrate = conn.cursor()
        print("Final Query ==>", insertQuery)
        cursorCrate.execute(insertQuery);
        conn.commit()
    except Exception:
        print("Exception In Catch Block");
        return False;
    return True;


def showdatbases():
    data = [];
    try:
        cursorCrate = conn.cursor()
        cursorCrate.execute("SHOW DATABASES");
        for (databases) in cursorCrate:
            data.append(databases[0]);
    except Exception:
        print("Show datbases In Catch Block");
        return False;
    return data;


def validateDatabase(datbasename):
    listOfDatabaseNames = showdatbases()
    matching = [s for s in listOfDatabaseNames if datbasename in s]
    matching = ''.join(matching)

    if matching == datbasename:
        return True;
    else:
        print("Database Not Found")
        return False


def validateTableName(datbasename, tableName):
    if any(c in tableName for c in showTables(datbasename)):
        return True;
    else:
        print("Table Not Found")
        return False


def newModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype):
    try:
        s3 = boto3.resource('s3')
        # obj = s3.Object('hack-continuous-integration', 'csv/index.csv').get()['Body']
        obj = s3.Object('hack-continuous-integration', inputpath).get()['Body']
        input_file = obj.read()
        print("Input_FIle", input_file)
        print(type(input_file))
        cnt = 0;
        headerData = ""
        data = []
        for line in input_file.decode("utf-8").split('\n'):
            print("Line Of Data ===>", line)
            if (cnt == 0):
                headerData = line;
                cnt = cnt + 1;
            else:
                data.append(line.replace('\r', ''))

        print("data ===>", data)
        print("headerData ==>", headerData)
        reader = data;
        print("ReaderANJI ===>", reader)
        rs = headerData.split(delimiter)
        print("delimiter==>", delimiter)
        print("rs ==>", rs)
        name = "CREATE TABLE " + tablename + "( ";
        if (validateDatabase(datbasename)):
            pass;
        else:
            return "1";
        columnsName = ""
        if (validateTableName(datbasename, tablename)):
            pass;
        else:
            return "2";
        if (header == "True"):
            print("Header", header)
            for id in rs:
                print("id value is ", rs)
                id = id.replace('\r', '') + " varchar(528),";
                name = name + id;
        if (header == "False"):
            for i in range(len(rs)):
                id = "_c" + str(i) + " varchar(528),";
                name = name + id;
        print("name ==>", name)

        if (header == "True"):
            for colsNames in rs:
                columnsName = columnsName + "," + colsNames;
        if (header == "False"):
            for i in range(len(rs)):
                columnsName = columnsName + "," + "_c" + str(i)

        columnsName = columnsName[1:]
        print("ColumnsNames ===>", columnsName)

        name = name[:-1]
        name = name + ")";
        insertAddQuery = "INSERT INTO  " + tablename + "(" + columnsName + ") values("
        print("Name For Create table ", name)
        response = createTable(name);
        if (response):
            pass;
        else:
            return "12";
        data = []
        for rowId in reader:
            query = "";
            insertQuery = "";
            for ids in rowId.split(delimiter):
                insertQuery = insertQuery + "'" + ids + "',";
            insertQuery = insertQuery[:-1] + ")";
            responseInsertion = insertQueryTable(insertAddQuery + insertQuery)
            if (responseInsertion):
                pass;
            else:
                return "13";
        return "0";

    except Exception:
        print("Exception Here")
        return "9";


def refreshModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype):
    try:
        headerData = ""
        s3 = boto3.resource('s3')
        obj = s3.Object('hack-continuous-integration', inputpath).get()['Body']
        input_file = obj.read()
        print("Input_FIle", input_file)
        print(type(input_file))
        cnt = 0;
        data = []
        for line in input_file.decode("utf-8").split('\n'):
            print("Line Of Data ===>", line)
            if (cnt == 0):
                headerData = line;
                cnt = cnt + 1;
            else:
                data.append(line.replace('\r', ''))
        reader = data;
        rs = headerData.split(delimiter)
        if (validateDatabase(datbasename)):
            pass;
        else:
            return "1";

        if (validateTableName(datbasename, tablename)):
            pass;
        else:
            return "2";
        response = deleteEntireTableData(tablename);
        if (response):
            pass;
        else:
            return "14";
        print("Response From Delete ", response)
        insertAddQuery = "INSERT INTO  " + tablename + " VALUES ("
        for rowId in reader:
            query = "";
            insertQuery = "";
            for ids in rowId.split(delimiter):
                insertQuery = insertQuery + "'" + ids + "',";
            insertQuery = insertQuery[:-1] + ")";
            responseInsertion = insertQueryTable(insertAddQuery + insertQuery)
            if (responseInsertion):
                pass;
            else:
                return "13";

        return "0";

    except Exception:
        print("Exception From Refresh Mode Execution")
        return "9";


def appendModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype):
    try:
        headerData = ""
        s3 = boto3.resource('s3')
        obj = s3.Object('hack-continuous-integration', inputpath).get()['Body']
        input_file = obj.read()
        print("Input_FIle", input_file)
        print(type(input_file))
        cnt = 0;
        data = []
        for line in input_file.decode("utf-8").split('\n'):
            print("Line Of Data ===>", line)
            if (cnt == 0):
                headerData = line;
                cnt = cnt + 1;
            else:
                data.append(line.replace('\r', ''))
        reader = data;

        if (validateDatabase(datbasename)):
            pass;
        else:
            return "1";
        if (validateTableName(datbasename, tablename)):
            pass;
        else:
            return "2";

        columnsName = ""
        if (header == "True"):
            rs = headerData.split(delimiter)
            for colsNames in rs:
                columnsName = columnsName + "," + colsNames;
            columnsName = columnsName[1:]
            insertAddQuery = "INSERT INTO  " + tablename + "(" + columnsName + ") values("

        if (header == "False"):
            insertAddQuery = "INSERT INTO  " + tablename + " VALUES ("

        data = []
        for rowId in reader:
            query = "";
            insertQuery = "";
            for ids in rowId.split(delimiter):
                insertQuery = insertQuery + "'" + ids + "',";
            insertQuery = insertQuery[:-1] + ")";
            responseInsertion = insertQueryTable(insertAddQuery + insertQuery)
            if (responseInsertion):
                pass;
            else:

                return "13";
        return "0";

    except Exception:
        print("Exception Here")
        return "9";

    except Exception:
        print("Exception From AppendModeExecution Mode Execution")
        return "9";


def validationProcess(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype, modetype):
    print("DataBasenane", datbasename)
    if (False == bool(datbasename)):
        return "1"
    elif (False == bool(tablename)):
        return "2"
    elif (False == bool(inputpath)):
        return "3"
    elif (False == bool(header)):
        return "4"
    elif (False == bool(delimiter)):
        return "5"
    elif (False == bool(sourcetype)):
        return "6"
    elif (False == bool(targettype)):
        return "7"
    elif (False == bool(modetype)):
        return "8"
    elif (False == (sourcetype == "csv" or sourcetype == "txt")):
        return "15"
    elif (False == (targettype == "rds" or targettype == "mysql")):
        return "16"
    elif (False == (modetype == "new" or modetype == "refresh" or modetype == "append")):
        return "17"
    elif (False == (header == "True" or header == "False")):
        return "18"
    else:
        return "0"


def lambda_handler(event, context):
    datbasename = event['datbasename']
    tablename = event['tablename']
    delimiter = event['delimiter']
    sourcetype = event['sourcetype']
    targettype = event['targettype']
    mode = event['mode']
    header = event['header']
    inputpath = event['inputpath']

    validationResponse = validationProcess(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype,
                                           mode);
    print("ValidationError ===>", validationResponse)
    response = "10";
    if (validationResponse == "0"):
        if (mode == "new"):
            response = newModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype);
            print("Response From New Mode ==>", response)
        elif (mode == "refresh"):
            print("refresh Mode Execution Here")
            response = refreshModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype,
                                            targettype);
            print("Response From Refresh Mode ==>", response)
        elif (mode == "append"):
            response = appendModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype,
                                           targettype);
            print("Response From append Mode ==>", response)

    return {
        'statusCode': response,
    }


def lambda_handler_working(event, context):
    datbasename = 'mmtechso_webdev'
    tablename = "newprocess_withHeade_mm"
    delimiter = ";"
    sourcetype = "csv"
    targettype = "rds"
    mode = "append"
    header = "True";
    inputpath = "F:/Tech_Learn/Hatchron/index.csv";

    validationResponse = validationProcess(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype,
                                           mode);
    print("ValidationError ===>", validationResponse)
    response = "10";
    if (validationResponse == "0"):
        if (mode == "new"):
            response = newModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype, targettype);
            print("Response From New Mode ==>", response)
        elif (mode == "refresh"):
            print("refresh Mode Execution Here")
            response = refreshModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype,
                                            targettype);
            print("Response From Refresh Mode ==>", response)
        elif (mode == "append"):
            response = appendModeExecution(datbasename, tablename, inputpath, header, delimiter, sourcetype,
                                           targettype);
            print("Response From append Mode ==>", response)

    return {
        'statusCode': response,
    }

  
  
# import pymysql

# #database connection

# connection = pymysql.connect(host="localhost", user="root", passwd="root", database="webdb")
# cursor = connection.cursor()
# print(cursor.connection)


# # queries for inserting values
# insert1 = "INSERT INTO myjob values(110,'anji','tv',1022,CURRENT_TIMESTAMP)"

# #executing the quires
# cursor.execute(insert1)


# #commiting the connection then closing it.
# connection.commit()
# connection.close()
