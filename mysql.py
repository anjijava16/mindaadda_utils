  
import pymysql

#database connection

connection = pymysql.connect(host="localhost", user="root", passwd="root", database="webdb")
cursor = connection.cursor()
print(cursor.connection)


# queries for inserting values
insert1 = "INSERT INTO myjob values(110,'anji','tv',1022,CURRENT_TIMESTAMP)"

#executing the quires
cursor.execute(insert1)


#commiting the connection then closing it.
connection.commit()
connection.close()
