#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import thread

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'Rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieId'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
global Sortingthread
Sortingthread = [1]

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    sql="SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + ";"
    cursor = openconnection.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    min_value=row[0]
    #print "MINVALUE IS:"+str(row[0])
    sql="SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + ";"
    cursor.execute(sql)
    row = cursor.fetchone()
    max_value=row[0]
    step = (max_value - min_value)/5
    sql="CREATE TABLE IF NOT EXISTS " + OutputTable + " (LIKE " + InputTable + ");"
    cursor.execute(sql)
    
    thread.start_new_thread(ParaSortThread,("1", min_value, min_value+step, InputTable, SortingColumnName, OutputTable, openconnection,))
    thread.start_new_thread(ParaSortThread,("2", min_value+step, min_value+2*step, InputTable, SortingColumnName, OutputTable, openconnection,))
    thread.start_new_thread(ParaSortThread,("3", min_value+2*step, min_value+3*step, InputTable, SortingColumnName, OutputTable, openconnection,))
    thread.start_new_thread(ParaSortThread,("4", min_value+3*step, min_value+4*step, InputTable, SortingColumnName, OutputTable, openconnection,))
    thread.start_new_thread(ParaSortThread,("5", min_value+4*step, max_value, InputTable, SortingColumnName, OutputTable, openconnection,))

def ParaSortThread (para, lowerB, upperB, InputTable, SortingColumnName, OutputTable, openconnection):
    sql=""
    if(int(para)==1):
        sql="INSERT INTO " + OutputTable + " (SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + ">=" + str(lowerB) + " AND " + SortingColumnName + "<=" + str(upperB) +" ORDER BY " + SortingColumnName + ");"
    else:
        sql="INSERT INTO " + OutputTable + " (SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + ">" + str(lowerB) + " AND " + SortingColumnName + "<=" + str(upperB) +" ORDER BY " + SortingColumnName + ");"
    global Sortingthread
    while(int(Sortingthread[0])!=int(para)):
        para=para
    cursor = openconnection.cursor()
    cursor.execute(sql)
    openconnection.commit()
    Sortingthread[0]+=1

global Joiningthread
Joiningthread = [1]
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    sql="SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cursor = openconnection.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    min_value1=float(row[0])
    sql="SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cursor.execute(sql)
    row = cursor.fetchone()
    min_value2=float(row[0])
    min_value = min_value1 if min_value1<min_value2 else min_value2
    sql="SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cursor.execute(sql)
    row = cursor.fetchone()
    max_value1=float(row[0])
    sql="SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cursor.execute(sql)
    row = cursor.fetchone()
    max_value2=float(row[0])
    max_value= max_value1 if max_value1>max_value2 else max_value2
    step = (max_value - min_value)/5
    sql="CREATE TABLE IF NOT EXISTS " + OutputTable + " AS (SELECT * FROM " + InputTable1 + " INNER JOIN " + InputTable2 + " ON " +InputTable1+"."+Table1JoinColumn+" = "+ InputTable2 + "."+Table2JoinColumn + " WHERE "+InputTable1+"."+Table1JoinColumn+"<"+ str(min_value) + ");"
    cursor.execute(sql)
    thread.start_new_thread(ParaJoinThread,("1", min_value, min_value+step, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
    thread.start_new_thread(ParaJoinThread,("2", min_value+step, min_value+2*step, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
    thread.start_new_thread(ParaJoinThread,("3", min_value+2*step, min_value+3*step, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
    thread.start_new_thread(ParaJoinThread,("4", min_value+3*step, min_value+4*step, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
    thread.start_new_thread(ParaJoinThread,("5", min_value+4*step, max_value, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
    
def ParaJoinThread (para, lowerB, upperB, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    innerjoinsql=""
    if(int(para)!=1):
        innerjoinsql="SELECT * FROM " + InputTable1 + " INNER JOIN " + InputTable2 + " ON " +InputTable1+"."+Table1JoinColumn+" = "+ InputTable2 + "."+Table2JoinColumn + " WHERE "+InputTable1+"."+Table1JoinColumn+">"+ str(lowerB) + " AND " + InputTable1+"."+Table1JoinColumn + "<=" + str(upperB)
    else:
        innerjoinsql="SELECT * FROM " + InputTable1 + " INNER JOIN " + InputTable2 + " ON " +InputTable1+"."+Table1JoinColumn+" = "+ InputTable2 + "."+Table2JoinColumn + " WHERE "+InputTable1+"."+Table1JoinColumn+">="+ str(lowerB) + " AND " + InputTable1+"."+Table1JoinColumn + "<=" + str(upperB)
    sql="INSERT INTO " + OutputTable + " ("+innerjoinsql+");"
    global Joiningthread
    cursor = openconnection.cursor()
    cursor.execute(sql)
#    while(int(Joiningthread[0])!=int(para)):
#        para=para
    openconnection.commit()
    Joiningthread[0]+=1



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='123', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment3
        print "Creating Database named as ddsassignment3"
        createDB();
        
        # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
        
        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
