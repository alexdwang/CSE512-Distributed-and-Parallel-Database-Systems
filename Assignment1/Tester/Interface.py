#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import StringIO
import os
import sys

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    # connection
    con = openconnection
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
    cur.execute("CREATE TABLE "+ratingstablename+"(UserID integer, MovieID integer,Rating double precision);")
    file=open(ratingsfilepath,"r")
    records=file.readlines()
    inputRecords=""
    for record in records:
        inputRecords+=record[:record.rindex("::")].replace("::","\t")+"\n"
    IR=StringIO.StringIO(inputRecords)
    cur.copy_from(IR, ratingstablename, columns=('UserID', 'MovieID', 'Rating'))
    cur.close()
    file.close()
    pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    N=numberofpartitions
    if N<=0 or (str(N).isdigit() is False):
        print("numberofpartition "+str(N)+" must be a integer greater than 0")
        return
    # tables will use in this method
    TNAME=[1] * N
    MTNAMES="RP_Numofpart"

    partition=float(5)/numberofpartitions
    RangeValue=[1] * (N+1)
    RangeValue[N]=5
    con = openconnection
    cur = con.cursor()
    cur.execute("CREATE TABLE "+MTNAMES+" (TableName varchar(20), Min double precision);")
    inputRecord=""
    # create table for insert and calculate range
    for i in range(1,N+1):
        TNAME[i-1]="range_part"+str(i-1)
        RangeValue[i-1]=(5*i/float(N)-5/float(N))
        cur.execute("INSERT INTO "+MTNAMES+" (TableName, Min)VALUES(%s, %s)",(TNAME[i-1], RangeValue[i-1]))
    # create partition tables
    for i in range(1,N+1):
        Min=RangeValue[i-1]
        Max=RangeValue[i]
        sql="CREATE TABLE "+TNAME[i-1]+" AS SELECT * FROM "+ratingstablename+" WHERE RATING>"+str(Min)+" AND RATING<="+str(Max)+";"
        if Min==0:  sql="CREATE TABLE "+TNAME[i-1]+" AS SELECT * FROM "+ratingstablename+" WHERE RATING BETWEEN "+str(Min)+" AND "+str(Max)+";"
        cur.execute(sql)
    cur.close()
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    N=numberofpartitions
    if N<=0 or (str(N).isdigit() is False):
        print("numberofpartition "+str(N)+" must be a integer greater than 0")
        return

    # tables will use in this method
    TNAME=[1] * N
    MTNAMES="RR_Numofpart"

    #con = getopenconnection(dbname='test_dds_assgn1')
    con = openconnection
    cur = con.cursor()
    cur.execute("CREATE TABLE "+MTNAMES+" (TableName varchar(20), insertHere boolean, number integer);")
    # create tables
    for i in range(0,N):
        TNAME[i]="rrobin_part"+str(i)
        cur.execute("INSERT INTO "+MTNAMES+" (TableName, insertHere, number)VALUES(%s, %s, %s)",(TNAME[i], "false", i))
        sql="CREATE TABLE "+TNAME[i]+" AS SELECT USERID, MOVIEID, RATING FROM (SELECT *, Row_Number() over()rn FROM "+ratingstablename+")n WHERE (rn-1)%"+str(N)+"="+str(i)+";"
        cur.execute(sql)
    cur.execute("SELECT COUNT(*) FROM "+ratingstablename)
    insertLoc=cur.fetchone()[0]%N
    sql="UPDATE "+MTNAMES+" set insertHere=true where TableName='"+TNAME[insertLoc]+"';"
    cur.execute(sql)
    for i in range(1,N+1):
        IR=StringIO.StringIO(inputRecords[i-1])
        cur.copy_from(IR, TNAME[i-1], columns=('UserID', 'MovieID', 'Rating'))
    #con.commit()
    cur.close()
    #con.close()
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if userid<1 or itemid<1 or rating<0:
        print("inserted value is not legal")
        return
    MTNAMES="RR_Numofpart"
    #con = getopenconnection(dbname='test_dds_assgn1')
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT number FROM "+MTNAMES+" WHERE insertHere=true")
    records = cur.fetchall()
    insertLoc=records[0][0]
    inputRecord=str(userid)+"\t"+str(itemid)+"\t"+str(rating)+"\n"
    IR=StringIO.StringIO(inputRecord)
    cur.copy_from(IR, "rrobin_part"+str(insertLoc), columns=('UserID', 'MovieID', 'Rating'))
    sql="UPDATE "+MTNAMES+" set insertHere=false where number="+str(insertLoc)+";"
    cur.execute(sql)
    sql="UPDATE "+MTNAMES+" set insertHere=true where number="+str(insertLoc+1)+";"    
    cur.execute(sql)
    #con.commit()
    cur.close()
    #con.close()
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if userid<1 or itemid<1 or rating<0 or rating>5:
        print("inserted value is not legal")
        return
    MTNAMES="RP_Numofpart"
    #con = getopenconnection(dbname='test_dds_assgn1')
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from "+MTNAMES+";")
    records=cur.fetchall()
    N=records[0][0]
    myRange=""
    cur.execute("SELECT * FROM "+MTNAMES+" ORDER BY Min DESC;")
    records=cur.fetchall()
    notFound=True
    for record in records:
        if notFound and rating>record[1]:
            myRange=record[0]
            notFound=False
    inputRecord=str(userid)+"\t"+str(itemid)+"\t"+str(rating)+"\n"
    IR=StringIO.StringIO(inputRecord)
    cur.copy_from(IR, myRange, columns=('UserID', 'MovieID', 'Rating'))
    #con.commit()
    cur.close()
    #con.close()
    pass


def deletepartitionsandexit(openconnection):
    MTNAMES=[""]*2
    MTNAMES[0]="RP_Numofpart"
    MTNAMES[1]="RR_Numofpart"
    #con = getopenconnection(dbname='test_dds_assgn1')
    con = openconnection
    cur = con.cursor()
    for i in range(0,2):
        cur.execute("select * from "+MTNAMES[i]+";")
        records=cur.fetchall()
        for record in records:
            cur.execute("DROP TABLE IF EXISTS "+record[0])
        cur.execute("DROP TABLE IF EXISTS "+MTNAMES[i])
    cur.execute("DROP TABLE IF EXISTS RATINGS;")
    #con.commit()
    cur.close()
    #con.close()
    sys.exit(1)
    pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings','ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
