#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    sql="SELECT tablename FROM pg_tables WHERE tablename ~ 'roundrobinratingspart\d+' ORDER BY tablename;"
    cursor = openconnection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    result=""
    for row in rows:
            sql="SELECT count(*) from "+row[0]+" WHERE rating>="+str(ratingMinValue)+" and rating<="+str(ratingMaxValue)
            cursor.execute(sql)
            num=cursor.fetchone()[0]
            if num!=0:
                    sql="SELECT * from "+row[0]+" WHERE rating>="+str(ratingMinValue)+" and rating<="+str(ratingMaxValue)
                    cursor = openconnection.cursor()
                    cursor.execute(sql)
                    records = cursor.fetchall()
                    rownum = row[0][16:]
                    for record in records:
                            result+="RoundRobinRatingsPart"+str(rownum)+","+str(record[0])+","+str(record[1])+","+str(record[2])+"\r\n"
    sql="SELECT tablename FROM pg_tables WHERE tablename ~ 'rangeratingspart\d+' ORDER BY tablename;"
    cursor = openconnection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
            sql="SELECT count(*) from "+row[0]+" WHERE rating>="+str(ratingMinValue)+" and rating<="+str(ratingMaxValue)
            cursor.execute(sql)
            num=cursor.fetchone()[0]
            if num!=0:
                    sql="SELECT * from "+row[0]+" WHERE rating>="+str(ratingMinValue)+" and rating<="+str(ratingMaxValue)
                    cursor = openconnection.cursor()
                    cursor.execute(sql)
                    records = cursor.fetchall()
                    rownum = row[0][16:]
                    for record in records:
                            result+="RangeRatingsPart"+str(rownum)+","+str(record[0])+","+str(record[1])+","+str(record[2])+"\r\n"
    f = open("RangeQueryOut.txt", "w")
    f.write(result)
    f.close()
    pass
def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    sql="SELECT tablename FROM pg_tables WHERE tablename ~ 'roundrobinratingspart\d+' ORDER BY tablename;"
    cursor = openconnection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    result=""
    for row in rows:
            sql="SELECT * from "+row[0]+" WHERE rating="+str(ratingValue)
            cursor = openconnection.cursor()
            cursor.execute(sql)
            records = cursor.fetchall()
            rownum = row[0][21:]
            for record in records:
                result+="RoundRobinRatingsPart"+str(rownum)+","+str(record[0])+","+str(record[1])+","+str(record[2])+"\r\n"
    sql="SELECT tablename FROM pg_tables WHERE tablename ~ 'rangeratingspart\d+' ORDER BY tablename;"
    cursor = openconnection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
            sql="SELECT * from "+row[0]+" WHERE rating="+str(ratingValue)
            cursor = openconnection.cursor()
            cursor.execute(sql)
            records = cursor.fetchall()
            rownum = row[0][21:]
            for record in records:
                result+="RangeRatingsPart"+str(rownum)+","+str(record[0])+","+str(record[1])+","+str(record[2])+"\r\n"
    f = open("PointQueryOut.txt", "w")
    f.write(result)
    f.close()
    pass
