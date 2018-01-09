#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: Yidi Wang
#
# coding=utf-8

import codecs
from pymongo import MongoClient
import os
import sys
import json
import math
import re

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    result = []
    rexExp = re.compile("^"+cityToSearch+"$", re.IGNORECASE)
    for x in collection.find({"city":rexExp},["name","full_address","city","state"]):
        content = x["name"] + "$" + x["full_address"] + "$" + x["city"] + "$" + x["state"]
        content = content.replace('\n',',')
        result.append(content)
    openFile = codecs.open(saveLocation1, "w", "utf-8")
    for row in result:
        openFile.write(row.upper() + '\r\n')
    openFile.close

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    result = []
    R = 3959
    Phi1 = math.radians(float(myLocation[0]))
    for x in collection.find({"categories":{"$in":categoriesToSearch}}):
        Phi2 = math.radians(float(x["latitude"]))
        Phi = math.radians(float(x["latitude"]) - float(myLocation[0]))
        Lambda = math.radians(float(x["longitude"]) - float(myLocation[1]))
        a = math.sin(Phi/2) * math.sin(Phi/2) + math.cos(Phi1) * math.cos(Phi2) * math.sin(Lambda/2) * math.sin(Lambda/2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a));
        d = R * c
        if d < maxDistance:
            content = x["name"]
            result.append(content)
    openFile = codecs.open(saveLocation2, "w", "utf-8")
    for row in result:
        openFile.write(row.upper() + '\r\n')
    openFile.close
