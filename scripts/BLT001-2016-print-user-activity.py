#!/usr/bin/python

# Code that other people have written that we want to use.
import sys
import os
import gzip
import json
import datetime
import collections
import csv
        
outFile = open("user-activity.txt", "w")        

dataFolder = "events"
if os.path.isdir(dataFolder):
    for filename in os.listdir(dataFolder):
        if filename.endswith(".log.gz"):
            with gzip.open(os.path.join(dataFolder, filename)) as zippedLogFile:
                for line in zippedLogFile:
                    jsonData = json.loads(str(line, "utf-8"))
                    userIdString = str(jsonData["context"]["user_id"])
                    
                    if userIdString == "[INSERT SOME ID HERE]":
                        outFile.write(json.dumps(jsonData) + "\n")