#!/usr/bin/python

# Code that other people have written that we want to use.
import sys
import os
import gzip
import json
import datetime

'''
--------------------------------------------------------------------------------
 edX Event Common Fields
--------------------------------------------------------------------------------
 + accept_language
 + agent
 + context
   + course_id
   + org_id
   + path
   + user_id
   + course_user_tags (not on all)
   + module (not on all)
 + event
 + event_source
 + event_type
 + host
 + ip
 + name
 + page
 + referer
 + session
 + time
 + username
 
See http://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs.html
for more fields that are not common to all events.
'''

filenamePrefix = "problem-44102822a04447e2b8d1bc127d69f188-"

class EdxDataEventProcessor:
    def __init__(self):
        self.problems = {}

    '''
    Return true for the events which should be used and return false for the ones that we don't want to use.
    '''
    def filter(self, eventData):
        res = eventData["event_source"] == "server"
        res = res and eventData["event_type"] == "problem_check"
        res = res and eventData["event"]["problem_id"] == "block-v1:ChalmersX+ChM002x+3T2016+type@problem+block@44102822a04447e2b8d1bc127d69f188"
        return res
        
    '''
    Return true if we should run the preprocessing function in a first iteration.
    '''
    def shouldPreprocess(self):
        return True
    
    '''
    Collect data on a first iteration that we need for creating the real data on the second iteration.
    '''
    def preprocess(self, eventData):
        for problemKey in eventData["event"]["answers"]:
            self.problems[problemKey] = 1
        
    '''
    Return the csv header for the final data file.
    '''
    def csvHeaders(self):
        res = "user_id,time,group"
        for problemKey in self.problems:
            res += "," + problemKey
        return res
    
    '''
    Return the csv row for the given data.
    '''
    def csvDataRow(self, eventData):
        res = str(eventData["context"]["user_id"]) + "," + eventData["time"] + "," + eventData["context"]["course_user_tags"]["xblock.partition_service.partition_1645604335"]
        for problemKey in self.problems:
            if problemKey in eventData["event"]["answers"]:
                if isinstance(eventData["event"]["answers"][problemKey], str) or isinstance(eventData["event"]["answers"][problemKey], unicode):
                    res += ',"' + eventData["event"]["answers"][problemKey].replace('"', '""') + '"'
                else:
                    res += ',"' + '","'.join(eventData["event"]["answers"][problemKey]) + '"'
            else:
                res += ","
        return res
        
        
print "[1] Checking if events folder is present..."
dataFolder = "events"
if os.path.isdir(dataFolder):
    print "[1] Folder is present."
    eventProcessor = EdxDataEventProcessor()
    if eventProcessor.shouldPreprocess():
        print "[2] Iterating over files in folder for preprocessing..."
        for filename in os.listdir(dataFolder):
            if filename.endswith(".log.gz"):
                print "[2] Preprocessing file: " + filename
                with gzip.open(os.path.join(dataFolder, filename)) as zippedLogFile:
                    for line in zippedLogFile:
                        jsonData = json.loads(line)
                        if eventProcessor.filter(jsonData):
                            eventProcessor.preprocess(jsonData)
    outputFile = open(filenamePrefix + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".csv", "w")
    outputFile.write(eventProcessor.csvHeaders() + "\n")
    print "[3] Iterating over files in folder for creating final data..."
    for filename in os.listdir(dataFolder):
        if filename.endswith(".log.gz"):
            print "[3] Processing file: " + filename
            with gzip.open(os.path.join(dataFolder, filename)) as zippedLogFile:
                for line in zippedLogFile:
                    jsonData = json.loads(line)
                    if eventProcessor.filter(jsonData):
                        outputFile.write(eventProcessor.csvDataRow(jsonData).encode("utf-8") + "\n")
else:
    print "[1] Events folder is missing. It should be in the same folder as the script."