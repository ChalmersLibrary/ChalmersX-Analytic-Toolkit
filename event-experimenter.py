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

filenamePrefix = ""

class EdxDataEventProcessor:
    def __init__(self):
        self.problems = {}

    '''
    Return true for the events which should be used and return false for the ones that we don't want to use.
    '''
    def filter(self, eventData):
        res = True
        return res
        
    '''
    Return true if we should run the preprocessing function in a first iteration.
    '''
    def shouldPreprocess(self):
        return False
    
    '''
    Collect data on a first iteration that we need for creating the real data on the second iteration.
    '''
    def preprocess(self, eventData):
        
        
    '''
    Return the csv header for the final data file.
    '''
    def csvHeaders(self):
        res = ""
        return res
    
    '''
    Return the csv row for the given data.
    '''
    def csvDataRow(self, eventData):
        res = ""
        return res
        
        
# Check if we have exactly one command line argument given.
if len(sys.argv) == 2:
    print "[1] Checking if the given edX event data folder is valid..."
    dataFolder = sys.argv[1]
    if os.path.isdir(dataFolder):
        print "[1] Folder is valid."
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
                            outputFile.write(eventProcessor.csvDataRow(jsonData) + "\n")
    else:
        print "[1] Folder is invalid."
else:
    print "Need to enter path to edX event data folder."