#!/usr/bin/python

# Code that other people have written that we want to use.
import sys
import os
import gzip
import json
import datetime
import collections
import csv

class UserActivity:
    def __init__(self):
        self.name = ""
        self.email = ""
        self.eventIds = {}
        self.latestActivity = datetime.datetime(1970,1,1)

class DataProcessor:
    def __init__(self):
        self.userActivity = {}

    def formatName(self, firstName, lastName):
        firstNameStripped = firstName.strip()
        lastNameStripped = lastName.strip()
        
        res = ""
        
        if firstNameStripped == "" and lastNameStripped == "":
            res = "N/A"
        elif firstNameStripped == "":
            res = lastNameStripped
        elif lastNameStripped == "":
            res = firstNameStripped
        else:
            res = lastNameStripped + " " + firstNameStripped
            
        return res

dataProcessor = DataProcessor()
with open("Chalmers-BLT001-2016-auth_user-prod-edge-analytics.sql", newline="", encoding="utf-8") as csvfile:
    datareader = csv.reader(csvfile, delimiter='\t', quotechar='"')
    for userRecord in datareader:
        if userRecord[0] != "id":
            newUserActivity = UserActivity()
            newUserActivity.name = dataProcessor.formatName(userRecord[2], userRecord[3])
            newUserActivity.email = userRecord[4]
            dataProcessor.userActivity[userRecord[0]] = newUserActivity
        
dataFolder = "events"
if os.path.isdir(dataFolder):
    for filename in os.listdir(dataFolder):
        if filename.endswith(".log.gz"):
            with gzip.open(os.path.join(dataFolder, filename)) as zippedLogFile:
                for line in zippedLogFile:
                    jsonData = json.loads(str(line, "utf-8"))
                    userIdString = str(jsonData["context"]["user_id"])
                    
                    if userIdString in dataProcessor.userActivity:
                        if jsonData["event_type"] == "stop_video" or jsonData["event_type"] == "edx.video.stopped":
                            eventData = json.loads(jsonData["event"])
                            dataProcessor.userActivity[userIdString].eventIds[eventData["id"]] = 1;
                            activityTime = datetime.datetime.strptime(jsonData["time"].split('.')[0], "%Y-%m-%dT%H:%M:%S")
                            if dataProcessor.userActivity[userIdString].latestActivity < activityTime:
                                dataProcessor.userActivity[userIdString].latestActivity = activityTime
                    else:
                        print("Warning: Couldn't find user " + userIdString + " in user activity list.")
else:
    print("Events folder is missing. It should be in the same folder as the script.")
    
with open("user-activity.csv", "w", newline="") as csvfile:
    activityWriter = csv.writer(csvfile, delimiter=",",
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
    activityWriter.writerow(["name", "email", "watchedVideoCount", "latestActivity"])
    for key in dataProcessor.userActivity:
        activityWriter.writerow( 
            [dataProcessor.userActivity[key].name,
            dataProcessor.userActivity[key].email, 
            str(len(dataProcessor.userActivity[key].eventIds)),
            dataProcessor.userActivity[key].latestActivity.strftime("%Y-%m-%dT%H:%M:%S")])