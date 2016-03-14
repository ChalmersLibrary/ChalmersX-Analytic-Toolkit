#ChalmersX Analytic Toolkit
This is a collection of scripts, most of them written in Perl, which are used to fetch data from edX courses and prepare it for use in internal research and communication. The main idea is for an edX data czar to be able to easily extract data for a specific course from the great big pile of data that all the courses make up. The script also does some minimal anonymization of the data together with some basic aggregation from the big semi-structured event data.

The Perl scripts have some dependencies which will give errors if missing when you run them. Just install these dependencies and run the script again to solve it. The name of the dependencies should be given in the error message.

##The Scripts
The scripts expects the data to be present in the same directory as the script, in the following hierarchy.

    course-data
        |-- chalmersx-2015-08-23
        |-- chalmersx-2015-08-30
    edx-course-data
        |-- edge
        |-- edx
             |-- events
                   |-- 2015

The sync-edx-data.bat script below fetches the data with this hierarchy. The course data should be unpacked and decrypted. The event data should only be decrypted.

###sync-edx-data.bat
Fetches data from Amazon Simple Storage Service where edX are hosting the data. Also backups the current folder to a given location. Takes the organization as first argument and the backup location as second argument from the command line. Need to have configured the S3 buckets and authenticated before running this script.

###get-data-for-course-run.pl
Extracts the data for a given course run.

###create-survey-data.pl
Gathers pre and post survey data given the output data from get-data-for-course-run.pl.

###create-improved-video-data.pl
Creates improved video data given the output data from get-data-for-course-run.pl.

###create-improved-forum-data.pl
Creates improved forum data given the output data from get-data-for-course-run.pl.

###create-improved-email-opt-in-data.pl
Exports a CSV file with mail, name, gender, year of birth and country for all the students which have chosen the opt in alternative in a given course.

###create-analytics-material.pl
Old way of creating the analytics material. Now replaced with the workflow originating in get-data-for-course-run.pl.

Takes organization, course id, course run and a timespan as input, not from the command line; processes the latest course data and all the event data for that timespan; and finally tries to output all the data that are needed for a researcher to be able to investigate a specific course. Currently only fetches event data from the edx folder. The script produces an output directory with many files, most of them are described below.
 
 * **info.txt**: Information about the script run.
 * **[organization]-[course id]-[course run]-[table name or equivalent].[sql|xml.tar.gz|json]**: Data files that are simply copied from the course-data folder.
 * **[organization]-[course id]-[course run]-SAMCED.db**: SAMCED stands for Simple Analytics Material Creator for edX Data and is an SQLite database containing tables imported straight from the .sql files and also tables with aggregated data compiled mainly from the event data.
 * **SAMCED-[table name].csv**: All the tables in the SAMCED SQLite database exported as CSV files for easier processing with other tools and programs.

###get-events-for-user.pl
Currently gets all the video events for a specific user and prints them to a CSV file. This script is mostly used to check up on weird inconsistent data that have been encountered in the SAMCED aggregates.

##The SAMCED Data
The SAMCED SQLite database contains the following tables.

###Standard edX Tables
  * auth_userprofile
  * certificates_generatedcertificate
  * courseware_studentmodule
  * student_courseenrollment
  * student_languageproficiency
  * user_api_usercoursetag
  * wiki_article
  * wiki_articlerevision
  
###Non-standard Tables
  * discussion_forum_activity: One row for each user, representing that users usage of the discussion forum.
  * discussion_forum_activity_ng: One row for each post with id, poster, thread and response_to.
  * pre_survey_answers: One row for each user, representing that users answers on the pre survey questionnaire. Requires the pre survey to have been a problem with specific display name.
  * post_survey_answers: One row for each user, representing that users answers on the post survey questionnaire. Required the post survey to have been a problem with specific display name.
  * video_activity: One row for each user, representing that users interaction with the courses video material.
  * video_activity_ng: One row for each unique combination of user and video interaction.
  * pre_roll_video_activity: One row for each user, representing that users interaction with pre roll videos.
  * problem_activity: One row for each user, representing that users interaction with the courses problems.
  * problem_<edx_problem_id>: One table for each problem. One row for each user with some data about that users interaction with that specific problem.
  * textbook_activity: One row for each user, representing that users interaction with the textbooks in the course.
  * page_activity_xxx: One row for each user and one column for each day, containing the number of times that user accessed the page xxx.