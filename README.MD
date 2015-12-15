================================================================================
=                Simple Analytics Material Creator for edX Data                =
================================================================================

1. The SAMCED SQLite database contains all the data in the following tables.

    Standard edX tables
    ------------------------------------------------------------------------
    + auth_userprofile
    + certificates_generatedcertificate
    + courseware_studentmodule
    + student_courseenrollment
    + student_languageproficiency
    + user_api_usercoursetag
    + user_id_map
    + wiki_article
    + wiki_articlerevision
  
    Non-standard tables
    ------------------------------------------------------------------------
    + discussion_forum_activity
        One row for each user, representing that users usage of the 
        discussion forum.
    + discussion_forum_activity_ng
        One row for each post with id, poster, thread and response_to.
    + pre_survey_answers
        One row for each user, representing that users answers on the pre
        survey questionnaire.
    + post_survey_answers
        One row for each user, representing that users answers on the post
        survey questionnaire.
    + video_activity
        One row for each user, representing that users interaction with
        the courses video material.
    + video_activity_ng
        One row for each unique combination of user and video interaction.
    + pre_roll_video_activity
        One row for each user, representing that users interaction with
        pre roll videos.
    + problem_activity
        One row for each user, representing that users interaction with the
        courses problems.
    + problem_<edx_problem_id>
        One table for each problem. One row for each user with some data 
        about that users interaction with that specific problem.
    + textbook_activity
        One row for each user, representing that users interaction with
        the textbooks in the course.
    + page_activity_xxx
        One row for each user and one column for each day, containing the 
        number of times that user accessed the page xxx.
        
2. Tips and tricks
Save a sqlite table to a CSV file.
    1. sqlite> .mode csv
    2. sqlite> .output test.csv
    3. sqlite> .schema tbl1
    4. sqlite> select * from tbl1;
    5. sqlite> .output stdout