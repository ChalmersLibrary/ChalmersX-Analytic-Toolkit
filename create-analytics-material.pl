#!/usr/bin/perl
#use strict;
#use warnings;

my %file_whitelist = ( "course_structure-prod-analytics.json"           => "OK",
                    "course-prod-analytics.xml.tar.gz"                  => "OK",
                    "courseware_studentmodule-prod-analytics.sql"       => "OK",
                    "student_courseenrollment-prod-analytics.sql"       => "OK",
                    "student_languageproficiency-prod-analytics.sql"    => "OK",
                    "user_api_usercoursetag-prod-analytics.sql"         => "OK",
                    "wiki_article-prod-analytics.sql"                   => "OK",
                    "wiki_articlerevision-prod-analytics.sql"           => "OK" );
                    
my %table_whitelist = ("auth_userprofile"               => "OK",
                    "courseware_studentmodule"          => "OK",
                    "student_courseenrollment"          => "OK",
                    "student_languageproficiency"       => "OK",
                    "user_api_usercoursetag"            => "OK",
                    "wiki_article"                      => "OK",
                    "wiki_articlerevision"              => "OK",
                    "certificates_generatedcertificate" => "OK");

# id, user_id, name, language, location, meta, courseware, gender, mailing_address, year_of_birth, level_of_education, goals, allow_certificate, country, city, bio, profile_image_uploaded_at
my @auth_userprofile_allowed_rows = (1, 1, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0);

# id, user_id, download_url, grade, course_id, key, distinction, status, verify_uuid, download_uuid, name, created_date, modified_date, error_reason, mode
my @certificates_generatedcertificate_allowed_rows = (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1);

use DBI;
use IO::Zlib;
use Date::Parse;
use Date::Format;
use Date::Calc      qw( Delta_Days Time_to_Date Add_Delta_Days );
use Cwd             qw( abs_path );
use File::Basename  qw( dirname );
use JSON::XS;
use File::Copy;
use DateTime;

# Auto flush the default buffer.
$| = 1;

# Get the current working directory.
my $cwd = dirname(abs_path($0));

# Gather the information that we need.
print "================================================================================\n";
print "=                Simple Analytics Material Creator for edX Data                =\n";
print "================================================================================\n";
print "Please enter organization.\n> ";
my $organization = <>;
chomp $organization;
print "Thank you!\n\nNow, please enter course number.\n> ";
my $course_id = <>;
chomp $course_id;
print "How nice of you!\n\nNow, please enter course run.\n> ";
my $course_run = <>;
chomp $course_run;
print "Thank you very much!\n\nNow I need a start and end date for the time window where we should gather data.\n";
print "Start date (YYYYMMDD): ";
my $start_date_string = <>;
chomp $start_date_string;
my $start_date = str2time($start_date_string) or die "Couldn't parse date.";
print "End date (YYYYMMDD): ";
my $end_date_string = <>;
chomp $end_date_string;
my $end_date = str2time($end_date_string) or die "Couldn't parse date.";
print "Splendid!\n\nI am now ready to create analytics material for course run $organization/$course_id/$course_run.\nStarting at $start_date_string and ending at $end_date_string.\n\n";
print "How do you want to continue?\n";
print "------------------------------------\n";
print "Process only course data files.  (C)\n";
print "Process only event data files.   (E)\n";
print "Process all data.                (A)\n";
print "Exit program.                    (X)\n";
print "------------------------------------\n";
my $input = "";
while (!($input =~ m/^[CEAX]$/i)) {
    print " (C/E/A/X)\n> ";
    $input = <>;
    $input =~ s/\r?\n//;
    
    if ($input =~ m/^[^CEAX]?$/i) {
        print "\nUnknown command, please enter a letter from the table above.\n\n";
    }
}

# Check if something exists with the same name as our destination directory.
my $dest_dir = "$organization-$course_id-$course_run-" . time2str('%y%m%d', $start_date) . "-" . time2str('%y%m%d', $end_date) . "-" . time2str('%Y%m%d%H%M%S',time);
if (-d $dest_dir) {
    print "\nDirectory '$dest_dir' already exists. Exiting...\n";
    $input = "X";
}
elsif (-e $dest_dir) {
    print "\nFile with the same name as the destination directory '$dest_dir' already exists. Exiting...\n";
    $input = "X";
}


my %event_database_meta = ();
my %event_database = ();

my %student_activity_data = ();

my %table_file_streams = ();

if ($input =~ m/^[CEA]$/i) {
    mkdir $dest_dir;

    my $dbh = DBI->connect("dbi:SQLite:dbname=$dest_dir/$organization-$course_id-$course_run-SAMCED.db","","") 
        or die "Failed to create db";

    if ($input =~ m/^[CA]$/i) { # Process course data
        #Figure out which directory we should fetch our course data from
        my @course_data_dirs = grep { -d } glob $cwd . '/course-data/*';
        
        my $course_snapshot_dir = "";
        my $course_snapshot_date = undef;
        
        foreach (@course_data_dirs) {
            if (/(\d{4}-\d{2}-\d{2})$/) {
                my $potential_new_course_snapshot_date = str2time($1);
                if (is_better_snapshot_date($course_snapshot_date, $potential_new_course_snapshot_date, $end_date)) { # Use the course snapshot that is closest after course run end date.
                    $course_snapshot_dir = $_ . "/" . lc $organization . "-" . time2str('%Y-%m-%d', $potential_new_course_snapshot_date);
                    $course_snapshot_date = $potential_new_course_snapshot_date;
                }
            }
        }
        
        print "\nWill fetch data from course snapshot in the following directory.\n$course_snapshot_dir.\n\nIs this correct? (Y/N)\n";
        my $second_input = <>;
        $second_input =~ s/\r?\n//;
        if ($second_input =~ m/^[Y]$/i) {
            my @course_data_files = grep { -f } glob $course_snapshot_dir . '/*';
            foreach (@course_data_files) {
                # Copy to destination directory if file in whitelist.
                if (/$organization-$course_id-$course_run-(.*)$/) {
                    if ($file_whitelist{$1}) {
                        copy($_, $dest_dir);
                    }
                }
          
                if (/$organization-$course_id-$course_run-(.*)-prod-analytics\.sql$/) {
                    if ($table_whitelist{ $1 }) {
                        # Create SQL tables from all SQL files.
                        print "\nStoring data for '$1' in database...  ";
                        open(my $fh, '<:encoding(UTF-8)', $_)
                            or die "Could not open file '$_' $!\n";
                      
                        my $line_count = 0;
                        my $col_count = 0;
                        while (my $row = <$fh>) {
                            chomp $row;
                            
                            if ($line_count == 0) {
                                # First row of the sql file, time to create a table to hold the data.
                                my $create_table_sql = "CREATE TABLE $1(";
                                my @column_names = split(/\t/, $row);
                                $col_count = 0;
                                foreach (@column_names) {
                                    if (is_column_allowed($1, $col_count)) {
                                        $create_table_sql .= $_ . " TEXT,";
                                    }
                                    $col_count++;
                                }
                                chop($create_table_sql);
                                $create_table_sql .= ");";
                                exec_query($dbh, $create_table_sql);
                            }
                            
                            # Time to insert the data into our newly created table.
                            my $insert_data_sql = "INSERT INTO $1 VALUES (";
                            my @column_data = split(/\t/, $row);
                            $col_count = 0;
                            foreach (@column_data) {
                                if (is_column_allowed($1, $col_count)) {
                                    my $fixed_data = $_;
                                    $fixed_data =~ tr/"/'/;
                                    $fixed_data =~ tr/\r?\n/ /;
                                    $insert_data_sql .= "\"$fixed_data\",";
                                }
                                $col_count++;
                            }
                            chop($insert_data_sql);
                            $insert_data_sql .= ");";
                            exec_query($dbh, $insert_data_sql);
                            
                            $line_count++;
                        }
                        print "Done.";
                    }
                }
               
                if (/($organization-$course_id-$course_run-prod\.mongo)$/) {
                    # Process forum data
                    print "\nProcessing forum data from '$1'...  ";
                    open(my $fh, '<:bytes', $_)
                        or die "Could not open file '$_' $!\n";
                        
                    my @forum_posts = ();
                    my %forum_message_id_mapping = ();
                  
                    while (my $row = <$fh>) {
                        chomp $row;
                        my $forum_post = JSON::XS->new->utf8->decode($row);
                        my $user_id = $forum_post->{ 'author_id' };
                        my $forum_post_type = $forum_post->{ '_type' };
                        
                        $student_activity_data{ "discussion_forum_activity_keys" }{ $forum_post_type } = 1;
                        
                        $student_activity_data{ $user_id }{ "discussion_forum_activity" }{ $forum_post_type }++;
                        
                        push @forum_posts, $forum_post;
                    }
                    
                    # Build table with id, poster, thread and response-to.
                    my @forum_posts_ordered_by_created_asc = sort { $a->{ "created_at" }->{ '$date' } cmp $b->{ "created_at" }->{ '$date' } } @forum_posts;
                    
                    create_table($dbh, "discussion_forum_activity_ng", {
                        "id" => "TEXT",
                        "poster" => "TEXT",
                        "thread" => "TEXT",
                        "response_to" => "TEXT"
                    });
                    
                    my $line_count = 0;
                    foreach my $forum_post (@forum_posts_ordered_by_created_asc) {
                        my $fixed_post_id = $line_count + 1;
                        my $thread_id = $forum_post->{ "comment_thread_id" }->{ '$oid' };
                        $forum_message_id_mapping{ $forum_post->{ "_id" }->{ '$oid' } } = $fixed_post_id;
                        
                        insert_row_into_table($dbh, "discussion_forum_activity_ng", { 
                            "id" => "\"" . $fixed_post_id . "\"",
                            "poster" => "\"" . $forum_post->{ "author_id" } . "\"",
                            "thread" => "\"" . $forum_message_id_mapping{ $thread_id ? $thread_id : $forum_post->{ "_id" }->{ '$oid' } } . "\"",
                            "response_to" => "\"" . $forum_message_id_mapping{ $forum_post->{ "parent_id" }->{ '$oid' } } . "\""
                        });
                            
                        $line_count++;
                    }
                    
                    # Build old discussion forum table with aggregated data.
                    create_std_user_table_from_table_names_stored_as_keys($dbh, "discussion_forum_activity", "INTEGER", %{ $student_activity_data{ "discussion_forum_activity_keys" } });
                    
                    keys %student_activity_data;
                    while (my($student_id, $activity_data) = each %student_activity_data) {
                        insert_student_activity_data_into_table($dbh, $student_id, $activity_data, "discussion_forum_activity");
                    }
                    
                    print "Done.";
                }
            }
        } else {
            print "Then I can't process course data :(\n\n";
        }
        
        print "\n\n";
    }
    
    if ($input =~ m/^[EA]$/i) { # Process event data
        # Process the event data and save aggregates and particularly interesting data in temporary variables.
        my %problem_tables = ();
        my $iter_year = (Time_to_Date($start_date))[0];
        my $end_year = (Time_to_Date($end_date))[0];
        while ($iter_year <= $end_year) {
            my $course_event_dir = $cwd . '/edx-course-data/edx/events/' . $iter_year;
            my @course_event_data_files = grep { -f } glob $cwd . '/edx-course-data/edx/events/' . $iter_year . '/*';
            foreach (@course_event_data_files) {
                if (/.*([0-9]{4}-[0-9]{2}-[0-9]{2})\.log\.gz$/) {
                    my $event_date = str2time($1);
                    
                    if ($event_date >= $start_date && $event_date <= $end_date) {
                        print "Processing event file for date $1...\n";
                        my $gzfh = new IO::Zlib;
                        $gzfh->open($_, "rb")
                            or die "Could not open file '$_' $!\n";
                      
                        my $events_belonging_to_course = 0;
                        my $events_not_belonging_to_course = 0;
                        while (my $row = <$gzfh>) {
                            chomp $row;
                            my $event = JSON::XS->new->utf8->decode($row);
                            
                            my $event_user_id = $event->{ "context" }->{ "user_id" };
                            my $event_course_id = $event->{ "context" }->{ "course_id" };
                            my $event_event_type = $event->{ "event_type" };
                            my $event_time = $event->{ "time" };
                            my $event_date = "UNKNOWN";
                            if ($event_time =~ /^([0-9]+-[0-9]+-[0-9]+)T/) {
                                $event_date = $1;
                                $event_date =~ s/-//g;
                            }
                            my $event_source = $event->{ "event_source" };
                            
                            if (index($event_course_id, $organization) != -1 && index($event_course_id, $course_id) != -1 && index($event_course_id, $course_run) != -1) {
                                if ($event_event_type eq "problem_check") {
                                    my $problem_display_name = $event->{ "context" }->{ "module" }->{ "display_name" };
                                    my $event_event_source = $event->{ "event_source" };

                                    # Pre-course survey answers
                                    if ($problem_display_name eq "Pre-course survey" && $event_event_source eq "server") {
                                        if (!$student_activity_data{ $event_user_id }{ "pre_survey_answers" }{ "time" } ||
                                            str2time($event_time) > str2time($student_activity_data{ $event_user_id }{ "pre_survey_answers" }{ "time" })) 
                                        {
                                            $student_activity_data{ "pre_survey_answers_keys" }{ "time" } = 1;
                                            $student_activity_data{ $event_user_id }{ "pre_survey_answers" }{ "time" } = "\"" . $event_time . "\"";
                                            foreach my $raw_problem_name (keys %{ $event->{ "event" }->{ "answers" } }) {
                                                if ($raw_problem_name =~ /(_[0-9]+_[0-9]+)$/) {
                                                    my $problem_name = "problem$1";
                                                    $student_activity_data{ "pre_survey_answers_keys" }{ $problem_name } = 1;

                                                    my $answer_value = $event->{ "event" }->{ "answers" }{ $raw_problem_name };

                                                    # Special handling for arrays (multiple answer questions)
                                                    if(ref($answer_value) eq "ARRAY") {
                                                        $student_activity_data{ $event_user_id }{ "pre_survey_answers" }{ $problem_name } = "\"" . join(',',@{ $answer_value }) . "\"";
                                                    } else {
                                                        $answer_value =~ tr/"/'/;
                                                        $answer_value =~ tr/\r?\n/ /;
                                                        $student_activity_data{ $event_user_id }{ "pre_survey_answers" }{ $problem_name } = "\"" . $answer_value . "\"";
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    # Post-course survey answers
                                    if ($problem_display_name eq "Post-survey" && $event_event_source eq "server") {
                                        if (!$student_activity_data{ $event_user_id }{ "post_survey_answers" }{ "time" } ||
                                            str2time($event_time) > str2time($student_activity_data{ $event_user_id }{ "post_survey_answers" }{ "time" })) 
                                        {
                                            $student_activity_data{ "post_survey_answers_keys" }{ "time" } = 1;
                                            $student_activity_data{ $event_user_id }{ "post_survey_answers" }{ "time" } = "\"" . $event_time . "\"";
                                            foreach my $raw_problem_name (keys %{ $event->{ "event" }->{ "answers" } }) {
                                                if ($raw_problem_name =~ /(_[0-9]+_[0-9]+)$/) {
                                                    my $problem_name = "problem$1";
                                                    $student_activity_data{ "post_survey_answers_keys" }{ $problem_name } = 1;

                                                    my $answer_value = $event->{ "event" }->{ "answers" }{ $raw_problem_name };

                                                    # Special handling for arrays (multiple answer questions)
                                                    if(ref($answer_value) eq "ARRAY") {
                                                        $student_activity_data{ $event_user_id }{ "post_survey_answers" }{ $problem_name } = "\"" . join(',',@{ $answer_value }) . "\"";
                                                    } else {
                                                        $answer_value =~ tr/"/'/;
                                                        $answer_value =~ tr/\r?\n/ /;
                                                        $student_activity_data{ $event_user_id }{ "post_survey_answers" }{ $problem_name } = "\"" . $answer_value . "\"";
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    # Data for specific problem checks
                                    if ($event_event_source eq "server") {
                                        my $table_id = $event->{ "event" }->{ "problem_id" };
                                        $table_id =~ s/^i4x:\/\/$organization\/$course_id\/problem\///;
                                        $table_id = "problem_$table_id";
                                        my $problem_attempts = $event->{ "event" }->{ "attempts" };
                                        my $problem_success = $event->{ "event" }->{ "success" };
                                        my $problem_grade = $event->{ "event" }->{ "grade" };
                                        my $problem_max_grade = $event->{ "event" }->{ "max_grade" };
                                        
                                        if (!$student_activity_data{ $event_user_id }{ $table_id }{ "time" } || 
                                            str2time($event_time) > str2time($student_activity_data{ $event_user_id }{ $table_id }{ "time" })) 
                                        {
                                            $problem_tables{ $table_id } = 1;

                                            $student_activity_data{ $event_user_id }{ $table_id }{ "attempts" } = $event->{ "event" }->{ "attempts" };
                                            $student_activity_data{ $event_user_id }{ $table_id }{ "success" } = "\"" . $event->{ "event" }->{ "success" } . "\"";
                                            $student_activity_data{ $event_user_id }{ $table_id }{ "grade" } = $event->{ "event" }->{ "grade" };
                                            $student_activity_data{ $event_user_id }{ $table_id }{ "max_grade" } = $event->{ "event" }->{ "max_grade" };
                                            $student_activity_data{ $event_user_id }{ $table_id }{ "time" } = "\"" . $event_time . "\"";
                                        }
                                    }
                                }
                                
                                # Video interaction events.
                                if ($event_event_type eq "hide_transcript" || $event_event_type eq "load_video" || $event_event_type eq "pause_video" || 
                                    $event_event_type eq "play_video" || $event_event_type eq "seek_video" || $event_event_type eq "show_transcript" ||
                                    $event_event_type eq "speed_change_video" || $event_event_type eq "stop_video" || $event_event_type eq "video_hide_cc_menu" ||
                                    $event_event_type eq "video_show_cc_menu")
                                {
                                    $student_activity_data{ "video_activity_keys" }{ $event_event_type } = 1;
                                    $student_activity_data{ $event_user_id }{ "video_activity" }{ $event_event_type }++;
                                    
                                    my $video_event_event = $event->{ "event" };                               
                                    if ($video_event_event && ref($video_event_event) ne "HASH") {
                                        $video_event_event = JSON::XS->new->utf8->decode($video_event_event);
                                    }
                                    
                                    $event_database_meta{ "video_activity_ng" }{ "user_id" } = "TEXT";
                                    $event_database_meta{ "video_activity_ng" }{ "video_event_id" } = "TEXT";
                                    $event_database_meta{ "video_activity_ng" }{ $event_event_type } = "INTEGER";
                                    $event_database{ "video_activity_ng" }{ $event_user_id . ":" . $video_event_event->{ "id" }}{ "user_id" } = "\"" . $event_user_id . "\"";
                                    $event_database{ "video_activity_ng" }{ $event_user_id . ":" . $video_event_event->{ "id" }}{ "video_event_id" } = "\"" . $video_event_event->{ "id" } . "\"";
                                    $event_database{ "video_activity_ng" }{ $event_user_id . ":" . $video_event_event->{ "id" }}{ $event_event_type }++;
                                }
                                
                                # Problem interaction events.
                                if ($event_event_type eq "edx.problem.hint.demandhint_displayed" || $event_event_type eq "edx.problem.hint.feedback_displayed" ||
                                    $event_event_type eq "problem_check" || $event_event_type eq "problem_check_fail" || $event_event_type eq "problem_graded" || 
                                    $event_event_type eq "problem_rescore" || $event_event_type eq "problem_rescore_fail" || $event_event_type eq "problem_reset" || 
                                    $event_event_type eq "problem_save" || $event_event_type eq "problem_show" || $event_event_type eq "reset_problem" || 
                                    $event_event_type eq "reset_problem_fail" || $event_event_type eq "save_problem_fail" || $event_event_type eq "save_problem_success" || 
                                    $event_event_type eq "show_answer") 
                                {
                                    my $event_event_type_copy = $event_event_type;
                                
                                    if ($event_event_type eq "problem_check") {
                                        $event_event_type_copy = $event_event_type . "_" . $event->{ "event_source" };
                                    }
                                    
                                    $student_activity_data{ "problem_activity_keys" }{ $event_event_type_copy } = 1;
                                    $student_activity_data{ $event_user_id }{ "problem_activity" }{ $event_event_type_copy }++;
                                }
                                
                                # Pre-roll video interaction events
                                if ($event_event_type eq "edx.video.bumper.dismissed" || $event_event_type eq "edx.video.bumper.loaded" || $event_event_type eq "edx.video.bumper.played" || 
                                    $event_event_type eq "edx.video.bumper.played" || $event_event_type eq "edx.video.bumper.skipped" || $event_event_type eq "edx.video.bumper.stopped" || 
                                    $event_event_type eq "edx.video.bumper.transcript.hidden" || $event_event_type eq "edx.video.bumper.transcript.menu.hidden" || 
                                    $event_event_type eq "edx.video.bumper.transcript.menu.shown" || $event_event_type eq "edx.video.bumper.transcript.shown") 
                                {
                                    $student_activity_data{ "pre_roll_video_activity_keys" }{ $event_event_type } = 1;
                                    $student_activity_data{ $event_user_id }{ "pre_roll_video_activity" }{ $event_event_type }++;
                                }
                                
                                # Textbook interaction events
                                if ($event_event_type eq "book" || $event_event_type eq "textbook.pdf.thumbnails.toggled" || $event_event_type eq "textbook.pdf.thumbnail.navigated" || 
                                    $event_event_type eq "textbook.pdf.outline.toggled" || $event_event_type eq "textbook.pdf.chapter.navigated" || $event_event_type eq "textbook.pdf.page.navigated" || 
                                    $event_event_type eq "textbook.pdf.zoom.buttons.changed" || $event_event_type eq "textbook.pdf.zoom.menu.changed" || 
                                    $event_event_type eq "textbook.pdf.display.scaled" || $event_event_type eq "textbook.pdf.display.scrolled" ||
                                    $event_event_type eq "textbook.pdf.search.executed" || $event_event_type eq "textbook.pdf.search.navigatednext" ||
                                    $event_event_type eq "textbook.pdf.search.highlight.toggled" || $event_event_type eq "textbook.pdf.search.casesensitivity.toggled") 
                                {
                                    $student_activity_data{ "textbook_activity_keys" }{ $event_event_type } = 1;
                                    $student_activity_data{ $event_user_id }{ "textbook_activity" }{ $event_event_type }++;
                                }
                                
                                # Web page activity
                                if ($event_event_type =~ /\/progress$/) {
                                    $event_database{ "page_activity_progress" }{ $event_user_id }{ "user_id" } = "\"" . $event_user_id . "\"";
                                    $event_database{ "page_activity_progress" }{ $event_user_id }{ "date_" . $event_date }++;
                                }
                                if ($event_event_type =~ /\/info$/) {
                                    $event_database{ "page_activity_info" }{ $event_user_id }{ "user_id" } = "\"" . $event_user_id . "\"";
                                    $event_database{ "page_activity_info" }{ $event_user_id }{ "date_" . $event_date }++;
                                }
                                if ($event_event_type =~ /\/courseware$/) {
                                    $event_database{ "page_activity_courseware" }{ $event_user_id }{ "user_id" } = "\"" . $event_user_id . "\"";
                                    $event_database{ "page_activity_courseware" }{ $event_user_id }{ "date_" . $event_date }++;
                                }
                                if ($event_event_type =~ /\/discussion\/forum$/) {
                                    $event_database{ "page_activity_discussion" }{ $event_user_id }{ "user_id" } = "\"" . $event_user_id . "\"";
                                    $event_database{ "page_activity_discussion" }{ $event_user_id }{ "date_" . $event_date }++;
                                }

                                $events_belonging_to_course++;
                            } else {
                                $events_not_belonging_to_course++;
                            }
                        }
                        
                        print "Found $events_belonging_to_course events that belongs to course.\nDiscarded $events_not_belonging_to_course events that does not belong to this course.\n\n";
                        $gzfh->close();
                    }
                }
            }
            $iter_year += 1;
        }

        # Column names for tables 'page_activity_X'.
        $event_database_meta{ "page_activity_progress" }{ "user_id" } = "TEXT";
        $event_database_meta{ "page_activity_info" }{ "user_id" } = "TEXT";
        $event_database_meta{ "page_activity_courseware" }{ "user_id" } = "TEXT";
        $event_database_meta{ "page_activity_discussion" }{ "user_id" } = "TEXT";
        my $page_activity_iter_date = DateTime->new(
            day => (Time_to_Date($start_date))[2],
            month => (Time_to_Date($start_date))[1],
            year => (Time_to_Date($start_date))[0],
        );
        my $page_activity_end_date = DateTime->new(
            day => (Time_to_Date($end_date))[2],
            month => (Time_to_Date($end_date))[1],
            year => (Time_to_Date($end_date))[0],
        );
        while ($page_activity_iter_date <= $page_activity_end_date) {
            $page_activity_iter_date->add(days => 1);
            my $iter_date_str = "date_" . $page_activity_iter_date->ymd('');
            $event_database_meta{ "page_activity_progress" }{ $iter_date_str } = "INTEGER";
            $event_database_meta{ "page_activity_info" }{ $iter_date_str } = "INTEGER";
            $event_database_meta{ "page_activity_courseware" }{ $iter_date_str } = "INTEGER";
            $event_database_meta{ "page_activity_discussion" }{ $iter_date_str } = "INTEGER";
        }

        # Finish up data for specific problem checks
        foreach my $problem_table_id (keys %problem_tables) {
            $student_activity_data{ $problem_table_id . "_keys" }{ "attempts" } = 1;
            $student_activity_data{ $problem_table_id . "_keys" }{ "TEXT:success" } = 1;
            $student_activity_data{ $problem_table_id . "_keys" }{ "grade" } = 1;
            $student_activity_data{ $problem_table_id . "_keys" }{ "max_grade" } = 1;
            $student_activity_data{ $problem_table_id . "_keys" }{ "TEXT:time" } = 1;
        }

        # Create all needed tables and populate them with our aggregated and particularly interesting data.
        print "Storing aggregated and particulary interesting data that has been fetched from event files...\n\n";
        create_std_user_table_from_table_names_stored_as_keys($dbh, "pre_survey_answers", "TEXT", %{ $student_activity_data{ "pre_survey_answers_keys" } });
        create_std_user_table_from_table_names_stored_as_keys($dbh, "post_survey_answers", "TEXT", %{ $student_activity_data{ "post_survey_answers_keys" } });
        create_std_user_table_from_table_names_stored_as_keys($dbh, "video_activity", "INTEGER", %{ $student_activity_data{ "video_activity_keys" } });
        create_std_user_table_from_table_names_stored_as_keys($dbh, "problem_activity", "INTEGER", %{ $student_activity_data{ "problem_activity_keys" } });
        create_std_user_table_from_table_names_stored_as_keys($dbh, "pre_roll_video_activity", "INTEGER", %{ $student_activity_data{ "pre_roll_video_activity_keys" } });
        create_std_user_table_from_table_names_stored_as_keys($dbh, "textbook_activity", "INTEGER", %{ $student_activity_data{ "textbook_activity_keys" } });
        
        foreach my $problem_table_id (keys %problem_tables) {
            create_std_user_table_from_table_names_stored_as_keys($dbh, $problem_table_id, "INTEGER", %{ $student_activity_data{ $problem_table_id . "_keys" } });
        }

        keys %student_activity_data;
        while (my($student_id, $activity_data) = each %student_activity_data) {
            insert_student_activity_data_into_table($dbh, $student_id, $activity_data, "pre_survey_answers");
            insert_student_activity_data_into_table($dbh, $student_id, $activity_data, "post_survey_answers");
            insert_student_activity_data_into_table($dbh, $student_id, $activity_data, "video_activity");
            insert_student_activity_data_into_table($dbh, $student_id, $activity_data, "problem_activity");
            insert_student_activity_data_into_table($dbh, $student_id, $activity_data, "pre_roll_video_activity");
            insert_student_activity_data_into_table($dbh, $student_id, $activity_data, "textbook_activity");
            
            foreach my $problem_table_id (keys %problem_tables) {
                insert_student_activity_data_into_table($dbh, $student_id, $activity_data, $problem_table_id);
            }
        }   
        
        # Next gen event processing
        keys %event_database_meta;
        while (my($table_id, $table_metadata) = each %event_database_meta) {
            create_table($dbh, $table_id, $table_metadata);
        }
        
        keys %event_database;
        while (my($table_id, $table_data) = each %event_database) {
            foreach my $row_data (values %{ $table_data }) {
                insert_row_into_table($dbh, $table_id, $row_data);
            }
        }
    }
    
    print "Done with everything!\n\nExiting\n";
} else {
    print "Exiting without creating analytics material.\n";
}

sub is_better_snapshot_date {
    my ($current_date, $potential_date, $goal_date) = @_;
    my $res = 0;
    
    if (!defined($current_date)) {
        $res = 1;
    } else {
        my $cd_gd_delta = $goal_date - $current_date; #Delta_Days($current_date, $goal_date);
        my $cd_pd_delta = $potential_date - $current_date; #Delta_Days($current_date, $potential_date);
        my $pd_gd_delta = $goal_date - $potential_date; #Delta_Days($potential_date, $goal_date);
        
        if ($cd_gd_delta > 0) { # current date is smaller than goal date
            $res = $cd_pd_delta > 0 || abs($pd_gd_delta) < abs($cd_gd_delta);
        } elsif ($cd_gd_delta < 0) { # current date is larger than goal date
            $res = abs($pd_gd_delta) < abs($cd_gd_delta);
        }
    }
    
    return $res;
}

sub exec_query {
    my ($dbh, $sql_query) = @_;

    my $sth = $dbh->prepare($sql_query)
        or die ("Can't prepare $sql_query: $!.");
    $sth->execute()
        or die "Can't execute $sql_query: $!.";
}

sub create_std_user_table_from_table_names_stored_as_keys {
    my ($dbh, $table_name, $value_type, %table_names_stored_as_keys) = @_;
    
    (my $sanitized_table_name = $table_name) =~ s/[^A-Za-z0-9_]//g;
    
    my $create_table_sql = "CREATE TABLE $sanitized_table_name(user_id TEXT";
    foreach my $key (keys %table_names_stored_as_keys) {
        my @key_split = split(/:/, $key);
        if (scalar @key_split > 1) {
            $create_table_sql .= ", " . $key_split[1] . " " . $key_split[0];
        } else {
            $create_table_sql .= ", " . $key . " $value_type";
        }
    }
    $create_table_sql .= ");";
    exec_query($dbh, $create_table_sql);
}

sub insert_student_activity_data_into_table {
    my ($dbh, $student_id, $activity_data, $activity_data_type, $key_replace_sub) = @_;
    
    (my $sanitized_table_name = $activity_data_type) =~ s/[^A-Za-z0-9_]//g;
    
    if ($activity_data->{ $activity_data_type }) {   
        my $insert_values_sql = "INSERT INTO $sanitized_table_name (user_id,";
        foreach my $key (keys %{ $activity_data->{ $activity_data_type } }) {
            if ($key_replace_sub) {
                $key_replace_sub->($key);
            }
            $insert_values_sql .= "$key,";
        }
        chop($insert_values_sql);
        $insert_values_sql .= ") VALUES (\"$student_id\",";
        foreach my $value (values %{ $activity_data->{ $activity_data_type } }) {
            $insert_values_sql .= "$value,";
        }
        chop($insert_values_sql);
        $insert_values_sql .= ");";
        exec_query($dbh, $insert_values_sql);
    }
}

sub create_table {
    my ($dbh, $table_name, $column_ids) = @_;
    
    (my $sanitized_table_name = $table_name) =~ s/[^A-Za-z0-9_]//g;
    
    my $create_table_sql = "CREATE TABLE $sanitized_table_name(";
    keys %{ $column_ids };
    while (my($col_id, $col_data_type) = each %{ $column_ids }) {
        $create_table_sql .= "$col_id $col_data_type,";
    }
    chop($create_table_sql);
    $create_table_sql .= ");";
    exec_query($dbh, $create_table_sql);
}

sub insert_row_into_table {
    my ($dbh, $table_name, $row_data) = @_;
    
    (my $sanitized_table_name = $table_name) =~ s/[^A-Za-z0-9_]//g;
    
    my $insert_values_sql = "INSERT INTO $sanitized_table_name (";
    foreach my $column_id (keys %{ $row_data }) {
        $insert_values_sql .= "$column_id,";
    }
    chop($insert_values_sql);
    $insert_values_sql .= ") VALUES (";
    foreach my $data_record_value (values %{ $row_data }) {
        $insert_values_sql .= "$data_record_value,";
    }
    chop($insert_values_sql);
    $insert_values_sql .= ");";
    exec_query($dbh, $insert_values_sql);
}

sub is_column_allowed {
    my ($table_name, $column_index) = @_;
    
    if ($table_name eq "auth_userprofile") {
        return $auth_userprofile_allowed_rows[$column_index];
    } elsif ($table_name eq "certificates_generatedcertificate") {
        return $certificates_generatedcertificate_allowed_rows[$column_index];
    } else {
        return 1;
    }
}