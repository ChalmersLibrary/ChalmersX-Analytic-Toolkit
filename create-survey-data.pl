#!/usr/bin/perl
use strict;
use warnings;

use IO::Zlib;
use Date::Parse;
use Date::Format;
use Date::Calc      qw( Delta_Days Time_to_Date Add_Delta_Days );
use Cwd             qw( abs_path );
use File::Basename  qw( dirname );
use File::Copy;
use DateTime;
use JSON::XS;

# Auto flush the default buffer.
$| = 1;

# Get the current working directory.
my $cwd = dirname(abs_path($0));

# Gather the information that we need.
print "================================================================================\n";
print "=                              CREATE SURVEY DATA                              =\n";
print "================================================================================\n";
print "Please enter the name for the folder with the event data for one specific course.\nThis folder is created if you run get-data-for-course.pl.\n> ";
my $data_folder = <>;
chomp $data_folder;
print "Thank you!\n\nI will put the generated data in $data_folder/survey-data.\n\nCreating survey data...\n";

# Check if something exists with the same name as our destination directory.
my $dest_dir = "$data_folder/survey-data";
if (-d $dest_dir) {
    print "\nDirectory '$dest_dir' already exists. Exiting...\n";
} elsif (-e $dest_dir) {
    print "\nFile with the same name as the destination directory '$dest_dir' already exists. Exiting...\n";
} else {
    mkdir $dest_dir;
    
    my %student_pre_survey_keys = ();
    my %student_pre_survey_data = ();
    my %student_post_survey_keys = ();
    my %student_post_survey_data = ();

    # Iterate through all the event data files and do stuff with the video data.
    my @event_data_files = grep { -f } glob $data_folder . '/events/*';
    foreach (@event_data_files) {
        if (/.*([0-9]{4}-[0-9]{2}-[0-9]{2})\.log\.gz$/) {
            my $event_date = str2time($1);
                    
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
            
                if (not defined $event_user_id) {
                    $event_user_id = "undefined";
                }

                if ($event_event_type eq "problem_check") {
                    my $problem_display_name = $event->{ "context" }->{ "module" }->{ "display_name" };
                    
                    if (not defined $problem_display_name) {
                        $problem_display_name = "undefined";
                    }
                    
                    my $event_event_source = $event->{ "event_source" };

                    # Pre-course survey answers
                    if (index(lc($problem_display_name), "pre") != -1 && index(lc($problem_display_name), "survey") != -1 && $event_event_source eq "server") {

                        # Need to remove quotes from old value before we can parse time.
                        my $latest_problem_time = $student_pre_survey_data{ $event_user_id }{ "pre_survey_answers" }{ "time" };
                        
                        if (not defined $latest_problem_time) {
                            $latest_problem_time = "19700101";
                        }                        
                        
                        $latest_problem_time =~ s/"//g;

                        if (!$student_pre_survey_data{ $event_user_id }{ "pre_survey_answers" }{ "time" } || str2time($event_time) > str2time($latest_problem_time)) {
                            $student_pre_survey_keys{ "time" } = 1;
                            $student_pre_survey_data{ $event_user_id }{ "pre_survey_answers" }{ "time" } = "\"" . $event_time . "\"";
                            foreach my $raw_problem_name (keys %{ $event->{ "event" }->{ "answers" } }) {
                                if ($raw_problem_name =~ /(_[0-9]+_[0-9]+)$/) {
                                    my $problem_name = "problem$1";
                                    $student_pre_survey_keys{ $problem_name } = 1;

                                    my $answer_value = $event->{ "event" }->{ "answers" }{ $raw_problem_name };

                                    # Special handling for arrays (multiple answer questions)
                                    if(ref($answer_value) eq "ARRAY") {
                                        $student_pre_survey_data{ $event_user_id }{ "pre_survey_answers" }{ $problem_name } = "\"" . join(',',@{ $answer_value }) . "\"";
                                    } else {
                                        $answer_value =~ tr/"/'/;
                                        $answer_value =~ tr/\r?\n/ /;
                                        $student_pre_survey_data{ $event_user_id }{ "pre_survey_answers" }{ $problem_name } = "\"" . $answer_value . "\"";
                                    }
                                }
                            }
                        }
                    }
                    
                    # Post-course survey answers
                    if (index(lc($problem_display_name), "post") != -1 && index(lc($problem_display_name), "survey") != -1 && $event_event_source eq "server") {

                        # Need to remove quotes from old value before we can parse time.
                        my $latest_problem_time = $student_post_survey_data{ $event_user_id }{ "post_survey_answers" }{ "time" };
                        
                        if (not defined $latest_problem_time) {
                            $latest_problem_time = "19700101";
                        }   
                        
                        $latest_problem_time =~ s/"//g;

                        if (!$student_post_survey_data{ $event_user_id }{ "post_survey_answers" }{ "time" } || str2time($event_time) > str2time($latest_problem_time)) {
                            $student_post_survey_keys{ "time" } = 1;
                            $student_post_survey_data{ $event_user_id }{ "post_survey_answers" }{ "time" } = "\"" . $event_time . "\"";
                            foreach my $raw_problem_name (keys %{ $event->{ "event" }->{ "answers" } }) {
                                if ($raw_problem_name =~ /(_[0-9]+_[0-9]+)$/) {
                                    my $problem_name = "problem$1";
                                    $student_post_survey_keys{ $problem_name } = 1;

                                    my $answer_value = $event->{ "event" }->{ "answers" }{ $raw_problem_name };

                                    # Special handling for arrays (multiple answer questions)
                                    if(ref($answer_value) eq "ARRAY") {
                                        $student_post_survey_data{ $event_user_id }{ "post_survey_answers" }{ $problem_name } = "\"" . join(',',@{ $answer_value }) . "\"";
                                    } else {
                                        $answer_value =~ tr/"/'/;
                                        $answer_value =~ tr/\r?\n/ /;
                                        $student_post_survey_data{ $event_user_id }{ "post_survey_answers" }{ $problem_name } = "\"" . $answer_value . "\"";
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    
    # Save pre survey answers to file
    my $filename = "$dest_dir/pre_survey_answers.csv";
    open (my $fh, '>:encoding(UTF-8)', $filename) or die "Failed to open file '$filename' for writing.";
    
    my $csv_first_line = "user_id,";
    foreach my $pre_survey_problem_name (keys %student_pre_survey_keys) {
        $csv_first_line .= "$pre_survey_problem_name,";
    }
    chop($csv_first_line);
    print $fh "$csv_first_line\n";
    
    foreach my $user_id (keys %student_pre_survey_data) {
        my $user_answers = "$user_id,";
        foreach my $pre_survey_problem_name (keys %student_pre_survey_keys) {
            my $answer = $student_pre_survey_data{ $user_id }{ "pre_survey_answers" }{ $pre_survey_problem_name };
            if ($answer) {
                $user_answers .= $answer . ",";
            } else {
                $user_answers .= ",";
            }
        }
        chop($user_answers);
        print $fh "$user_answers\n";
    }

    
    # Save post survey answers to file
    $filename = "$dest_dir/post_survey_answers.csv";
    open ($fh, '>:encoding(UTF-8)', $filename) or die "Failed to open file '$filename' for writing.";
    
    $csv_first_line = "user_id,";
    foreach my $post_survey_problem_name (keys %student_post_survey_keys) {
        $csv_first_line .= "$post_survey_problem_name,";
    }
    chop($csv_first_line);
    print $fh "$csv_first_line\n";
    
    foreach my $user_id (keys %student_post_survey_data) {
        my $user_answers = "$user_id,";
        foreach my $post_survey_problem_name (keys %student_post_survey_keys) {
            my $answer = $student_post_survey_data{ $user_id }{ "post_survey_answers" }{ $post_survey_problem_name };
            if ($answer) {
                $user_answers .= $answer . ",";
            } else {
                $user_answers .= ",";
            }
        }
        chop($user_answers);
        print $fh "$user_answers\n";
    }
    

    print "Done.\n\n";
}