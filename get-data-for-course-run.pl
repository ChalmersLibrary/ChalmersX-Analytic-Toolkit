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

my %course_file_whitelist = ( "course_structure-prod-analytics.json"                => "OK",
                              "course-prod-analytics.xml.tar.gz"                    => "OK",
                              "courseware_studentmodule-prod-analytics.sql"         => "OK",
                              "student_courseenrollment-prod-analytics.sql"         => "OK",
                              "student_languageproficiency-prod-analytics.sql"      => "OK",
                              "user_api_usercoursetag-prod-analytics.sql"           => "OK",
                              "wiki_article-prod-analytics.sql"                     => "OK",
                              "wiki_articlerevision-prod-analytics.sql"             => "OK" );

my %forum_file_whitelist =  ( "prod.mongo"                                          => "OK" );



# id, user_id, name, language, location, meta, courseware, gender, mailing_address, year_of_birth, level_of_education, goals, allow_certificate, country, city, bio, profile_image_uploaded_at
my @auth_userprofile_allowed_rows = (1, 1, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0);

# id, user_id, download_url, grade, course_id, key, distinction, status, verify_uuid, download_uuid, name, created_date, modified_date, error_reason, mode
my @certificates_generatedcertificate_allowed_rows = (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1);

# Auto flush the default buffer.
$| = 1;

# Get the current working directory.
my $cwd = dirname(abs_path($0));

# Gather the information that we need.
print "================================================================================\n";
print "=                            GET DATA FOR COURSE RUN                           =\n";
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
print "Splendid!\n\nI am now ready to extract data for course run $organization/$course_id/$course_run.\nStarting at $start_date_string and ending at $end_date_string.\n\n";
print "How do you want to continue?\n";
print "------------------------------------\n";
print "Extract only course data.        (C)\n";
print "Extract only event data.         (E)\n";
print "Extract only forum data.         (F)\n";
print "Extract all data.                (A)\n";
print "Exit program.                    (X)\n";
print "------------------------------------\n";
my $input = "";
while (!($input =~ m/^[CEFAX]$/i)) {
    print " (C/E/F/A/X)\n> ";
    $input = <>;
    $input =~ s/\r?\n//;
    
    if ($input =~ m/^[^CEFAX]?$/i) {
        print "\nUnknown command, please enter a letter from the table above.\n\n";
    }
}

# Check if something exists with the same name as our destination directory.
my $dest_dir = "course-run-data-$organization-$course_id-$course_run-" . time2str("%y%m%d", $start_date) . "-" . time2str("%y%m%d", $end_date) . "-" . time2str("%Y%m%d%H%M%S",time);
if (-d $dest_dir) {
    print "\nDirectory '$dest_dir' already exists. Exiting...\n";
    $input = "X";
} elsif (-e $dest_dir) {
    print "\nFile with the same name as the destination directory '$dest_dir' already exists. Exiting...\n";
    $input = "X";
}

if ($input =~ m/^[CEFA]$/i) {
    mkdir $dest_dir;

    # Open log stream
    my $filename = "$dest_dir/info.txt";
    open (my $info_fh, '>', $filename) or die "Failed to open file '$filename' for writing.";

    # Print some general information to log file
    print $info_fh "================================================================================\n";
    print $info_fh "=                            GET DATA FOR COURSE RUN                           =\n";
    print $info_fh "================================================================================\n";
    print $info_fh "\n";
    print $info_fh " Organization:\t\t\t\t$organization\n";
    print $info_fh " Course ID:\t\t\t\t$course_id\n";
    print $info_fh " Course Run:\t\t\t\t$course_run\n";
    print $info_fh " Data start date:\t\t\t" . time2str("%Y-%m-%d", $start_date) . "\n";
    print $info_fh " Data end date:\t\t\t\t" . time2str("%Y-%m-%d", $end_date) . "\n";
    print $info_fh "\n";
    
    # Process course data and/or forum data
    if ($input =~ m/^[CFA]$/i) {
        if ($input =~ m/^[CA]$/i) {
            print $info_fh " Course data processed.\n";
        }

        #Figure out which directory we should fetch our course data from
        my @course_data_dirs = grep { -d } glob $cwd . '/course-data/*';
        
        my $course_snapshot_dir = "";
        my $course_snapshot_date = undef;
        
        foreach (@course_data_dirs) {
            if (/(\d{4}-\d{2}-\d{2})$/) {
                my $potential_new_course_snapshot_date = str2time($1);
                # Use the course snapshot that is closest after course run end date.
                if (is_better_snapshot_date($course_snapshot_date, $potential_new_course_snapshot_date, $end_date)) {
                    $course_snapshot_dir = $_ . "/" . lc $organization . "-" . time2str('%Y-%m-%d', $potential_new_course_snapshot_date);
                    $course_snapshot_date = $potential_new_course_snapshot_date;
                }
            }
        }
        
        if ($input =~ m/^[FA]$/i) {
            print $info_fh " Forum data processed.\n";
        }
        
        print "\nWill fetch data from course snapshot in the following directory.\n$course_snapshot_dir.\n\nIs this correct? (Y/N)\n";
        my $second_input = <>;
        $second_input =~ s/\r?\n//;
        if ($second_input =~ m/^[Y]$/i) {
            print $info_fh " Course snapshot:\t\t\t" . time2str("%Y-%m-%d", $course_snapshot_date) . "\n";
            my @course_data_files = grep { -f } glob $course_snapshot_dir . '/*';
            foreach (@course_data_files) {
                # Copy to destination directory if file in whitelist.
                if (/$organization-$course_id-$course_run-(.+)$/) {
                    my $course_data_filename = $1;
                    if (($input =~ m/^[CA]$/i && $course_file_whitelist{$course_data_filename}) ||
                        ($input =~ m/^[FA]$/i && $forum_file_whitelist{$course_data_filename})) {
                        copy($_, $dest_dir);
                    }
                }
            }
        } else {
            print "Then I can't process course data :(\n\n";
        }

        print $info_fh "\n";
        print "\n\n";
    }
    
    # Process event data
    if ($input =~ m/^[EA]$/i) {
        print $info_fh " Event data processed.\n";
        mkdir "$dest_dir/events";
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
                        
                        my $filename = "$dest_dir/events/$1.log.gz";
                        my $gzwfh = new IO::Zlib;
                        $gzwfh->open($filename, "wb")
                            or die "Could not open file '$filename' $!\n";
                        
                        my $gzfh = new IO::Zlib;
                        $gzfh->open($_, "rb")
                            or die "Could not open file '$_' $!\n";
                      
                        my $events_belonging_to_course = 0;
                        my $events_not_belonging_to_course = 0;
                        while (my $row = <$gzfh>) {
                            chomp $row;
                            print $gzwfh "$row\n";
                        }
                        
                        $gzwfh->close();
                        
                        print "Found $events_belonging_to_course events that belongs to course.\nDiscarded $events_not_belonging_to_course events that does not belong to this course.\n\n";
                        $gzfh->close();
                    }
                }
            }
            $iter_year += 1;
        }

        print $info_fh "\n";
    }
    
    close $info_fh;
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