#!/usr/bin/perl
#use strict;
#use warnings;

use DBI;
use IO::Zlib;
use Date::Parse;
use Date::Format;
use Date::Calc      qw( Delta_Days Time_to_Date );
use Cwd             qw( abs_path );
use File::Basename  qw( dirname );
use JSON::XS;
use File::Copy;

# Auto flush the default buffer.
$| = 1;

# Get the current working directory.
my $cwd = dirname(abs_path($0));

# Gather the information that we need.
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
print "User ID: ";
my $user_id = <>;
chomp $user_id;
my $end_date = str2time($end_date_string) or die "Couldn't parse date.";
print "Splendid!\n\nI am now ready to fetch events for user $user_id in course run $organization/$course_id/$course_run.\nStarting at $start_date_string and ending at $end_date_string.\n\n";
print "Do you want to continue?\n";
my $input = "";
while (!($input =~ m/^[YN]$/i)) {
    print " (Y/N)\n> ";
    $input = <>;
    $input =~ s/\r?\n//;
    
    if ($input =~ m/^[^YN]?$/i) {
        print "\nUnknown command, please enter either Y for Yes or N for No.\n\n";
    }
}

# Check if something exists with the same name as our destination directory.
my $dest_dir = "user-$user_id-events-$organization-$course_id-$course_run-" . time2str('%y%m%d', $start_date) . "-" . time2str('%y%m%d', $end_date) . "-" . time2str('%Y%m%d%H%M%S',time);
if (-d $dest_dir) {
    print "\nDirectory '$dest_dir' already exists. Exiting...\n";
    $input = "X";
} elsif (-e $dest_dir) {
    print "\nFile with the same name as the destination directory '$dest_dir' already exists. Exiting...\n";
    $input = "X";
}

if ($input =~ m/^[Y]$/i) {
    mkdir $dest_dir;

    # Process the event data and save aggregates and particularly interesting data in temporary variables.
    my %problem_tables = ();
    my $iter_year = (Time_to_Date($start_date))[0];
    my $end_year = (Time_to_Date($end_date))[0];
    
    open(my $fh_video_events, '>', "$dest_dir/video_events") or die "Could not open file '$dest_dir/video_events' $!";
    
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
                        
                        my $event_event_type = $event->{ "event_type" };
                        
                        if (index($event_course_id, $organization) != -1 && index($event_course_id, $course_id) != -1 && index($event_course_id, $course_run) != -1) {
                            if ($event->{ "context" }->{ "user_id" } == $user_id) {
                                # Video interaction events.
                                if ($event_event_type eq "hide_transcript" || $event_event_type eq "load_video" || $event_event_type eq "pause_video" || 
                                    $event_event_type eq "play_video" || $event_event_type eq "seek_video" || $event_event_type eq "show_transcript" ||
                                    $event_event_type eq "speed_change_video" || $event_event_type eq "stop_video" || $event_event_type eq "video_hide_cc_menu" ||
                                    $event_event_type eq "video_show_cc_menu")
                                {
                                    print $fh_video_events "$row\n";
                                }
                            }
                        }
                    }
                    $gzfh->close();
                }
            }
        }
        $iter_year += 1;
    }
    
    close $fh_video_events;
    
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