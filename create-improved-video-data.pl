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
print "=                          CREATE IMPROVED VIDEO DATA                          =\n";
print "================================================================================\n";
print "Please enter the name for the folder with the event data for one specific course.\nThis folder is created if you run get-data-for-course.pl.\n> ";
my $data_folder = <>;
chomp $data_folder;
print "Thank you!\n\nI will put the generated data in $data_folder/improved-video-data.\n\nCreating improved video data...\n";

# Check if something exists with the same name as our destination directory.
my $dest_dir = "$data_folder/improved-video-data";
if (-d $dest_dir) {
    print "\nDirectory '$dest_dir' already exists. Exiting...\n";
} elsif (-e $dest_dir) {
    print "\nFile with the same name as the destination directory '$dest_dir' already exists. Exiting...\n";
} else {
    mkdir $dest_dir;
    
    my %student_activity_video_keys = ();
    my %student_activity_video_data = ();
    
    my %student_activity_video_ng_keys = ();
    my %student_activity_video_ng_data = ();

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
            
                # Video interaction events.
                if ($event_event_type eq "hide_transcript" || $event_event_type eq "load_video" || $event_event_type eq "pause_video" || 
                    $event_event_type eq "play_video" || $event_event_type eq "seek_video" || $event_event_type eq "show_transcript" ||
                    $event_event_type eq "speed_change_video" || $event_event_type eq "stop_video" || $event_event_type eq "video_hide_cc_menu" ||
                    $event_event_type eq "video_show_cc_menu")
                {
                    $student_activity_video_keys{ $event_event_type } = 1;
                    $student_activity_video_data{ $event_user_id }{ $event_event_type }++;
                    
                    my $video_event_event = $event->{ "event" };                               
                    if ($video_event_event && ref($video_event_event) ne "HASH") {
                        $video_event_event = JSON::XS->new->utf8->decode($video_event_event);
                    }
                    
                    $student_activity_video_ng_keys{ $event_event_type } = 1;
                    $student_activity_video_ng_data{ $event_user_id . ":" . $video_event_event->{ "id" } }{ "user_id" } = $event_user_id;
                    $student_activity_video_ng_data{ $event_user_id . ":" . $video_event_event->{ "id" } }{ "video_event_id" } = $video_event_event->{ "id" };
                    $student_activity_video_ng_data{ $event_user_id . ":" . $video_event_event->{ "id" } }{ $event_event_type }++;
                }
            }
        }
    }
    
    # Build video activity table with aggregated data.
    my $filename = "$dest_dir/video_activity.csv";
    open (my $va_fh, '>', $filename) or die "Failed to open file '$filename' for writing.";
    
    my $csv_first_line = "user_id,";
    foreach my $key (keys %student_activity_video_keys) {
        $csv_first_line .= "$key,";
    }
    chop($csv_first_line);
    print $va_fh "$csv_first_line\n";

    foreach my $user_id (keys %student_activity_video_data) {
        my $video_activity_data_record = "$user_id,";
        foreach my $activity_type (keys %student_activity_video_keys) {
            my $activity_data = $student_activity_video_data{ $user_id }{ $activity_type };
            if ($activity_data) {
                $video_activity_data_record .= $activity_data . ",";
            } else {
                $video_activity_data_record .= ",";
            }
        }
        chop($video_activity_data_record);
        print $va_fh "$video_activity_data_record\n";
    }
    
    # Build video activity ng table with aggregated data.
    $filename = "$dest_dir/video_activity_ng.csv";
    open (my $vang_fh, '>', $filename) or die "Failed to open file '$filename' for writing.";
    
    $csv_first_line = "user_id,video_event_id,";
    foreach my $key (keys %student_activity_video_ng_keys) {
        $csv_first_line .= "$key,";
    }
    chop($csv_first_line);
    print $vang_fh "$csv_first_line\n";

    foreach my $data (values %student_activity_video_ng_data) {
        my $video_activity_data_record = %{ $data }{ "user_id" } . "," . %{ $data }{ "video_event_id" } . ",";
        foreach my $activity_type (keys %student_activity_video_ng_keys) {
            my $activity_data = %{ $data }{ $activity_type };
            if ($activity_data) {
                $video_activity_data_record .= $activity_data . ",";
            } else {
                $video_activity_data_record .= ",";
            }
            
        }
        chop($video_activity_data_record);
        print $vang_fh "$video_activity_data_record\n";
    }

    print "Done.\n\n";
}