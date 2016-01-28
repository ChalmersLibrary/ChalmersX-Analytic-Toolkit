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
print "=                          CREATE IMPROVED FORUM DATA                          =\n";
print "================================================================================\n";
print "Please enter the name for the folder with the forum data for one specific course.\nThis folder is created if you run get-data-for-course.pl.\n> ";
my $data_folder = <>;
chomp $data_folder;
print "Thank you!\n\nI will put the generated data in $data_folder/improved-forum-data.\n\nCreating improved forum data...\n";

# Check if something exists with the same name as our destination directory.
my $dest_dir = "$data_folder/improved-forum-data";
if (-d $dest_dir) {
    print "\nDirectory '$dest_dir' already exists. Exiting...\n";
} elsif (-e $dest_dir) {
    print "\nFile with the same name as the destination directory '$dest_dir' already exists. Exiting...\n";
} else {
    mkdir $dest_dir;

    # Iterate through all the course data files and do stuff with the forum data.
    my @course_data_files = grep { -f } glob $data_folder . '/*';
    foreach (@course_data_files) {
        if (/-(prod\.mongo)$/) {
            # Process forum data
            print "Processing forum data from '$1'...\n";
            open(my $fh, '<:bytes', $_)
                or die "Could not open file '$_' $!\n";
                
            my @forum_posts = ();
            my %forum_message_id_mapping = ();
            
            my %student_activity_forum_keys = ();
            my %student_activity_forum_data = ();
          
            while (my $row = <$fh>) {
                chomp $row;
                my $forum_post = JSON::XS->new->utf8->decode($row);
                my $user_id = $forum_post->{ 'author_id' };
                my $forum_post_type = $forum_post->{ '_type' };
                
                $student_activity_forum_keys{ $forum_post_type } = 1;
                $student_activity_forum_data{ $user_id }{ $forum_post_type }++;
                
                push @forum_posts, $forum_post;
            }
            
            # Build table with id, poster, thread and response-to.
            my @forum_posts_ordered_by_created_asc = sort { $a->{ "created_at" }->{ '$date' } cmp $b->{ "created_at" }->{ '$date' } } @forum_posts;
            
            my $filename = "$dest_dir/discussion_forum_activity_ng.csv";
            open (my $dfang_fh, '>', $filename) or die "Failed to open file '$filename' for writing.";
            
            print $dfang_fh "id,poster,thread,response_to\n";
            
            my $line_count = 0;
            foreach my $forum_post (@forum_posts_ordered_by_created_asc) {
                my $fixed_post_id = $line_count + 1;
                my $thread_id = $forum_post->{ "comment_thread_id" }->{ '$oid' };
                $forum_message_id_mapping{ $forum_post->{ "_id" }->{ '$oid' } } = $fixed_post_id;
                
                print $dfang_fh "$fixed_post_id," . 
                    $forum_post->{ "author_id" } . "," . 
                    $forum_message_id_mapping{ $thread_id ? $thread_id : $forum_post->{ "_id" }->{ '$oid' } } . "," .
                    ($forum_post->{ "parent_id" } ? $forum_message_id_mapping{ $forum_post->{ "parent_id" }->{ '$oid' } } : "") . "\n";
                    
                $line_count++;
            }
            
            # Build old discussion forum table with aggregated data.
            $filename = "$dest_dir/discussion_forum_activity.csv";
            open (my $dfa_fh, '>', $filename) or die "Failed to open file '$filename' for writing.";
            
            my $csv_first_line = "user_id,";
            foreach my $key (keys %student_activity_forum_keys) {
                $csv_first_line .= "$key,";
            }
            chop($csv_first_line);
            print $dfa_fh "$csv_first_line\n";

            foreach my $user_id (keys %student_activity_forum_data) {
                my $forum_activity_data_record = "$user_id,";
                foreach my $activity_type (keys %student_activity_forum_keys) {
                    my $activity_data = $student_activity_forum_data{ $user_id }{ $activity_type };
                    if ($activity_data) {
                        $forum_activity_data_record .= $activity_data . ",";
                    } else {
                        $forum_activity_data_record .= ",";
                    }
                    
                }
                chop($forum_activity_data_record);
                print $dfa_fh "$forum_activity_data_record\n";
            }
            
            print "Done.\n\n";
        }
    }
}