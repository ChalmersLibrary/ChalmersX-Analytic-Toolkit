#!/usr/bin/perl
#use strict;
#use warnings;

use Date::Parse;
use Date::Format;
use Date::Calc      qw( Delta_Days Time_to_Date );
use Cwd             qw( abs_path );
use File::Basename  qw( dirname );
use Text::CSV;

# Get the current working directory.
my $cwd = dirname(abs_path($0));

# Setup CSV parsing.
my $csv = Text::CSV->new ( { binary => 1 } )
     or die "Cannot use CSV: ".Text::CSV->error_diag ();

print "Please enter organization.\n> ";
my $organization = <>;
chomp $organization;
print "Thank you!\n\n";

#Figure out which directory we should fetch our course data from, we take the latest data.
my @course_data_dirs = grep { -d } glob $cwd . '/course-data/*';

my $course_snapshot_dir = "";
my $course_snapshot_date = undef;

foreach (@course_data_dirs) {
    if (/(\d{4}-\d{2}-\d{2})$/) {
        my $potential_new_course_snapshot_date = str2time($1);
        if (!$course_snapshot_date || $course_snapshot_date < $potential_new_course_snapshot_date) {
            $course_snapshot_dir = $_ . "/" . lc $organization . "-" . time2str('%Y-%m-%d', $potential_new_course_snapshot_date);
            $course_snapshot_date = $potential_new_course_snapshot_date;
        }
    }
}

my %auth_user_data = ();
my %auth_userprofile_data = ();

print "\nWill fetch data from course snapshot in the following directory.\n$course_snapshot_dir.\n\nIs this correct? (Y/N)\n";
my $second_input = <>;
$second_input =~ s/\r?\n//;
if ($second_input =~ m/^[Y]$/i) {
    # Preload data that we want to add to the email_opt_in data.
    my @course_data_files = grep { -f } glob $course_snapshot_dir . '/*';
    foreach (@course_data_files) {
        if (/$organization-[^-]+-[^-]+-auth_user-prod-analytics\.sql$/) {
            open(my $fh, '<:encoding(UTF-8)', $_)
                or die "Could not open file '$_' $!\n";
           
            while (my $row = <$fh>) {
                chomp $row;
                my @auth_user_row = split('\t', $row);
                $auth_user_data{ $auth_user_row[4] } = [@auth_user_row];
            }
        }
        if (/$organization-[^-]+-[^-]+-auth_userprofile-prod-analytics\.sql$/) {
            open(my $fh, '<:encoding(UTF-8)', $_)
                or die "Could not open file '$_' $!\n";
                
            while (my $row = <$fh>) {
                chomp $row;
                my @auth_userprofile_row = split('\t', $row);
                $auth_userprofile_data{ $auth_userprofile_row[1] } = [@auth_userprofile_row];
            } 
        }           
    }

    # Load the email_opt_data, merge with some of the preloaded data and save it in a new CSV-file.
    open(my $fh, '<:encoding(UTF-8)', "$course_snapshot_dir/$organization-email_opt_in-prod-analytics.csv")
        or die "Could not open file '$course_snapshot_dir/$organization-email_opt_in-prod-analytics.csv' $!\n";
        
    open(my $output_fh, '>:encoding(UTF-8)', "email_opt_in.csv") or die "Could not open file 'email_opt_in.csv' $!";
    
    print $output_fh "mail, name, gender, year_of_birth, country\n";
    
    while (my $row = $csv->getline( $fh )) {
        if ($row->[3] eq "True") {
            my $user_email = $row->[0];
            my @auth_user = @{ $auth_user_data{ $user_email } };
            my @auth_userprofile = @{ $auth_userprofile_data{ $auth_user[0] } };
            
            print $output_fh "$user_email, " . $auth_userprofile[2] . ", " . $auth_userprofile[7] . ", " . $auth_userprofile[9] . ", " . $auth_userprofile[13] . "\n";
        }
    }
}