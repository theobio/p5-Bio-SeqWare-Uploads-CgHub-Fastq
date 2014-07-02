#!/usr/bin/env perl

use warnings;
use strict;
use DBI;
use Getopt::Long;

our $VERSION = 0.000031;
# SRJ: Unversioned -> v0.000031

my ($username, $password, $dbhost, $seqware_meta_db);

my $getOptResult = GetOptions(
    'username=s' => \$username,
    'password=s' => \$password,
    'dbhost=s'   => \$dbhost,
    'db=s'       => \$seqware_meta_db,
);

# Connect to db
my $dbn = "DBI:Pg:dbname=$seqware_meta_db;host=$dbhost";
my $database=DBI->connect( $dbn, $username, $password, {RaiseError => 1} );

print "-------------------------------------\n";
print "Starting cgsubmit ", scalar(localtime), "\n";
print "-------------------------------------\n";

while (<STDIN>) {
	s/\n//g;

		if ( -d $_ ) {
			&cghub_submit($database,$_);
			} 
		else { print "$_ does not exist\n"; }

	}





sub cghub_submit {
my $DEBUG = 1;

my $genetorrent_exec = "/usr/bin/gtupload ";
# my $credential_file = "/datastore/tcga/cghub/cghub_hoadley.pem";
# my $credential_file = "/datastore/tcga/cghub/cghub_hoadley.pem.keep";
my $credential_file = "/datastore/alldata/tcga/CGHUB/Key.20130213/mykey.pem";
my $cghub_server = "https://cghub.ucsc.edu/";
my $cgsubmit_exec = "/usr/bin/cgsubmit";

# # For cgsubmit Version 2.1
# my $cgsubmit_validate_only_success = "Pass: Metadata valid";
# my $cgsubmit_validate_submit_success = "Pass: Submission Accepted";

# For cgsubmit Version 3.1.1"
my $cgsubmit_validate_only_success = "Metadata Validation Succeeded";

#for cgsubmit Version 3.3.1
my $cgsubmit_validate_submit_success = "Metadata Submission Succeeded";

my $cgsubmit_validate_submit_error = "Error    : You are attempting to submit an analysis using a uuid that already exists within the system and is not in the upload or submitting state";

my $genetorrent_submit_success = "100.000";
my $genetorrent_submit_error = "Error    : Your are attempting to upload to a uuid which already exists within the system and is not in the submitted or uploading state. This is not allowed.";


	my($dbh,$uuid_full_path) = @_;
	my $status = 0;

	$uuid_full_path =~ s/\/+$//g;

	my @paths = ();
	@paths = split(/\/+/,$uuid_full_path);

	my $uuid_dir = $paths[$#paths];

	my $uuid_base = "";
	foreach my $i(0..($#paths -1)) { $uuid_base = $uuid_base . "/" . $paths[$i]; }

	my $log = "START $uuid_full_path\nINFO:  $uuid_base  $uuid_dir and $cgsubmit_exec\n";

        # Ensure that we don't process the same sample twice.
        my $touchFile = "$uuid_base/$uuid_dir/unc_cghub_upload.touch";

	if (-e $touchFile) {
	  print "$touchFile exists so exiting!\n";
	  exit 0;
	}
	system("touch $touchFile");

	&update_db($dbh,$uuid_dir,"CGHUB_Metadatavalidation_Start");


my $validate_only = `$cgsubmit_exec -s $cghub_server  -u $uuid_base/$uuid_dir --validate-only`;

my $validation_passed = 0;
if($validate_only =~ /$cgsubmit_validate_only_success/) { $validation_passed = 1; }

if($validation_passed ==1) {

	&update_db($dbh,$uuid_dir,"CGHUB_Metadatavalidation_Success");

	my $cgsubmit_cmd = "$cgsubmit_exec -s $cghub_server  -c $credential_file -u $uuid_base/$uuid_dir";

	print "$cgsubmit_cmd\n";

	my $validate_submit = `$cgsubmit_cmd`; 

my $validation_submit_passed = 0;
if($validate_submit =~ /$cgsubmit_validate_submit_success/) { $validation_submit_passed = 1; }
if ($validate_submit =~ /$cgsubmit_validate_submit_error/) { $validation_submit_passed = 2; }

if($validation_submit_passed >0   ) {

		&update_db($dbh,$uuid_dir,"CGHUB_genetorrent_Start");
		my $genetorrent_cmd = "$genetorrent_exec -vvvv -c $credential_file  -u $uuid_base/$uuid_dir/manifest.xml -p $uuid_base 2>&1";
                print "$genetorrent_cmd\n";
		my $genetorrent_submit = `cd $uuid_base/$uuid_dir; $genetorrent_cmd`;

		if ($genetorrent_submit =~ /$genetorrent_submit_success/)  { 
			$log .= "SUCCESS:$uuid_base/$uuid_dir\n"; 
			$status = 1;
			}


		else { 

			$log .= "ERROR: GENETORRENT\n$genetorrent_exec -c $credential_file  -u $uuid_base/$uuid_dir/manifest.xml -p $uuid_base\n$genetorrent_submit\n"; 
			$log .= "FAILURE: $uuid_base/$uuid_dir\n";
			}
	}

else {
	$log .=  "ERROR:CGSUBMIT\n$cgsubmit_exec -s $cghub_server  -c $credential_file -u $uuid_base/$uuid_dir\n$validate_submit\n";
	$log .=  "FAILURE: :$uuid_base/$uuid_dir\n";


	}

} else {
     print "*** Validation failed for $uuid_base/$uuid_dir\n";
}

$log .= "END $uuid_full_path\n";

if ( ($log =~ /FAILURE/) || ($log =~ /ERROR/) || ($DEBUG)) { print "$log\n"; }

print "-------------------------------------\n";
print "cgsubmit done. ", scalar(localtime), "\n";
print "-------------------------------------\n";

if($status ==1 ) { &update_db($dbh,$uuid_dir,"CGHUB_Complete"); }
return($status);

}

sub update_db {
	my($dbh,$analysis_uuid,$status) = @_;
	my $sql = "UPDATE upload SET STATUS='$status' WHERE cghub_analysis_id='$analysis_uuid'";
	my $sth = $dbh->prepare($sql);
	my $r = $sth->execute or die "$sth->errstr\n";
	}
