#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;

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


my $sql = "select u.cghub_analysis_id, u.metadata_dir from upload u
    where target = 'CGHUB' and u.status = 'METADATA_GENERATED' ";
#and v.status = 'completed' order by v.priority, u.tstmp limit 1";


# Get sample / upload info
my $sth1 = $database->prepare( $sql);

$sth1->execute();
my ($analysis_id, $metadata_dir) = $sth1->fetchrow_array;
$sth1->finish();

if (defined($analysis_id) && defined($metadata_dir)) {
    print "$metadata_dir/$analysis_id\n";
}
