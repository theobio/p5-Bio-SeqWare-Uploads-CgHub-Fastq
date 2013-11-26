#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;
use File::Path;
use POSIX;

# Sample info retriever

my ( $username,         # Database user
     $password,         # Database password
     $dbhost,           # Database host
     $seqware_meta_db,  # Database name with meta_db information
     $mode
);

my $getOptResult = GetOptions(
    'username=s' => \$username,
    'password=s' => \$password,
    'dbhost=s'   => \$dbhost,
    'db=s'       => \$seqware_meta_db,
    'mode=s'     => \$mode,
);

# Connect to db
my $dbn = "DBI:Pg:dbname=$seqware_meta_db;host=$dbhost";
my $database=DBI->connect( $dbn, $username, $password, {RaiseError => 1} );

my $metadata_sth = $database->prepare(
    "select 
        sample, flowcell, lane, barcode 
     from 
        vw_tcga_v2 
     where 
        status = 'completed' and sample_id not in (select sample_id from upload)
     order by
        sample");

my $upload_sth = $database->prepare(
    "select 
        v.sample, v.flowcell, v.lane, v.barcode 
     from 
        vw_tcga_v2 v, upload u
     where 
        v.sample_id = u.sample_id and
        v.status = 'completed' and
        u.status = 'METADATA_GENERATED'
     order by
        sample");

my $sth;

if ($mode eq "ready-for-metadata") {
	$sth =  $metadata_sth;
} elsif ($mode eq "ready-for-upload") {
	$sth = $upload_sth;
} else {
	print ("Please specify a mode [ready-for-metadata, ready-for-upload]");
	exit (-1);
}

$sth->execute();
while(my @row = $sth->fetchrow_array) {
	print ("$row[0]\t$row[1]\t$row[2]");

	if (defined($row[3])) {
		print ("\t$row[3]");
	}

	print ("\n");
}