#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;
use File::Path;
use POSIX;


# SRJ: Unversioned -> v0.000.002. Changes include:
#   Added version
#   Added pod
#   Output includes "" entry for missing barcode (always has fourth column).
# SRJ: v0.000.003
# SRJ: v0.000.031
# Sync with installed version, deprecated cghub upload process.
# Use correct table, prevents duplicate problems.

=head1 NAME

get_samples.pl - Output samples to process (by step) for cghub 

=cut

=head1 VERSION

Version 0.000.031

=cut

our $VERSION = 0.000031;

=head1 SYNOPSIS

    # To output samples for input to generate_cghub_metadata.pl
    get_samples.pl \
        --username seqware
        --password ***
        --db seqware_meta_db \
        --dbhost swprod.bioinf.unc.edu \
        --mode ready-for-metadata

    # DEPRECATED
    # To output samples for input to generate_cghub_metadata.pl
    get_samples.pl \
        --username seqware
        --password ***
        --db seqware_meta_db \
        --dbhost swprod.bioinf.unc.edu \
        --mode ready-for-upload

    # OPTIONS (all required)

        --username SeqWare database login user name
        --password SeqWare database login password
        --db       SeqWare database name
        --dbhost   SeqWare database host machine

        --mode     ready-for-metadata | ready-for-upload (deprecated)

=cut

=head1 DESCRIPTION

Depending on mode specified by --mode, selects samples from the vw_tcga_v2
view samples that are ready for metadata generation (--mode ready-for-metadata).
A list of samples meeting criteria will be dumped to standard out as if a sample
file had been echoed. This is then redirected to another file as input to allow
chained processing.

For meta-data generation, will select all samples in the vw_tcga_v2 table that
have completed processing, but do not have an upload record. The metadata
generation script will create an upload record, preventing samples from being
rerun.

=cut


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

# Should no longer be using ready-for-upload mode.
#
# my $upload_sth = $database->prepare(
#    "select 
#        v.sample, v.flowcell, v.lane, v.barcode
#     from
#        vw_tcga_v2 v, upload u
#     where
#        v.sample_id = u.sample_id and
#        v.status = 'completed' and
#        u.status = 'METADATA_GENERATED'
#     order by
#        sample"
# );

my $sth;

if ($mode eq "ready-for-metadata") {
	$sth =  $metadata_sth;
} elsif ($mode eq "ready-for-upload") {
    die "Illegal use of deprecated mode."
#	$sth = $upload_sth;
} else {
	print ("Please specify a mode [ready-for-metadata, ready-for-upload]");
	exit (-1);
}

$sth->execute();
while(my @row = $sth->fetchrow_array) {
	print ("$row[0]\t$row[1]\t$row[2]\t");

	if (defined($row[3])) {
		print ("$row[3]");
	}

	print ("\n");
}
$sth->finish();
