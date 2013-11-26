#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;

my ( $username,         # Database user
     $password,         # Database password
     $dbhost,           # Database host
     $seqware_meta_db,  # Database name with meta_db information
     $sample_type,      # Type of samples
     $person,           # User executing the upload
);

my $getOptResult = GetOptions(
    'username=s'    => \$username,
    'password=s'    => \$password,
    'dbhost=s'      => \$dbhost,
    'db=s'          => \$seqware_meta_db,
    'sample_type=s' => \$sample_type,
    'person=s'      => \$person
);

if (($username eq "") or ($sample_type eq "") or ($dbhost eq "") or ($seqware_meta_db eq "") or ($sample_type eq "") or ($person eq "")) {
    print("Usage: mark_for_cghub.pl --username <username> --password <password> --dbhost <dbhost> --db <db> --sample_type <type of sample.  i.e. KIRC> --person <your user id (i.e. junyuanw)>\n");
    exit(-1);
}

# Connect to db
my $dbn = "DBI:Pg:dbname=$seqware_meta_db;host=$dbhost";
my $database=DBI->connect( $dbn, $username, $password, {RaiseError => 1} );

my $insert = $database->prepare(
    "insert into flowcell_decider_info (flowcell, lane, index, info, description) values 
    (?, ?, ?, 'tcga_samples', ?)");

my $whoami = `whoami`;
chomp($whoami);
my $hostname = `hostname`;
chomp($hostname);

my $description = "Sample type: [$sample_type] uploaded by: [$person] whoami: [$whoami] hostname: [$hostname]";

print "Starting mark for cghub\n";
print "$description\n";
print "------------- " . scalar(localtime) . " --------------\n";

my $count = 0;

while (<STDIN>) {
    chomp;
    my @fields = split( /\s+/ );
    my $flowcell = $fields[1];
    my $lane = $fields[2];
    my $barcode = $fields[3];

    if ($flowcell eq "" or $lane eq "") {
        print "Invalid sample definition: $_\n";
        exit(-1);
    }

    if (defined($barcode) and ($barcode eq "")) {
        $barcode = undef;
    }

    if (defined($barcode)) {
        print "Inserting [$flowcell] [$lane] [$barcode]\n";
    } else {
    	print "Inserting [$flowcell] [$lane] [NO BARCODE]\n";
    }

    $insert->execute($flowcell, $lane, $barcode, $description);
    $count += 1;
}

$insert->finish();

print "------------- " . scalar(localtime) . " --------------\n";
print "Mark for cghub complete.  [$count] samples marked for upload.\n";