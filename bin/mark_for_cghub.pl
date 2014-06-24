#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;

=head1 NAME

mark_for_cghub.pl - Tag samples for attention by the cghub upload sytem.

=cut

=head1 VERSION

Version 0.000.031

=cut

=head1 SYNOPSIS

    cat sample_file.tsv | mark_for_cghub.pl \
        --username seqware \
        --password *** \
        --db seqware_meta_db \
        --dbhost swprod.bioinf.unc.edu \
        --person "Stuart R. Jefferys"

    Options (all required)

        --username   SeqWare database login user name
        --password   SeqWare database login password
        --db         SeqWare database name
        --dbhost     SeqWare database host machine

        --person     User executing this script triggering an upload.

=cut

=head1 DESCRIPTION

Creates an entry in the flowcell_decider_info table for each sample in the input
file. Output from this script can be redirectred to a log file.

=cut.

=head2 Input file format

This is a standard sample file, with tab separated columns. The first four
columns are fixed, and must be sample, flowcell, lane, and barcode. If a sample
does not have an index, barcode should be the empty string, but the preceding
tab is required.

Empty lines and lines beginning with "#" or "<whitespace>#"
are parsed as comment lines and ignored. This means sample names may not start
with "#". Any leading or trailing blanks are stripped.

With barcode:

        # This line ignored
    [sample][\t][flowcell][\t][lane][\t][index]
    [sample][\t][flowcell][\t][lane][\t][index][\t][moreStuff]

Without barcode:

       # This line ignored.
    [sample][\t][flowcell][\t][lane][\t][]
    [sample][\t][flowcell][\t][lane][\t][][\t][moreStuff]


For legacy compatability, if no tabs are found in a data line it will be
split on whitespace.

=cut

our $VERSION = 0.000031;

# SRJ: Unversioned -> v0.000.001.
# SRJ:  v0.000.001 -> v0.000.002.
#   Added $VERSION.
#   Added pod documentation.
#   Changed shebang line to work with perlbrew.
#   Ignore blank input sample lines, lines with only white-space, and lines
#     that begin with the comment character '#', including when '#' is preceeded
#     by whitespace.
# SRJ:  v0.000.002 -> v0.000.003.
#   Allow for sample names with embedded spaces.
# SRJ:  v0.000.003 -> v0.000.031
#   Sync with installed version
my ( $username,         # Database user
     $password,         # Database password
     $dbhost,           # Database host
     $seqware_meta_db,  # Database name with meta_db information
     $person,           # User executing the upload
);

my $getOptResult = GetOptions(
    'username=s'    => \$username,
    'password=s'    => \$password,
    'dbhost=s'      => \$dbhost,
    'db=s'          => \$seqware_meta_db,
    'person=s'      => \$person
);

if (($username eq "") or ($password eq "") or ($dbhost eq "") or ($seqware_meta_db eq "") or ($person eq "")) {
    print("Usage: mark_for_cghub.pl --username <username> --password <password> --dbhost <dbhost> --db <db> --sample_type <type of sample.  i.e. KIRC> --person <your user id (i.e. junyuanw)>\n");
    exit(-1);
}

# Connect to db
my $dbn = "DBI:Pg:dbname=$seqware_meta_db;host=$dbhost";
my $database=DBI->connect( $dbn, $username, $password, {RaiseError => 1} );

# Pre pare library of SQL queries to use
my $insert = $database->prepare(
    "insert into flowcell_decider_info (flowcell, lane, index, info, description) values 
    (?, ?, ?, 'tcga_samples', ?)");

# Get data for and assemble the description describing why this line has been
# created in the flowcell_decider_info table
my $whoami = `whoami`;
chomp($whoami);
my $hostname = `hostname`;
chomp($hostname);

my $description = "Uploaded by: [$person] whoami: [$whoami] hostname: [$hostname]";

# Logging text (assuming output redirected)
print "Starting mark for cghub\n";
print "$description\n";
print "------------- " . scalar(localtime) . " --------------\n";

# Main loop over input lines, assumed file text redirected to this program.
my $count = 0;
while (<STDIN>) {
    chomp;
    # Skip lines with nothing but white space
    if (/^\s*$/) {
        next;
    }
    # Skip lines where first non-white space character is a [#].
    if (/^\s*\#/) {
        next;
    }

    # Allow for sample names with spaces in them as long as using
    # tab-separated input file, otherwise revert to splitting on
    # all spaces.

#   my @fields = split( /\s+/ );
    my @fields = split( /\s*\t\s*/ );
    if (scalar @fields < 3) {
        @fields = split( /\s+/ );
    }

    my $flowcell = $fields[1];
    my $lane = $fields[2];
    my $barcode = $fields[3];

    if ($flowcell eq "" || $lane eq "") {
        print "Invalid sample definition: $_\n";
        exit(-1);
    }

    if (defined($barcode) && ($barcode eq "" || $barcode =~ /^\s+$/)) {
        $barcode = undef;
    }

    if (defined($barcode)) {
        print "Inserting [$flowcell] [$lane] [$barcode]\n";
    } else {
    	print "Inserting [$flowcell] [$lane] [NO BARCODE]\n";
    }

    $insert->execute($flowcell, $lane, $barcode, $description);
    $count += 1;
} # End inserting lines

$insert->finish();

print "------------- " . scalar(localtime) . " --------------\n";
print "Mark for cghub complete.  [$count] samples marked for upload.\n";