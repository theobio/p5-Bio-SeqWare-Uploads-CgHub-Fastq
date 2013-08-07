#! /usr/bin/env perl

use warnings;
use strict;

use Pod::Usage;
use Getopt::Long;
use Data::Dumper;

use Bio::SeqWare::Config;
use Bio::SeqWare::Uploads::CgHub::Fastq;

=head1 NAME

upload-cghub-fastq - Zip and upload fastq files to cghub.

=cut

=head1 VERSION

Version 0.000.001   # PRE-RELEASE

=cut

our $VERSION = '0.000001';   # PRE-RELEASE

# TODO: consider allow pre-parsing cli parameters for config file name.

my $configParser = Bio::SeqWare::Config->new();
my $configOptions = $configParser->getKnown();

my $opt = _processCommandLine( $configOptions );
print Dumper( $opt );
my $instance = Bio::SeqWare::Uploads::CgHub::Fastq->new( $opt );

$instance->run();

=head1 SYNOPSIS

upload-cghub-fastq [options]

    Options:

        # DB parameters (normally from config file)
        --dbUser      SeqWare database login user name
        --dbPassword  SeqWare database login password
        --dbSchema    SeqWare database name
        --dbHost      SeqWare database host machine

        # Selection constraints
        --sample      Sample name string (title)
        --flowcell    Sequencer run flowcell name
        --lane        The lane.lane number (lane_index + 1)
        --lane_index  The lane.lane_index number (0 based)
        --barcode     The lane.barcode sequece identification tag
        --uploadId    Upload table id of bam upload to cghub
        --bamFileId   File id of bam file uploaded to cghub

        # Other parameters
        --verbose     Print status messages while running
        --version     Report version and exit
        --help        Show this message

        # Run mode is routinely used only as the default
        --runMode     "ZIP" | "META"   | "VALIDATE"
                            | "UPLOAD" | "ALL" (default)

=cut

=head1 DESCRIPTION

Run the cghub upload process. Reads most of its options from the seqware config
file, all options can be over-ridden by command line options, but no option
is required.

Breaks the task into four pieces, each of which has its own C<--runMode>.
The default runMode is "ALL" which runs each of the four pieces in turn.

=head2 runMode

The C<--runMode> parameter detemines what this program does when invoked, and
can be any of the following 5 values. (default is "ALL").

=over 3

=item  ZIP

Generate a zip/tar archive of the single or paired fastq files for a sample,
Updates the database with appropriate information.

Needs to identify the workflow_runIids for the uploaded bam files,
      "--select file_ids for fastqs from workflow_runs where the mapsplice files\n"
    . "--have been uploaded but the fastq zip file has not be created"

select vf.file_id from vw_files where
   vw.workflow_run_id in (select workflow_run id where bams are uploaded)
   AND vw.workflow_run_id not in (select workflow_run id where fastq-zips are uploaded)
   AND 
   
=item META

Generates the meta-data for upload for this sample.

=item VALIDATE

Validates the meta-data associated with this sample.

=item UPLOAD

Does the upload.

=item ALL

Run all steps, in order ("ZIP", "META", "VALIDATE", "UPLOAD").

=back

=cut

sub _processCommandLine {
    my $optionsHR = shift;
    my %options = %$optionsHR;

    $options{'argv'} = [ @ARGV ];

    GetOptions(
        'dbUser=s'     => \$options{'dbUser'},
        'dbPassword=s' => \$options{'dbPassword'},
        'dbHost=s'     => \$options{'dbHost'},
        'dbSchema=s'   => \$options{'dbSchema'},

        'sample=s'      => \$options{'sample'},
        'flowcell=s'    => \$options{'flowcell'},
        'lane=i'        => \$options{'lane'},
        'lane_index=i'  => \$options{'lane_index'},
        'barcode=s'     => \$options{'barcode'},
        'uploadId=i'    => \$options{'uploadId'},
        'bamFileId=i'   => \$options{'bamFileId'},
        'laneId=i'      => \$options{'laneId'},
        'sampleType=i'  => \$options{'sampleType'},

        'runMode=s'    => \$options{'runMode'},
        'verbose'      => \$options{'verbose'},
        'version'      => sub {
            print "upload-cghub-fastq.pl v$Bio::SeqWare::Uploads::CgHub::Fastq::VERSION\n";
            exit 1;
        },
        'help'         => sub {
            pod2usage( { -verbose => 2, -exitval => 1 });
        },
    ) or pod2usage( { -verbose => 1, -exitval => 2 });

    return \%options
}