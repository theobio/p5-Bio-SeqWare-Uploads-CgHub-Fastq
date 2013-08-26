#! /usr/bin/env perl

use warnings;
use strict;

use Pod::Usage;
use Getopt::Long;
use Data::Dumper;
use File::ShareDir qw(dist_dir);

use Bio::SeqWare::Config;
use Bio::SeqWare::Uploads::CgHub::Fastq;

=head1 NAME

upload-cghub-fastq - Zip and upload fastq files to cghub.

=cut

=head1 VERSION

Version 0.000.003   PRE_RELEASE

=cut

our $VERSION = $Bio::SeqWare::Uploads::CgHub::Fastq::VERSION;   # PRE-RELEASE

# TODO: consider allow pre-parsing cli parameters for config file name.

my $configParser = Bio::SeqWare::Config->new();
my $configOptions = $configParser->getKnown();

my $opt = _processCommandLine( $configOptions );
if ($opt->{'verbose'}) {
    print Dumper( $opt );
}

# Main program run loop, wrapped in top level error handler.
# $self->{'error'} should be set before any untrapped die or croak.
my $instance;
eval {
    $instance = Bio::SeqWare::Uploads::CgHub::Fastq->new( $opt );
    $instance->run();
};
if ($@) {
    my $error = $@;
    chomp $error;
    if ( $instance->{'error'} ) {
        die "ERROR - \"$instance->{'error'}\" detected;\n$error\n";
    }
    else {
        die "FATAL ERROR - \"unexpected_failure\". Program died with unexpected error;\n$error\n";
    }
}


=head1 SYNOPSIS

upload-cghub-fastq [options]

    Options:

        # Parameters normally from config file
        --dbUser      SeqWare database login user name
        --dbPassword  SeqWare database login password
        --dbSchema    SeqWare database name
        --dbHost      SeqWare database host machine
        --dataRoot    Seqware data root dir. All data under here
                          (like $dataRoot/flowcell/workflow/...)
        --uploadFastqDir Data root for upload file archiving

        # Validation rules
        --minFastqSize  Error if fastq size < this, in bytes
        --rerun         Deletes output file on collision instead of failing

        # Selection constraints [NOT CURRENTLY IMPLEMENTED]
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

Each sample that has already had its bam file uploaded to cgHub is processed
through this script to zip and upload its fastq files. Normally run by cron,
this script reads some of its options from the seqware config file
(./seqware/settings) on the machine hosting the cron. All options, including the
config file options, can be over-ridden from the command line. No command line
option is required.

Zipping and uploading fastq files to cgHub is broken into four separate tasks
each of which has its own C<--runMode>. The default runMode is "ALL" which runs
each of the four tasks in turn: "ZIP" "META", "VALIDATE", and then "SUBMIT".


=cut

=head1 OPTIONS

=over 4

=item runMode

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

=item SUBMIT

Does the upload.

=item ALL

Run all steps, in order ("ZIP", "META", "VALIDATE", "SUBMIT").

=back

=back

=cut

sub _processCommandLine {

    # Values from vconfig file
    my $configOptionsHR = shift;

    # Local defaults
    my $optionsHR = {
        'minFastqSize'     => 10 * 1000 * 1000,
        'runMode'          => 'ALL',
        'xmlSchema'        => 'SRA_1-5',
        'templateBaseDir'  => dist_dir('Bio-SeqWare-Uploads-CgHub-Fastq'),
    };

    # Combine local defaults with ()over-ride by) config file options
    my %opt = ( %$optionsHR, %$configOptionsHR );

    # Record command line arguments
    $opt{'argv'} = [ @ARGV ];

    # Override local/config options with command line options
    GetOptions(
        'dbUser=s'     => \$opt{'dbUser'},
        'dbPassword=s' => \$opt{'dbPassword'},
        'dbHost=s'     => \$opt{'dbHost'},
        'dbSchema=s'   => \$opt{'dbSchema'},

        'uploadFastqBaseDir=s' => \$opt{'uploadFastqBaseDir'},
        'uploadBamBaseDir=s'   => \$opt{'uploadBamBaseDir'},
        'dataRoot=s'           => \$opt{'dataRoot'},
        'minFastqSize=i'       => \$opt{'minFastqSize'},
        'rerun'                => \$opt{'rerun'},
        'runMode=s'            => \$opt{'runMode'},

        'xmlSchema=s'          => \$opt{'xmlSchema'},
        'templateBaseDir=s'    => \$opt{'templateBaseDir'},

#        'sample=s'      => \$opt{'sample'},
#        'flowcell=s'    => \$opt{'flowcell'},
#        'lane=i'        => \$opt{'lane'},
#        'lane_index=i'  => \$opt{'lane_index'},
#        'barcode=s'     => \$opt{'barcode'},
#        'uploadId=i'    => \$opt{'uploadId'},
#        'bamFileId=i'   => \$opt{'bamFileId'},
#        'laneId=i'      => \$opt{'laneId'},
#        'sampleType=i'  => \$opt{'sampleType'},

        'verbose'      => \$opt{'verbose'},
        'version'      => sub {
            print "upload-cghub-fastq.pl v$VERSION\n";
            exit 1;
        },
        'help'         => sub {
            pod2usage( { -verbose => 2, -exitval => 1 });
        },

    ) or pod2usage( { -verbose => 1, -exitval => 2 });

    return \%opt;
}