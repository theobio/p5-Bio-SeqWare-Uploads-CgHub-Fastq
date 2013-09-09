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

Version 0.000.016

=cut

# Actual version should be the same as the base module.
our $VERSION = $Bio::SeqWare::Uploads::CgHub::Fastq::VERSION;

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
        --dbUser           SeqWare database login user name
        --dbPassword       SeqWare database login password
        --dbSchema         SeqWare database name
        --dbHost           SeqWare database host machine
        --dataRoot         Seqware data root dir.
        --uploadFastqDir   Data root for meta-data for uploaded fastqs
        --uploadBamBaseDir Data root for meta-data for uploaded bams

        # Run mode
        --runMode     "ZIP" | "META"   | "VALIDATE"
                            | "UPLOAD" | "ALL" (default)

        # Content and locations
        --xmlSchema        SRA schema name, sub-dir for templates
        --templateBaseDir  Root dir for templates, contains xmlSchema dirs

        # Validation rules
        --minFastqSize  Error if fastq size < this, in bytes
        --rerun         Deletes output file on collision instead of failing

        # Other parameters
        --verbose     Print status messages while running
        --version     Report version and exit
        --help        Show this message

=cut

=head1 DESCRIPTION

Each sample that has already had its bam file uploaded to cgHub is processed
through this script to zip and upload its fastq files. Normally run by cron,
this script reads some of its options from the seqware config file
(./seqware/settings) on the machine hosting the cron. All options, including the
config file options, can be over-ridden from the command line. No command line
option is required, but the runMode option is often used.

Zipping and uploading fastq files to cgHub is broken into four separate tasks
each of which has its own C<--runMode>. The default runMode is "ALL" which runs
each of the four tasks in turn: "ZIP" "META", "VALIDATE", and then "SUBMIT".
Each mode can run independently, and will only run on samples where the prior
mode succeeded. The initial "ZIP" mode only runs on samples that have had their
bam files successfuly upload. To signal completion of each step for a sample,
the status of the CGHUB_FASTQ upload record is set to <step>_finished, e.g. to
"zip_finished". when any step, except ZIP is run, it figures out what to work
on by looking for an upload reacord with target CGHUB_FASTQ and status
"<parent_step>_finished" and claims it by setting its status to
"<step>_running". When done every step changes this same records status to
<step>_completed, or to <step>_failed_<error_name> if fails. The first step
decides in needs to run based on the existance of an upload record with targed
"CGHUB" and an external status of "live". Zip then claim this sample for
processing by inserting a new record for a sample with target "CGHUB_FASTQ"
and status of "zip_running".

The database tracks processing state for every sample, and processing changes
are made atomically (finding a record and setting its state are done within a
transaction) this program can be run in parallel. Each parallel run only
works on one step of one sample at a time, and will only work on a sample-step
that is not already running.

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

Generates the meta-data for upload for this sample. The data from the previous
upload of a bam file is used for the experiment.xml and run.xml. The
analysis.xml file is generated by filling in a template installed to the
shared distro directory:

  Bio-Seqware-Uploads-CgHub-Fastq/SRA_1-5/analysis_fastq.xml.template.

When running META mode, looks for an upload record with target "CGHUB_FASTQ"
and status "zip_completed", and selects it by changing the status to
"meta_running". When complete, changes the status to "meta_completed". Errors
should change the status to meta_failed_<reason>, but that is not fully
implemented. Probably just leaves them hanging as meta_running. This step
should not take more than a minute to run.

=item VALIDATE

Validates the meta-data associated with this sample. Uses an external program
to check a website and validate the data against existing data and well-formed
constraints.

When running VALIDATE mode, looks for an upload record with target "CGHUB_FASTQ"
and status "meta_completed", and selects it by changing the status to
"validate_running". When complete, changes the status to "validate_completed".
Errors should change the status to "validate_failed<reason>" but that is not
fully implemented. Probably just leaves them hanging as validate_running. This
step should not take more than a minute to run.

=item SUBMIT

Does the upload.

=item ALL

Run all steps, in order ("ZIP", "META", "VALIDATE", "SUBMIT").

=back

=item --minFastqSize

The size in bytes indicating that the file being zipped is probably too small
to really be a valid fastq file. The default is 10 MB. Generally the files are
1000 times that size (10 GB).

=item --xmlSchema

The name of the schema to generate, including the version. This determines
the directory of templates used and may affect the data gathering process.
Currently only the default is avaialble: SRA_1-5

=item --templateBaseDir

Allows selecting a directory to look in for templates. The default is the
shared module directory (see --runMode META). The templates are expected to be
in a subdir as specified by the --xmlSchema, i.e. the analysis.xml template
would be in <templateBaseDir>/<xmlSchema>.

=item --analysisTemplateName

I<Not implemented>

=item --rerun

I<Not implemented> Will allow rerunning by over-riding previously created.

=item --verbose

If set, causes sql queries and data values to print at each stage. Off by
default.

=item --sampleId INT

Filter sample to upload by the database sample_id.

=item --sample STRING, --sampleTitle STRING

Filter sample to upload by the tcga descriptive id.

=item --sampleAccession INT

Filter sample to upload by the UNC database record accession.

=item --sampleAlias STRING

Filter sample to upload by the alternate "alias" name.

=item --sampleType STRING, --tumorType STRING

Filter sample to upload by the type group of the sample, e.g. "BRCA"

=item --sampleUuid STRING

Filter sample to upload by the tcga uuid.

=item --uploadId INT

Filter sample to upload by upload id.

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

        # Select for upload, default is to use none.
        'sampleId=i'             => \$opt{'sampleId'},
        'sampleTitle|sample=s'   => \$opt{'sampleTitle'},
        'sampleAccession=i'      => \$opt{'sampleAccession'},
        'sampleAlias=s'          => \$opt{'sampleAlias'},
        'sampleType|tumorType=s' => \$opt{'sampleType'},
        'sampleUuid=s'           => \$opt{'sampleUuid'},
#        'uploadId=i'                => \$opt{'uploadId'},
#        'uploadTarget=s'            => \$opt{'uploadTarget'},
#        'uploadStatus=s'            => \$opt{'uploadStatus'},
#        'uploadExternalStatus=s'    => \$opt{'uploadExternalStatus'},
#        'uploadMetadataDir=s'       => \$opt{'uploadMetadataDir'},
#        'uploadCgHubAnalysisUuid=s' => \$opt{'uploadCgHubAnalysisUuid'},
#
#        'fastq'  => \$opt{'fastq'},
#        'v2'     => \$opt{'v2'},

        'uploadFastqBaseDir=s' => \$opt{'uploadFastqBaseDir'},
        'uploadBamBaseDir=s'   => \$opt{'uploadBamBaseDir'},
        'dataRoot=s'           => \$opt{'dataRoot'},

        'minFastqSize=i'       => \$opt{'minFastqSize'},
        'rerun'                => \$opt{'rerun'},
        'runMode=s'            => \$opt{'runMode'},

        'xmlSchema=s'          => \$opt{'xmlSchema'},
        'templateBaseDir=s'    => \$opt{'templateBaseDir'},

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