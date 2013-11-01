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

Version 0.000.026

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
        --runMode     "ZIP" | "META"   | "VALIDATE" | "SUBMIT_META" |
                      "SUBMIT_FASTQ" | "LIVE" | "ALL" (default)

        # Content and locations
        --xmlSchema        SRA schema name, sub-dir for templates
        --templateBaseDir  Root dir for templates, contains xmlSchema dirs

        # Sample selection
        --sampleId               sample.sample_id
        --sampleTitle|sample     The TCGA sample name
        --sampleAccession=i'     The seqware sample accession id
        --sampleAlias=s'         The pipe id for the sample
        --sampleType|tumorType   The disease type abbreviation
        --sampleUuid=s'          The TCGA uuid for the sample

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

Zipping and uploading fastq files to cgHub is broken into multiple separate
tasks, each of which has its own C<--runMode>. The default runMode is "ALL"
which runs each of the tasks in turn: "ZIP" "META", "VALIDATE", "UPLOAD_META",
"UPLOAD_FASTQ", and "LIVE". Each mode can run independently, and will only run
on samples where the prior mode succeeded.

The database tracks processing state for every sample, and processing changes
are made atomically (finding a record and setting its state are done within a
transaction) this program can be run in parallel. Each parallel run only
works on one step of one sample at a time, and will only work on a sample-step
that is not already running.

The initial "ZIP" mode only runs on
samples that have had their bam files successfuly upload. To signal completion
of each step for a sample, the status of the CGHUB_FASTQ upload record is set
to E<lt>stepE<gt>_completed, e.g. to "zip_completed". When any step, except ZIP is run,
it figures out what to work on by looking for an upload reacord with target
CGHUB_FASTQ and status "E<lt>parent_stepE<gt>_completed" and claims it by setting its
status to "E<lt>runModeE<gt>_running". When done every step changes this same record's
status to E<lt>stepE<gt>_completed, or to E<lt>stepE<gt>_failed_E<lt>error_nameE<gt> if the step failed.

The initial (ZIP) step is special. It decides it needs to run based on the
existance of an upload record with targed "CGHUB" and an external status of
"live" and was part of a specific upload block of bams (as identified by upload
directroy /datastore/tcga/cghub/v2_uploads). Zip then claim such a sample for
processing by inserting a new record for a sample with target "CGHUB_FASTQ"
and status of "zip_running".

The RERUN mode is special and merely clears a sample that is more than
--rerunWait days old that filed in a previous attempt to upload. It does not
actually rerun the sample, but clears everything to allow the sample to be
seletected again for upload. If RERUN fails, it will tag the sample with
failed_rerun_E<lt>messageE<gt>. Note, this counts as a sample to rerun, after enough
time passes. To prevent a samle rerunning, change its status to something not
containing 'fail' or 'failed', like "FAIL" or, better yet, 'BAD'.
=cut

=head1 DATABASE

=head2 Table: upload

The upload table records are expected to describe the current status of sample
upload events. Each upload event is associated with a specific sample and is
targeted to a specific location (CGHUB, CGHUB_FASTQ, etc). Uploads go through a
variety of internal states during the process of uploading. After uploading
they also have an external status based on the visibility of the sample at the
target location. The location on the file system where the uploaded metadata and
links to uploaded data files are stored is recored. The path is represented as
a data root and (within that directory) a separate directory for each uploaded
sample. The sample directory is a 'uuid' and is a unique global identifier for
an upload. It is expected that this will be used both by us and by the upload
target site.

=over 3

=item upload_id INT

The primary key for this record. This is also used in the uplaod_file linker
table to allow many to many associations with file records. An upload event may
be associated with more than one file, and it is possible that the same file
may be assoicated with more than one upload event.

=item sample_id INT

A foreign key reference to the sample associated with this upload event. One and
only one sample can be associated with a given upload event. This does not
allow for the upload of "sample set" files that summarize data across multiple
samples. [TODO: An additional upload_set table would be required to support that.]

=item target TEXT

The name of the target for this upload. This is an abstract name used to
identifiy an upload "protocol" for a type of uploads. Examples include "CGHUB"
and "CGHUB_FASTQ"

=item status TEXT

This field represents the internal status for the upload. The meaning of this
status depends on the upload.target. For CGHUB_FASTQ it is used as a place to
coordinate "job-state" and to manage the sub-tasks of an upload process. It
consists of two parts: E<lt>task_stageE<gt>_E<lt>statusE<gt>. Task stages can be "zip", "meta",
"validate", "submit-meta", "submit-fastq", "rerun" or "live" (in that order),
status can be "running", "completed", "failed" or "bad". If status is "failed"
or "bad", there is a third component giving a reason for failure. The status
changes from E<lt>parent-stageE<gt>_completed to E<lt>child-stageE<gt>_running, and then to
either E<lt>child-stageE<gt>_completed or E<lt>child_stageE<gt>_failed_E<lt>reasonE<gt> as each step in
the upload process runs and completes. The status "live_completed" or
E<lt>stageE<gt>_bad_E<lt>reasonE<gt> is the final status for all CGHUB_FASTQ submissions. An
example failure status might be "zip_failed_fastq_md5_mismatch". [TODO: A better
means of associating error messages with upload records is needed]. If the
status contains "...fail...", then after a suitable period, the sample will
automatically be rerun. To prevent that, manual review of failed sample is
required, with the status being changed to "...bad...", indicating no upload
for a given sample to a given target is possible.

=item external_status TEXT

This field represents the external status of the sample upload as visible at
the target. This is validated by querying the external site directory. A status
of "live" indicates the record is present. A missing status (NULL) indicates the
data has not be checked for at the external site. This is usually because the
upload is not done or has been completed too recently for the data to be visible
at the remote site. There is usually a visibility delay because of the remote
site's internal process. A message of "recheck-waiting" means a check was
performed, but nothing was recieved. Other external_status values can indicate
various error states, but this is redundant with the status of
E<lt>liveE<gt>_failed_E<lt>error-message>.

=item metadata_dir FULL-PATH-DIR

The base directory where the files and links generated during the upload process
are kept. Each upload event will have its own sub-directory (see cghub_analysis_id).

=item cghub_analysis_id UUID

The uusd identifying this sample x target upload event in a global unique way.
Also used as the specific directory for the files and links generated during the
upload process. A subdirectory within upload.metadata_dir.

=item tstmp TIMESTAMP

A timestamp for the last time this record was created or updated. Automatically
set on insert and changed on update. Used to determine intervals for rerun and
for detecting hung processes where state of "running" has persisted too long.
[TODO implement this].

=back

=cut

=head1 OPTIONS

=head2 The --runMode option

=over 3

=item --runMode

The C<--runMode> parameter detemines what this program does when invoked, and
can be any of the following 5 values. (default is "ALL").

=over 4

=item  ZIP

Generate a zip/tar archive of the single or paired fastq files for a sample,
Updates the database with appropriate information.

Needs to identify the workflow_runIids for the uploaded bam files,
      "--select file_ids for fastqs from workflow_runs where the mapsplice files\n"
    . "--have been uploaded but the fastq zip file has not be created"

select vf.file_id from vw_files where
   vw.workflow_run_id in (select workflow_run id where bams are uploaded)
   AND vw.workflow_run_id not in (select workflow_run id where fastq-zips are uploaded)
   AND ...

=item META

Generates the meta-data for upload for this sample. The data from the previous
upload of a bam file is used for the experiment.xml and run.xml. The
analysis.xml file is generated by filling in a template installed to the
shared distro directory:

  Bio-Seqware-Uploads-CgHub-Fastq/SRA_1-5/analysis_fastq.xml.template.

When running META mode, looks for an upload record with target "CGHUB_FASTQ"
and status "zip_completed", and selects it by changing the status to
"meta_running". When complete, changes the status to "meta_completed". Errors
should change the status to meta_failed_E<lt>reasonE<gt>, but that is not fully
implemented. Probably just leaves them hanging as meta_running. This step
should not take more than a minute to run.

=item VALIDATE

Validates the meta-data associated with this sample. Uses an external program
to check a website and validate the data against existing data and well-formed
constraints.

When running VALIDATE mode, looks for an upload record with target "CGHUB_FASTQ"
and status "meta_completed", and selects it by changing the status to
"validate_running". When complete, changes the status to "validate_completed".
Errors should change the status to "validate_failed_E<lt>reasonE<gt>" but that is not
fully implemented. Probably just leaves them hanging as validate_running. This
step should not take more than a minute to run.

=item SUBMIT-META

Performs the upload of the meta data with cgsubmit

=item SUBMIT-FASTQ

Performs the upload of the fastq file with gtsubmit

=item LIVE

Checks if data is visible to the community, updates external_status to "live" or
"recheck-waiting" or "failed_live_...". Only checks records for target
CGHUB-FASTQ and status "submit-fastq_completed". Changes status to live_running
while checking, and then to live_completed, live_waiting, or failed_live.

=item ALL

Run all steps, in order: "ZIP", "META", "VALIDATE", "SUBMIT-META", "SUBMIT-FASTQ", "LIVE".

=back

=back

=head2 Processing options

=over 3

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
would be in E<lt>templateBaseDirE<gt>/E<lt>xmlSchemaE<gt>.

=item --analysisTemplateName

I<Not implemented>

=item --rerun

I<Not implemented> Will allow rerunning by over-riding previously created.

=item --recheckWaitHours 24

Delay before retrying something that should be tried again automatically
without review, but depends on some external process that may or may not update
on a regular schedule. i.e. checking to see if a sample is live; see
C<--runmode LIVE> above. The default value is 24 hours.

=item --verbose

If set, causes sql queries and data values to print at each stage. Off by
default.

=back

=head2 Selection options.

Whichever stage is run, this will limit selection of what to process to
input that meets ALL critera specified. By default none of these are are
used.

=over 3

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

=item --rerunWait INT

Waiting time (in hours) before attempting atuo rerun. Default is 168 (7 days).

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
        'recheckWaitHours' => 24,
        'rerunWait'        => 168,
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
        'rerunWait'            => \$opt{'rerunWait'},
        'runMode=s'            => \$opt{'runMode'},
        'recheckWaitHours'     => \$opt{'recheckWaitHours'},

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