package Bio::SeqWare::Uploads::CgHub::Fastq;

use 5.014;         # Eval $@ safe to use.
use strict;        # Don't allow unsafe perl constructs.
use warnings;      # Enable all optional warnings.
use Carp;          # Base the locations of reported errors on caller's code.
# $Carp::Verbose = 1;
use Data::Dumper;  # Quick data structure printing
use Time::HiRes qw( time );      # Epoch time with decimals
use Text::Wrap qw( wrap );       # Wrapping of text in paragraphs.
$Text::Wrap::columns = 132;      #    Wrap at column 132
$Text::Wrap::huge = 'overflow';  #    Don't break words >= 132 characters

use File::Spec;                   # Normal path handling
use File::Path qw(make_path);     # Create multiple-directories at once
use File::Copy qw(cp);            # Copy a file
use File::ShareDir qw(dist_dir);  # Access data files from install.
use Cwd;                          # get current working directory.

use DBI;
use Template;

use Bio::SeqWare::Config;                  # Read the seqware config file
use Bio::SeqWare::Db::Connection 0.000002; # Dbi connection, with parameters

=head1 NAME

Bio::SeqWare::Uploads::CgHub::Fastq - Support uploads of fastq files to cghub

=cut

=head1 VERSION

Version 0.000.014

=cut

our $VERSION = '0.000014';

=head1 SYNOPSIS

    use Bio::SeqWare::Uploads::CgHub::Fastq;

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new( $paramHR );
    $obj->run();
    $obj->run( "ZIP" );

=cut

=head1 DESCRIPTION

Supports the upload of zipped fastq file sets for samples to cghub. Includes
db interactions, zip command line convienience functions, and meta-data
generation control. The meta-data uploads are a hack on top of a current
implementation.

=head2 Conventions

Errors are reported via setting $self->{'error} and returning undef.

Any run mode can be repeated; they should be self-protecting by persisting
approriate text to the upload record status as <runMode>_<running|completed|failed_<message>>.

Each runmode should support the --rerun flag, eventually. That probably
requires separating the selection and the processing logic, with --rerun only
supported by the processing logic.

=cut

=head1 CLASS METHODS

=cut

=head2 new()

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new( $paramHR );

Creates and returns a Bio::SeqWare::Uploads::CgHub::Fastq object. Takes
a hash-ref of parameters, each of which is made avaialble to the object.
Don't use parameters beging with a _ (underscore). These may be overwritten.
The parameter 'error' is cleared automatically, 'myName' is set to
"upload-cghub-fastq_$VERSION" where version is the version of this module,
like 0.000007"

=cut

sub new {
    my $class = shift;
    my $param = shift;
    unless (defined $param && ref( $param ) eq 'HASH') {
        croak( "A hash-ref parameter is required." );
    }
    my %copy = %$param;
    my $self = {
        'error'   => undef,
        'myName' => 'upload-cghub-fastq_0.0.1',

        '_laneId'         => undef,
        '_sampleId'       => undef,
        '_fastqUploadId'  => undef,
        '_fastqs'         => undef,
        '_zipFile'        => undef,
        '_zipMd5Sum'      => undef,
        '_zipFileId'      => undef,
        '_fastqProcessingId'      => undef,
        '_fastqWorkflowAccession' => undef,
        %copy,
    };
    bless $self, $class;

    return $self;
}

=head2 getUuid()

=cut

sub getUuid() {
    my $class = shift;
    my $uuid;

    $uuid = `uuidgen`;
    if ($?) {
        croak ('ERROR: `uuidgen` exited with error, exit value was: ' . $?);
    }
    if (! defined $uuid ) {
        croak( 'ERROR: `uuidgen` failed silently');
    }

    chomp $uuid;
    return $uuid;
}

=head2 reformatTimeStamp()

    Bio::SeqWare::Uploads::CgHub::Fastq->reformatTimeStamp( $timeStamp );

Takes a postgresql formatted timestamp (without time zone) and converts it to
an aml time stamp by replacing the blank space between the date and time with
a capital "T". Expects the incoming $timestamp to be formtted as
C<qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}\d{2}\.?\d*$/>

=cut

sub reformatTimeStamp() {
    my $class = shift;
    my $postgresTimestampWithoutTimeZone = shift;
    if ($postgresTimestampWithoutTimeZone !~ /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.?\d*$/ ) {
        croak( "Incorectly formatted time stamp: $postgresTimestampWithoutTimeZone\n"
             . ' expected 24 hour fromat like "YYYY-MM-DD HH:MM:SS.frac'
              . " with optional part. No other spaces allowed.\n"
        );
    }

    my $xmlFormattedTimeStamp = $postgresTimestampWithoutTimeZone;
    $xmlFormattedTimeStamp =~ s/ /T/;

    return  $xmlFormattedTimeStamp;
}

=head2 getFileBaseName

   my ($base, $ext) =
       Bio::SeqWare::Uploads::CgHub::Fastq->getFileBaseName( "$filePath" );

Given a $filePath, extracts the filename and returns the file base name $base
and extension $ext. Everything up to the first "."  is returned as the $base,
everything after as the $ext. $filePath may or may not include directories,
relative or absolute, but the last element is assumed to be a filename (unless
it ends with a directory marker, in which case it is treated the same as if
$filePath was ""). If there is nothing before/after the ".", an empty string
will be returned for the $base and/or $ext. If there is no ., $ext will be
undef. Directory markers are "/", ".", or ".." on Unix

=head3 Examples:

             $filePath       $base        $ext
    ------------------  ----------  ----------
       "base.ext"           "base"       "ext"
       "base.ext.more"      "base"  "ext.more"
            "baseOnly"  "baseOnly"       undef
           ".hidden"            ""    "hidden"
       "base."              "base"          ""
           "."                  ""          ""
                    ""          ""       undef
                 undef      (dies)            
    "path/to/base.ext"      "base"       "ext"
   "/path/to/base.ext"      "base"       "ext"
    "path/to/"              ""           undef
    "path/to/."             ""           undef
    "path/to/.."            ""           undef

=cut

sub getFileBaseName {

    my $class = shift;
    my $path = shift;

    if (! defined $path) {
        croak "ERROR: Undefined parmaeter, getFileBaseName().\n";
    }

    my ($vol, $dir, $file) = File::Spec->splitpath( $path );
    if ($file eq "") {
        return ("", undef);
    }
    $file =~ /^([^\.]*)(\.?)(.*)$/;
    my ($base, $ext);
    if ($2 eq '.') {
        ($base, $ext) = ("", "");
    }
    if ($1) {
        $base = $1;
    }
    if ($3) {
        $ext = $3;
    }
    return ($base, $ext);
}

=head1 INSTANCE METHODS

=cut

=head2 run()

    $obj->run();
  # or
    $obj->run( $runMode );
    # $runMode one of: ZIP META VALIDATE SUBMIT_META SUBMIT_FASTQ ALL
  # or
    $obj->run( $runMode, $dbh );
  # or
    $obj->run( undef, $dbh );

This is the "main" program loop, associated with running C<upload-cghub-fastq>
This method can be called with or without a parameter. If called with no
$runmode, it uses the current value of the instance's 'runMode' property. All
allowed values for that parameter are supported here: case insenistive "ZIP",
"META", "VALIDATE", "SUBMIT_META", "SUBMIT_FASTQ" and "ALL". Each parameter
causes the associated "do..." method to be invoked, although "ALL"" causes
each of the 5 do... methods to be invoked in order as above.

This method will either succeed and return 1 or set $self->{'error'}
and die. If an upload record id is known when an error occurs, the upload.status
field for that upload record will be updated to "$STAGE_failed_$ERROR", where
$STAGE is the stage in which the error occurs, and $ERROR is $self->{'error'}.

This method calls itself to allow nested processing, and allows passing a
database handle as a parameter to support that. If not provided, a connection
will be created. Due to the length of some of the processing, the passed
connection may be invalid an fail.
=cut

sub run {
    my $self = shift;
    my $runMode = shift;
    my $dbh = shift;

    # Validate runMode parameter
    if (! defined $runMode) {
        $runMode = $self->{'runMode'};
    }
    if (! defined $runMode || ref $runMode ) {
        $self->{'error'} = "failed_run_param_mode";
        croak "Can't run unless specify a runMode.";
    }
    $runMode = uc $runMode;

    # Database connection = from param, or else from self, or else get new one.
    if (! defined $dbh) {
        $dbh = $self->{'dbh'};
    }
    if (! defined $dbh ) {
        eval {
            my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $self );
            if (! defined $connectionBuilder) {
                $self->{'error'} = "failed_run_constructing_connection";
                croak "Failed to create Bio::SeqWare::Db::Connection.\n";
            }

            print ("DEBUG: " . Dumper($connectionBuilder));
            $dbh = $connectionBuilder->getConnection(
                 {'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1, 'ShowErrorStatement' => 1}
            );
        };
        if ($@ || ! $dbh) {
            $self->{'error'} = "failed_run_db_connection";
            croak "Failed to connect to the database $@\n$!\n";
        }
    }

    # Allow UUID to be provided, basically for testing as this is a random value.
    if (! $self->{'_fastqUploadUuid'}) {
        $self->{'_fastqUploadUuid'} = Bio::SeqWare::Uploads::CgHub::Fastq->getUuid();
    }
    if (! $self->{'_fastqUploadUuid'} =~ /[\dA-f]{8}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{12}/i) {
         $self->{'error'} = 'bad_uuid';
         croak( "Not a valid uuid: $self->{'_fastqUploadUuid'}" );
    }
    $self->sayVerbose("Starting run for $runMode.");
    $self->sayVerbose("Analysis UUID = $self->{'_fastqUploadUuid'}.");



    # Run as selected.
    eval {
        if ( $runMode eq "ALL" ) {
            $self->run('ZIP', $dbh);
            $self->run('META', $dbh);
            $self->run('VALIDATE', $dbh);
            $self->run('SUBMIT_META', $dbh);
            $self->run('SUBMIT_FASTQ', $dbh);
        }
        elsif ($runMode eq "ZIP" ) {
            $self->doZip( $dbh );
        }
        elsif ($runMode eq "META" ) {
            $self->doMeta( $dbh );
        }
        elsif ($runMode eq "VALIDATE" ) {
            $self->doValidate( $dbh );
        }
        elsif ($runMode eq "SUBMIT_META" ) {
            $self->doSubmitMeta( $dbh );
        }
        elsif ($runMode eq "SUBMIT_FASTQ" ) {
            $self->doSubmitFastq( $dbh );
        }
        else {
            $self->{'error'} = "failed_run_unknown_run_mode";
            croak "Illegal runMode \"$runMode\" specified.\n";
        }
    };

    if ($@) {
        my $error = $@;
        if ( $self->{'_fastqUploadId'})  {
            if (! $self->{'error'}) {
                $self->{'error'} = 'failed_run_unknown_error';
            }
            eval {
                $self->_updateUploadStatus( $dbh, $self->{'error'} );
            };
            if ($@) {
                $error .= " ALSO: Did not update UPLOAD: $self->{'_fastqUploadId'}\n";
            }
        }
        eval {
            $dbh->disconnect();
        };
        if ($@) {
            $error .= " ALSO: error disconnecting from database: $@\n";
        }
        if (! $self->{'error'}) {
            $self->{'error'} = 'failed_run_unknown_error';
        }
        croak $error;
    }
    else {
        $dbh->disconnect();
        if ($@) {
            my $error .= "$@";
            warn "Problem encountered disconnecting from the database - Likely ok: $error\n";
        }
        $self->sayVerbose("Finishing run for $runMode.");
        return 1;
    }
}

=head2 doZip()

 $obj->doZip( $dbh );

Initially identifies a lane for needing fastqs uploaded and tags it as running.
Then it creates the basic upload directory, retrieves upload meta data files
from prior runs, identifies the fastq files from that lane to uplaod, zips them,
makes appropriate entires in the seqware database, and then indicates when
done.

Either returns 1 to indicated completed successfully, or undef to indicate
failure. Check $self->{'error'} for details.

The status of this lanes upload is visible externally through the upload
record with target = CGHUB_FASTQ and status 'zip-running', 'zip-completed', or
'zip_failed_<error_message>'. Possible error states the upload record could be
set to are:

'zip_failed_no_wfID', 'zip_failed_missing_fastq' 'zip_failed_tiny_fastq',
'zip_failed_fastq_md5', 'zip_failed_gzip_failed', 'zip_failed_unknown'
'zipval_error_missing_zip', 'zipval_error_tiny_zip', 'zipval_error_md5',
'zipval_error_file_insert' 'zipval_error_unknown'

=over

=item 1

Identify a lane to zip. If none found, exits, else inserts a new upload record
with C< target = 'CGHUB' and status = 'zip_running' >. The is done as a
transaction to allow parallel running. For a lane to be selected, it must have
an existing upload record with C< target = 'CGHUB' and external_status = 'live' >.
That upload record is linked through a C< file > table record via the C< vw_files >
view to obtain the lane. If there are any upload records associated with the
selected lane that have C< target = CGHUB_FASTQ >, then that lane will not
be selected.

=item 2

Once selected, a new (uuidgen named) directory for this upload is created in
the instance's 'uploadDataRoot' directory. The previously generated
C<experiment.xml> and C<run.xml> are copied there. The analysis.xml is not
copied as it will be recreated by the META step. The upload record is
modified to record this new information, or to indicate failure if it did not
work.

B<NOTE>: Using copies of original xml may not work for older runs as they may
have been consitent only with prior versions of the uplaod schema.

=item 3

The biggest problem is finding the fastq file or files for this lane. Since
there is no way to directly identify the input fastq files used by the
Mapsplice run, the assumption is made that The file/s generated by
any completed FinalizeCasava run for this lane, if present, are used. IF not
present then the file/s generated by any completed srf2fastq run is used. If
neither are present, then an error is signalled and the upload record is updated.

=item 4

The fastq files identified are then validated on the system, and then tar/gzipped
to the spedified OutputDataRoot, in a subdirectory named for this program, named
like flowcell_lane_barcode.fastq.tar.zip. If errors occur, updates upload
status.

=item 5

When done, validate output, calculate md5 sum, insert a new file record and
new processing_files records (linking the new file and the processing_ids for
the  input fastq). Updates upload record to zip_completed to indicate done.

=back

=cut

sub doZip {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'failed_zip_param_doZip_dbh';
        croak ("doZip() missing \$dbh parameter.");
    }

    eval {
        $self->_tagLaneToUpload($dbh, "zip_running" );
        $self->_getFilesToZip( $dbh );
        $self->_zip();
        $self->_insertFile( $dbh );
        $self->_insertUploadFileRecord( $dbh );
        $self->_updateUploadStatus( $dbh, $self->{'_fastqUploadId'}, "zip_completed");
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'unknown_error';
        }
        $self->{'error'} = 'failed_zip_' . $self->{'error'};
        croak $error;
    }

    return 1;
}

=head2 doMeta()

 $obj->doMeta();

From $obj, reads:
 _metaDataRoot      - Absolute path to some directory
 _fastqUploadId     - Id for new fastq upload record
 _mapSpliceUploadId - Id for old mapsplice record
 _uuidgenExec       - Executable for uuid generation
 _fastqzTemplateDir - Directory where analysis.xml template is.
 _realFileForUpload - Full path filename to fastqDir.

To $obj, adds
 _metaDataUuid   - The generated UUID used for this uploads meta-data.
 _metaDataPath   - Full path, = _metaDataRoot + __metaDataUuid
 _linkFileName   - The local name

=cut

sub doMeta {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'failed_meta_param_doMeta_dbh';
        croak ("doMeta() missing \$dbh parameter.");
    }

    eval {
        my $uploadHR = $self->_changeUploadRunStage( $dbh, 'zip_completed', 'meta_running' );
        if ($uploadHR)  {
            my $dataHR = $self->_getTemplateData( $dbh, $uploadHR->{'upload_id'} );
            $self->_makeFileFromTemplate( $dataHR, "analysis.xml",     "analysis_fastq.xml.template" );
            $self->_makeFileFromTemplate( $dataHR, "run.xml",               "run_fastq.xml.template" );
            $self->_makeFileFromTemplate( $dataHR, "experiment.xml", "experiment_fastq.xml.template" );
            $self->_updateUploadStatus( $dbh, $uploadHR->{'upload_id'}, "meta_completed");
        }
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'unknown_error';
        }
        $self->{'error'} = 'failed_meta_' . $self->{'error'};
        croak $error;
    }

    return 1;
}

=head2 = doValidate()

 $obj->doValidate();

=cut

sub doValidate() {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'failed_validate_param_doValidate_dbh';
        croak ("doValidate() missing \$dbh parameter.");
    }

    eval {
        my $uploadHR = $self->_changeUploadRunStage( $dbh, 'meta_completed', 'validate_running' );
        if ($uploadHR)  {
            my $ok = $self->_validateMeta( $uploadHR );
            $self->_updateUploadStatus( $dbh, $uploadHR->{'upload_id'}, "validate_completed");
        }
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'unknown_error';
        }
        $self->{'error'} = 'failed_validate_' . $self->{'error'};
        croak $error;
    }
    return 1;
}

=head2 = doSubmitMeta()

    $obj->doSubmitMeta();

=cut

sub doSubmitMeta() {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'failed_submit-meta_param_doSubmitMeta_dbh';
        croak ("doSubmitMeta() missing \$dbh parameter.");
    }

    eval {
        my $uploadHR = $self->_changeUploadRunStage( $dbh, 'validate_completed', 'submit-meta_running' );
        if ($uploadHR)  {
            my $ok = $self->_submitMeta( $uploadHR );
            $self->_updateUploadStatus( $dbh, $uploadHR->{'upload_id'}, "submit-meta_completed");
        }
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'unknown_error';
        }
        $self->{'error'} = 'failed_submit-meta_' . $self->{'error'};
        croak $error;
    }

    return 1;
}

=head2 = doSubmitFastq()

    $obj->doSubmitFastq();

=cut

sub doSubmitFastq() {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'failed_submit-fastq_param_doSubmitFastq_dbh';
        croak ("doSubmitFastq() missing \$dbh parameter.");
    }

    eval {
        my $uploadHR = $self->_changeUploadRunStage( $dbh, 'submit-meta_completed', 'submit-fastq_running' );
        if ($uploadHR)  {
            my $ok = $self->_submitFastq( $uploadHR );
            $self->_updateUploadStatus( $dbh, $uploadHR->{'upload_id'}, "submit-fastq_completed");
        }
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'unknown_error';
        }
        $self->{'error'} = 'failed_submit-fastq_' . $self->{'error'};
        croak $error;
    }

    return 1;
}

=head2 = getAll()

  my $settingsHR = $obj->getAll();
  
Retrieve a copy of the properties assoiciated with this object.
=cut

sub getAll() {
    my $self = shift;
    my $copy;
    for my $key (keys %$self) {
        # Skip internal only (begin with "_") properties
        if ($key !~ /^_/) {
            $copy->{$key} = $self->{$key};
        }
    }
    return $copy;
}


=head2 getTimeStamp()

    Bio::SeqWare::Uploads::CgHub::Fastq->getTimeStamp().
    Bio::SeqWare::Uploads::CgHub::Fastq->getTimeStamp( $unixTime ).

Returns a timestamp formated like YYYY-MM-DD_HH:MM:SS, zero padded, 24 hour
time. If a parameter is passed, it is assumed to be a unix epoch time (integer
or float seconds since Unix 0). If no parameter is passed, the current time will
be queried. Time is parsed through perl's localtime().

=cut

sub getTimeStamp {
    my $class = shift;
    my $time = shift;
    if (!$time) {
       $time = time();
    }
    my ($sec, $min, $hr, $day, $mon, $yr) = localtime($time);
    return sprintf ( "%04d-%02d-%02d_%02d:%02d:%02d",
                     $yr+1900, $mon+1, $day, $hr, $min, $sec);
}


=head1 INTERNAL METHODS

NOTE: These methods are for I<internal use only>. They are documented here
mainly due to the effort needed to separate user and developer documentation.
Pay no attention to code behind the underscore. These are not the methods you are
looking for. If you use these function I<you are doing something wrong.>

=cut

=head2 _tagLaneToUpload()

    $self->_tagLaneToUpload( $dbh );

=cut

sub _tagLaneToUpload {
    my $self = shift;
    my $dbh = shift;
    my $newUploadStatus = shift;

    unless ($dbh) {
        $self->{'error'} = "param__tagLaneToUpload_dbh";
        croak ("_tagLaneToUpload() missing \$dbh parameter.");
    }

    if (! $newUploadStatus) {
        $self->{'error'} = "param__tagLaneToUpload_newUploadStatus";
        croak ("_tagLaneToUpload() missing \$newUploadStatus parameter.");
    }

    eval {
        # Transaction to ensures 'find' and 'tag as found' occur in one step,
        # allowing for parallel running.
        $dbh->begin_work();
        $self->_findNewLaneToZip( $dbh );
        $self->_createUploadWorkspace();
        $self->_insertZipUploadRecord( $dbh, $newUploadStatus);
        $dbh->commit()
    };
    if ($@) {
        my $error = "Error selecting lane to run on: $@";
        eval {
            $dbh->rollback();
        };
        if ($@) {
            $error .= " ALSO: error rolling back tagLaneToUpload transaction: $@\n";
        }
        if (! $self->{'error'}) {
            $self->{'error'} = 'tagging_lane';
        }
        croak $error;
    }

    return 1;

}

=head2 _findNewLaneToZip()

Identifies a lane that needs its data uploaded to CgHub. To qualify for
uploading, a lane must have had a succesful bam file uploaded, and not have had
a fastq upload (succesful, unsuccesful, or in progress). A lane is uniquely
identified by its db lane.lane_id.

If a bam file was uploaded succesfully, an upload (u) record will exist with
fields C< u.lane_id == lane.lane_id >, C< u.target == "CGHUB" > and
C< u.external_status == 'live' >. This record also has a field C< u.sample_id >
which holds the sample.sample_id for the lane it represents.

IF a lane has ever been considered for fastq-upload, it will have an upload
record with fields C< u.lane_id == lane.lane_id > and C< u.target == "CGHUB" >.
The status of this record indicates what state the processing of this is in,
but don't care. Whatever state it is in, don't pick it up here.

If verbose is set, this will echo the SQL query used and the results obtained.

To prevent collisions with parallel runs, this query should be combined in a
transaction with and update to insert an upload record that tags a lane as
being processed for fastq upload (i.e. with upload.target = CGHUB_FASTQ).

Uses

    $self->{'verbose'} => echo SQL, set values

Sets

    '_laneId'           = vw_files.lane_id (joined with upload, upload_files)
    '_sampleId'         = upload.sample_id
    '_bamUploadId'      = upload.upload_id
    '_bamUploadBaseDir' = upload.metadata_dir
    '_bamUploadUuid'    = upload.cghub_analysis_id
    '_bamUploadDir'     = catdir( '_bamUploadBaseDir', '_bamUploadUuid' )

Errors

    zip_failed_lane_lookup => Db error when attempting SELECT query. Note:
                              emtpy result set ok (nothing to do).
    zip_failed_no_bam_upload_dir_found => No dir matches query result.

=cut

sub _findNewLaneToZip {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__findNewLaneToZip_dbh';
        croak ("_findNewLaneToZip() missing \$dbh parameter.");
    }

    # Setup SQL
    my $sqlTargetForMapspliceUpload = 'CGHUB';
    my $sqlExternalStatusForSuccesfulMapspliceUpload = 'live';
    my $sqlTargetForFastqUpload = 'CGHUB_FASTQ';

    my $selectionSQL =
       "SELECT vwf.lane_id, u.sample_id, u.upload_id, u.metadata_dir, u.cghub_analysis_id
        FROM vw_files AS vwf, upload_file AS uf, upload AS u
        WHERE vwf.file_id       = uf.file_id
          AND uf.upload_id      = u.upload_id
          AND u.target          = ?
          AND u.external_status = ?
          AND u.metadata_dir    = '/datastore/tcga/cghub/v2_uploads'
          AND vwf.sample_id NOT IN (
              SELECT u.sample_id
              FROM upload AS u
              WHERE u.target      = ?
          ) order by vwf.lane_id DESC limit 1";

    # Execute SQL
    my $rowHR;
    eval {
        my $selectionSTH = $dbh->prepare( $selectionSQL );
        $selectionSTH->execute(
            $sqlTargetForMapspliceUpload,
            $sqlExternalStatusForSuccesfulMapspliceUpload,
            $sqlTargetForFastqUpload
        );
        $rowHR = $selectionSTH->fetchrow_hashref();
        $selectionSTH->finish();
    };
    if ($@) {
        $self->{'error'} = "lane_lookup";
        croak "Lookup of new lane failed: $@";
    }

    if (! defined $rowHR) {
        $self->sayVerbose("No lanes to zip.");
        return 1;  # NORMAL RETURN - Can't find candidate lanes for zipping.
    }

    # Looks like we got data, so save it off.
    $self->{'_laneId'}           = $rowHR->{'lane_id'};
    $self->{'_sampleId'}         = $rowHR->{'sample_id'};
    $self->{'_bamUploadId'}      = $rowHR->{'upload_id'};
    $self->{'_bamUploadBaseDir'} = $rowHR->{'metadata_dir'};
    $self->{'_bamUploadUuid'}    = $rowHR->{'cghub_analysis_id'};
    $self->sayVerbose( "Found zip candidate"
            . ". " . "LANE: "                       . $self->{'_laneId'}
            . "; " . "SAMPLE: "                     . $self->{'_sampleId'}
            . "; " . "BAM UPLOAD_ID: "        . $self->{'_bamUploadId'}
            . "; " . "BAM UPLOAD_BASE_DIR: "  . $self->{'_bamUploadBaseDir'}
            . "; " . "BAM UPLOAD_UUID: "      . $self->{'_bamUploadUuid'}
    );

    unless (   $self->{'_laneId'}
            && $self->{'_sampleId'}
            && $self->{'_bamUploadId'}
            && $self->{'_bamUploadBaseDir'}
            && $self->{'_bamUploadUuid'}
    ) {
        $self->{'error'} = "lane_lookup_data";
        croak "Failed to retrieve lane, sample, and/or bam upload data.";
    }

    $self->{'_bamUploadDir'} = File::Spec->catdir(
        $self->{'_bamUploadBaseDir'}, $self->{'_bamUploadUuid'}
    );
    unless ( -d $self->{'_bamUploadDir'} ) {
        $self->{'error'} = "no_bam_upload_dir_found";
        croak "Failed to find the expected bam upload dir: $self->{'_bamUploadDir'}\n";
    }

    return 1;
}

=head2 _createUploadWorkspace

    $self->_createUploadWorkspace( $dbh );

The $dbh parameter is ignored. Returns 1 for sucess or sets 'error' and croaks with
error message.

Uses internal values: 

    uploadFastqBaseDir => from config, base directory for fastq zip uploads.
    _bamUploadDir      => directory for the previously completed 
                          mapsplice genome bam uploads for this lane.
    _fastqUploadUuid   => A uuid previously generated for this run.

Sets internal values:

    _fastqUploadDir  => directory for fastq uploads for this lane,
                        made from the fastqUploadBaseDir + new uuid lane value

Side effects:

Creates <_fastqUploadDir> directory = <fastqUploadBaseDir> / <_fastqUploadUuid>.
Copies run.xml and experiment.xml from <_bamUploadDir> to
<_fastqUploadDir>

Errors:

    zip_failed_no_fastq_base_dir       => No such dir: fastqUploadBaseDir
    zip_failed_fastq_upload_dir_exists => Exists: uuid upload dir
    zip_failed_creating_meta_dir       => Not Created: uuid upload dir
    zip_failed_copying_run_meta_file   => Not copied: run.xml
    zip_failed_copying_experiment_meta_file  => Not copied: expwriment.xml

=cut

sub _createUploadWorkspace {

    my $self = shift;

    if (! -d $self->{'uploadFastqBaseDir'}) {
        $self->{'error'} = "no_fastq_base_dir";
        croak "Can't find the fastq upload base dir: $self->{'uploadFastqBaseDir'}";
    }

    $self->{'_fastqUploadDir'} = File::Spec->catdir(
        $self->{'uploadFastqBaseDir'}, $self->{'_fastqUploadUuid'}
    );

    $self->sayVerbose("New upload directory: $self->{'_fastqUploadDir'}");

    if (-d $self->{'_fastqUploadDir'}) {
        $self->{'error'} = 'fastq_upload_dir_exists';
        croak "Upload directory already exists. That shouldn't happen: $self->{'_fastqUploadDir'}\n";
    }

    eval {
        make_path($self->{'_fastqUploadDir'}, { mode => 0775 });
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = "creating_upload_dir";
        croak "Could not create the upload output dir: $self->{'_fastqUploadDir'}\n$!\n$@\n";
    }

    my $fromRunFilePath = File::Spec->catfile( $self->{'_bamUploadDir'},   "run.xml" );
    my $toRunFilePath   = File::Spec->catfile( $self->{'_fastqUploadDir'}, "run.xml" );
    eval {
        cp( $fromRunFilePath, $toRunFilePath );
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = "copying_run_xml";
        croak "Could not copy the run.xml meta file FROM: $fromRunFilePath\nTO: $toRunFilePath\n$!\n$error\n";
    }

    my $fromExperimentFilePath = File::Spec->catfile( $self->{'_bamUploadDir'},   "experiment.xml" );
    my $toExperimentFilePath   = File::Spec->catfile( $self->{'_fastqUploadDir'}, "experiment.xml" );
    eval {
        cp( $fromExperimentFilePath, $toExperimentFilePath );
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = "copying_experiment_xml";
        croak "Could not copy the experiment.xml meta file FROM: $fromExperimentFilePath\nTO: $toExperimentFilePath\n$!\n$error\n";
    }

    return 1;
}

=head2 _insertZipUploadRecord()

  $self->_insertZipUploadRecord( $dbh, $new status )

Inserts a new upload record for the fastq upload being initiated. Takes as
a parameter the status of this new upload record.

Either returns 1 for success, or sets 'error' and croaks with an error message.

Inserts a new upload table record for CGHUB_FASTQ, for the same sample
as an existing upload record for CGHUB, when the CGHUB record is for a live
mapsplice upload and no CGHUB_FASTQ exists for that sample.

Uses

    'uploadFastqBaseDir' => From config, base for writing data to.
    'verbose'            => If set, echo SQL and values set.
    '_sampleId'          => The sampe this is used for.
    '_fastqUploadUuid'   => The uuid for this samples fastq upload.

Sets

    '_fastqUploadId' => upload.upload_id for the new record.

Side Effect

    Inserts a new Upload record for CGHUB_FASTQ, not yet linked to a file
    record, but with an existing meta-data directory defined.

Errors

    'zip_failed_db_insert_upload_fastq' => Insert of record failed
    'zip_failed_fastq_upload_data'      => Failed to set upload_id value
    
=cut

sub _insertZipUploadRecord {
    my $self = shift;
    my $dbh = shift;
    my $newUploadStatus = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__insertZipUploadRecord_dbh';
        croak ("_insertZipUploadRecord() missing \$dbh parameter.");
    }

    if (! $newUploadStatus) {
        $self->{'error'} = 'param__insertZipUploadRecord_newUploadStatus';
        croak( "_insertZipUploadRecord() missing \$newUploadStatus parameter." );
    }

    # Setup SQL
    my $sqlTargetForFastqUpload = 'CGHUB_FASTQ';

    my $insertUploadSQL =
        "INSERT INTO upload ( sample_id, target, status, metadata_dir, cghub_analysis_id )
         VALUES ( ?, ?, ?, ?, ? )
         RETURNING upload_id";

    my $rowHR;
    eval {
        my $insertSTH = $dbh->prepare($insertUploadSQL);
        $insertSTH->execute(
            $self->{'_sampleId'},
            $sqlTargetForFastqUpload,
            $newUploadStatus,
            $self->{'uploadFastqBaseDir'},
            $self->{'_fastqUploadUuid'}
        );
        $rowHR = $insertSTH->fetchrow_hashref();
        $insertSTH->finish();
    };
    if ($@) {
        $self->{'error'} = 'db_insert_upload_fastq';
        croak "Insert of new upload failed: $@";
    } 
    if (! defined $rowHR) {
        $self->{'error'} = 'db_insert_upload_fastq';
        croak "Failed to retireve the id of the upload record inserted. Maybe it failed to insert\n";
    }

    $self->{'_fastqUploadId'} = $rowHR->{'upload_id'};

    $self->sayVerbose("Inserted fastq UPLOAD: $self->{'_fastqUploadId'}");

    if (! $self->{'_fastqUploadId'}) {
        $self->{'error'} = 'fastq_upload_data';
        croak "Failed to set id of upload record inserted\n";
    }

    return 1;
}

=head2 _fastqFilesSqlSubSelect( ... )

    my someFileSQL = "... AND file.file_id EXISTS ("
       . _fastqFilesSqlSubSelect( $wf_accession ) . " )";


Given a workflow accession, returns a string that is an SQL subselect. When
executed, this subselect will return a list of file.fastq_id for fastq files
from the given workflow id.

This is required because different workflows need different amounts of
information to identify the fastq files relative to other files generated
from the same workflow. Using this allows separating the (fixed) SQL needed
to select the sample and the (varying) code to select the fastq files for that
sample.

If the wf_accession is not known, will return undef. The wf_accession may
be provided by an internal object property '_fastqWorkflowAccession' 

For example:

    my $sqlSubSelect = _fastqFilesSqlSubSelect( 613863 );
    print ($sqlSubSelect);

    SELECT file.file_id FROM file WHERE file.workflowAccession = 613863
        AND file.algorithm = 'FinalizeCasava'

    my $SQL = "SELECT f.path, f.md5Sum"
            . " FROM file f"
            . " WHERE sample_id = 3245"
            . " AND f.file IN ( " . _fastqFilesSqlSubSelect( 613863 ) . " )"

=cut

sub _fastqFilesSqlSubSelect {
    my $self = shift;
    my $fastqWorkflowAccession = shift;

    if (! defined $fastqWorkflowAccession ) {
        $fastqWorkflowAccession = $self->{'_workflowAccession'};
    }
    if (! defined $fastqWorkflowAccession ) {
        $self->{'error'} = 'no_fastq_wf_accession';
        croak("Fastq workflow accession not specified and not set internally.");
    }

    my $subSelect;
    if ( $fastqWorkflowAccession == 613863 ) {
        $subSelect = "SELECT vw_files.file_id FROM vw_files"
                  . " WHERE vw_files.workflow_accession = 613863"
                  . " AND vw_files.algorithm = 'FinalizeCasava'"
    }
    elsif ( $fastqWorkflowAccession == 851553 ) {
        $subSelect = "SELECT vw_files.file_id FROM vw_files"
                  . " WHERE vw_files.workflow_accession = 851553"
                  . " AND vw_files.algorithm = 'srf2fastq'"
    }
    return $subSelect;
}

=head2 _getFilesToZip()

    $self->_getFilesToZip( $dbh, $workflowAccession );

Identifies the fastq files that go with the uploaded bam file. If the
$workflowAccession is given, that is assumed to be the workflow the
fastq files come from. If this is not defined, it will first look at 613863
(FinalizeCasava) and then 851553 (srf2fastq) and use the first ones it finds.
Reports whatever riles it finds (one or two) without otherwise checking for
single or paired ends.



Dies for a lot of database errors.

=cut

sub _getFilesToZip {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__getFilesToZip_dbh';
        croak ("_getFilesToZip() missing \$dbh parameter.");
    }

    my $workflowAccession = shift;
    if (defined $workflowAccession) {
        $self->{'_workflowAccession'} = $workflowAccession;
    }
    else {
        $self->{'_workflowAccession'} = 613863;
    }

    my $sampleSelectSQL =
    "SELECT vwf.file_path, vwf.md5sum,     vwf.workflow_run_id,
            vwf.flowcell,  vwf.lane_index, vwf.barcode, pf.processing_id
     FROM vw_files vwf, processing_files pf
     WHERE vwf.file_id = pf.file_id
       AND vwf.status = 'completed'
       AND vwf.sample_id = ?
       AND vwf.lane_id = ?";
    my $fileSelectSQL = $sampleSelectSQL
                      . " AND vwf.file_id IN ( " . $self->_fastqFilesSqlSubSelect() . " )";

    # $self->sayVerbose("SQL to look for fastq files (from FinalizeCasava): \n$fileSelectSQL\n");

    my $row1HR;
    my $row2HR;
    eval {
        my $selectionSTH = $dbh->prepare( $fileSelectSQL );
        $selectionSTH->execute( $self->{'_sampleId'}, $self->{'_laneId'} );
        $row1HR = $selectionSTH->fetchrow_hashref();

        # If no 613863 fastq exists, there may be a 851553 fastq, but only check
        # If no workflow accession was explicitly specified as a sub param.
        if (! defined $row1HR && ! defined $workflowAccession ) {
            $selectionSTH->finish();
            $self->{'_workflowAccession'} = 851553;
            $fileSelectSQL = $sampleSelectSQL
                           . " AND vwf.file_id IN ( " . $self->_fastqFilesSqlSubSelect() . " )";
            # $self->sayVerbose("SQL to look for fastq files (from srf2fastq): \n$fileSelectSQL\n");

            $selectionSTH = $dbh->prepare($fileSelectSQL);
            $selectionSTH->execute( $self->{'_sampleId'}, $self->{'_laneId'} );
            $row1HR = $selectionSTH->fetchrow_hashref();    # row 1 as previous failed for any.

            # Checked for all source fastq files. IF none exist, we are done
            if (! defined $row1HR) {
                $self->{'_workflowAccession'} = undef;
                $self->{'error'} = 'no_fastq_files';
                croak "Can't find any fastq files\n";
            }
        }

        # Found a first row already, or exited on error. Now look for second row
        $row2HR = $selectionSTH->fetchrow_hashref();
        $selectionSTH->finish();
    };

    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
              $self->{'error'} = 'fastq_files_lookup';
        }
        croak "Error looking up fastq files: $error";
    }

    # Found at least one fastq file - record it
    $self->{'_workflowRunId'}                 = $row1HR->{'workflow_run_id'};
    $self->{'_flowcell'}                      = $row1HR->{'flowcell'};
    $self->{'_laneIndex'}                     = $row1HR->{'lane_index'};
    $self->{'_barcode'}                       = $row1HR->{'barcode'};
    $self->{'_fastqs'}->[0]->{'filePath'}     = $row1HR->{'file_path'};
    $self->{'_fastqs'}->[0]->{'md5sum'}       = $row1HR->{'md5sum'};
    $self->{'_fastqs'}->[0]->{'processingId'} = $row1HR->{'processing_id'};


    my $message = "\nFound fastq 1:";
    $message .= " WORKFLOW_RUN_ID: " . $self->{'_workflowRunId'};
    $message .= " FLOWCELL: "        . $self->{'_flowcell'};
    $message .= " LANE_INDEX: "      . $self->{'_laneIndex'};
    if ($self->{'_barcode'}) {
       $message .= " BARCODE: " . $self->{'_barcode'};
    };
    $message .= " FILE_PATH: "     . $self->{'_fastqs'}->[0]->{'filePath'};
    $message .= " MD5: "           . $self->{'_fastqs'}->[0]->{'md5sum'};
    $message .= " PROCESSING_ID: " . $self->{'_fastqs'}->[0]->{'processingId'};

    $self->sayVerbose( "$message" );

    unless ($self->{'_workflowRunId'}
        && $self->{'_flowcell'}
        && defined $self->{'_laneIndex'}
        && $self->{'_fastqs'}->[0]->{'filePath'}
        && $self->{'_fastqs'}->[0]->{'md5sum'}
        && $self->{'_fastqs'}->[0]->{'processingId'}
    ) {
        $self->{'error'} = 'fastq_file_1_data';
        croak "Missing data for fastq file 1."
    }

    # Second fastq may exist
    if (defined $row2HR) {
        $self->{'_fastqs'}->[1]->{'filePath'}     = $row2HR->{'file_path'};
        $self->{'_fastqs'}->[1]->{'md5sum'}       = $row2HR->{'md5sum'};
        $self->{'_fastqs'}->[1]->{'processingId'} = $row2HR->{'processing_id'};

        my $message = "\nFound fastq 2:";
        $message .= " WORKFLOW_RUN_ID: " . $row2HR->{'workflow_run_id'};
        $message .= " FLOWCELL: "        . $row2HR->{'flowcell'};
        $message .= " LANE_INDEX: "      . $row2HR->{'lane_index'};
        if ($row2HR->{'_barcode'}) {
           $message .= " BARCODE: " . $row2HR->{'_barcode'};
        };
        $message .= " FILE_PATH: "     . $self->{'_fastqs'}->[1]->{'filePath'};
        $message .= " MD5: "           . $self->{'_fastqs'}->[1]->{'md5sum'};
        $message .= " PROCESSING_ID: " . $self->{'_fastqs'}->[1]->{'processingId'};

        $self->sayVerbose( "$message" );

        unless ($row2HR->{'workflow_run_id'}
            && $row2HR->{'flowcell'}
            && defined $row2HR->{'lane_index'}
            && $self->{'_fastqs'}->[1]->{'filePath'}
            && $self->{'_fastqs'}->[1]->{'md5sum'}
            && $self->{'_fastqs'}->[1]->{'processingId'}
        ) {
            $self->{'error'} = 'fastq_file_2_data';
            croak "Missing data for fastq file 2."
        }

        # If find second fastq file, make sure all info for both match
        my ($vol, $fq1Dir, $file1) = File::Spec->splitpath(
             $self->{'_fastqs'}->[0]->{'filePath'}
        );
        my ($vol1, $fq2Dir, $file2) = File::Spec->splitpath(
             $self->{'_fastqs'}->[1]->{'filePath'}
        );

        if (   $row2HR->{'workflow_run_id'} != $self->{'_workflowRunId'}
            || $row2HR->{'flowcell'}        ne $self->{'_flowcell'}
            || $row2HR->{'lane_index'}      != $self->{'_laneIndex'}
            || (  defined $row2HR->{'barcode'} && ! defined $self->{'_barcode'})
            || (! defined $row2HR->{'barcode'} &&   defined $self->{'_barcode'})
            || (  defined $row2HR->{'barcode'} &&   defined $self->{'_barcode'}
                  && $row2HR->{'barcode'} ne $self->{'_barcode'} )
            || $fq1Dir ne $fq2Dir
        ) {
            $self->{'error'} = 'fastq-data-mismatch';
            croak "The two fastq file records don't match.";
        }
    }

    return 1;
}

=head2 _updateUploadStatus( ... )

    $self->_updateUploadStatus( $dbh, $newStatus );

Set the status of the internally referenced upload record to the specified
$newStatus string.

=cut

sub _updateUploadStatus {

    my $self = shift;
    my $dbh = shift;
    my $uploadId = shift;
    my $newStatus = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__updateUploadStatus_dbh';
        croak ("_updateUploadStatus() missing \$dbh parameter.");
    }
    unless ($uploadId) {
        $self->{'error'} = 'param__updateUploadStatus_uploadId';
        croak ("_updateUploadStatus() missing \$uploadId parameter.");
    }
    unless ($newStatus) {
        $self->{'error'} = 'param__updateUploadStatus_newStatus';
        croak ("_updateUploadStatus() missing \$newStatus parameter.");
    }

    my $updateSQL =
        "UPDATE upload
         SET status = ?
         WHERE upload_id = ?";

    eval {
        $dbh->begin_work();
        my $updateSTH = $dbh->prepare($updateSQL);
        $updateSTH->execute( $newStatus, $uploadId );
        my $rowsAffected = $updateSTH->rows();
        $updateSTH->finish();

        if (! defined $rowsAffected || $rowsAffected != 1) {
            $self->{'error'} = 'update_upload';
            croak "Update appeared to fail.";
        }
        $dbh->commit();
    };
    if ($@) {
        my $error = $@;
        eval {
            $dbh->rollback();
        };
        if ($@) {
            $error .= " ALSO: error rolling back _updateUploadStatus transaction: $@\n";
        }
        if (! $self->{'error'}) {
            $self->{'error'} = 'update_upload'
        }
        croak "Failed to update status of upload record upload_id=$uploadId to $newStatus: $error\n";
    }

    $self->sayVerbose("Set upload status for upload_id $uploadId to \"$newStatus\".");
    return 1;
}

=head2 _insertFile()

    $self->_insertFile( $dbh )

=cut

sub _insertFile {

    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__insertFile_dbh';
        croak ("_insertFile() missing \$dbh parameter.");
    }

    eval {
        $dbh->begin_work();
        $self->_insertFileRecord( $dbh );
        $self->_insertProcessingFileRecords( $dbh );
        $dbh->commit();
    };
    if ($@) {
        my $error = $@;
        eval {
            $dbh->rollback();
        };
        if ($@) {
            $error .= " ALSO: error rolling back insertFile transaction: $@\n";
        }
        unless ($self->{'error'}) {
            $self->{'error'} = 'insert_file_transaction';
        }
        croak $error;
    }

    return 1;
}

=head2 _insertFileRecord

    $self->_insertFileRecord( $dbh )

=cut

sub _insertFileRecord {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__insertFileRecord_dbh';
        croak ("_insertFileRecord() missing \$dbh parameter.");
    }

    my $zipFileMetaType = "application/tar-gz";
    my $zipFileType = "fastq-by-end-tar-bundled-gz-compressed";
    my $zipFileDescription = "The fastq files from one lane's sequencing run, tarred and gzipped. May be one or two files (one file per end).";

    my $newFileSQL =
        "INSERT INTO file ( file_path, meta_type, type, description, md5sum )"
     . " VALUES ( ?, ?, ?, ?, ? )"
     . " RETURNING file_id";

    # $self->sayVerbose( "Insert file record SQL: $newFileSQL" );

    my $rowHR;
    eval {
        my $newFileSTH = $dbh->prepare($newFileSQL);
        $newFileSTH->execute(
            $self->{'_zipFileName'},
            $zipFileMetaType,
            $zipFileType,
            $zipFileDescription,
            $self->{'_zipFileMd5'} );
        $rowHR = $newFileSTH->fetchrow_hashref();
        $newFileSTH->finish();
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = 'db_insert_file';
        croak "Insert of file record failed: $error\n";
    }
    if (! defined $rowHR) {
        $self->{'error'} = 'db_insert_file_returning';
        croak "Insert of file record appeared to fail\n";
    }

    $self->{'_zipFileId'} = $rowHR->{'file_id'};
    $self->sayVerbose( "Inserted FILE_ID: $self->{'_zipFileId'}" );

    if (! $self->{'_zipFileId'}) {
        $self->{'error'} = 'insert_file_data';
        croak "Insert of file record did not return the new record id\n";
    }

    return 1;
}

=head2 _insertProcessingFileRecords

    $self->_insertProcessingFileRecords( $dbh )

=cut

sub _insertProcessingFileRecords {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__insertProcessingFileRecords_dbh';
        croak ("_insertProcessingFileRecords() missing \$dbh parameter.");
    }

    my $newProcessingFilesSQL =
        "INSERT INTO processing_files (processing_id, file_id)"
     . " VALUES (?,?)";

    # $self->sayVerbose( "Insert processing_files record SQL: $newProcessingFilesSQL" );

    eval {
        my $newProcessingFilesSTH = $dbh->prepare($newProcessingFilesSQL);
        $newProcessingFilesSTH->execute(
            $self->{'_fastqs'}->[0]->{'processingId'}, $self->{'_zipFileId'} 
        );
        my $rowsInserted = $newProcessingFilesSTH->rows();
        if ($rowsInserted != 1) {
            $self->{'error'} = "insert_processsing_files_1";
            croak "failed to insert processing_files record for fastq 1\n";
        }
        $self->sayVerbose("Inserted processing_file record 1.");
        if ($self->{'_fastqs'}->[1]->{'processingId'}) {
            $newProcessingFilesSTH->execute(
                $self->{'_fastqs'}->[1]->{'processingId'}, $self->{'_zipFileId'}
            );
            $rowsInserted = $newProcessingFilesSTH->rows();
            if ($rowsInserted != 1) {
                $self->{'error'} = "insert_processsing_files_2";
                croak "failed to insert processing_files record for fastq 2\n";
            }
            $self->sayVerbose("Inserted processing_file record 2.");
        }
    };

    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'insert_processing_files';
        }
        croak "Processing files insert failed: $error\n";
    }

    return 1;
}

=head2 _insertUploadFileRecord()

    $self->_insertUploadFileRecord( $dbh )

=cut

sub _insertUploadFileRecord {

    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__insertUploadFileRecord_dbh';
        croak ("_insertUploadFileRecord() missing \$dbh parameter.");
    }

    my $newUploadFileSQL =
        "INSERT INTO upload_file (upload_id, file_id)"
     . " VALUES (?,?)";

    # $self->sayVerbose("Insert upload_file record SQL: $newUploadFileSQL");

    eval {
        $dbh->begin_work();
        my $newUploadFileSTH = $dbh->prepare($newUploadFileSQL);
        $newUploadFileSTH->execute(
            $self->{'_fastqUploadId'}, $self->{'_zipFileId'}
        );
        my $rowsInserted = $newUploadFileSTH->rows();
        if ($rowsInserted != 1) {
            $self->{'error'} = "insert_upload_file";
            croak "failed to insert upload_file record\n";
        }
        $dbh->commit();
    };

    if ($@) {
        my $error = $@;
        eval {
            $dbh->rollback();
        };
        if ($@) {
            $error .= "\n ALSO: error rolling back _updateUploadStatus transaction: $@\n";
        }
        if (! $self->{'error'}) {
            $self->{'error'} = 'insert_upload_files';
        }
        croak "Upload_file insert failed: $error\n";
    }

    $self->sayVerbose("Inserted upload_file record");

    return 1;

}

=head2 _zip()

    $self->_zip();

Actually does the zipping, and returns 1, or dies setting 'error' and returning
an error message.


=cut

sub _zip() {

    my $self = shift;

    # Validation
    for my $fileHR (@{$self->{'_fastqs'}}) {
         my $file = $fileHR->{'filePath'};
         if (! -f $file) {
             $self->{'error'} = "fastq_not_found";
             croak "Not on file system: $file\n";
         }
         if (( -s $file) < $self->{'minFastqSize'}) {
             $self->{'error'} = "fastq_too_small";
             croak "File size of " . (-s $file) . " is less than min of $self->{'minFastqSize'} for file $file\n";
         }
         my $md5result = `md5sum $file`;
         $md5result = (split(/ /, $md5result))[0];
         if (! defined $md5result || $md5result ne $fileHR->{'md5sum'}) {
             $self->{'error'} = "fastq_md5_mismatch";
             croak "Current md5 of $md5result does not match original md5 of $fileHR->{'md5sum'} for file $file\n";
         }
    }

    # Setup target directory
    my $zipFileDir = File::Spec->catdir(
        $self->{'dataRoot'}, $self->{'_flowcell'}, $self->{'myName'}
    );
    if (! -d $zipFileDir) {
        my $ok = make_path($zipFileDir, { mode => 0775 });
        if (! $ok) {
            $self->{'error'} = "creating_data_output_dir";
            croak "Error creating directory to put zipFile info in: $zipFileDir - $!\n";
        }
    }
    my $zipFile = $self->{'_flowcell'} . "_" . ($self->{'_laneIndex'} + 1);
    if (defined $self->{'_barcode'}) {
        $zipFile .= "_" . $self->{'_barcode'};
    }
    $zipFile .= ".tar.gz";
    $zipFile = File::Spec->catfile( $zipFileDir, $zipFile);
    if (-e $zipFile) {
         if ($self->{'rerun'}) {
             my $ok = unlink $zipFile;
             if (! $ok) {
                 $self->{'error'} = "removing_prior_file";
                 croak "Error deleting previous file: $zipFile - $!\n";
             }
         }
         else{
             $self->{'error'} = "prior_zip_file_exists";
             croak "Error: not rerunning and have preexisting zip file: $zipFile\n";
         }
    }

    # Do zip
    my ($vol0, $fqDir0, $file0) = File::Spec->splitpath(
             $self->{'_fastqs'}->[0]->{'filePath'}
    );
    my $command = "tar -czh -C $fqDir0 -f $zipFile $file0";
    if (   defined $self->{'_fastqs'}->[1]
        && defined $self->{'_fastqs'}->[1]->{'filePath'}
    ) {
        my ($vol1, $fqDir1, $file1) = File::Spec->splitpath(
                 $self->{'_fastqs'}->[1]->{'filePath'}
        );
        $command .= " $file1";
    }

    $self->sayVerbose( "ZIP COMMAND: $command\n");
    my $ok = system( $command );

    if ( $ok != 0 || ! (-f $zipFile) || (-s $zipFile) < (( $self->{'minFastqSize'} / 100 ) + 1 )) {
        $self->{'error'} = "executing_tar_gzip";
        croak "Failed executing the zip command [$command] with error: $ok\n";
    }
    $self->{'_zipFileName'} = $zipFile;

    my $md5result = `md5sum $zipFile`;
    if (defined $md5result) {
        $md5result = (split(/ /, $md5result))[0];
    }
    if (! defined $md5result) {
        $self->{'error'} = "zipfile_md5_generation";
        croak "Generation of zip file md5 failed.";
    }
    $self->{'_zipFileMd5'} = $md5result;

    return 1;
}

=head2 _changeUploadRunStage

    $obj->_changeUploadRunStage( $dbh $fromStatus, $toStatus );
    
Loks for an upload record with the given $fromStatus status. If can't find any,
just returns undef. If finds one, then changes its status to the given $toStatus
and returns that upload record as a HR with the column names as keys.

This does not set error as failure would likely be redundant.

Croaks without parameters, if there are db errors reported, or if no upload
can be retirived.

=cut

sub _changeUploadRunStage {

    my $self = shift;
    my $dbh = shift;
    my $fromStatus = shift;
    my $toStatus = shift;

    unless ($dbh) {
        $self->{'error'} = "param__changeUploadRunStage_dbh";
        croak ("_changeUploadRunStage() missing \$dbh parameter.");
    }
    unless ($fromStatus) {
        $self->{'error'} = "param__changeUploadRunStage_fromStatus";
        croak ("_changeUploadRunStage() missing \$fromStatus parameter.");
    }
    unless ($toStatus) {
        $self->{'error'} = "param__changeUploadRunStage_toStatus";
        croak ("_changeUploadRunStage() missing \$toStatus parameter.");
    }

    my %upload;

    # Setup SQL
    my $sqlTargetForFastqUpload = 'CGHUB_FASTQ';

    my $selectionSQL =
       "SELECT *
        FROM upload
        WHERE target = ?
          AND status = ?
        ORDER by upload_id DESC limit 1";

    my $updateSQL =
       "UPDATE upload
        SET status = ?
        WHERE upload_id = ?";

    $self->sayVerbose("Looking for upload record with status \"$fromStatus\".");

    # DB transaction
    eval {
        $dbh->begin_work();

        my $selectionSTH = $dbh->prepare( $selectionSQL );
        $selectionSTH->execute( $sqlTargetForFastqUpload, $fromStatus );
        my $rowHR = $selectionSTH->fetchrow_hashref();
        $selectionSTH->finish();

        if (! defined $rowHR) {
            # Nothing to update - this is a normal exit
            $dbh->commit();
            undef %upload;  # Just to be clear.
            $self->sayVerbose("Found no uplaad record with status \"$fromStatus\"." );
        }
        else {
            # Will be passing this back, so make copy.
            %upload = %$rowHR;

            unless ( $upload{'upload_id'} ) {
                $self->{'error'} = "status_query_$fromStatus" . "_to_" . $toStatus;
                croak "Failed to retrieve upload data.\n";
            }
            unless ( $upload{'sample_id'} ) {
                $self->{'error'} = "status_query_$fromStatus" . "_to_" . $toStatus;
                croak "Failed to retrieve sample id in upload data.\n";
            }

            $self->sayVerbose("Found upload record with status \"$fromStatus\" - sample id = $upload{'sample_id'} upload id = $upload{'upload_id'}.");
            $self->sayVerbose("Changing status of upload record (id = $upload{'upload_id'}) from \"$fromStatus\" to \"$toStatus\".\n" );

            my $updateSTH = $dbh->prepare( $updateSQL );
            $updateSTH->execute( $toStatus, $upload{'upload_id'}, );
            if ($updateSTH->rows() != 1) {
                $self->{'error'} = "status_update_$fromStatus" . "_to_" . $toStatus;
                croak "Failed to update upload status.\n";
            }
            $updateSTH->finish();

            $dbh->commit();

            $self->{'_fastqUploadId'} = $upload{'upload_id'};
            $upload{'status'} = $toStatus; # Correct local copy to match db.
        }
    };
    if ($@) {
        my $error = $@;
        eval {
            $dbh->rollback();
        };
        if ($@) {
            $error .= "\nALSO: ***Rollback appeared to fail*** $@ \n";
        }
        if (! $self->{'error'}) {
                $self->{'error'} = "status_change_$fromStatus" . "_to_" . $toStatus;
        }
        croak "Error changing upload status from $fromStatus to $toStatus:\n$error\n";
    }

    if (! %upload) {
        return undef;
    }
    else {
        return \%upload;
    }
}

=head2 _getTemplateData

    $obj->_getTemplateData( $dbh );

=cut

sub _getTemplateData {

    my $self = shift;
    my $dbh = shift;
    my $uploadId = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__getTemplateData_dbh';
        croak ("_getTemplateData() missing \$dbh parameter.");
    }

    unless ($uploadId) {
        $self->{'error'} = 'param__getTemplateData_uploadId';
        croak ("_getTemplateData() missing \$uploadId parameter.");
    }

    my $selectAllSQL =
       "SELECT vf.tstmp             as file_timestamp,
               vf.tcga_uuid         as sample_tcga_uuid,
               l.sw_accession       as lane_accession,
               vf.file_sw_accession as file_accession,
               vf.md5sum            as file_md5sum,
               vf.file_path,
               u.metadata_dir       as fastq_upload_basedir,
               u.cghub_analysis_id  as fastq_upload_uuid,
               e.sw_accession       as experiment_accession,
               s.sw_accession       as sample_accession,
               e.description        as experiment_description,
               e.experiment_id,
               p.instrument_model,
               u.sample_id
        FROM upload u, upload_file uf, vw_files vf, lane l, experiment e, sample s, platform p
        WHERE u.upload_id = ?
          AND u.upload_id = uf.upload_id
          AND uf.file_id = vf.file_id
          AND vf.lane_id = l.lane_id
          AND s.sample_id = u.sample_id
          AND e.experiment_id = s.experiment_id
          AND e.platform_id = p.platform_id";

    #$self->sayVerbose( "SQL to get template data:\n$selectAllSQL" );

    my $data = {};
    eval {
        my $selectionSTH = $dbh->prepare( $selectAllSQL );
        $selectionSTH->execute( $uploadId );
        my $rowHR = $selectionSTH->fetchrow_hashref();
#        $selectionSTH->finish();
        my $fileName = (File::Spec->splitpath( $rowHR->{'file_path'} ))[2];
        my $localFileLink =
            "UNCID_"
            . $rowHR->{'file_accession'} . '.'
            . $rowHR->{'sample_tcga_uuid'} . '.'
            . $fileName;

        $data = {
            'program_version'      => $VERSION,
            'sample_tcga_uuid'     => $rowHR->{'sample_tcga_uuid'},
            'lane_accession'       => $rowHR->{'lane_accession'},
            'file_md5sum'          => $rowHR->{'file_md5sum'},
            'file_accession'       => $rowHR->{'file_accession'},
            'upload_file_name'     => $localFileLink,
            'uploadIdAlias'        => "upload $uploadId",
            'experiment_accession' => $rowHR->{'experiment_accession'},
            'sample_accession'     => $rowHR->{'sample_accession'},
            'experiment_description' => $rowHR->{'experiment_description'},
            'instrument_model' => $rowHR->{'instrument_model'},
            'read_ends'        => 
                $self->_getTemplateDataReadEnds(
                    $dbh, $rowHR->{'experiment_id'} ),
            'base_coord'   => -1  +
                $self->_getTemplateDataReadLength(
                    $dbh, $rowHR->{'sample_id'} ),
            'file_path_base'  => 
                (Bio::SeqWare::Uploads::CgHub::Fastq->getFileBaseName(
                    $rowHR->{'file_path'} ))[0],
            'analysis_date'   =>
                Bio::SeqWare::Uploads::CgHub::Fastq->reformatTimeStamp(
                    $rowHR->{'file_timestamp'} ),
        };
        if ($data->{'read_ends'} == 1) {
            $data->{'library_layout'} = 'SINGLE';
        }
        elsif ($data->{'read_ends'} == 2) {
            $data->{'library_layout'} = 'PAIRED';
        }
        else {
            $self->{'error'} = 'bad_read_ends';
            croak("XML only defined for read_ends 1 or 2, not $data->{'read_ends'}\n");
        }
 
        if ($self->{'verbose'}) {
            my $message = "Template Data:\n";
            for my $key (sort keys %$data) {
                $message .= "\t\"$key\" = \"$data->{$key}\"\n";
            }
            $self->sayVerbose( $message );
        }
        for my $key (sort keys %$data) {
            if (! defined $data->{$key} || length $data->{$key} == 0) {
                $self->{'error'} = 'bad_tempalte_datq';
                croak("No value obtained for template data element \'$key\'\n");
            }
        }

        $self->{'_fastqUploadDir'} = File::Spec->catdir(
                    $rowHR->{'fastq_upload_basedir'},
                    $rowHR->{'fastq_upload_uuid'},
        );
        if (! -d $self->{'_fastqUploadDir'}) {
            $self->{'error'} = 'dir_fastqUpload_missing';
            die("Can't find fastq upload targed directory \"$data->{'_fastqUploadDir'}\"\n");
        }

        symlink( $rowHR->{'file_path'}, File::Spec->catfile( $self->{'_fastqUploadDir'}, $localFileLink ));
        $self->sayVerbose("Created local link \"$localFileLink\"");
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'collecting_template_data';
        }
        croak ("Failed collecting data for template use: $@");
    }

    return $data;
}

=head2 _getTemplateDataReadEnds

    $ends = $self->_getTemplateDataReadEnds( $dbh, $eperiment.sw_accession );

Returns 1 if single ended, 2 if paired-ended. Based on the number
of application reads in the associated experiment_spot_design_read_spec.
Dies if any other number found, or if any problem with db access.

=cut

sub _getTemplateDataReadEnds {

    my $self         = shift;
    my $dbh          = shift;
    my $experimentId = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__getTemplateDataReadEnds_dbh';
        croak ("_getTemplateDataReadEnds() missing \$dbh parameter.");
    }

    unless ($experimentId) {
        $self->{'error'} = 'param__getTemplateDataReadEnds_experimentId';
        croak ("_getTemplateDataReadEnds() missing \$experimentId parameter.");
    }

    my $readCountSQL = 
        "SELECT count(*) as read_ends
         FROM experiment_spot_design_read_spec AS rs,
                        experiment_spot_design AS d,
                                    experiment AS e
         WHERE  e.experiment_id                 = ?
           AND  e.experiment_spot_design_id     = d.experiment_spot_design_id
           AND rs.experiment_spot_design_id     = d.experiment_spot_design_id
           AND rs_class                         =  'Application Read'
           AND rs.read_type                    !=  'BarCode'";

    my $readEnds;
    eval {
        my $readCoundSTH = $dbh->prepare( $readCountSQL );
        $readCoundSTH->execute( $experimentId );
        my $rowHR = $readCoundSTH->fetchrow_hashref();
        $readEnds = $rowHR->{'read_ends'};
        if (! defined $readEnds) {
             croak "Nothing retrieved from database.\n";
        }
        unless ($readEnds == 1 || $readEnds == 2) {
             croak "Found $readEnds read ends, expected 1 or 2.\n";
        }
        $readCoundSTH->finish();
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = 'db_query_ends';
        croak ( "Can't retrieve count of ends: $@" );
    }

    return $readEnds;
}

=head2 _getTemplateDataReadLength

   $baseCountPerRead = _getTemplateDataReadLength( $dbh, sampleId );

Examines first 1000 lines of the BAM file associated with this fastq looking
for the max read length. Finding the BAM file is easier than getting back to
the fastq.

=cut

sub _getTemplateDataReadLength {

    my $self     = shift;
    my $dbh      = shift;
    my $sampleId = shift;

    unless ($dbh) {
        $self->{'error'} = 'param__getTemplateDataReadLength_dbh';
        croak ("_getTemplateDataReadLength() missing \$dbh parameter.");
    }

    unless ($sampleId) {
        $self->{'error'} = 'param__getTemplateDataReadLength_sampleId';
        croak ("_getTemplateDataReadLength() missing \$sampleId parameter.");
    }

    my $SAMTOOLS_EXEC = '/datastore/tier1data/nextgenseq/seqware-analysis/software/samtools/samtools';
    my $MIN_READ_LENGTH = 17;

    my $bamFileSQL =
        "SELECT f.file_path
         FROM upload AS u, upload_file AS uf, file AS f
         WHERE u.target          = 'CGHUB'
           AND u.external_status = 'live'
           AND u.metadata_dir    = '/datastore/tcga/cghub/v2_uploads'
           AND u.sample_id       = ?";

    my $bamFile;
    eval {
        my $bamFileSTH = $dbh->prepare( $bamFileSQL );
        $bamFileSTH->execute( $sampleId );
        my $rowHR = $bamFileSTH->fetchrow_hashref();
        $bamFile = $rowHR->{'file_path'};
        if (! defined $bamFile) {
             croak "Nothing retrieved from database.\n";
        }
        unless (-f $bamFile) {
             croak "No such File: \"$bamFile\"\n";
        }
        $bamFileSTH->finish();
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = 'db_get_bam';
        croak ( "Can't retrieve bam file path for read end counting: $error" );
    }

    my $readLength = 0;

    eval {
        my $command = "$SAMTOOLS_EXEC view $bamFile | head -1000 | cut -f 10";
        $self->sayVerbose( "READ LENGTH COMMAND: \"$command\"" );
        my $errorMessage = "";
        my $readStr = qx/$command/;

        if ($?) {
            $self->{'error'} = "samtools-exec-error-$?";
            die ("Error getting read length. exit=$?; $!\n"
                . "Original command was:\n$command\n" );
        }
        if (! $readStr) {
            $self->{'error'} = "samtools-exec-no-output";
            die( "Validation error: neither error nor result generated. Strange.\n"
                        . "Original command was:\n$command\n" );
        }

        my @reads = split (/\n/, $readStr);
        foreach my $read (@reads) {
            my $length = length($read);
            if ($length > $readLength) {
                $readLength = $length;
            }
        }

        if ( $readLength < $MIN_READ_LENGTH ) {
            $self->{'error'} = "low-read-length";
            die( "Bad read length: = $readLength.\n" );
        }
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'bam_read_length';
        }
        croak ( "Can't determine bam max read length: $error" );
    }

    return $readLength;
}

=head2 _makeFileFromTemplate

  $obj->_makeFileFromTemplate( $dataHR, $outFile );
  $obj->_makeFileFromTemplate( $dataHR, $outFile, $templateFile );

Takes the $dataHR of template values and uses it to fill in the
a template ($templateFile) and generate an output file ($outFile). Returns
the absolute path to the created $outFile file, or dies with error.
When $outFile and/or $templateFile are relative, default direcotries are
used from the object. The $tempateFile is optional, if not given, uses
$outFile.template as the template name.

USES

    'templateBaseDir' = Absolute basedir to use if $templateFile is relative.
    'xmlSchema'       = Schema version, used as subdir under templateBaseDir
                        if $templateFile is relative.
    '_fastqUploadDir' = Absolute basedir to use if $outFile is relative.

=cut

sub _makeFileFromTemplate {

    my $self   = shift;
    my $dataHR = shift;
    my $outFile = shift;
    my $templateFile = shift;

    unless ($dataHR) {
        $self->{'error'} = 'param__makeFileFromTemplate_dataHR';
        croak ("_makeFileFromTemplate() missing \$dataHR parameter.");
    }

    unless ($outFile) {
        $self->{'error'} = 'param__makeFileFromTemplate_outFile';
        croak ("_makeFileFromTemplate() missing \$outFile parameter.");
    }

    unless ($templateFile) {
        $templateFile = $outFile . ".template";
    }

    # Ensure have absolute paths for outFile and templateFile
    my $outAbsFilePath;
    my $templateAbsFilePath;
    if ( File::Spec->file_name_is_absolute( $outFile )) {
        $outAbsFilePath = $outFile
    }
    else {
        $outAbsFilePath = File::Spec->catfile( $self->{'_fastqUploadDir'}, $outFile );
    }
    if ( File::Spec->file_name_is_absolute( $templateFile )) {
         $templateAbsFilePath = $templateFile
    }
    else {
        $templateAbsFilePath = File::Spec->catfile(
            $self->{'templateBaseDir' },
            $self->{'xmlSchema' },
            $templateFile
        );
    }

    $self->sayVerbose( "TEMPLATE: $templateAbsFilePath\n"
            ."OUTFILE: $outAbsFilePath\n"
            ."DATA: \n" . Dumper($dataHR)
    );

    # Stamp output file from merged dataHR and template file
    eval {
        my $templateManagerConfigHR = {
            'ABSOLUTE' => 1,
        };
        my $templateManager = Template->new( $templateManagerConfigHR );
        my $ok = $templateManager->process( $templateAbsFilePath, $dataHR, $outAbsFilePath  );
        if (! $ok) {
            die( $templateManager->error() . "\n");
        }
        unless (-f $outAbsFilePath) {
            die( "Can't find the file I should have just created: $outAbsFilePath\n" );
        }
    };
    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'creating_file_from_template';
        }
        croak ("Failed creating a file $outAbsFilePath from template $templateAbsFilePath: $error");
    }

    return $outAbsFilePath;
}

=head2 _validateMeta

    self->_validateMeta( $uploadHR );

=cut

sub _validateMeta {

    my $self     = shift;
    my $uploadHR = shift;

    unless ( $uploadHR ) {
        $self->{'error'} = 'param_validateMeta_$uploadHR';
        croak( "Missing parameter: requires specifying upload record." );
    }

    my $CGSUBMIT_EXEC = '/usr/bin/cgsubmit';
    my $CGHUB_URL = 'https://cghub.ucsc.edu/';
    my $OK_VALIDATED_REGEXP = qr/Metadata Validation Succeeded\./m;

    my $fastqOutDir = File::Spec->catdir(
        $uploadHR->{'metadata_dir'},
        $uploadHR->{'cghub_analysis_id'}
    );

    my $command = "$CGSUBMIT_EXEC -s $CGHUB_URL  -u $fastqOutDir --validate-only 2>&1";

    $self->sayVerbose( "VALIDATE COMMAND: \"$command\"" );

    my $oldCwd = getcwd();
    chdir( $fastqOutDir );

    my $errorMessage = "";
    my $validateResult = qx/$command/;

    if ($?) {
        if ($validateResult) {
            $self->{'error'} = "exec-error-$?-with-output";
            die ("Validation error: exited with error value \"$?\". Output was:\n$validateResult\n"
                . "Original command was:\n$command\n" );
        }
        else {
            $self->{'error'} = "exec-error-$?-no-output";
            die ("Validation error: exited with error value \"$?\". No output was generated.\n"
                . "Original command was:\n$command\n" );
        }
    }
    if (! $validateResult) {
        $self->{'error'} = "exec-no-output";
        die( "Validation error: neither error nor result generated. Strange.\n"
                    . "Original command was:\n$command\n" );
    }
    if ( $validateResult !~ $OK_VALIDATED_REGEXP ) {
        $self->{'error'} = "exec-unexpected-output";
        die( "Validation error: Apparently failed to validate.\n"
            . "Actual validation result was:\n$validateResult\n\n"
            . "Original command was:\n$command\n" );
    }

    $self->sayVerbose("CgSubmit program (in validate mode) returned:\n$validateResult\n");

    chdir( $oldCwd );
    return 1;
}


=head2 _submitMeta

   $obj->_submitMeta( $uploadHR );

=cut

sub _submitMeta {

    my $self     = shift;
    my $uploadHR = shift;

    unless ( $uploadHR ) {
        $self->{'error'} = "param_submitMeta_uploadHR";
        croak( "Missing parameter: requires specifying upload record." );
    }

    my $CGSUBMIT_EXEC = '/usr/bin/cgsubmit';
    my $CGHUB_URL = 'https://cghub.ucsc.edu/';
    my $SECURE_CERTIFICATE = "/datastore/alldata/tcga/CGHUB/Key.20130213/mykey.pem";
    my $OK_SUBMIT_META_REGEXP = qr/Metadata Submission Succeeded\./m;
    my $ERROR_RESUBMIT_META_REGEXP = qr/Error\s*: You are attempting to submit an analysis using a uuid that already exists within the system and is not in the upload or submitting state/m;

    my $fastqOutDir = File::Spec->catdir(
        $uploadHR->{'metadata_dir'},
        $uploadHR->{'cghub_analysis_id'}
    );

    my $command = "$CGSUBMIT_EXEC -s $CGHUB_URL -c $SECURE_CERTIFICATE -u $fastqOutDir  2>&1";

    $self->sayVerbose( "SUBMIT META COMMAND: \"$command\"\n" );

    my $oldCwd = getcwd();
    chdir( $fastqOutDir );

    my $errorMessage = "";
    my $submitMetaResult = qx/$command/;

    if ($?) {
        # Unsure of exit value when get this message, so here twoce
        if (! $submitMetaResult ) {
            $self->{'error'} = "exec-error-$?-no-output";
            die ("Submit meta error: exited with error value \"$?\". No output was generated.\n"
                . "Original command was:\n$command\n" );
        }
        elsif ( $submitMetaResult =~ $ERROR_RESUBMIT_META_REGEXP ) {
            $self->{'error'} = "exec-error-$?-repeat";
            die( "Submit meta error: Already submitted. Exited with error value \"$?\".\n"
                . "Actual submit meta result was:\n$submitMetaResult\n\n"
                . "Original command was:\n$command\n" );
        }
        else {
            $self->{'error'} = "exec-error-$?-with-output";
            die ("Submit meta error: exited with error value \"$?\". Output was:\n$submitMetaResult\n"
                . "Original command was:\n$command\n" );
        }
    }
    if (! $submitMetaResult) {
        $self->{'error'} = "exec-no-output";
        die( "Submit meta error: neither error nor result generated. Strange.\n"
                    . "Original command was:\n$command\n" );
    }
    if ( $submitMetaResult =~ $ERROR_RESUBMIT_META_REGEXP ) {
        $self->{'error'} = "exec-repeat";
        die( "Submit meta error: Already submitted.\n"
            . "Actual submit meta result was:\n$submitMetaResult\n\n"
            . "Original command was:\n$command\n" );
    }
    if ( $submitMetaResult !~ $OK_SUBMIT_META_REGEXP ) {
        $self->{'error'} = "exec-unexpected-output";
        die( "Submit meta error: Apparently failed to submit.\n"
            . "Actual submit meta result was:\n$submitMetaResult\n\n"
            . "Original command was:\n$command\n" );
    }

    $self->sayVerbose("CgSubmit program (in submission mode) returned:\n$submitMetaResult\n");

    chdir( $oldCwd );
    return 1;

}

=head2 _submitFastq

   $obj->_submitFastq( $uploadHR );

=cut

sub _submitFastq {

    my $self     = shift;
    my $uploadHR = shift;

    unless ( $uploadHR ) {
        $self->{'error'} = 'param_submitFastq_uploadHR';
        croak( "Missing parameter: requires specifying upload record." );
    }

    my $GTUPLOAD_EXEC = '/usr/bin/gtupload';
    my $SECURE_CERTIFICATE = "/datastore/alldata/tcga/CGHUB/Key.20130213/mykey.pem";
    my $OK_SUBMIT_FASTQ_REGEXP = qr/100\.000/m;
    my $ERROR_RESUBMIT_FASTQ_REGEXP = qr/Error\s*: Your are attempting to upload to a uuid which already exists within the system and is not in the submitted or uploading state\. This is not allowed\./;

    my $fastqOutDir = File::Spec->catdir(
        $uploadHR->{'metadata_dir'},
        $uploadHR->{'cghub_analysis_id'}
    );

    my $uploadManifest = File::Spec->catfile( $fastqOutDir, 'manifest.xml' );

    # Similar to cgsubmot, except not allowed to specify upload url; only way to
    # tell finished is to use verbose and capture output, and (stupidly) the
    # verbose output goes to standard error instead of standard out, so have to
    # capture that to know when done. Also appears to require being in the
    # upload directory, despite giving full path directory names for all
    # parameters?
    my $command = "$GTUPLOAD_EXEC -vvvv -c $SECURE_CERTIFICATE -u $uploadManifest -p $fastqOutDir 2>&1";

    $self->sayVerbose( "SUBMIT FASTQ COMMAND: \"$command\"");

    my $oldCwd = getcwd();
    chdir( $fastqOutDir );

    my $errorMessage = "";
    my $submitFastqResult = qx/$command/;

    if ($?) {
        # Unsure of exit value when get this message, so here twoce
        if (! $submitFastqResult ) {
            $self->{'error'} = "exec-error-$?-no-output";
            die ("Submit fastq error: exited with error value \"$?\". No output was generated.\n"
                . "Original command was:\n$command\n" );
        }
        elsif ( $submitFastqResult =~ $ERROR_RESUBMIT_FASTQ_REGEXP ) {
            $self->{'error'} = "exec-error-$?-repeat";
            die( "Submit fastq error: Already submitted. Exited with error value \"$?\".\n"
                . "Actual submit fastq result was:\n$submitFastqResult\n\n"
                . "Original command was:\n$command\n" );
        }
        else {
            $self->{'error'} = "exec-error-$?-with-output";
            die ("Submit fastq error: exited with error value \"$?\". Output was:\n$submitFastqResult\n"
                . "Original command was:\n$command\n" );
        }
    }
    if (! $submitFastqResult) {
        $self->{'error'} = "exec-no-output";
        die( "Submit fastq error: neither error nor result generated. Strange.\n"
                    . "Original command was:\n$command\n" );
    }
    if ( $submitFastqResult =~ $ERROR_RESUBMIT_FASTQ_REGEXP ) {
        $self->{'error'} = "exec-repeat";
        die( "Submit fastq error: Already submitted.\n"
            . "Actual submit fastq result was:\n$submitFastqResult\n\n"
            . "Original command was:\n$command\n" );
    }
    if ( $submitFastqResult !~ $OK_SUBMIT_FASTQ_REGEXP ) {
        $self->{'error'} = "exec-unexpected-output";
        die( "Submit fastq error: Apparently failed to submit.\n"
            . "Actual submit fastq result was:\n$submitFastqResult\n\n"
            . "Original command was:\n$command\n" );
    }

    $self->sayVerbose("GtUpload program returned:\n$submitFastqResult\n");

    chdir( $oldCwd );
    return 1;

}

=head2 $self->sayVerbose()

    $self->sayverbose( $message ).

Prints the given message preceeded with "[INFO] $timestamp - ", wrapped to
132 columns with all lines after the first indented with a "\t". The
timestamp is generated by C<getTimeStamp()>.

=cut

sub sayVerbose {
    my $self = shift;
    my $message = shift;
    unless ( $self->{'verbose'} ) {
        return;
    }
    if (! defined $message) {
        $message = "__NULL__";
    }
    my $timestamp = Bio::SeqWare::Uploads::CgHub::Fastq->getTimeStamp();
    my $uuid_tag = $self->{'_fastqUploadUuid'};
    if ($uuid_tag && $uuid_tag =~ /([A-F0-9]{8})$/i) {
        $uuid_tag = $1;
    }
    else {
        $uuid_tag = '12345678';
    }
    print( wrap("$uuid_tag: [INFO] $timestamp - ", "\t", "$message\n" ));
}

=head1 AUTHOR

Stuart R. Jefferys, C<< <srjefferys (at) gmail (dot) com> >>

Contributors:
  Lisle Mose (get_sample.pl and generate_cghub_metadata.pl)
  Brian O'Conner


=cut

=head1 DEVELOPMENT

This module is developed and hosted on GitHub, at
L<p5-Bio-SeqWare-Config https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq>.
It is not currently on CPAN, and I don't have any immediate plans to post it
there unless requested by core SeqWare developers (It is not my place to
set out a module name hierarchy for the project as a whole :)

=cut

=head1 INSTALLATION

You can install a version of this module directly from github using

   $ cpanm git://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.014
 or
   $ cpanm https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.014.tar.gz

Any version can be specified by modifying the tag name, following the @;
the above installs the latest I<released> version. If you leave off the @version
part of the link, you can install the bleading edge pre-release, if you don't
care about bugs...

You can select and download any package for any released version of this module
directly from L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/releases>.
Installing is then a matter of unzipping it, changing into the unzipped
directory, and then executing the normal (C>Module::Build>) incantation:

     perl Build.PL
     ./Build
     ./Build test
     ./Build install

=cut

=head1 BUGS AND SUPPORT

No known bugs are present in this release. Unknown bugs are a virtual
certainty. Please report bugs (and feature requests) though the
Github issue tracker associated with the development repository, at:

L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/issues>

Note: you must have a GitHub account to submit issues.

=cut

=head1 ACKNOWLEDGEMENTS

This module was developed for use with L<SegWare | http://seqware.github.io>.

=cut

=head1 LICENSE AND COPYRIGHT

Copyright 2013 Stuart R. Jefferys.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut

1; # End of Bio::SeqWare::Uploads::CgHub::Fastq
