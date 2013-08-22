package Bio::SeqWare::Uploads::CgHub::Fastq;

use 5.014;         # Eval $@ safe to use.
use strict;        # Don't allow unsafe perl constructs.
use warnings;      # Enable all optional warnings.
use Carp;          # Base the locations of reported errors on caller's code.
# $Carp::Verbose = 1;
use Bio::SeqWare::Config;   # Read the seqware config file
use Bio::SeqWare::Db::Connection 0.000002; # Dbi connection, with parameters
use Data::Dumper;
use File::Spec;
use File::Path qw(make_path);
use File::Copy qw(cp);
use DBI;

=head1 NAME

Bio::SeqWare::Uploads::CgHub::Fastq - Support uploads of fastq files to cghub

=cut

=head1 VERSION

Version 0.000.003   # PRE_RELEASE

=cut

our $VERSION = '0.000003';   # PRE-RELEASE

=head1 SYNOPSIS

    use Bio::SeqWare::Uploads::CgHub::Fastq;

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new();

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

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new();

Creates and returns a Bio::SeqWare::Uploads::CgHub::Fastq object. Takes
no parameters, providing one is a fatal error.

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

        '_laneId'    => undef,
        '_sampleId'  => undef,
        '_fastqUploadId'  => undef,
        '_fastqs'    => undef,
        '_zipFile'   => undef,
        '_zipMd5Sum' => undef,
        '_zipFileId' => undef,
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
    my $uuid = `uuidgen`;
    chomp $uuid;
    return $uuid;
}

=head1 INSTANCE METHODS

=cut

=head2 run()

  $obj->run();
  my @allowedModes = qw( ZIP META VALIDATE UPLOAD ALL ); # Case unimportant
  $obj->run( "all" );

This is the "main" program loop, associated with running C<upload-cghub-fastq>
This method can be called with or without a parameter. If called without a
parameter, it uses the value of the instance's 'runMode' property. All allowed
values for that parameter are supported here: case insenistive "ZIP", "META",
"VALIDATE", "UPLOAD", and "ALL". Each parameter causes the associated "do..."
method to be invoked, although "ALL"" causes each of the 4 do... methods to be
invoked in order as above.

This method will should either succeed and return 1 or set $self->{'error'}
and returns undef.

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
        $self->{'error'} = "bad_run_mode";
        croak "Can't run unless specify a runMode.";
    }
    $runMode = uc $runMode;

    # Database connection likewise
    if (! defined $dbh) {
        $dbh = $self->{'dbh'};
    }
    if (! defined $dbh ) {
        eval {
            my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $self );
            if (! defined $connectionBuilder) {
                $self->{'error'} = "constructing_connection";
                croak "Failed to create Bio::SeqWare::Db::Connection.\n";
            }

            print ("DEBUG: " . Dumper($connectionBuilder));
            $dbh = $connectionBuilder->getConnection(
                 {'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1}
            );
        };
        if ($@ || ! $dbh) {
            croak "Failed to connect to the database $@\n$!\n";
        }
    }

    # Run as selected.
    eval {
        if ( $runMode eq "ALL" ) {
            $self->run('ZIP', $dbh);
            $self->run('META', $dbh);
            $self->run('VALIDATE', $dbh);
            $self->run('UPLOAD', $dbh);
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
        elsif ($runMode eq "UPLOAD" ) {
            $self->doUpload( $dbh );
        }
        else {
            $self->{'error'} = "unknown_run_mode";
            croak "Illegal runMode \"$runMode\" specified.\n";
        }
    };

    if ($@) {
        my $error = $@;
        if ( $self->{'_fastqUploadId'}) {
            $self->_updateUploadStatus( $dbh, "zip_failed_" . $self->{'error'})
                or $error .= " ALSO: Failed to update UPLOAD: $self->{'fastqUploadId'} with ERROR: $self->{'error'}";
        }
        eval {
            $dbh->disconnect();
        };
        if ($@) {
            $error .= " ALSO: error disconnecting from database: $@\n";
        }
        croak $error;
    }
    else {
        $dbh->disconnect();
        if ($@) {
            my $error .= "$@";
            warn "Problem encountered disconnecting from the database - Likely ok: $error\n";
        }
        return 1;
    }
}

=head2 = doZip()

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

    eval {
        # Allow UUID to be provided, basically for testing as this is a random value.
        if (! $self->{'_fastqUploadUuid'}) {
            $self->{'_fastqUploadUuid'} = Bio::SeqWare::Uploads::CgHub::Fastq->getUuid();
        }
        if (! $self->{'_fastqUploadUuid'} =~ /[\dA-f]{8}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{12}/i) {
             croak( "Not a valid uuid: $self->{'_fastqUploadUuid'}" );
        }
        $self->_tagLaneToUpload($dbh, "zip_running");
        $self->_getFilesToZip( $dbh );
        $self->_zip( $dbh );
        $self->_insertFile( $dbh );
        $self->_insertUploadFileRecord( $dbh );
        $self->_updateUploadStatus( $dbh, "zip_completed");
    };
    if ($@) {
        my $error = $@;
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

sub doMeta() {
    my $self = shift;
    my $dbh = shift;


    # Select/tag upload.status = 'meta-running'.
    # Create new meta directory using uuidgen
    # Copy mapsplice experiment.xml and run.xml to this directory.
    # Generate new analysis.xml from template this directory.
    # Create file link.
    # Tag upload as meta-completed

    eval {
        croak("doMeta() not implemented!\n")
    };
    if ($@) {
        my $error = $@;
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

    eval {
        croak("doValidate() not implemented!\n")
    };
    if ($@) {
        my $error = $@;
        croak $error;
    }

    return 1;
}

=head2 = doUpload()

 $obj->doUpload();

=cut

sub doUpload() {
    my $self = shift;
    my $dbh = shift;

    eval {
        croak("doUpload() not implemented!\n")
    };
    if ($@) {
        my $error = $@;
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
    if (! $newUploadStatus) {
        $self->{'error'} = 'no_status_param';
        croak( "No status parameter specified." );
    }

    eval {
        # Transaction to ensures 'find' and 'tag as found' occur in one step,
        # allowing for parallel running.
        $dbh->begin_work();

        $self->_findNewLaneToZip( $dbh );
        $self->_createUploadWorkspace( $dbh );
        $self->_insertZipUploadRecord( $dbh, $newUploadStatus);
        $dbh->commit()
    };
    if ($@) {
        my $error = $@;
        eval {
            $dbh->rollback();
        };
        if ($@) {
            $error .= " ALSO: error rolling back tagLaneToUpload transaction: $@\n";
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
          AND vwf.sample_id NOT IN (
              SELECT u.sample_id
              FROM upload AS u
              WHERE u.target      = ?
          ) order by vwf.lane_id DESC limit 1";

    if ($self->{'verbose'}) {
        print ("SQL to find a lane for zipping:\n$selectionSQL\n");
    }

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
        return 1;  # NORMAL RETURN - Can't find candidate lanes for zipping.
    }

    # Looks like we got data, so save it off.
    $self->{'_laneId'}           = $rowHR->{'lane_id'};
    $self->{'_sampleId'}         = $rowHR->{'sample_id'};
    $self->{'_bamUploadId'}      = $rowHR->{'upload_id'};
    $self->{'_bamUploadBaseDir'} = $rowHR->{'metadata_dir'};
    $self->{'_bamUploadUuid'}    = $rowHR->{'cghub_analysis_id'};
    if ($self->{'verbose'}) {
        print( "Found zip candidate"
            . ". " . "LANE: "                       . $self->{'_laneId'}
            . "; " . "SAMPLE: "                     . $self->{'_sampleId'}
            . "; " . "BAM UPLOAD_ID: "        . $self->{'_bamUploadId'}
            . "; " . "BAM UPLOAD_BASE_DIR: "  . $self->{'_bamUploadBaseDir'}
            . "; " . "BAM UPLOAD_UUID: "      . $self->{'_bamUploadUuid'}
            . "\n");
    }

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
    my $dbh = shift;

    if (! -d $self->{'uploadFastqBaseDir'}) {
        $self->{'error'} = "no_fastq_base_dir";
        croak "Can't find the fastq upload base dir: $self->{'uploadFastqBaseDir'}";
    }

    $self->{'_fastqUploadDir'} = File::Spec->catdir(
        $self->{'uploadFastqBaseDir'}, $self->{'_fastqUploadUuid'}
    );

    if (-d $self->{'_fastqUploadDir'}) {
        $self->{'error'} = 'fastq_upload_dir_exists';
        croak "Upload directory already exists. That shouldn't happen: $self->{'_fastqUploadDir'}\n";
    }

    eval {
        make_path($self->{'_fastqUploadDir'}, { mode => 0775 });
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = "creating_meta_dir";
        croak "Could not create the upload output dir: $self->{'_fastqUploadDir'}\n$!\n$@\n";
    }

    my $fromRunFilePath = File::Spec->catfile( $self->{'_bamUploadDir'},   "run.xml" );
    my $toRunFilePath   = File::Spec->catfile( $self->{'_fastqUploadDir'}, "run.xml" );
    eval {
        cp( $fromRunFilePath, $toRunFilePath );
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = "copying_run_meta_file";
        croak "Could not copy the run.xml meta file FROM: $fromRunFilePath\nTO: $toRunFilePath\n$!\n$error\n";
    }

    my $fromExperimentFilePath = File::Spec->catfile( $self->{'_bamUploadDir'},   "experiment.xml" );
    my $toExperimentFilePath   = File::Spec->catfile( $self->{'_fastqUploadDir'}, "experiment.xml" );
    eval {
        cp( $fromExperimentFilePath, $toExperimentFilePath );
    };
    if ($@) {
        my $error = $@;
        $self->{'error'} = "copying_experiment_meta_files";
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
    if (! $newUploadStatus) {
        $self->{'error'} = 'no_status_param';
        croak( "No status parameter specified." );
    }

    # Setup SQL
    my $sqlTargetForFastqUpload = 'CGHUB_FASTQ';

    my $insertUploadSQL =
        "INSERT INTO upload ( sample_id, target, status, metadata_dir, cghub_analysis_id )
         VALUES ( ?, ?, ?, ?, ? )
         RETURNING upload_id";

    if ($self->{'verbose'}) {
        print ("SQL to insert new upload record for zip:\$insertUploadSQL\n");
    }

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

    if ($self->{'verbose'}) {
        print( "\nInserted fastq UPLOAD: $self->{'_fastqUploadId'}\n");
    }

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
    $self->{'step'} = "getFastqFiles";

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

    if ($self->{'verbose'}) {
        print ("SQL to look for fastq files (from FinalizeCasava): \$fileSelectSQL\n");
    }

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
            if ($self->{'verbose'}) {
                print ("SQL to look for fastq files (from srf2fastq): \$fileSelectSQL\n");
            }

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

    if ($self->{'verbose'}) {
        print( "\nFound fastq1 - WORKFLOW_RUN_ID: $self->{'_workflowRunId'}"
               . " FLOWCELL: $self->{'_flowcell'}"
               . " LANE_INDEX: $self->{'_laneIndex'}"
               . ($self->{'_barcode'}) ? " BARCODE: $self->{'_barcode'}" : ""
               . " FILE_PATH: $self->{'_fastqs'}->[0]->{'filePath'}"
               . " MD5: $self->{'_fastqs'}->[0]->{'md5sum'}"
               . " PROCESSING_ID: $self->{'_fastqs'}->[0]->{'processingId'}"
               . "\n"
        );
    }

    unless ($self->{'_workflowRunId'}            && $self->{'_flowcell'}
        && $self->{'_laneIndex'}               && $self->{'_fastqs'}->[0]->{'filePath'}
        && $self->{'_fastqs'}->[0]->{'md5sum'} && $self->{'_fastqs'}->[0]->{'processingId'}
    ) {
        $self->{'error'} = 'fastq_file_1_data';
        croak "Missing data for fastq file 1."
    }

    # Second fastq may exist
    if (defined $row2HR) {
        $self->{'_fastqs'}->[1]->{'filePath'}     = $row2HR->{'file_path'};
        $self->{'_fastqs'}->[1]->{'md5sum'}       = $row2HR->{'md5sum'};
        $self->{'_fastqs'}->[1]->{'processingId'} = $row2HR->{'processing_id'};

        if ($self->{'verbose'}) {
            print( "\nFound fastq2 - WORKFLOW_RUN_ID: $row2HR->{'workflow_run_id'}"
                   . " FLOWCELL: $row2HR->{'flowcell'}"
                   . " LANE_INDEX: $row2HR->{'lane_index'}"
                   . ($row2HR->{'barcode'}) ? " BARCODE: $row2HR->{'barcode'}" : ""
                   . " FILE_PATH: $self->{'_fastqs'}->[1]->{'filePath'}"
                   . " MD5: $self->{'_fastqs'}->[1]->{'md5sum'}"
                   . " PROCESSING_ID: $self->{'_fastqs'}->[1]->{'processingId'}"
                   . "\n"
            );
        }

        unless (   $row2HR->{'workflow_run_id'}          && $row2HR->{'flowcell'}
                && $row2HR->{'lane_index'}              && $self->{'_fastqs'}->[1]->{'filePath'}
                && $self->{'_fastqs'}->[1]->{'md5sum'}  && $self->{'_fastqs'}->[1]->{'processingId'}
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
    my $newStatus = shift;
    if (! defined $newStatus || ! ($self->{'_fastqUploadId'}) ) {
        croak "Can't update upload record ID = $self->{'_fastqUploadId'}"
        . " to status = '$newStatus'.";
    }
    my $updateSQL =
        "UPDATE upload
         SET status = ?
         WHERE upload_id = ?";
    if ($self->{'verbose'}) {
        print "Update upload SQL: $updateSQL\n";
    }
    eval {
        $dbh->begin_work();
        my $updateSTH = $dbh->prepare($updateSQL);
        $updateSTH->execute($newStatus, $self->{'_fastqUploadId'});
        my $rowsAffected = $updateSTH->rows();
        $updateSTH->finish();

        if (! defined $rowsAffected || $rowsAffected != 1) {
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
        croak "Failed to update status of $self->{'_fastqUploadId'} to $newStatus: $error\n";
    }
    
    return 1;
}

=head2 _insertFile()

    $self->_insertFile( $dbh )

=cut

sub _insertFile {

    my $self = shift;
    my $dbh = shift;

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

    my $zipFileMetaType = "application/tar-gz";
    my $zipFileType = "fastq-by-end-tar-bundled-gz-compressed";
    my $zipFileDescription = "The fastq files from one lane's sequencing run, tarred and gzipped. May be one or two files (one file per end).";

    my $newFileSQL =
        "INSERT INTO file ( file_path, meta_type, type, description, md5sum )"
     . " VALUES ( ?, ?, ?, ?, ? )"
     . " RETURNING file_id";

    if ($self->{'verbose'}) {
         print "Insert file record SQL: $newFileSQL\n";
    }

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
    if ($self->{'verbose'}) {
        print "Inserted FILE_ID: $self->{'_zipFileId'}\n";
    }
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

    my $newProcessingFilesSQL =
        "INSERT INTO processing_files (processing_id, file_id)"
     . " VALUES (?,?)";

    if ($self->{'verbose'}) {
         print "Insert processing_files record SQL: $newProcessingFilesSQL\n";
    }

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
        $newProcessingFilesSTH->execute(
            $self->{'_fastqs'}->[1]->{'processingId'}, $self->{'_zipFileId'}
        );
        $rowsInserted = $newProcessingFilesSTH->rows();
        if ($rowsInserted != 1) {
            $self->{'error'} = "insert_processsing_files_2";
            croak "failed to insert processing_files record for fastq 2\n";
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

    my $newUploadFileSQL =
        "INSERT INTO upload_file (upload_id, file_id)"
     . " VALUES (?,?)";

    if ($self->{'verbose'}) {
         print "Insert upload_file record SQL: $newUploadFileSQL\n";
    }

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

    return 1;

}

=head2 _zip()

    $self->_zip();

Actually does the zipping, and returns 1, or dies setting 'error' and returning
an error message.


=cut

sub _zip() {
    my $self = shift;
    my $dbh = shift;

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
    if  ($self->{'verbose'} ) {
        print "ZIP COMMAND: $command\n";
    }

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

   $ cpanm git://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.003
 or
   $ cpanm https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.003.tar.gz

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
