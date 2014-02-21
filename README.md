# NAME

Bio::SeqWare::Uploads::CgHub::Fastq - Support uploads of fastq files to cghub

# VERSION

Version 0.000.030

# SYNOPSIS

    use Bio::SeqWare::Uploads::CgHub::Fastq;

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new( $paramHR );
    $obj->run();
    $obj->run( "ZIP" );

# DESCRIPTION

Supports the upload of zipped fastq file sets for samples to cghub. Includes
db interactions, zip command line convienience functions, and meta-data
generation control. The meta-data uploads are a hack on top of a current
implementation.

## Conventions

Errors are reported via setting $self->{'error} and returning undef.

Any run mode can be repeated; they should be self-protecting by persisting
approriate text to the upload record status as <runMode>\_<running|completed|failed\_<message>>.

Each runmode should support the --rerun flag, eventually. That probably
requires separating the selection and the processing logic, with --rerun only
supported by the processing logic.

# CLASS METHODS

## new()

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new( $paramHR );

Creates and returns a Bio::SeqWare::Uploads::CgHub::Fastq object. Takes
a hash-ref of parameters, each of which is made avaialble to the object.
Don't use parameters beging with a \_ (underscore). These may be overwritten.
The parameter 'error' is cleared automatically, 'myName' is set to
"upload-cghub-fastq\_$VERSION" where version is the version of this module,
like 0.000007"

## getUuid()

## reformatTimeStamp()

    Bio::SeqWare::Uploads::CgHub::Fastq->reformatTimeStamp( $timeStamp );

Takes a postgresql formatted timestamp (without time zone) and converts it to
an aml time stamp by replacing the blank space between the date and time with
a capital "T". Expects the incoming $timestamp to be formtted as
`qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}\d{2}\.?\d*$/`

## getFileBaseName

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

### Examples:

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

# INSTANCE METHODS

## run()

      $obj->run();
    # or
      $obj->run( $runMode );
      # $runMode one of: ZIP META VALIDATE SUBMIT_META SUBMIT_FASTQ LIVE ALL
    # or
      $obj->run( $runMode, $dbh );
    # or
      $obj->run( undef, $dbh );

This is the "main" program loop, associated with running `upload-cghub-fastq`
This method can be called with or without a parameter. If called with no
$runmode, it uses the current value of the instance's 'runMode' property. All
allowed values for that parameter are supported here: case insenistive "ZIP",
"META", "VALIDATE", "SUBMIT\_META", "SUBMIT\_FASTQ" and "ALL". Each parameter
causes the associated "do..." method to be invoked, although "ALL"" causes
each of the 5 do... methods to be invoked in order as above.

This method will either succeed and return 1 or set $self->{'error'}
and die. If an upload record id is known when an error occurs, the upload.status
field for that upload record will be updated to "$STAGE\_failed\_$ERROR", where
$STAGE is the stage in which the error occurs, and $ERROR is $self->{'error'}.

This method calls itself to allow nested processing, and allows passing a
database handle as a parameter to support that. If not provided, a connection
will be created. Due to the length of some of the processing, the passed
connection may be invalid an fail.

## doZip()

    $obj->doZip( $dbh );

Initially identifies a lane for needing fastqs uploaded and tags it as running.
Then it creates the basic upload directory, retrieves upload meta data files
from prior runs, identifies the fastq files from that lane to uplaod, zips them,
makes appropriate entires in the seqware database, and then indicates when
done.

Either returns 1 to indicated completed successfully, or undef to indicate
failure. Check $self->{'error'} for details.

The status of this lanes upload is visible externally through the upload
record with target = CGHUB\_FASTQ and status 'zip-running', 'zip-completed', or
'zip\_failed\_<error\_message>'. Possible error states the upload record could be
set to are:

'zip\_failed\_no\_wfID', 'zip\_failed\_missing\_fastq' 'zip\_failed\_tiny\_fastq',
'zip\_failed\_fastq\_md5', 'zip\_failed\_gzip\_failed', 'zip\_failed\_unknown'
'zipval\_error\_missing\_zip', 'zipval\_error\_tiny\_zip', 'zipval\_error\_md5',
'zipval\_error\_file\_insert' 'zipval\_error\_unknown'

1. Identify a lane to zip. If none found, exits, else inserts a new upload record
with ` target = 'CGHUB' and status = 'zip_running' `. The is done as a
transaction to allow parallel running. For a lane to be selected, it must have
an existing upload record with ` target = 'CGHUB' and external_status = 'live' `.
That upload record is linked through a ` file ` table record via the ` vw_files `
view to obtain the lane. If there are any upload records associated with the
selected lane that have ` target = CGHUB_FASTQ `, then that lane will not
be selected.
2. Once selected, a new (uuidgen named) directory for this upload is created in
the instance's 'uploadDataRoot' directory. The previously generated
`experiment.xml` and `run.xml` are copied there. The analysis.xml is not
copied as it will be recreated by the META step. The upload record is
modified to record this new information, or to indicate failure if it did not
work.

    __NOTE__: Using copies of original xml may not work for older runs as they may
    have been consitent only with prior versions of the uplaod schema.

3. The biggest problem is finding the fastq file or files for this lane. Since
there is no way to directly identify the input fastq files used by the
Mapsplice run, the assumption is made that The file/s generated by
any completed FinalizeCasava run for this lane, if present, are used. IF not
present then the file/s generated by any completed srf2fastq run is used. If
neither are present, then an error is signalled and the upload record is updated.
4. The fastq files identified are then validated on the system, and then tar/gzipped
to the spedified OutputDataRoot, in a subdirectory named for this program, named
like flowcell\_lane\_barcode.fastq.tar.zip. If errors occur, updates upload
status.
5. When done, validate output, calculate md5 sum, insert a new file record and
new processing\_files records (linking the new file and the processing\_ids for
the  input fastq). Updates upload record to zip\_completed to indicate done.

## doMeta()

    $obj->doMeta( $dbh );

From $obj, reads:
 \_metaDataRoot      - Absolute path to some directory
 \_fastqUploadId     - Id for new fastq upload record
 \_mapSpliceUploadId - Id for old mapsplice record
 \_uuidgenExec       - Executable for uuid generation
 \_fastqzTemplateDir - Directory where analysis.xml template is.
 \_realFileForUpload - Full path filename to fastqDir.

To $obj, adds
 \_metaDataUuid   - The generated UUID used for this uploads meta-data.
 \_metaDataPath   - Full path, = \_metaDataRoot + \_\_metaDataUuid
 \_linkFileName   - The local name

## doValidate()

    $obj->doValidate( $dbh );

## doSubmitMeta()

    $obj->doSubmitMeta( $dbh );

## doSubmitFastq()

    $obj->doSubmitFastq( $dbh );

## doLive()

    $obj->doLive( $dbh );

Sets the external status for an upload record. Because there may be a delay
between submitting a file and cghub setting it live, This uses the
\--recheckWaitHours parameter -- as self-"{'recheckWaitHours'} -- to determine
if it can pick up the record yet

## doRerun

     $obj->doRerun( $dbh )

Uses --rerunWait <days>

Reruns failed sample fastq file uploads automatically. Failed samples are
identified, associated database records (other than the upload record) are
cleared, and any files on the file system are deleted. Parent data directories
are NOT deleted. After everything finishes, the upload record itself will be
deleted to allow for rerunning from scratch. To prevent a sample from being
rerun, replace 'fail' or 'failed' in the status with 'bad'

Selecting a sample for rerun:

A CGHUB-FASTQ upload record is a rerun candidate if upload.status contains
'fail', upload.external\_status is 'live' and upload.tstmp is more than
\--rerunWait hours ago. If selected, it will have its status changed to
'rerun\_running'. If rerun fails, the status will be changed to
'rerun\_failed\_<error\_tag>'. Note that failed reruns will themselves
automatically be rerun after waiting. In-process samples will not be selected
as they have no "fail" in their status. \[TODO - deal with stuck "running"
samples\]. Unprocessed samples will not be selected as they have no CGHUB-FASTQ
upload record.

1\. Gather information:

Once a sample is selected for clearing, the following reverse walk is performed
to obtain information

    uploadId          <= upload
    sampleId          <= upload
    metadataPath      <= upload
    fileId            <= upload_file
    fastqZipFilePath  <= file
    processingFilesId <= processing_files

2\. Clear file system:

Remove the fastqZipFilePath fastq file and the metadataPath metadata directory
(with all its contents). \[TODO: Figure out saftey check for this.\]

3\. Clear database as transaction:

removing in order processing\_files records, upload\_file records, file records.

4\. Use upload record to signla done

If no errors, delete the upload record, otherwise update its status to
'rerun\_failed\_<message> status.

## \_getRerunData

    my $dataHR = $self->_getRerunData( $dbh, $uploadHR );

Returns a hashref of all the data needed to clear an upload, including the
uploadHR data. Missing data indicated by key => undef.

    $dataHR->{'upload'}->{'upload_id'}
                       ->{'sample_id'}
                       ->{'target'}
                       ->{'status'}
                       ->{'external_status'}
                       ->{'metadata_dir'}
                       ->{'cghub_analysis_id'}
                       ->{'tstmp'}
           ->{'file_id'}
           ->{'file_path'}
           ->{'processing_files_id_1'}
           ->{'processing_files_id_2'}

## \_getAssociatedFileId

    my $fileId = $self->_getAssociatedFileId( $dbh, $uploadId)

Looks up the upload\_file records assoicated with the specified upload table.
If there are no such records, returns undef. If there is one record, returns
the file\_id. If there are multiple such records, dies.

## \_getFilePath

    my $filePath = $self->_getFilePath( $dbh, $fileId)

Looks up the file record associated with the given file id and returns the
path associated with that file.

## \_getAssociatedProcessingFilesIds

    my ($id1, $id2) = $self->_getAssociatedProcessingFilesIds( $dbh, $fileId)

Looks up the (0 to 2) file records in processing\_files associated with the given
file id and returns the ids of those record. If more than 2 records are
returned, an error is thrown. If only one record is found, returns ($id1, undef).
If no records are found, returns (undef, undef).

## \_cleanDatabase

    $self->_cleanDatabase( $dbh, $rerunDataHR) or die( "failed" ).

Clears all database records as specified by the $rerunDataHR provided,
as follows:

    If $dataHR->{'processing_files_id_2'},
         deletes 1 processing_files record with that id.
    If $dataHR->{'processing_files_id_1'},
         deletes 1 processing_files record with that id.
    If $dataHR->{'file_id'},
         deletes 1 upload_file record with that file_id and upload_id.
    If $dataHR->{'file_path'},
         deletes 1 file record with that file_id.
    If $dataHR->{'upload'}->{upload_id},
         deletes 1 upload record with that upload_id.

If any data is missing (except the upload\_id), no attempt will be made to
delete the associated record  If at any time the number of records scheduled
for deletion is not 1, then all deletions will be rolled back and this will
exit with error. It is also an error if the upload\_id is not provided.

Note: No checks are made to ensure that the provided data are related, i.e. any
id's can be provided. However, there are foreign key constraints between
processing\_files and file, and between upload and file, it is unlikely that
deletions will all complete if not correctly associated.

## \_deleteProcessingFilesRec

    $self->_deleteProcessingFilesRec( $dbh, $pfId1, pfId2 );

Deletes the processing\_files table records with the specified ids. Expects 1 and
only 1 record to be deleted for each defined $pfId provided. otherwise throws
error and sets $self->{'error'}.

Performs no parameter checks, assumes $dbh and $pfId1 and pfId2 have been
checked prior, but ok if one or both of the ids are undef.

Returns 1 if doesn't die.

## \_deleteUploadFileRec

    $self->_deleteUploadFileRec( $dbh, $uploadId, $fileId );

Deletes the upload\_file table record with the specified $uploadId and $fileId.
This is not a key-based delete, but still expects 1 and only 1 record to be
deleted, otherwise throws error and sets $self->{'error'}. Once this is removed
it will not be easy to resotre the link between the upload record and the file
record, if they are not also removed.

Performs no parameter checks, assumes $dbh, $uploadId, and $fileId have been checked
prior.

Returns 1 if doesn't die.

## \_deleteFileRec

    $self->_deleteFileRec( $dbh, $fileId );

Deletes the file table record with the specified id. Expects 1 and only 1 record to be
deleted, otherwise throws error and sets $self->{'error'}. Will fail if
linked upload\_file record or processing\_files records not removed first.

Performs no parameter checks, assumes $dbh and $fileId have been checked
prior.

Returns 1 if doesn't die.

## \_deleteUploadRec

    $self->_deleteUploadRec( $dbh, $uploadId );

Deletes the upload table record with the specified id. Expects 1 and only 1 record to be
deleted, otherwise throws error and sets $self->{'error'}. Will fail if
linked upload\_file record not removed first.

Performs no parameter checks, assumes $dbh and $uploadId have been checked
prior.

Returns 1 if succeeds.

## \_cleanFileSystem

    $self->_cleanFileSystem( $dbh, $rerunDataHR );

Deletes the zip file, if it existts and the upload directory and contents, if it
exists. Does not delete the directory the zip file is in even if it is the
only file there.

Returns 1 if succeeds, dies if fails. The lack of a file or directroy is not an
error. The lack of data in the rerunDataHR is not an error. It is only a failure
to delete when attempting to do so that will trigger an error.

It is an error if the parameters are not specified, or if
$rerunDataHR->upload->upload\_id is not available. It is also an error if
an upload directory has been specified (upload.metadata\_dir) and there is
no uploadFastqBaseDir specified in the seqware config file or the application
parameters.

## \_deleteFastqZipFile

    $self->_deleteFastqZipFile( $file );

Helper to delete a file if specified. Intended to be the fastq.tar.gz, dies if
filename isn't full path or if doesn't match "\*fastq\*.tar.gz"

## \_deleteUploadDir

    $self->_deleteUploaddir( $uploadDir );

Helper to delete the upload dir if specified. Intended to be the uuid dir, dies
if dirname isn't a full path or if doesn't end with a uuid directory.

## getAll()

    my $settingsHR = $obj->getAll();

Retrieve a copy of the properties assoiciated with this object.

## getTimeStamp()

    Bio::SeqWare::Uploads::CgHub::Fastq->getTimeStamp().
    Bio::SeqWare::Uploads::CgHub::Fastq->getTimeStamp( $unixTime ).

Returns a timestamp formated like YYYY-MM-DD\_HH:MM:SS, zero padded, 24 hour
time. If a parameter is passed, it is assumed to be a unix epoch time (integer
or float seconds since Unix 0). If no parameter is passed, the current time will
be queried. Time is parsed through perl's localtime().

# INTERNAL METHODS

NOTE: These methods are for _internal use only_. They are documented here
mainly due to the effort needed to separate user and developer documentation.
Pay no attention to code behind the underscore. These are not the methods you are
looking for. If you use these function _you are doing something wrong._

## \_tagLaneToUpload()

    $self->_tagLaneToUpload( $dbh, $newUploadStatus );

## \_getSampleSelectionSql()

    my $sql = $obj->_getSampleSelectionSql();
    

Examines the various sample filter parameters and generates the sql
query that selects for a sample that meets all filter values. Parameters
looked for in the $self hash-ref are sampleId, sampleTitle, sampleAccession,
sampleAlias, sampleAlias, and sampleUuid, which are matched by equality to
the associated database sample table field.

## \_findNewLaneToZip()

Identifies a lane that needs its data uploaded to CgHub. To qualify for
uploading, a lane must have had a succesful bam file uploaded, and not have had
a fastq upload (succesful, unsuccesful, or in progress). A lane is uniquely
identified by its db lane.lane\_id.

If a bam file was uploaded succesfully, an upload (u) record will exist with
fields ` u.lane_id == lane.lane_id `, ` u.target == "CGHUB" ` and
` u.external_status == 'live' `. This record also has a field ` u.sample_id `
which holds the sample.sample\_id for the lane it represents.

IF a lane has ever been considered for fastq-upload, it will have an upload
record with fields ` u.lane_id == lane.lane_id ` and ` u.target == "CGHUB" `.
The status of this record indicates what state the processing of this is in,
but don't care. Whatever state it is in, don't pick it up here.

If verbose is set, this will echo the SQL query used and the results obtained.

To prevent collisions with parallel runs, this query should be combined in a
transaction with and update to insert an upload record that tags a lane as
being processed for fastq upload (i.e. with upload.target = CGHUB\_FASTQ).

Uses

    $self->{'verbose'} => echo SQL, set values

    $self->{'sampleId'}        => select by sample.sample_id
    $self->{'sampleAccession'} => select by sample.sw_accession
    $self->{'sampleAlias'}     => select by sample.alias
    $self->{'sampleUuid'}      => select by sample.tcga_uuid
    $self->{'sampleTitle'}     => select by sample.title
    $self->{'sampleType'}      => select by sample.type

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

## \_createUploadWorkspace

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

Creates <\_fastqUploadDir> directory = <fastqUploadBaseDir> / <\_fastqUploadUuid>.
Copies run.xml and experiment.xml from <\_bamUploadDir> to
<\_fastqUploadDir>

Errors:

    zip_failed_no_fastq_base_dir       => No such dir: fastqUploadBaseDir
    zip_failed_fastq_upload_dir_exists => Exists: uuid upload dir
    zip_failed_creating_meta_dir       => Not Created: uuid upload dir
    zip_failed_copying_run_meta_file   => Not copied: run.xml
    zip_failed_copying_experiment_meta_file  => Not copied: expwriment.xml

## \_insertZipUploadRecord()

    $self->_insertZipUploadRecord( $dbh, $new status )

Inserts a new upload record for the fastq upload being initiated. Takes as
a parameter the status of this new upload record.

Either returns 1 for success, or sets 'error' and croaks with an error message.

Inserts a new upload table record for CGHUB\_FASTQ, for the same sample
as an existing upload record for CGHUB, when the CGHUB record is for a live
mapsplice upload and no CGHUB\_FASTQ exists for that sample.

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
    

## \_fastqFilesSqlSubSelect( ... )

    my someFileSQL = "... AND file.file_id EXISTS ("
       . _fastqFilesSqlSubSelect( $wf_accession ) . " )";

Given a workflow accession, returns a string that is an SQL subselect. When
executed, this subselect will return a list of file.fastq\_id for fastq files
from the given workflow id.

This is required because different workflows need different amounts of
information to identify the fastq files relative to other files generated
from the same workflow. Using this allows separating the (fixed) SQL needed
to select the sample and the (varying) code to select the fastq files for that
sample.

If the wf\_accession is not known, will return undef. The wf\_accession may
be provided by an internal object property '\_fastqWorkflowAccession' 

For example:

    my $sqlSubSelect = _fastqFilesSqlSubSelect( 613863 );
    print ($sqlSubSelect);

    SELECT file.file_id FROM file WHERE file.workflowAccession = 613863
        AND file.algorithm = 'FinalizeCasava'

    my $SQL = "SELECT f.path, f.md5Sum"
            . " FROM file f"
            . " WHERE sample_id = 3245"
            . " AND f.file IN ( " . _fastqFilesSqlSubSelect( 613863 ) . " )"

## \_getFilesToZip()

    $self->_getFilesToZip( $dbh, $workflowAccession );

Identifies the fastq files that go with the uploaded bam file. If the
$workflowAccession is given, that is assumed to be the workflow the
fastq files come from. If this is not defined, it will first look at 613863
(FinalizeCasava) and then 851553 (srf2fastq) and use the first ones it finds.
Reports whatever riles it finds (one or two) without otherwise checking for
single or paired ends.

Dies for a lot of database errors.

## \_updateUploadStatus( ... )

    $self->_updateUploadStatus( $dbh, $uploadId, $newStatus );

Set the status of the internally referenced upload record to the specified
$newStatus string.

## \_updateExternalStatus( ... )

    $self->_updateExternalStatus( $dbh, $uploadId, $newStatus, $externalStatus );

Set the status and external status of the internally referenced upload record
to the specified $newStatus and $externalStatus strings.

## \_insertFile()

    $self->_insertFile( $dbh )

## \_insertFileRecord

    $self->_insertFileRecord( $dbh )

## \_insertProcessingFilesRecords

    $self->_insertProcessingFilesRecords( $dbh )

## \_insertUploadFileRecord()

    $self->_insertUploadFileRecord( $dbh )

## \_zip()

    $self->_zip();

Actually does the zipping, and returns 1, or dies setting 'error' and returning
an error message.

## \_changeUploadRunStage

    $obj->_changeUploadRunStage( $dbh, $fromStatus, $toStatus );
       # or
    $obj->_changeUploadRunStage( $dbh, $fromStatus, $toStatus, $hourDelay );

Looks for an upload record with status like the given $fromStatus status. If
can't find any, just returns undef. If finds one, then changes its status to the
given $toStatus and returns that upload record as a HR with the column names
as keys.

If the $hourDelay parameter is specified, then the update time of the upload
record will also be checked, and only records whose status was changed more than
the specified number of hours ago will be changed.

This does not change the upload record on failure as that would need to call
this function, which just failed...

Croaks if there are db errors reported, or if upload record is retirived but
is not valid or can not be updated. Again, it is not an error to find no
matching record on select (returns undef in that case).

## \_getTemplateData

    $obj->_getTemplateData( $dbh );

## \_getTemplateDataReadEnds

    $ends = $self->_getTemplateDataReadEnds( $dbh, $eperiment.sw_accession );

Returns 1 if single ended, 2 if paired-ended. Based on the number
of application reads in the associated experiment\_spot\_design\_read\_spec.
Dies if any other number found, or if any problem with db access.

## \_getTemplateDataReadLength

    $baseCountPerRead = _getTemplateDataReadLength( $dbh, sampleId );

Examines first 1000 lines of the BAM file associated with this fastq looking
for the max read length. Finding the BAM file is easier than getting back to
the fastq.

## \_makeFileFromTemplate

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

## \_validateMeta

    self->_validateMeta( $uploadHR );

## \_submitMeta

    $obj->_submitMeta( $uploadHR );

## \_submitFastq

    $obj->_submitFastq( $uploadHR );

## \_live

    $obj->_live( $uploadHR );

## \_makeCghubAnalysisQueryUrl

    my $queryUrl = $self->_makeCghubAnalysisQueryUrl( $uploadHR );

Generate the URL query string for retrieving the analysis xml data from CGHUB.

## \_pullXmlFromUrl

    my $xmlString = $self->_pullAnalysisXML( $queryUrl );

Call out to web and get xml for a given URL back as string.

## \_xmlToHashRef

    my xmlHR = $self->_xmlToHashRef( $analysisXML );

Convert the xml to a hash-ref.

## \_evaluateExternalStatus

    my $externlStatus = $self->_evaluateExternalStatus( $xmlAsHR );

Determine the appropriate external status given the infromation retrieved
from cghub (parsed from downloaded metadata).

## \_statusFromExternal

    my $newStatus = $self->_statusFromExternal( $externalStatus );

Logic to determine the appropriate status given the external status.

## sayVerbose()

    $self->sayverbose( $message ).

Prints the given message preceeded with "\[INFO\] $timestamp - ", wrapped to
132 columns with all lines after the first indented with a "\\t". The
timestamp is generated by `getTimeStamp()`.

# AUTHOR

Stuart R. Jefferys, `<srjefferys (at) gmail (dot) com>`

Contributors:
  Lisle Mose (get\_sample.pl and generate\_cghub\_metadata.pl)
  Brian O'Conner

# DEVELOPMENT

This module is developed and hosted on GitHub, at
["/github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq" in p5-Bio-SeqWare-Config https:](https://metacpan.org/pod/p5-Bio-SeqWare-Config&#x20;https:#github.com-theobio-p5-Bio-SeqWare-Uploads-CgHub-Fastq).
It is not currently on CPAN, and I don't have any immediate plans to post it
there unless requested by core SeqWare developers (It is not my place to
set out a module name hierarchy for the project as a whole :)

# INSTALLATION

You can install a version of this module directly from github using

      $ cpanm git://github.com/theobio/archive/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.030
    or
      $ cpanm https://github.com/theobio/archive/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.030.tar.gz

Any version can be specified by modifying the tag name, following the @;
the above installs the latest _released_ version. If you leave off the @version
part of the link, you can install the bleading edge pre-release, if you don't
care about bugs...

You can select and download any package for any released version of this module
directly from [https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/releases](https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/releases).
Installing is then a matter of unzipping it, changing into the unzipped
directory, and then executing the normal (C>Module::Build>) incantation:

     perl Build.PL
     ./Build
     ./Build test
     ./Build install

# BUGS AND SUPPORT

No known bugs are present in this release. Unknown bugs are a virtual
certainty. Please report bugs (and feature requests) though the
Github issue tracker associated with the development repository, at:

[https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/issues](https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/issues)

Note: you must have a GitHub account to submit issues.

# ACKNOWLEDGEMENTS

This module was developed for use with [SegWare ](https://metacpan.org/pod/&#x20;http:#seqware.github.io).

# LICENSE AND COPYRIGHT

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
