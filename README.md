# NAME

Bio::SeqWare::Uploads::CgHub::Fastq - Support uploads of fastq files to cghub

# VERSION

Version 0.000.008

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
      # $runMode one of: ZIP META VALIDATE SUBMIT_META SUBMIT_FASTQ ALL
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

- 1

Identify a lane to zip. If none found, exits, else inserts a new upload record
with ` target = 'CGHUB' and status = 'zip_running' `. The is done as a
transaction to allow parallel running. For a lane to be selected, it must have
an existing upload record with ` target = 'CGHUB' and external_status = 'live' `.
That upload record is linked through a ` file ` table record via the ` vw_files `
view to obtain the lane. If there are any upload records associated with the
selected lane that have ` target = CGHUB_FASTQ `, then that lane will not
be selected.

- 2

Once selected, a new (uuidgen named) directory for this upload is created in
the instance's 'uploadDataRoot' directory. The previously generated
`experiment.xml` and `run.xml` are copied there. The analysis.xml is not
copied as it will be recreated by the META step. The upload record is
modified to record this new information, or to indicate failure if it did not
work.

__NOTE__: Using copies of original xml may not work for older runs as they may
have been consitent only with prior versions of the uplaod schema.

- 3

The biggest problem is finding the fastq file or files for this lane. Since
there is no way to directly identify the input fastq files used by the
Mapsplice run, the assumption is made that The file/s generated by
any completed FinalizeCasava run for this lane, if present, are used. IF not
present then the file/s generated by any completed srf2fastq run is used. If
neither are present, then an error is signalled and the upload record is updated.

- 4

The fastq files identified are then validated on the system, and then tar/gzipped
to the spedified OutputDataRoot, in a subdirectory named for this program, named
like flowcell\_lane\_barcode.fastq.tar.zip. If errors occur, updates upload
status.

- 5

When done, validate output, calculate md5 sum, insert a new file record and
new processing\_files records (linking the new file and the processing\_ids for
the  input fastq). Updates upload record to zip\_completed to indicate done.

## doMeta()

    $obj->doMeta();

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

## = doValidate()

    $obj->doValidate();

## = doSubmitMeta()

    $obj->doSubmitMeta();

## = doSubmitFastq()

    $obj->doSubmitFastq();

## = getAll()

    my $settingsHR = $obj->getAll();
    

Retrieve a copy of the properties assoiciated with this object.
=cut

sub getAll() {
    my $self = shift;
    my $copy;
    for my $key (keys %$self) {
        \# Skip internal only (begin with "\_") properties
        if ($key !~ /^\_/) {
            $copy->{$key} = $self->{$key};
        }
    }
    return $copy;
}

# INTERNAL METHODS

NOTE: These methods are for _internal use only_. They are documented here
mainly due to the effort needed to separate user and developer documentation.
Pay no attention to code behind the underscore. These are not the methods you are
looking for. If you use these function _you are doing something wrong._

## \_tagLaneToUpload()

    $self->_tagLaneToUpload( $dbh );

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

    $self->_updateUploadStatus( $dbh, $newStatus );

Set the status of the internally referenced upload record to the specified
$newStatus string.

## \_insertFile()

    $self->_insertFile( $dbh )

## \_insertFileRecord

    $self->_insertFileRecord( $dbh )

## \_insertProcessingFileRecords

    $self->_insertProcessingFileRecords( $dbh )

## \_insertUploadFileRecord()

    $self->_insertUploadFileRecord( $dbh )

## \_zip()

    $self->_zip();

Actually does the zipping, and returns 1, or dies setting 'error' and returning
an error message.



## \_changeUploadRunStage

    $obj->_changeUploadRunStage( $dbh $fromStatus, $toStatus );
    

Loks for an upload record with the given $fromStatus status. If can't find any,
just returns undef. If finds one, then changes its status to the given $toStatus
and returns that upload record as a HR with the column names as keys.

This does not set error as failure would likely be redundant.

Croaks without parameters, if there are db errors reported, or if no upload
can be retirived.

## \_getTemplateData

    $obj->_getTemplateData( $dbh );

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

    self->_validateMeta();

## \_submitMeta

    $obj->_submitMeta();

## \_submitMeta

    $obj->_submitMeta();

# AUTHOR

Stuart R. Jefferys, `<srjefferys (at) gmail (dot) com>`

Contributors:
  Lisle Mose (get\_sample.pl and generate\_cghub\_metadata.pl)
  Brian O'Conner



# DEVELOPMENT

This module is developed and hosted on GitHub, at
["/github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq" in p5-Bio-SeqWare-Config https:](http://search.cpan.org/perldoc?p5-Bio-SeqWare-Config https:#/github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq).
It is not currently on CPAN, and I don't have any immediate plans to post it
there unless requested by core SeqWare developers (It is not my place to
set out a module name hierarchy for the project as a whole :)

# INSTALLATION

You can install a version of this module directly from github using

      $ cpanm git://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.008
    or
      $ cpanm https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.008.tar.gz

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

This module was developed for use with [SegWare ](http://search.cpan.org/perldoc?http:#/seqware.github.io).

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
