#! /usr/bin/env perl
use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Spec;           # Portable file handling

use File::Temp;           # Simple files for testing
use DBI;

use Bio::SeqWare::Config; # Access SeqWare settings file as options
use Bio::SeqWare::Db::Connection;
use Bio::SeqWare::Uploads::CgHub::Fastq;

use DBD::Mock;
use DBD::Mock::Session;   # Test DBI data in/out, but not Testing Files and Test Modules
use Test::File::Contents;
use Test::More 'tests' => 14;   # Run this many Test::More compliant subtests.

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $TEMP_DIR = File::Temp->newdir();  # Auto-delete when out of scope

#
# Set up fastq files to test on
#

my $FASTQ1 = File::Spec->catfile( $DATA_DIR, "paired_end_one.fastq");
my $FASTQ1_MD5 = '83a37a8cb75d0aa656a4104b2c77a3f6';

my $FASTQ2 = File::Spec->catfile( $DATA_DIR, "paired_end_two.fastq");
my $FASTQ2_MD5 = 'a782ba63c137eda8b0094f34fecf68b8';

my $FASTQ_MISMATCH = File::Spec->catfile( $DATA_DIR, "single_end.fastq");
my $FASTQ_MISMATCH_MD5 = '23487060edd0e7121daab3f03177828f';

#
# Set up data to create objects
#

my $CONFIG = Bio::SeqWare::Config->new();
my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode'      => 'zIP',
    'minFastqSize' => 10,
    'dataRoot'     => $TEMP_DIR,
    'uploadFastqBaseDir' => $TEMP_DIR,
    'uploadBamBaseDir'   => undef,
    'myName'       => 'DELETE_ME-upload-cghub-fastq_0.0.1',
    'rerun'        => 1,
    'xmlSchema'    => 'SRA_1-5',
};

#
# Defaults for zip file record
#

my $ZIP_FILE_TYPE        = 'fastq-by-end-tar-bundled-gz-compressed';
my $ZIP_FILE_META_TYPE   = 'application/tar-gz';
my $ZIP_FILE_DESCRIPTION = "The fastq files from one lane's sequencing run, tarred and gzipped. May be one or two files (one file per end).";
my $ZIP_FILE_FAKE_MD5    = "ABBACADABBA";

my $FLOWCELL   = '130619_UNC13-SN749_0277_BC293YACXX';
my $LANE_INDEX = '0';
my $BARCODE    = 'CTTGTA';
my $BASE_NAME  = $FLOWCELL . "_" . ($LANE_INDEX + 1) . "_" . $BARCODE;

my $EXPECTED_OUT_DIR = File::Spec->catdir(
     $OPT_HR->{'dataRoot'}, $FLOWCELL, $OPT_HR->{'myName'}
);
my $EXPECTED_OUT_FILE = File::Spec->catfile(
    $EXPECTED_OUT_DIR, $BASE_NAME . ".tar.gz"
);

my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0 },
);

# Internal method tests
subtest( '_findNewLaneToZip()'         => \&test__findNewLaneToZip );
subtest( '_createUploadWorkspace()'    => \&test__createUploadWorkspace );
subtest( '_insertZipUploadRecord()'    => \&test__insertZipUploadRecord );
subtest( '_tagLaneToUpload()'          => \&test__tagLaneToUpload );
subtest( '_getFilesToZip()'            => \&test__getFilesToZip );
subtest( '_fastqFilesSqlSubSelect()'   => \&test__fastqFilesSqlSubSelect);
subtest( '_zip()'                      => \&test__zip );
subtest( '_insertFileRecord()'         => \&test__insertFileRecord);
subtest( '_insertProcessingFilesRecord()' => \&test__insertProcessingFilesRecord);
subtest( '_insertFile()'               => \&test__insertFile);
subtest( '_insertUploadFileRecord()'   => \&test__insertUploadFileRecord);
subtest( '_getSampleSelectionSql()'    => \&test__getSampleSelectionSql);
subtest( '_doZip()'                    => \&test__doZip );

subtest( 'run_zip' => \&test_run_zip );

#
# Subtests
#

sub test__findNewLaneToZip {
    plan( tests => 15 );

    my $sampleId    = -19;
    my $laneId      = -12;
    my $uploadId    = -21;
    my $metaDataDir = "t";
    my $uuidDir     = "Data";

    # Test when good data returned
    {
        my @dbEventsOk = ({
            'statement'   => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
            'bound_params' => [],
            'results'     => [
                [ 'lane_id', 'sample_id', 'upload_id', 'metadata_dir', 'cghub_analysis_id' ],
                [ $laneId,    $sampleId,   $uploadId,   $metaDataDir,   $uuidDir           ],
            ],
        });
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'getLanesToZip', @dbEventsOk );

        my $obj = $CLASS->new( $OPT_HR );

        {
            my $got  = $obj->_findNewLaneToZip( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, "Return 1 if found candidate to zip" );
        }
        {
            my $got  = $obj->{'_laneId'};
            my $want = $laneId;
            is( $got, $want, "Lane id stored in object" );
        }
        {
            my $got  = $obj->{'_sampleId'};
            my $want = $sampleId;
            is( $got, $want, "Sample id stored in object" );
        }
        {
            my $got  = $obj->{'_bamUploadId'};
            my $want = $uploadId;
            is( $got, $want, "Mapsplice upload id stored in object" );
        }
        {
            my $got  = $obj->{'_bamUploadBaseDir'};
            my $want = $metaDataDir;
            is( $got, $want, "Mapsplice upload meta data dir is stored in object" );
        }
        {
            my $got  = $obj->{'_bamUploadUuid'};
            my $want = $uuidDir;
            is( $got, $want, "Mapsplice upload UUID stored in object" );
        }
    }

    # Test filtered selection by all paramteters!
    {
        my $opt = {
            %$OPT_HR,
            'sampleId' => -19,
            'sampleAccession' => 999999,
            'sampleAlias' => "PIPE_0000",
            'sampleType' => 'BRCA',
            'sampleTitle' => 'TCGA-CS-6188-01A-11R-1896-07',
            'sampleUuid' => '00000000-0000-0000-0000-000000000000',
        };
        my $obj = $CLASS->new( $opt );

        my @dbEventsOk = ({
            'statement'   => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
            'bound_params' => [ $opt->{'sampleId'}, $opt->{'sampleAccession'},
                $opt->{'sampleAlias'}, $opt->{'sampleUuid'}, $opt->{'sampleTitle'}, $opt->{'sampleType'} ],
            'results'     => [
                [ 'lane_id', 'sample_id', 'upload_id', 'metadata_dir', 'cghub_analysis_id' ],
                [ $laneId,    $sampleId,   $uploadId,   $metaDataDir,   $uuidDir           ],
            ],
        });
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( @dbEventsOk );

        {
            my $got  = $obj->_findNewLaneToZip( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, "Return 1 if found candidate to zip" );
        }
    }

    # Test when no lane to zip is found.
    {
        my $obj = $CLASS->new( $OPT_HR );

        my @dbEventsNone = ({
            'statement' => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
            'bound_params' => [],
            'results'   => [[]],
        });
        $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( 'noLanesToZip', @dbEventsNone );

        {
            my $got  = $obj->_findNewLaneToZip( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, "Return 1 if found no candidate to zip" );
        }
        {
            my $got  = $obj->{'_laneId'};
            my $want = undef;
            is( $got, $want, "Lane id not retrieved for object" );
        }
        {
            my $got  = $obj->{'_sampleId'};
            my $want = undef;
            is( $got, $want, "Sample id not retrieved for object" );
        }
        {
            my $got  = $obj->{'_bamUploadId'};
            my $want = undef;
            is( $got, $want, "Mapsplice upload id not retrieved for object" );
        }
        {
            my $got  = $obj->{'_bamUploadBaseDir'};
            my $want = undef;
            is( $got, $want, "Mapsplice upload meta data dir not retrieved for object" );
        }
        {
            my $got  = $obj->{'_bamUploadUuid'};
            my $want = undef;
            is( $got, $want, "Mapsplice upload UUID not retrieved for object" );
        }
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_findNewLaneToZip();
        };
        {
          like( $@, qr/^_findNewLaneToZip\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__findNewLaneToZip_dbh', "Errror tag if no dbh param");
        }
    }

}

sub test__getSampleSelectionSql {
    plan( tests => 7 );

    my $wantSqlStart =
       "SELECT vwf.lane_id, u.sample_id, u.upload_id, u.metadata_dir, u.cghub_analysis_id
        FROM vw_files AS vwf, upload_file AS uf, upload AS u, sample as s
        WHERE vwf.file_id       = uf.file_id
          AND uf.upload_id      = u.upload_id
          AND u.sample_id       = s.sample_id
          AND u.target          = 'CGHUB'
          AND u.external_status = 'live'
          AND u.metadata_dir    = '/datastore/tcga/cghub/v2_uploads'
          AND vwf.sample_id NOT IN (
              SELECT u.sample_id
              FROM upload AS u
              WHERE u.target      = 'CGHUB_FASTQ'
          )";

    my  $wantSqlEnd = " order by vwf.lane_id DESC limit 1";

    {
        my $opt = { %$OPT_HR, 'sampleId' => -19 };
        my $obj = $CLASS->new( $opt );
        my $wantSqlMiddle = " AND s.sample_id = ?";
        {
            my $want = $wantSqlStart . $wantSqlMiddle . $wantSqlEnd;
            my $got = $obj->_getSampleSelectionSql();
            is( $got, $want, "sample select sql with sample_id");
        }
    }
    {
        my $opt = { %$OPT_HR, 'sampleAccession' => 999999 };
        my $obj = $CLASS->new( $opt );
        my $wantSqlMiddle = " AND s.sw_accession = ?";
        {
            my $want = $wantSqlStart . $wantSqlMiddle . $wantSqlEnd;
            my $got = $obj->_getSampleSelectionSql();
            is( $got, $want, "sample select sql with sw_accession");
        }
    }
    {
        my $opt = { %$OPT_HR, 'sampleAlias' => "PIPE_0000" };
        my $obj = $CLASS->new( $opt );
        my $wantSqlMiddle = " AND s.alias = ?";
        {
            my $want = $wantSqlStart . $wantSqlMiddle . $wantSqlEnd;
            my $got = $obj->_getSampleSelectionSql();
            is( $got, $want, "sample select sql with alias");
        }
    }
    {
        my $opt = { %$OPT_HR, 'sampleUuid' => '00000000-0000-0000-0000-000000000000' };
        my $obj = $CLASS->new( $opt );
        my $wantSqlMiddle = " AND s.tcga_uuid = ?";
        {
            my $want = $wantSqlStart . $wantSqlMiddle . $wantSqlEnd;
            my $got = $obj->_getSampleSelectionSql();
            is( $got, $want, "sample select sql with tcga_uuid");
        }
    }
    {
        my $opt = { %$OPT_HR, 'sampleTitle' => 'TCGA-CS-6188-01A-11R-1896-07' };
        my $obj = $CLASS->new( $opt );
        my $wantSqlMiddle = " AND s.title = ?";
        {
            my $want = $wantSqlStart . $wantSqlMiddle . $wantSqlEnd;
            my $got = $obj->_getSampleSelectionSql();
            is( $got, $want, "sample select sql with title");
        }
    }
    {
        my $opt = { %$OPT_HR, 'sampleType' => 'BRCA' };
        my $obj = $CLASS->new( $opt );
        my $wantSqlMiddle = " AND s.type = ?";
        {
            my $want = $wantSqlStart . $wantSqlMiddle . $wantSqlEnd;
            my $got = $obj->_getSampleSelectionSql();
            is( $got, $want, "sample select sql with type");
        }
    }
    {
        my $opt = {
            %$OPT_HR,
            'sampleId' => -19,
            'sampleAccession' => 999999,
            'sampleAlias' => "PIPE_0000",
            'sampleType' => 'BRCA',
            'sampleTitle' => 'TCGA-CS-6188-01A-11R-1896-07',
            'sampleUuid' => '00000000-0000-0000-0000-000000000000',
        };
        my $obj = $CLASS->new( $opt );
        my $wantSqlMiddle =
              " AND s.sample_id = ? AND s.sw_accession = ? AND s.alias = ?"
            . " AND s.tcga_uuid = ? AND s.title = ? AND s.type = ?";
        {
            my $want = $wantSqlStart . $wantSqlMiddle . $wantSqlEnd;
            my $got = $obj->_getSampleSelectionSql();
            is( $got, $want, "sample select sql with all optional options");
        }
    }
}

sub test__createUploadWorkspace {
    plan( tests => 6 );

    my $opt = { %$OPT_HR,
        'uploadFastqBaseDir' => $TEMP_DIR,
        '_fastqUploadUuid'   => 'UniqueUuid',
    };

    # Run the procedure on a good object
    {
        my $obj = $CLASS->new( $opt );
        {
            my $got  = $obj->_createUploadWorkspace();
            my $want = 1;
            is( $got, $want, "_createUploadWorkspace appears to run succesfully");
        }
    }

    # Should fail if recreate same directory
    {
        my $obj = $CLASS->new( $opt );
        eval {
            $obj->_createUploadWorkspace();
        };
        my $error = $@;
        {
            my $got  = $error;
            my $want = qr/Upload directory already exists/;
            like( $got, $want, "Error message correct if output dir already exists");
        }
        {
            my $got  = $obj->{'error'};
            my $want = 'fastq_upload_dir_exists';
            is( $got, $want, "Error tag correct if output dir already exists");
        }
    }

    # Run the procedure on a bad object
    {
        my $badDirName = "/NoSuchDir_I_h0PE/";
        my $opt = { %$OPT_HR,
            'uploadFastqBaseDir' => $badDirName,
            '_fastqUploadUuid'   => 'UniqueUuid',
        };

        my $obj = $CLASS->new( $opt );
        eval {
            $obj->_createUploadWorkspace();
        };
        my $error = $@;
        {
            ok(! (-d $badDirName), "Directory does not exist before test");
        }
        {
            my $got  = $error;
            my $want = qr/Can\'t find the fastq upload base dir: $badDirName/;
            like( $got, $want, "Error message correct if output dir already exists");
        }
        {
            my $got  = $obj->{'error'};
            my $want = 'no_fastq_base_dir';
            is( $got, $want, "Error tag correct if base dir doesn't exists");
        }
    }
}

sub test__insertZipUploadRecord {
    plan( tests => 6 );

    # Object will be modified, so need local
    my $sampleId    = -19;
    my $uploadId    = -21;
    my $metaDataDir = "t";
    my $uuidDir     = "Data";
    my $status      = "zip_running";

    my $obj = $CLASS->new( $OPT_HR );
    $obj->{'uploadFastqBaseDir'} = $metaDataDir;
    $obj->{'_sampleId'} = $sampleId;
    $obj->{'_fastqUploadUuid'} = $uuidDir;
    $obj->{'_fastqUploadDir'} = File::Spec->catdir( $metaDataDir, $uuidDir);

    my @dbEventsOk = ({
        'statement'   => qr/INSERT INTO upload.*/msi,
        'bound_params' => [ $sampleId, 'CGHUB_FASTQ', $status, $metaDataDir, $uuidDir],
        'results'  => [[ 'upload_id' ], [ $uploadId ]],
    });
    $MOCK_DBH->{'mock_session'} =
        DBD::Mock::Session->new( 'newUploadRecord', @dbEventsOk );

    {
        my $got = $obj->_insertZipUploadRecord( $MOCK_DBH, $status );
        my $want = 1;
        is( $got, $want, "Return 1 if inserted upload record to zip" );
    }
    {
       my $got  = $obj->{'_fastqUploadId'};
       my $want = $uploadId;
       is( $got, $want, "Upload id stored in object" );
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_insertZipUploadRecord();
        };
        {
          like( $@, qr/^_insertZipUploadRecord\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__insertZipUploadRecord_dbh', "Errror tag if no dbh param");
        }
    }

    # Bad param: $newUploadStatus
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_insertZipUploadRecord( $MOCK_DBH );
        };
        {
          like( $@, qr/^_insertZipUploadRecord\(\) missing \$newUploadStatus parameter\./, "Error if no newUploadStatus param");
          is( $obj->{'error'}, 'param__insertZipUploadRecord_newUploadStatus', "Errror tag if no newUploadStatus param");
        }
    }

}

sub test__fastqFilesSqlSubSelect {
    plan( tests => 7 );

    {
        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_workflowAccession'} = 613863;

        {
            my $got = $objToModify->_fastqFilesSqlSubSelect();
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 613863 AND vw_files.algorithm = 'FinalizeCasava'";
            is ($got, $want, "Got expected sub select string from internal.");
        }{
            my $got = $objToModify->_fastqFilesSqlSubSelect($objToModify->{'_workflowAccession'});
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 613863 AND vw_files.algorithm = 'FinalizeCasava'";
            is ($got, $want, "Got expected sub select string from explict.");
        }
    }{
        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_workflowAccession'} = 851553;

        {
            my $got = $objToModify->_fastqFilesSqlSubSelect();
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 851553 AND vw_files.algorithm = 'srf2fastq'";
            is ($got, $want, "Got alternate expected sub select string from internal.");
        }{
            my $got = $objToModify->_fastqFilesSqlSubSelect($objToModify->{'_workflowAccession'});
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 851553 AND vw_files.algorithm = 'srf2fastq'";
            is ($got, $want, "Got alternate expected sub select string from explict.");
        }
    }{
        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_workflowAccession'} = -1;

        {
            my $got = $objToModify->_fastqFilesSqlSubSelect();
            my $want = undef;
            is ($got, $want, "Got undefined from unknown internal" );
        }{
            my $got = $objToModify->_fastqFilesSqlSubSelect($objToModify->{'_workflowAccession'});
            my $want = undef;
            is ( $got, $want, "Got undefined from unknown expicit" );
        }
    }{
        my $objToModify = $CLASS->new( $OPT_HR );
        {
            eval {
                 $objToModify->_fastqFilesSqlSubSelect();
            };
            my $got = $@;
            my $want = qr/^Fastq workflow accession not specified and not set internally\./;
            like( $got, $want, "Fatal if don't have a workflow_accession" );
        }
    }
}

sub test__tagLaneToUpload {
    plan( tests => 6 );

    # Most of this was tested with the individual methods. This checks the
    # combination works.

    my $sampleId = -19;
    my $laneId   = -12;
    my $uploadId = -21;
    my $status   = 'zip_running';
    my $bamMetaDataDir = 't';
    my $bamUuidDir = 'Data';
    my $fastqMetaDataDir = $TEMP_DIR;
    my $fastqUuid = 'someRandomUuid';


    my $obj = $CLASS->new( $OPT_HR );
    $obj->{'_laneId'}   = $laneId;
    $obj->{'_sampleId'} = $sampleId;
    $obj->{'uploadFastqBaseDir'} = $fastqMetaDataDir;
    $obj->{'_fastqUploadUuid'} = $fastqUuid;

    my @dbEventsOk = ({
         'statement' => 'BEGIN WORK',
         'results'  => [[]],
    }, {
         'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
         'results'  => [[]],
    }, {
        'statement'   => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
        'bound_params' => [],
        'results'     => [
            [ 'lane_id', 'sample_id', 'upload_id', 'metadata_dir',  'cghub_analysis_id' ],
            [ $laneId,    $sampleId,   $uploadId,   $bamMetaDataDir, $bamUuidDir        ],
        ],
    }, {
        'statement'   => qr/INSERT INTO upload.*/msi,
        'bound_params' => [ $sampleId, 'CGHUB_FASTQ', $status, $fastqMetaDataDir, $fastqUuid],
        'results'  => [[ 'upload_id' ], [ $uploadId ]],
    }, {
        'statement' => 'COMMIT',
        'results'  => [[]],
    });

    $MOCK_DBH->{'mock_session'} =
        DBD::Mock::Session->new( 'newUploadRecord', @dbEventsOk );

    {
        my $got = $obj->_tagLaneToUpload( $MOCK_DBH, $status );
        my $want = 1;
        is( $got, $want, "Return 1 if found and inserted record to zip" );
    }
    {
       my $got = $obj->{'_fastqUploadId'};
       my $want = $uploadId;
       is( $got, $want, "Upload id stored in object" );
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_tagLaneToUpload();
        };
        {
          like( $@, qr/^_tagLaneToUpload\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__tagLaneToUpload_dbh', "Errror tag if no dbh param");
        }
    }

    # Bad param: $newUploadStatus
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_tagLaneToUpload( $MOCK_DBH );
        };
        {
          like( $@, qr/^_tagLaneToUpload\(\) missing \$newUploadStatus parameter\./, "Error if no newUploadStatus param");
          is( $obj->{'error'}, 'param__tagLaneToUpload_newUploadStatus', "Errror tag if no newUploadStatus param");
        }
    }

}

sub test__getFilesToZip {
    plan( tests => 78 );
    my $sampleId = -19;
    my $laneId = -12;
    my $uploadId = -21;
    my $fastqWorkflowRunId = -2315;
    my $processingId1 = -20;
    my $processingId2 = -2020;
    my $fileId1 = -6;
    my $fileId2 = -66;
    my $wf1 = 613863;
    my $wf2 = 851553;

    my @dbEventsOneCasavaFile = ({
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [
            [ 'file_path', 'md5sum', 'workflow_run_id',
              'flowcell',  'lane_index', 'barcode', 'processing_id' ],
            [ $FASTQ1, $FASTQ1_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId1 ],
        ]
    });

    my @dbEventsTwoCasavaFiles = ({
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [
            [ 'file_path', 'md5sum', 'workflow_run_id',
              'flowcell',  'lane_index', 'barcode', 'processing_id' ],
            [ $FASTQ1, $FASTQ1_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId1 ],
            [ $FASTQ2, $FASTQ2_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId2 ],
        ]
    });

    my @dbEventsOneSrfFile = ({
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [[]],
    },
    {
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'srf2fastq\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [
            [ 'file_path', 'md5sum', 'workflow_run_id',
              'flowcell',  'lane_index', 'barcode', 'processing_id' ],
            [ $FASTQ1, $FASTQ1_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId1 ],
        ]
    });

    my @dbEventsTwoSrfFiles = ({
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [[]],
    },
    {
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'srf2fastq\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [
            [ 'file_path', 'md5sum', 'workflow_run_id',
              'flowcell',  'lane_index', 'barcode', 'processing_id' ],
            [ $FASTQ1, $FASTQ1_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId1 ],
            [ $FASTQ2, $FASTQ2_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId2 ],
        ]
    });

    my @dbEventsNoFiles = (({
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [[]],
    },
    {
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'srf2fastq\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [[]],
    }) );

    {

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'one613863', @dbEventsOneCasavaFile );

        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_laneId'} = $laneId;
        $obj->{'_sampleId'} = $sampleId;
        {
            is( $obj->_getFilesToZip( $MOCK_DBH ), 1, "File one613863 retrieval ok." );
            is( $obj->{'_workflowAccession'}, $wf1, "ok one613863 _workflowAccession" );
            is( $obj->{'_workflowRunId'}, $fastqWorkflowRunId, "ok one613863 _flowcell" );
            is( $obj->{'_flowcell'}, $FLOWCELL, "ok one613863 _flowcell" );
            is( $obj->{'_laneIndex'}, $LANE_INDEX, "ok one613863 _laneIndex" );
            is( $obj->{'_barcode'}, $BARCODE, "ok one613863 _barcode" );

            is( $obj->{'_fastqs'}->[0]->{'filePath'}, $FASTQ1, "ok one613863 filePath0" );
            is( $obj->{'_fastqs'}->[0]->{'md5sum'}, $FASTQ1_MD5, "ok one613863 md5sum0" );
            is( $obj->{'_fastqs'}->[0]->{'processingId'}, $processingId1, "ok one613863 processingId0" );
            is( $obj->{'_fastqs'}->[1], undef );
        }
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'two613863', @dbEventsTwoCasavaFiles );

        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_laneId'} = $laneId;
        $obj->{'_sampleId'} = $sampleId;
        {
            is( $obj->_getFilesToZip( $MOCK_DBH ), 1, "File two613863 retrieval ok." );
            is( $obj->{'_workflowAccession'}, $wf1, "ok two613863 _workflowAccession" );
            is( $obj->{'_workflowRunId'}, $fastqWorkflowRunId, "ok two613863 _flowcell" );
            is( $obj->{'_flowcell'}, $FLOWCELL, "ok two613863 _flowcell" );
            is( $obj->{'_laneIndex'}, $LANE_INDEX, "ok two613863 _laneIndex" );
            is( $obj->{'_barcode'}, $BARCODE, "ok two613863 _barcode" );

            is( $obj->{'_fastqs'}->[0]->{'filePath'}, $FASTQ1, "ok two613863 filePath0" );
            is( $obj->{'_fastqs'}->[0]->{'md5sum'}, $FASTQ1_MD5, "ok two613863 md5sum0" );
            is( $obj->{'_fastqs'}->[0]->{'processingId'}, $processingId1, "ok two613863 processingId0" );
            is( $obj->{'_fastqs'}->[1]->{'filePath'}, $FASTQ2, "ok two613863 filePath1" );
            is( $obj->{'_fastqs'}->[1]->{'md5sum'}, $FASTQ2_MD5, "ok two613863 md5sum2" );
            is( $obj->{'_fastqs'}->[1]->{'processingId'}, $processingId2, "ok two613863 processingId0" );
        }
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'one851553', @dbEventsOneSrfFile );

        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_laneId'} = $laneId;
        $obj->{'_sampleId'} = $sampleId;
        {
            is( $obj->_getFilesToZip( $MOCK_DBH ), 1, "File one851553 retrieval ok." );
            is( $obj->{'_workflowAccession'}, $wf2, "ok one851553 _workflowAccession" );
            is( $obj->{'_workflowRunId'}, $fastqWorkflowRunId, "ok one851553 _flowcell" );
            is( $obj->{'_flowcell'}, $FLOWCELL, "ok one851553 _flowcell" );
            is( $obj->{'_laneIndex'}, $LANE_INDEX, "ok one851553 _laneIndex" );
            is( $obj->{'_barcode'}, $BARCODE, "ok one851553 _barcode" );

            is( $obj->{'_fastqs'}->[0]->{'filePath'}, $FASTQ1, "ok one851553 filePath0" );
            is( $obj->{'_fastqs'}->[0]->{'md5sum'}, $FASTQ1_MD5, "ok one851553 md5sum0" );
            is( $obj->{'_fastqs'}->[0]->{'processingId'}, $processingId1, "ok one851553 processingId0" );
            is( $obj->{'_fastqs'}->[1], undef );
        }
    }
    {

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'two851553', @dbEventsTwoSrfFiles );

        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_laneId'} = $laneId;
        $obj->{'_sampleId'} = $sampleId;
        {
            is( $obj->_getFilesToZip( $MOCK_DBH ), 1, "File two851553 retrieval ok." );
            is( $obj->{'_workflowAccession'}, $wf2, "ok two851553 _workflowAccession" );
            is( $obj->{'_workflowRunId'}, $fastqWorkflowRunId, "ok two851553 _flowcell" );
            is( $obj->{'_flowcell'}, $FLOWCELL, "ok two851553 _flowcell" );
            is( $obj->{'_laneIndex'}, $LANE_INDEX, "ok two851553 _laneIndex" );
            is( $obj->{'_barcode'}, $BARCODE, "ok two851553 _barcode" );

            is( $obj->{'_fastqs'}->[0]->{'filePath'}, $FASTQ1, "ok two851553 filePath0" );
            is( $obj->{'_fastqs'}->[0]->{'md5sum'}, $FASTQ1_MD5, "ok two851553 md5sum0" );
            is( $obj->{'_fastqs'}->[0]->{'processingId'}, $processingId1, "ok two851553 processingId0" );
            is( $obj->{'_fastqs'}->[1]->{'filePath'}, $FASTQ2, "ok two851553 filePath0" );
            is( $obj->{'_fastqs'}->[1]->{'md5sum'}, $FASTQ2_MD5, "ok two851553 md5sum0" );
            is( $obj->{'_fastqs'}->[1]->{'processingId'}, $processingId2, "ok two851553 processingId1" );
        }
    }
    {

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'noFiles', @dbEventsNoFiles );

        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_laneId'} = $laneId;
        $obj->{'_sampleId'} = $sampleId;
        $obj->{'_uploadId'} = $uploadId;
        eval {
            $obj->_getFilesToZip( $MOCK_DBH );
        };
        {
            like( $@, qr/Error looking up fastq files\: Can't find any fastq files/, "No file is error.");
            is( $obj->{'error'}, "no_fastq_files", "No file error tag");
            is( $obj->{'_workflowAccession'}, undef, "ok noFiles _workflowAccession" );
            is( $obj->{'_workflowRunId'}, undef, "ok noFiles _flowcell" );
            is( $obj->{'_flowcell'}, undef, "ok noFiles _flowcell" );
            is( $obj->{'_laneIndex'}, undef, "ok noFiles _laneIndex" );
            is( $obj->{'_barcode'}, undef, "ok noFiles _barcode" );
            is( $obj->{'_fastqs'}, undef, "ok noFiles _fastqs" );
        }
    }
    {
        my @dbEventsTwoFilesMissingData = ({
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [
                [ 'file_path', 'md5sum', 'workflow_run_id',
                  'flowcell',  'lane_index', 'barcode', 'processing_id' ],
                [ $FASTQ1, $FASTQ1_MD5, $fastqWorkflowRunId,
                  $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId1 ],
                [ $FASTQ2, $FASTQ2_MD5, $fastqWorkflowRunId,
                  $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId2 ],
            ]
        });

        my $loaclOpts = { %$OPT_HR };
        $loaclOpts->{'_laneId'} = $laneId;
        $loaclOpts->{'_sampleId'} = $sampleId;
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[2] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 2\./, "error if no (file2) _fastqWorkflowRunId val" );
                is( $obj->{'error'}, 'fastq_file_2_data', "error message name if no (file2) _fastqWorkflowRunId val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[2] = $fastqWorkflowRunId;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[3] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 2\./, "error if no (file2) _flowcell val" );
                is( $obj->{'error'}, 'fastq_file_2_data', "error message name if no (file2) _flowcell val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[3] = $FLOWCELL;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[4] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 2\./, "error if no (file2) _laneIndex val" );
                is( $obj->{'error'}, 'fastq_file_2_data', "error message name if no (file2) _laneIndex val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[4] = $LANE_INDEX;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[2] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 1\./, "error if no (file1) _fastqWorkflowRunId val" );
                is( $obj->{'error'}, 'fastq_file_1_data', "error message name if no (file1) _fastqWorkflowRunId val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[2] = $fastqWorkflowRunId;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[3] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 1\./, "error if no (file1) _flowcell val" );
                is( $obj->{'error'}, 'fastq_file_1_data', "error message name if no  (file1) _flowcell val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[3] = $FLOWCELL;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[4] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 1\./, "error if no (file1) _laneIndex val" );
                is( $obj->{'error'}, 'fastq_file_1_data', "error message name if no (file1) _laneIndex val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[4] = $LANE_INDEX;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[0] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 2\./, "error if no _fastqs->1->filePath val" );
                is( $obj->{'error'}, 'fastq_file_2_data', "error message name if no _fastqs->1->filePath val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[0] = $FASTQ2;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[1] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 2\./, "error if no _fastqs->1>md5sum val" );
                is( $obj->{'error'}, 'fastq_file_2_data', "error message name if no _fastqs->1->md5sum val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[1] = $FASTQ2_MD5;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[6] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 2\./, "error if no _fastqs->1->processingId val" );
                is( $obj->{'error'}, 'fastq_file_2_data', "error message name if no _fastqs->1->processingId val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[2]->[6] = $processingId2;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[0] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                  local $SIG{__WARN__} = sub { };
                  $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 1./, "error if no _fastqs->0->filePath val" );
                is( $obj->{'error'}, 'fastq_file_1_data', "error message name if no _fastqs->0->filePath val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[0] = $FASTQ1;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[1] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 1\./, "error if no _fastqs->0>md5sum val" );
                is( $obj->{'error'}, 'fastq_file_1_data', "error message name if no _fastqs->0->md5sum val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[1] = $FASTQ1_MD5;
        }
        {
            my $obj = $CLASS->new( $loaclOpts );
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[6] = undef;
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'two613863', @dbEventsTwoFilesMissingData );
            eval {
                local $SIG{__WARN__} = sub { };
                $obj->_getFilesToZip( $MOCK_DBH );
            };
            {
                like( $@, qr/Missing data for fastq file 1\./, "error if no _fastqs->0->processingId val" );
                is( $obj->{'error'}, 'fastq_file_1_data', "error message name if no _fastqs->0->processingId val" );
            }
            $dbEventsTwoFilesMissingData[0]->{'results'}->[1]->[6] = $processingId1;
        }
    }
        # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getFilesToZip();
        };
        {
          like( $@, qr/^_getFilesToZip\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__getFilesToZip_dbh', "Errror tag if no dbh param");
        }
    }

}

sub test__zip {
    plan( tests => 25 );

    my $oneFastq = [{
        'filePath' => $FASTQ1,
        'md5sum'   => $FASTQ1_MD5,
    }];
    my $twoFastq = [{
        'filePath' => $FASTQ1,
        'md5sum'   => $FASTQ1_MD5,
    }, {
        'filePath' => $FASTQ2,
        'md5sum'   => $FASTQ2_MD5,
    }];

    my $obj1File = $CLASS->new( $OPT_HR );
    $obj1File->{'minFastqSize'} = 10;
    $obj1File->{'_fastqs'}      = $oneFastq;
    $obj1File->{'_flowcell'}    = $FLOWCELL;
    $obj1File->{'_laneIndex'}   = $LANE_INDEX;
    $obj1File->{'_barcode'}     = $BARCODE;
    $obj1File->{'_uploadId'}    = -21;
    $obj1File->{'rerun'}        = 1;

    my $obj2File = $CLASS->new( $OPT_HR );
    $obj2File->{'minFastqSize'} = 10;
    $obj2File->{'_fastqs'}      = $twoFastq;
    $obj2File->{'_flowcell'}    = $FLOWCELL;
    $obj2File->{'_laneIndex'}   = $LANE_INDEX;
    $obj2File->{'_barcode'}     = $BARCODE;
    $obj2File->{'_uploadId'}    = -21;
    $obj2File->{'rerun'}        = 1;

    {
         ok( $obj1File->_zip(), "Zip seemed to work for one fastq");
         ok( (-d $EXPECTED_OUT_DIR),  "Output dir created");
         ok( (-f $EXPECTED_OUT_FILE), "Created zip file");
         ok( `tar -xzf $EXPECTED_OUT_FILE -C $TEMP_DIR; ls -al $TEMP_DIR` );
         my $unzippedFile = File::Spec->catfile( $TEMP_DIR , "paired_end_one.fastq" );
         files_eq( $FASTQ1, $unzippedFile, "After zip + unzip, single end fastq 1 is the same" );
    }
    {
         ok( $obj2File->_zip(), "Zip seemed to work for two fastq");
         ok( `tar -xzf $EXPECTED_OUT_FILE -C $TEMP_DIR; ls -al $TEMP_DIR` );
         my $unzippedFile1 = File::Spec->catfile( $TEMP_DIR , "paired_end_one.fastq" );
         my $unzippedFile2 = File::Spec->catfile( $TEMP_DIR , "paired_end_two.fastq" );
         files_eq( $FASTQ1, $unzippedFile1, "After zip + unzip, paired end fastq 1 is the same" );
         files_eq( $FASTQ2, $unzippedFile2, "After zip + unzip, paired end fastq 2 is the same" );
    }
    {
         $obj1File->{'rerun'} = 0;
         eval{
             $obj1File->_zip();
         };
         like( $@, qr/^Error\: not rerunning and have preexisting zip file/, "Dies if zip exists, not rerunning");
         is( $obj1File->{'error'}, "prior_zip_file_exists", "Error message, zip exists, not rerunning");
         $obj1File->{'error'} = undef;
         $obj1File->{'rerun'} = 1;
    }
    {
         my $old = $obj1File->{'_fastqs'}->[0]->{'filePath'};
         $obj1File->{'_fastqs'}->[0]->{'filePath'} = "NOsuchPATHiHOPE";
         eval{
             $obj1File->_zip();
         };
         like( $@, qr/Not on file system\: /, "Dies if no se fastq file");
         is( $obj1File->{'error'}, "fastq_not_found", "Error message, zip1 fail with bad file 0 path");
         $obj1File->{'error'} = undef;
         $obj1File->{'_fastqs'}->[0]->{'filePath'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[0]->{'filePath'};
         $obj2File->{'_fastqs'}->[0]->{'filePath'} = "NOsuchPATHiHOPE";
         eval{
             $obj2File->_zip();
         };
         like( $@, qr/Not on file system\: /, "Dies if no pe 1 fastq file");
         is( $obj2File->{'error'}, "fastq_not_found", "Error message, zip2 fail with bad file 0 path");
         $obj2File->{'error'} = undef;
         $obj2File->{'_fastqs'}->[0]->{'filePath'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[1]->{'filePath'};
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = "NOsuchPATHiHOPE";
         eval{
             $obj2File->_zip();
         };
         like( $@, qr/Not on file system\: /, "Dies if no pe 2 fastq file");
         is( $obj2File->{'error'}, "fastq_not_found", "Error message, zip2 fail with bad file 1 path");
         $obj2File->{'error'} = undef;
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = $old;
    }
    {
         my $old = $obj1File->{'_fastqs'}->[0]->{'md5sum'};
         $obj1File->{'_fastqs'}->[0]->{'md5sum'} = "123";
         eval{
             $obj1File->_zip();
         };
         like( $@, qr/Current md5 of /, "Dies if bad se fastq md5 sum");
         is( $obj1File->{'error'}, "fastq_md5_mismatch", "Error message, zip1 fail with bad file 0 md5sum");
         $obj1File->{'error'} = undef;
         $obj1File->{'_fastqs'}->[0]->{'md5sum'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[0]->{'md5sum'};
         $obj2File->{'_fastqs'}->[0]->{'md5sum'} = "123";
         eval{
             $obj2File->_zip();
         };
         like( $@, qr/Current md5 of /, "Dies if bad pe 1 fastq md5 sum");
         is( $obj2File->{'error'}, "fastq_md5_mismatch", "Error message, zip2 fail with bad file 0 md5sum");
         $obj2File->{'error'} = undef;
         $obj2File->{'_fastqs'}->[0]->{'md5sum'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[1]->{'md5sum'};
         $obj2File->{'_fastqs'}->[1]->{'md5sum'} = "123";
         eval{
             $obj2File->_zip();
         };
         like( $@, qr/Current md5 of /, "Dies if bad pe 2 fastq md5 sum");
         is( $obj2File->{'error'}, "fastq_md5_mismatch", "Error message, zip2 fail with bad file 1 md5sum");
         $obj2File->{'error'} = undef;
         $obj2File->{'_fastqs'}->[1]->{'md5sum'} = $old;
    }
    {
         my $old = $obj2File->{'minFastqSize'};
         $obj2File->{'minFastqSize'} = 1000000;
         eval{
             $obj2File->_zip();
         };
         like( $@, qr/File size of /, "Dies if bad pe 1 fastq size too small");
         is( $obj2File->{'error'}, "fastq_too_small", "Error message, Zip fail with to small fastq file");
         $obj2File->{'error'} = undef;
         $obj2File->{'minFastqSize'} = $old;
    }
}

sub test__insertFileRecord {

    plan ( tests => 6 );

    my $fileId = -6;

    $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'insertFileRec', ({
        'statement'    => qr/INSERT INTO file.*/msi,
        'bound_params' => [ $EXPECTED_OUT_FILE, $ZIP_FILE_META_TYPE, $ZIP_FILE_TYPE, $ZIP_FILE_DESCRIPTION, $ZIP_FILE_FAKE_MD5 ],
        'results'  => [[ 'file_id' ], [ $fileId ]],
    } ));

    my $obj = $CLASS->new( $OPT_HR );
    $obj->{'_zipFileName'}    = $EXPECTED_OUT_FILE;
    $obj->{'_zipFileMd5'} = $ZIP_FILE_FAKE_MD5;

    {
        is( 1, $obj->_insertFileRecord( $MOCK_DBH ), "Insert file record" );
        is( $fileId, $obj->{'_zipFileId'}, "File ID set" );
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_insertFileRecord();
        };
        {
          like( $@, qr/^_insertFileRecord\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__insertFileRecord_dbh', "Errror tag if no dbh param");
        }
    }

    # Db insert failure
    {
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'BadInsertFileRec', ({
            'statement'    => qr/INSERT INTO file.*/msi,
            'bound_params' => [ $EXPECTED_OUT_FILE, $ZIP_FILE_META_TYPE, $ZIP_FILE_TYPE, $ZIP_FILE_DESCRIPTION, $ZIP_FILE_FAKE_MD5 ],
            'results'  => [ [] ],
        } ));
    
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_zipFileName'}    = $EXPECTED_OUT_FILE;
        $obj->{'_zipFileMd5'} = $ZIP_FILE_FAKE_MD5;
        eval {
             $obj->_insertFileRecord( $MOCK_DBH );
        };
        {
          like( $@, qr/^Insert of file record appeared to fail/, "Error if insert fails");
          is( $obj->{'error'}, 'db_insert_file_returning', "Errror tag if insert fails");
        }
    }

}

sub test__insertProcessingFilesRecord {
    plan ( tests => 7 );

    my $fileId = -6;
    my $processingId1 = -20;
    my $processingId2 = -2020;

    $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'insertProcessingFilesRec', ({
        'statement'    => qr/INSERT INTO processing_files.*/msi,
        'bound_params' => [ $processingId1, $fileId ],
        'results'  => [ [ 'rows' ], [] ],
    }, {
        'statement'    => qr/INSERT INTO processing_files.*/msi,
        'bound_params' => [ $processingId2, $fileId ],
        'results'  => [ [ 'rows' ], [] ],
    } ));

    my $obj = $CLASS->new( $OPT_HR );
    $obj->{'_zipFileId'} = "-6";
    $obj->{'_fastqs'}->[0]->{'processingId'} = "-20";
    $obj->{'_fastqs'}->[1]->{'processingId'} = "-2020";

    {
        is( 1, $obj->_insertProcessingFilesRecords( $MOCK_DBH ), "Insert processingfiles records" );
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_insertProcessingFilesRecords();
        };
        {
          like( $@, qr/^_insertProcessingFilesRecords\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__insertProcessingFilesRecords_dbh', "Errror tag if no dbh param");
        }
    }

    # Bad insert 2
    {
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'badSecondInsert', ({
            'statement'    => qr/INSERT INTO processing_files.*/msi,
            'bound_params' => [ $processingId1, $fileId ],
            'results'  => [ [ 'rows' ], [] ],
        }, {
            'statement'    => qr/INSERT INTO processing_files.*/msi,
            'bound_params' => [ $processingId2, $fileId ],
            'results'  => [ [] ],
        } ));
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_zipFileId'} = "-6";
        $obj->{'_fastqs'}->[0]->{'processingId'} = "-20";
        $obj->{'_fastqs'}->[1]->{'processingId'} = "-2020";
        eval {
             $obj->_insertProcessingFilesRecords( $MOCK_DBH );
        };
        {
          like( $@, qr/^Processing files insert failed\: failed to insert processing_files record for fastq 2/, "Error if failed processing file 2 insert");
          is( $obj->{'error'}, 'insert_processsing_files_2', "Errror tag if failed processing file 2 insert.");
        }
    }

    # Bad insert 1
    {
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'badFirstInsert', ({
            'statement'    => qr/INSERT INTO processing_files.*/msi,
            'bound_params' => [ $processingId1, $fileId ],
            'results'  => [ [] ],
        } ));
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_zipFileId'} = "-6";
        $obj->{'_fastqs'}->[0]->{'processingId'} = "-20";
        $obj->{'_fastqs'}->[1]->{'processingId'} = "-2020";
        eval {
             $obj->_insertProcessingFilesRecords( $MOCK_DBH );
        };
        {
          like( $@, qr/^Processing files insert failed\: failed to insert processing_files record for fastq 1/, "Error if failed processing file 1 insert");
          is( $obj->{'error'}, 'insert_processsing_files_1', "Errror tag if failed processing file 1 insert.");
        }
    }

}

sub test__insertFile {
 
    plan ( tests => 5 );

    my $fileId = -6;
    my $processingId1 = -20;
    my $processingId2 = -2020;

    $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'insertFileRec', ({
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/INSERT INTO file.*/msi,
        'bound_params' => [ $EXPECTED_OUT_FILE, $ZIP_FILE_META_TYPE, $ZIP_FILE_TYPE, $ZIP_FILE_DESCRIPTION, $ZIP_FILE_FAKE_MD5 ],
        'results'  => [[ 'file_id' ], [ $fileId ]],
    }, {
        'statement'    => qr/INSERT INTO processing_file.*/msi,
        'bound_params' => [ $processingId1, $fileId ],
        'results'  => [[ 'rows' ], []],
    }, {
        'statement'    => qr/INSERT INTO processing_file.*/msi,
        'bound_params' => [ $processingId2, $fileId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    } ));

    my $obj = $CLASS->new( $OPT_HR );
    $obj->{'_zipFileName'}    = $EXPECTED_OUT_FILE;
    $obj->{'_zipFileMd5'} = $ZIP_FILE_FAKE_MD5;
    $obj->{'_fastqs'}->[0]->{'processingId'} = $processingId1;
    $obj->{'_fastqs'}->[1]->{'processingId'} = $processingId2;

    {
        is( 1, $obj->_insertFile( $MOCK_DBH ), "Insert file record transaction" );
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_insertFile();
        };
        {
          like( $@, qr/^_insertFile\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__insertFile_dbh', "Errror tag if no dbh param");
        }
    }

    # Error, (triggered by underlying bad processing result 2)
    {
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'BadinsertFileRec', ({
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
            'statement'    => qr/INSERT INTO file.*/msi,
            'bound_params' => [ $EXPECTED_OUT_FILE, $ZIP_FILE_META_TYPE, $ZIP_FILE_TYPE, $ZIP_FILE_DESCRIPTION, $ZIP_FILE_FAKE_MD5 ],
            'results'  => [[ 'file_id' ], [ $fileId ]],
        }, {
            'statement'    => qr/INSERT INTO processing_file.*/msi,
            'bound_params' => [ $processingId1, $fileId ],
            'results'  => [[ 'rows' ], []],
        }, {
            'statement'    => qr/INSERT INTO processing_file.*/msi,
            'bound_params' => [ $processingId2, $fileId ],
            'results'  => [ [] ],
        }, {
           'statement' => 'ROLLBACK',
            'results'  => [[]],
        } ));
    
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_zipFileName'}    = $EXPECTED_OUT_FILE;
        $obj->{'_zipFileMd5'} = $ZIP_FILE_FAKE_MD5;
        $obj->{'_fastqs'}->[0]->{'processingId'} = $processingId1;
        $obj->{'_fastqs'}->[1]->{'processingId'} = $processingId2;
        
        eval {
             $obj->_insertFile( $MOCK_DBH );
        };
        {
          like( $@, qr/^Processing files insert failed\: failed to insert processing_files record for fastq 2/, "Error if failed processing file 2 insert");
          is( $obj->{'error'}, 'insert_processsing_files_2', "Errror tag if failed processing file 2 insert.");
        }
    }

}

sub test__insertUploadFileRecord {
    plan ( tests => 5 );

    my $fileId = -6;
    my $uploadId = -20;

    $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'insertFileRec', ({
         'statement' => 'BEGIN WORK',
         'results'  => [[]],
    }, {
        'statement'    => qr/INSERT INTO upload_file.*/msi,
        'bound_params' => [ $uploadId, $fileId ],
        'results'  => [ [ 'rows' ], [] ],
    }, {
        'statement' => 'COMMIT',
        'results'  => [[]],
    } ));

    my $obj = $CLASS->new( $OPT_HR );
    $obj->{'_zipFileId'} = $fileId;
    $obj->{'_fastqUploadId'} = $uploadId;

    {
        is( 1, $obj->_insertUploadFileRecord( $MOCK_DBH ), "Insert upload_file record" );
    }
 
    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_insertUploadFileRecord();
        };
        {
          like( $@, qr/^_insertUploadFileRecord\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__insertUploadFileRecord_dbh', "Errror tag if no dbh param");
        }
    }

    # Update failed
    {
         $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'badInsertFileRec', ({
         'statement' => 'BEGIN WORK',
         'results'  => [[]],
         }, {
             'statement'    => qr/INSERT INTO upload_file.*/msi,
             'bound_params' => [ $uploadId, $fileId ],
             'results'  => [[]],
         }, {
             'statement' => 'ROLLBACK',
             'results'  => [[]],
         } ));

        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_zipFileId'} = $fileId;
        $obj->{'_fastqUploadId'} = $uploadId;

        eval {
             $obj->_insertUploadFileRecord( $MOCK_DBH );
        };
        {
          like( $@, qr/^Upload_file insert failed\: failed to insert upload_file record/, "Error if upload fails");
          is( $obj->{'error'}, 'insert_upload_file', "Errror tag if insert upload failed");
        }
    }
}

sub test__doZip {
    plan ( tests => 5 );

    # Bam upload record info.
    my $laneId         = 12;
    my $sampleId       = 19;
    my $bamUploadId    = 221;
    my $bamMetaDataDir = 't';      # The parent directory for the fake bam uplaod
    my $bamUuid        = 'Data';   # The actual directory with the fake bam uplaod data files

    # Fastq upload record info;
    my $fastqUploadId    = 521;
    my $fastqMetaDataDir = $TEMP_DIR;  # Parent directory for the fastq upload
    my $fastqUuid        = '00000000-0000-0000-0000-000000000000'; # Actual directory with the fastq upload data files
    my $initialStatus    = 'zip_running';

    # vw_files info for fastq files retrieved to go with the uploaded lane.
    # Assuming the two sample pe fastq paths and their md5 sums were returned.
    my $fastqWorkflowRunId = 613863;   # FinalizeCasava, although makes no difference here
    my $processingId1      = 16;
    my $processingId2      = 1616;

    # zip file info
    # Assuming the flowcell, lane, and barcode example used throughout
    my $zipFileId = 266;

    # final upload value
    my $finalStatus = 'zip_completed';

    my @dbEventsOk = ({
         'statement' => 'BEGIN WORK',
         'results'  => [[]],
    }, {
         'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
         'results'  => [[]],
    }, {
        'statement'   => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
        'bound_params' => [],
        'results'     => [
            [ 'lane_id', 'sample_id', 'upload_id',  'metadata_dir',  'cghub_analysis_id' ],
            [ $laneId,    $sampleId,   $bamUploadId, $bamMetaDataDir, $bamUuid           ],
        ],
    }, {
        'statement'   => qr/INSERT INTO upload.*/msi,
        'bound_params' => [ $sampleId, 'CGHUB_FASTQ', $initialStatus, $fastqMetaDataDir, $fastqUuid ],
        'results'  => [ [ 'upload_id' ], [ $fastqUploadId ] ],
    }, {
        'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [
            [ 'file_path', 'md5sum', 'workflow_run_id',
              'flowcell',  'lane_index', 'barcode', 'processing_id' ],
            [ $FASTQ1, $FASTQ1_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId1 ],
            [ $FASTQ2, $FASTQ2_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId2 ],
        ]
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/INSERT INTO file.*/msi,
        # Skipping bound parameter check as can't get the real md5 sum which is inserted here.
        # 'bound_params' => [ $EXPECTED_OUT_FILE, $ZIP_FILE_META_TYPE, $ZIP_FILE_TYPE, $ZIP_FILE_DESCRIPTION, $obj->{'_zipFileMd5'} ],
        'results'  => [[ 'file_id' ], [ $zipFileId ]],
    }, {
        'statement'    => qr/INSERT INTO processing_file.*/msi,
        'bound_params' => [ $processingId1, $zipFileId ],
        'results'  => [[ 'rows' ], []],
    }, {
        'statement'    => qr/INSERT INTO processing_file.*/msi,
        'bound_params' => [ $processingId2, $zipFileId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/INSERT INTO upload_file.*/msi,
        'bound_params' => [ $fastqUploadId, $zipFileId ],
        'results'  => [ [ 'rows' ], [] ],
    }, {
        'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/UPDATE upload.*/msi,
        'bound_params' => [ $finalStatus, $fastqUploadId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    });

    {
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_fastqUploadUuid'} = '00000000-0000-0000-0000-000000000000';

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'doXip', @dbEventsOk );

        {
            is( 1, $obj->doZip( $MOCK_DBH ), "doZip completes ok" );
        }
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doZip();
        };
        {
          like( $@, qr/^doZip\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'failed_zip_param_doZip_dbh', "Errror tag if no dbh param");
        }
    }


    # Error propagation on error.
    {
        $MOCK_DBH->{mock_can_connect} = 0;
        my $obj = $CLASS->new( $OPT_HR );

        eval {
             $obj->doZip( $MOCK_DBH );
        };
        {
            like( $@, qr/^Error selecting lane to run on: /, "Error propogates out");
            is( $obj->{'error'}, 'failed_zip_tagging_lane', "Errror tag propogates out");
        }
        $MOCK_DBH->{mock_can_connect} = 1;
        $MOCK_DBH->{'AutoCommit'} = 1;
    }

}

sub test_run_zip {

     plan ( tests => 4 );

    # Bam upload record info.
    my $laneId         = 12;
    my $sampleId       = 19;
    my $bamUploadId    = 221;
    my $bamMetaDataDir = 't';      # The parent directory for the fake bam uplaod
    my $bamUuid        = 'Data';   # The actual directory with the fake bam uplaod data files

    # Fastq upload record info;
    my $fastqUploadId    = 521;
    my $fastqMetaDataDir = File::Temp->newdir();                   # Parent directory for the fastq upload
    my $fastqUuid        = '00000000-0000-0000-0000-000000000000'; # Actual directory with the fastq upload data files
    my $initialStatus    = 'zip_running';

    # vw_files info for fastq files retrieved to go with the uploaded lane.
    # Assuming the two sample pe fastq paths and their md5 sums were returned.
    my $fastqWorkflowRunId = 613863;   # FinalizeCasava, although makes no difference here
    my $processingId1      = 16;
    my $processingId2      = 1616;

    # zip file info
    # Assuming the flowcell, lane, and barcode example used throughout
    my $zipFileId = 266;

    # final upload value
    my $finalStatus = 'zip_completed';

    my @dbEventsOk = ({
         'statement' => 'BEGIN WORK',
         'results'  => [[]],
    }, {
         'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
         'results'  => [[]],
    }, {
        'statement'   => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
        'bound_params' => [],
        'results'     => [
            [ 'lane_id', 'sample_id', 'upload_id',  'metadata_dir',  'cghub_analysis_id' ],
            [ $laneId,    $sampleId,   $bamUploadId, $bamMetaDataDir, $bamUuid           ],
        ],
    }, {
        'statement'   => qr/INSERT INTO upload.*/msi,
        'bound_params' => [ $sampleId, 'CGHUB_FASTQ', $initialStatus, $fastqMetaDataDir, $fastqUuid ],
        'results'  => [ [ 'upload_id' ], [ $fastqUploadId ] ],
    }, {
        'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
        'bound_params' => [ $sampleId, $laneId],
        'results'  => [
            [ 'file_path', 'md5sum', 'workflow_run_id',
              'flowcell',  'lane_index', 'barcode', 'processing_id' ],
            [ $FASTQ1, $FASTQ1_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId1 ],
            [ $FASTQ2, $FASTQ2_MD5, $fastqWorkflowRunId,
              $FLOWCELL,  $LANE_INDEX, $BARCODE, $processingId2 ],
        ]
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/INSERT INTO file.*/msi,
        # Skipping bound parameter check as can't get the real md5 sum which is inserted here.
        # 'bound_params' => [ $EXPECTED_OUT_FILE, $ZIP_FILE_META_TYPE, $ZIP_FILE_TYPE, $ZIP_FILE_DESCRIPTION, $obj->{'_zipFileMd5'} ],
        'results'  => [[ 'file_id' ], [ $zipFileId ]],
    }, {
        'statement'    => qr/INSERT INTO processing_file.*/msi,
        'bound_params' => [ $processingId1, $zipFileId ],
        'results'  => [[ 'rows' ], []],
    }, {
        'statement'    => qr/INSERT INTO processing_file.*/msi,
        'bound_params' => [ $processingId2, $zipFileId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/INSERT INTO upload_file.*/msi,
        'bound_params' => [ $fastqUploadId, $zipFileId ],
        'results'  => [ [ 'rows' ], [] ],
    }, {
        'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/UPDATE upload.*/msi,
        'bound_params' => [ $finalStatus, $fastqUploadId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    });

    $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'doZip', @dbEventsOk );
    {
        my $opts = { %$OPT_HR,
            'runMode'            => 'ZIP',
            '_fastqUploadUuid'   => '00000000-0000-0000-0000-000000000000',
            'dataRoot'           => $fastqMetaDataDir,
            'uploadFastqBaseDir' => $fastqMetaDataDir,
        };
        my $obj = $CLASS->new( $opts );
        {
            my $testName = "ZIP run mode smoke test";
            my $got = $obj->run( "ZIP", $MOCK_DBH );
            my $want = 1;
            is( $got, $want, $testName);
        }
    }

    # Error propagation.
    {
        $MOCK_DBH->{mock_can_connect} = 0;
        $MOCK_DBH->{'mock_session'} = undef;  # = DBD::Mock::Session->new( 'doZip', @dbEventsOk );
        my $opts = { %$OPT_HR,
            'runMode'            => 'ZIP',
            '_fastqUploadUuid'   => '00000000-0000-0000-0000-000000000000',
            'dataRoot'           => $fastqMetaDataDir,
            'uploadFastqBaseDir' => $fastqMetaDataDir,
        };
        my $obj = $CLASS->new( $opts );

        eval {
             $obj->run( 'ZIP', $MOCK_DBH );
        };
        {
            like( $@, qr/^Error selecting lane to run on: /, "Error propogates out");
            is( $obj->{'error'}, 'failed_zip_tagging_lane', "Errror tag propogates out");
        }
        $MOCK_DBH->{mock_can_connect} = 1;
        $MOCK_DBH->{'AutoCommit'} = 1;
    }

    # Error propagation.
    {
        $MOCK_DBH->{mock_can_connect} = 0;
        $MOCK_DBH->{'mock_session'} = undef;  # = DBD::Mock::Session->new( 'doZip', @dbEventsOk );
        my $opts = { %$OPT_HR,
            'runMode'            => 'ZIP',
            '_fastqUploadUuid'   => '00000000-0000-0000-0000-000000000000',
            'dataRoot'           => $fastqMetaDataDir,
            'uploadFastqBaseDir' => $fastqMetaDataDir,
        };
        my $obj = $CLASS->new( $opts );
        $obj->{'_fastqUploadId'} = -42;

        eval {
            $obj->run( 'ZIP', $MOCK_DBH );
        };
        {
            like( $@, qr/ALSO: Did not update UPLOAD: $obj->{'_fastqUploadId'}/, "Error if can't update upload.");
        }
        $MOCK_DBH->{mock_can_connect} = 1;
        $MOCK_DBH->{'AutoCommit'} = 1;
    }
}