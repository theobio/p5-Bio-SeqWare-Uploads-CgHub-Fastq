use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Spec;           # Normal path handling
use File::Temp;           # Simple files for testing

use Bio::SeqWare::Config; # Read the seqware config file
use DBD::Mock;
use Test::More 'tests' => 5;    # Run this many Test::More compliant subtests

use Bio::SeqWare::Uploads::CgHub::Fastq;

my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $TEMP_DIR = File::Temp->newdir();  # Auto-delete self and contents when out of scope
my $filename = File::Temp->new(
    'TEMPLATE' => "dummyZipFile_XXXX", 'SUFFIX' => ".tar.gz"
)->filename;
my $TEMP_FILE = File::Spec->catfile( $TEMP_DIR, $filename);
`touch $TEMP_FILE`;

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $CONFIG = Bio::SeqWare::Config->new();
my $OPT = $CONFIG->getKnown();

my $OPT_HR = { %$OPT,
    'runMode'      => 'rerun',
    'rerunWait'    => 1,
};


# Fake database handle, to use in place of $dbh parameters (as long as have
# specified an attached session).
my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1 },
);

# Base upload record for use, without 'tstmp' field
my %MOCK_UPLOAD_REC = (
    'upload_id'         => -21,
    'sample_id'         => -19,
    'target'            => 'CGHUB_FASTQ',
    'status'            => 'zip_failed_dummy_error',
    'external_status'   => undef,
    'metadata_dir'      => $TEMP_DIR,
    'cghub_analysis_id' => '00000000-0000-0000-0000-000000000000',
);

# Fake tested method returs
sub mock__getAssociatedFileId_undef { return undef; }
sub mock__getAssociatedFileId { return -5; }
sub mock__getFilePath_undef { return undef; }
sub mock__getFilePath { return $TEMP_FILE; }
sub mock__getAssociatedProcessingFileIds_undef { return (undef, undef); }
sub mock__getAssociatedProcessingFileIds_1of2 { return (-201, undef); }
sub mock__getAssociatedProcessingFileIds_2of2 { return (-201, -202); }

#
# TESTS
#

subtest( '_changeUploadRerunStage()' => \&test__changeUploadRerunStage );
subtest( '_getAssociatedFileId()'    => \&test__getAssociatedFileId );
subtest( '_getFilePath()'            => \&test__getFilePath );
subtest( '_getRerunData()'           => \&test__getRerunData );
subtest( '_getAssociatedProcessingFileIds()' => \&test__getAssociatedProcessingFileIds );

#
# SUBTESTS
#

sub test__getRerunData {
    plan( tests => 9 );

    # No upload_file record available for retrieval
    {
        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => undef,
            'file_path' => undef,
            'processing_file_id_1' => undef,
            'processing_file_id_2' => undef,
        };
        my $obj = $CLASS->new( $OPT_HR );
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId_undef;
        {
            my $testThatThisSub = "Retrieves expeted data record if no upload_file record.";
            my $got = $obj->_getRerunData( $MOCK_DBH, \%MOCK_UPLOAD_REC );
            my $want = $rerunDataHR;
            is_deeply( $got, $want, $testThatThisSub);
        }
    }

    # upload_file but no file record available for retrieval
    {
        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => undef,
            'processing_file_id_1' => undef,
            'processing_file_id_2' => undef,
        };
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getFilePath = \&mock__getFilePath_undef;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Retrieves expeted data record if no file record.";
            my $got = $obj->_getRerunData( $MOCK_DBH, \%MOCK_UPLOAD_REC );
            my $want = $rerunDataHR;
            is_deeply( $got, $want, $testThatThisSub);
        }
    }

    # upload_file and file by no processing_file1
    {
        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_file_id_1' => undef,
            'processing_file_id_2' => undef,
        };
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getFilePath = \&mock__getFilePath;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedProcessingFileIds = \&mock__getAssociatedProcessingFileIds_undef;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Retrieves expeted data record if no processing_file1 record.";
            my $got = $obj->_getRerunData( $MOCK_DBH, \%MOCK_UPLOAD_REC );
            my $want = $rerunDataHR;
            is_deeply( $got, $want, $testThatThisSub);
        }
    }

    # upload_file, file processing_file1 but no processing_file2
    {
        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_file_id_1' => -201,
            'processing_file_id_2' => undef,
        };
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getFilePath = \&mock__getFilePath;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedProcessingFileIds = \&mock__getAssociatedProcessingFileIds_1of2;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Retrieves expeted data record if no processing_file2 record.";
            my $got = $obj->_getRerunData( $MOCK_DBH, \%MOCK_UPLOAD_REC );
            my $want = $rerunDataHR;
            is_deeply( $got, $want, $testThatThisSub);
        }
    }

    # Smoke test, with everything
    {
        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_file_id_1' => -201,
            'processing_file_id_2' => -202,
        };
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getFilePath = \&mock__getFilePath;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedProcessingFileIds = \&mock__getAssociatedProcessingFileIds_2of2;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Retrieves expeted data record if have all info.";
            my $got = $obj->_getRerunData( $MOCK_DBH, \%MOCK_UPLOAD_REC );
            my $want = $rerunDataHR;
            is_deeply( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getRerunData();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^_getRerunData\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getRerunData_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $uploadHR
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getRerunData( $MOCK_DBH );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no uploadHR parameter passed.";
            my $got = $@;
            my $want = qr/^_getRerunData\(\) missing \$uploadHR parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no uploadHR parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getRerunData_uploadHR';
            is( $got, $want, $testThatThisSub);
        }
    }
}

sub test__getAssociatedFileId {
    plan( tests => 8 );

    # Good run returning file_id
    {
        my $uploadId = -21;
        my $fileId   = -5;
        my @dbSession = (
            {
                'statement'    => qr/SELECT file_id/msi,
                'bound_params' => [ $uploadId ],
                'results'  => [[ 'file_id' ], [ $fileId ]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct file record returned (given fixed DBIO output)";
            my $got = $obj->_getAssociatedFileId( $MOCK_DBH, $uploadId );
            my $want = $fileId;
            is( $got, $want, $testThatThisSub);
        }
    }

    # Good run returning undef
    {
        my $uploadId = -21;
        my @dbSession = (
            {
                'statement'    => qr/SELECT file_id/msi,
                'bound_params' => [ $uploadId ],
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "No file record returned if not found (given fixed DBIO output)";
            my $got = $obj->_getAssociatedFileId( $MOCK_DBH, $uploadId );
            my $want = undef;
            is( $got, $want, $testThatThisSub);
        }
    }

    # Error if returns more than one file_id
    {
        my $uploadId = -21;
        my @dbSession = (
            {
                'statement'    => qr/SELECT file_id/msi,
                'bound_params' => [ $uploadId ],
                'results'  => [['file_id'], [-51], [-52] ],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_getAssociatedFileId( $MOCK_DBH, $uploadId );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if two or more file link records found";
            my $got = $error;
            my $want = qr/Found more than one file linked to upload $uploadId\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if two or more file link records found";
            my $got = $obj->{'error'};
            my $want = 'multiple_linked_files';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getAssociatedFileId();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^_getAssociatedFileId\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getAssociatedFileId_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $uploadId
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getAssociatedFileId( $MOCK_DBH );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no uploadId parameter passed.";
            my $got = $@;
            my $want = qr/^_getAssociatedFileId\(\) missing \$uploadId parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no uploadId parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getAssociatedFileId_uploadId';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__getFilePath {
    plan( tests => 6 );

    # Good run returning file_path
    {
        my $fileId   = -5;
        my $filePath = "/path/to/nowhere/file";
        my @dbSession = (
            {
                'statement'    => qr/SELECT file_path/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[ 'file_path' ], [ $filePath ]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct file path returned (given fixed DBIO output)";
            my $got = $obj->_getFilePath( $MOCK_DBH, $fileId );
            my $want = $filePath;
            is( $got, $want, $testThatThisSub);
        }
    }

    # Good run returning undef
    {
        my $fileId   = -5;
        my @dbSession = (
            {
                'statement'    => qr/SELECT file_path/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "No file path returned if not found (given fixed DBIO output)";
            my $got = $obj->_getFilePath( $MOCK_DBH, $fileId );
            my $want = undef;
            is( $got, $want, $testThatThisSub);
        }
    }


    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getFilePath();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^_getFilePath\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getFilePath_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $fileId
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getFilePath( $MOCK_DBH );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no fileId parameter passed.";
            my $got = $@;
            my $want = qr/^_getFilePath\(\) missing \$fileId parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no fileId parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getFilePath_fileId';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__getAssociatedProcessingFileIds {
    plan( tests => 9 );

    # Good run returning pair of processing_file ids
    {
        my $fileId   = -5;
        my $processingFileId1 = -201;
        my $processingFileId2 = -202;
        my @dbSession = (
            {
                'statement'    => qr/SELECT processing_files_id/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[ 'processing_files_id' ], [ $processingFileId1 ], [$processingFileId2]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct pair of processing_file records returned (given fixed DBIO output)";
            my @got = $obj->_getAssociatedProcessingFileIds( $MOCK_DBH, $fileId );
            my @want = ($processingFileId1, $processingFileId2);
            is( @got, @want, $testThatThisSub);
        }
    }

    # Good run returning one processing_file id
    {
        my $fileId   = -5;
        my $processingFileId1 = -201;
        my $processingFileId2;
        my @dbSession = (
            {
                'statement'    => qr/SELECT processing_files_id/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[ 'processing_files_id' ], [ $processingFileId1 ]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct single processing_file records returned (given fixed DBIO output)";
            my @got = $obj->_getAssociatedProcessingFileIds( $MOCK_DBH, $fileId );
            my @want = ($processingFileId1, $processingFileId2);
            is( @got, @want, $testThatThisSub);
        }
    }

    # Good run returning no processing_file ids
    {
        my $fileId   = -5;
        my $processingFileId1;
        my $processingFileId2;
        my @dbSession = (
            {
                'statement'    => qr/SELECT processing_files_id/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct no processing_file records returned (given fixed DBIO output)";
            my @got = $obj->_getAssociatedProcessingFileIds( $MOCK_DBH, $fileId );
            my @want = ($processingFileId1, $processingFileId2);
            is( @got, @want, $testThatThisSub);
        }
    }

    # Error if returns more than two processing_file_ids
    {
        my $fileId   = -5;
        my @dbSession = (
            {
                'statement'    => qr/SELECT processing_files_id/msi,
                'bound_params' => [ $fileId ],
                'results'  => [['file_id'], [-1], [-2], [-3] ],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_getAssociatedProcessingFileIds( $MOCK_DBH, $fileId );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if more than two processing_file link records found";
            my $got = $error;
            my $want = qr/Found more than two processing records linked to fileId $fileId\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if more than two processing_file link records found";
            my $got = $obj->{'error'};
            my $want = 'too_many_processing_links';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getAssociatedProcessingFileIds();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^_getAssociatedProcessingFileIds\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getAssociatedProcessingFileIds_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $fileId
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getAssociatedProcessingFileIds( $MOCK_DBH );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no fileId parameter passed.";
            my $got = $@;
            my $want = qr/^_getAssociatedProcessingFileIds\(\) missing \$fileId parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no fileId parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getAssociatedProcessingFileIds_fileId';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__changeUploadRerunStage {
    plan( tests => 12 );

    # Good run (no sample selection parameters)
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT \*/msi,
                'results'  => [
                    [ 'upload_id',    'sample_id',
                      'status',       'external_status',
                      'metadata_dir', 'cghub_analysis_id',
                      'target'  ],
                    [ $MOCK_UPLOAD_REC{'upload_id'},    $MOCK_UPLOAD_REC{'sample_id'},
                      $MOCK_UPLOAD_REC{'status'},       $MOCK_UPLOAD_REC{'external_status'},
                      $MOCK_UPLOAD_REC{'metadata_dir'}, $MOCK_UPLOAD_REC{'cghub_analysis_id'},
                      $MOCK_UPLOAD_REC{'target'},
                    ],
                ]
            }, {
                'statement'    => qr/UPDATE upload/msi,
                'bound_params' => [ 'rerun_running', $MOCK_UPLOAD_REC{'upload_id'} ],
                'results'  => [[ 'rows' ], []]
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct upload record (given fixed DBIO output)";
            my $got = $obj->_changeUploadRerunStage( $MOCK_DBH );
            my $want = { %MOCK_UPLOAD_REC, 'status' => 'rerun_running' };
            is_deeply( $got, $want, $testThatThisSub);
        }
    }

    # Good run, nothing to do
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
             'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
             'results'  => [[]],
            }, {
                'statement'    => qr/SELECT \*/msi,
                'results'  => [[]]
            }, {
               'statement' => 'COMMIT',
               'results'   => [[]],
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct upload record (given no matching record found)";
            my $got = $obj->_changeUploadRerunStage( $MOCK_DBH );
            my $want = undef;
            is( $got, $want, $testThatThisSub);
        }

    }

    # Good run (all sample selection parameters)
    {
        my $local_opt_HR = {
            %$OPT_HR,
            'sampleId' => -19,
            'sampleAccession' => 999999,
            'sampleAlias' => "PIPE_0000",
            'sampleType' => 'BRCA',
            'sampleTitle' => 'TCGA-CS-6188-01A-11R-1896-07',
            'sampleUuid' => '00000000-0000-0000-0000-000000000000',
        };
        my $obj = $CLASS->new( $local_opt_HR );

        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT \*/msi,
                'bound_params' => [
                    $local_opt_HR->{'sampleId'},    $local_opt_HR->{'sampleAccession'},
                    $local_opt_HR->{'sampleAlias'}, $local_opt_HR->{'sampleUuid'},
                    $local_opt_HR->{'sampleTitle'}, $local_opt_HR->{'sampleType'}
                ],
                'results'  => [
                    [ 'upload_id',    'sample_id',
                      'status',       'external_status',
                      'metadata_dir', 'cghub_analysis_id',
                      'target'  ],
                    [ $MOCK_UPLOAD_REC{'upload_id'},    $MOCK_UPLOAD_REC{'sample_id'},
                      $MOCK_UPLOAD_REC{'status'},       $MOCK_UPLOAD_REC{'external_status'},
                      $MOCK_UPLOAD_REC{'metadata_dir'}, $MOCK_UPLOAD_REC{'cghub_analysis_id'},
                      $MOCK_UPLOAD_REC{'target'},
                    ],
                ]
            }, {
                'statement'    => qr/UPDATE upload/msi,
                'bound_params' => [ 'rerun_running', $MOCK_UPLOAD_REC{'upload_id'} ],
                'results'  => [[ 'rows' ], []]
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        {
            my $testThatThisSub = "Correct upload record (given fixed DBIO output)";
            my $got = $obj->_changeUploadRerunStage( $MOCK_DBH );
            my $want = { %MOCK_UPLOAD_REC, 'status' => 'rerun_running' };
            is_deeply( $got, $want, $testThatThisSub);
        }
    }
    
    # Bad run, triggers error exit due to dbh error (returned two record on update).
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT \*/msi,
                'results'  => [
                    [ 'upload_id',    'sample_id',
                      'status',       'external_status',
                      'metadata_dir', 'cghub_analysis_id',
                      'target'  ],
                    [ $MOCK_UPLOAD_REC{'upload_id'},    $MOCK_UPLOAD_REC{'sample_id'},
                      $MOCK_UPLOAD_REC{'status'},       $MOCK_UPLOAD_REC{'external_status'},
                      $MOCK_UPLOAD_REC{'metadata_dir'}, $MOCK_UPLOAD_REC{'cghub_analysis_id'},
                      $MOCK_UPLOAD_REC{'target'},
                    ],
                ]
            }, {
                'statement'    => qr/UPDATE upload/msi,
                'bound_params' => [ 'rerun_running', $MOCK_UPLOAD_REC{'upload_id'} ],
                'results'  => [[ 'rows' ], [], []]
            }, {
               'statement' => 'ROLLBACK',
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_changeUploadRerunStage( $MOCK_DBH );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Rollback with error message if update fails";
            my $got = $error;
            my $want = qr/_changeUploadRerunStage failed to update rerun upload status for upload $MOCK_UPLOAD_REC{'upload_id'}\./;
            like( $got, $want, $testThatThisSub);
        }
        {
            my $testThatThisSub = "Adds wrapping error message to specific errpr response";
            my $got = $error;
            my $want = qr/Failed trying to tag a \'fail\' upload record for rerun\: _changeUploadRerunStage .+/;
            like( $got, $want, $testThatThisSub);
        }
        {
            my $testThatThisSub = "Sets error tag if bad update count";
            my $got = $obj->{'error'};
            my $want = "unstored_lookup_error";
            is( $got, $want, $testThatThisSub );
        }
    }

    # Bad run, triggers error exit due to no upload_id returned (shouldn't be possible).
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT \*/msi,
                'results'  => [
                    [ 'upload_id',    'sample_id',
                      'status',       'external_status',
                      'metadata_dir', 'cghub_analysis_id',
                      'target'  ],
                    [ undef,                            $MOCK_UPLOAD_REC{'sample_id'},
                      $MOCK_UPLOAD_REC{'status'},       $MOCK_UPLOAD_REC{'external_status'},
                      $MOCK_UPLOAD_REC{'metadata_dir'}, $MOCK_UPLOAD_REC{'cghub_analysis_id'},
                      $MOCK_UPLOAD_REC{'target'},
                    ],
                ]
            }, {
               'statement' => 'ROLLBACK',
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_changeUploadRerunStage( $MOCK_DBH );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Rollback with error message if no upload_id retreved";
            my $got = $error;
            my $want = qr/_changeUploadRerunStage retrieved record without an upload_id\. Strange\./;
            like( $got, $want, $testThatThisSub);
        }
        {
            my $testThatThisSub = "Sets error tag if no upload_id retrieved.";
            my $got = $obj->{'error'};
            my $want = "unstored_lookup_error";
            is( $got, $want, $testThatThisSub );
        }
    }

    # Bad run, triggers error exit due to no status returned (shouldn't be possible).
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT \*/msi,
                'results'  => [
                    [ 'upload_id',    'sample_id',
                      'status',       'external_status',
                      'metadata_dir', 'cghub_analysis_id',
                      'target'  ],
                    [ $MOCK_UPLOAD_REC{'upload_id'},    $MOCK_UPLOAD_REC{'sample_id'},
                      undef,                            $MOCK_UPLOAD_REC{'external_status'},
                      $MOCK_UPLOAD_REC{'metadata_dir'}, $MOCK_UPLOAD_REC{'cghub_analysis_id'},
                      $MOCK_UPLOAD_REC{'target'},
                    ],
                ]
            }, {
               'statement' => 'ROLLBACK',
                'results'  => [[]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_changeUploadRerunStage( $MOCK_DBH );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Rollback with error message if no status retreved";
            my $got = $error;
            my $want = qr/_changeUploadRerunStage retrieved record without a status\. Strange\./;
            like( $got, $want, $testThatThisSub);
        }
        {
            my $testThatThisSub = "Sets error tag if no status retrieved.";
            my $got = $obj->{'error'};
            my $want = "unstored_lookup_error";
            is( $got, $want, $testThatThisSub );
        }
    }

    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_changeUploadRerunStage();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^_changeUploadRerunStage\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__changeUploadRerunStage_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }
}

