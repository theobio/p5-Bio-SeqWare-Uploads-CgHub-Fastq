use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Spec;           # Normal path handling
use File::Temp;           # Simple files for testing

use Bio::SeqWare::Config; # Read the seqware config file
use DBD::Mock;
use Test::More 'tests' => 14;    # Run this many Test::More compliant subtests

use Bio::SeqWare::Uploads::CgHub::Fastq;

my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $TEMP_DIR = File::Temp->newdir();  # Auto-delete self and contents when out of scope
my $filename = File::Temp->new(
    'TEMPLATE' => "dummyZipFile_XXXX", 'SUFFIX' => ".fastq.tar.gz"
)->filename;

sub makeTempZipFile {
    my $fileName = File::Temp->new(
        'TEMPLATE' => "dummyZipFile_XXXX", 'SUFFIX' => ".fastq.tar.gz"
    )->filename;
    $fileName = File::Spec->catfile( $TEMP_DIR, $fileName);
    `touch $fileName`;
    if (! -f $fileName) {
        croak "Coudn't create temp file $fileName needed for testing.";
    }
    return $fileName;
}
my $TEMP_FILE = makeTempZipFile();

sub makeTempUploadDir {
    my $dir = shift;
    $dir = File::Spec->catdir($dir, '00000000-0000-0000-0000-000000000000');
    if (! -d $dir) {
        mkdir( $dir ) or
                croak "Coudn't create temp dir $dir needed for testing.";
    }
    my $fileName = File::Spec->catfile( $dir, 'analysis.xml');
    `touch $fileName`;
    if (! -f $fileName) {
        croak "Coudn't create temp file $fileName needed for testing.";
    }
    $fileName = File::Spec->catfile( $dir, 'run.xml');
    `touch $fileName`;
    if (! -f $fileName) {
        croak "Coudn't create temp file $fileName needed for testing.";
    }
    $fileName = File::Spec->catfile( $dir, 'experiment.xml');
    `touch $fileName`;
    if (! -f $fileName) {
        croak "Coudn't create temp file $fileName needed for testing.";
    }
    return $dir;
}

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
sub mock__getAssociatedFileId       { return -5; }
sub mock__getFilePath_undef { return undef; }
sub mock__getFilePath       { return $TEMP_FILE; }
sub mock__getAssociatedProcessingFilesIds_undef { return ( undef, undef ); }
sub mock__getAssociatedProcessingFilesIds_1of2  { return (  -201, undef ); }
sub mock__getAssociatedProcessingFilesIds_2of2  { return (  -201,  -202 ); }

sub mock__deleteUploadRec          { return 1; }
sub mock__deleteFileRec            { return 1; }
sub mock__deleteUploadFileRec      { return 1; }
sub mock__deleteProcessingFilesRec { return 1; }
sub mock__deleteProcessingFilesRec_die {
     my $self = shift;
     $self->{'error'} = 'delete_processing_files';
     croak "Not 1 but 0 rows returned when attempting delete from processing_files, id -201.\n";
}

#
# TESTS
#

subtest( '_changeUploadRerunStage()' => \&test__changeUploadRerunStage );

subtest( '_getAssociatedFileId()'            => \&test__getAssociatedFileId );
subtest( '_getFilePath()'                    => \&test__getFilePath );
subtest( '_getAssociatedProcessingFilesIds()' => \&test__getAssociatedProcessingFilesIds );
subtest( '_getRerunData()'                   => \&test__getRerunData );

subtest( '_deleteUploadRec()'          => \&test__deleteUploadRec );
subtest( '_deleteFileRec()'            => \&test__deleteFileRec );
subtest( '_deleteUploadFileRec()'      => \&test__deleteUploadFileRec );
subtest( '_deleteProcessingFilesRec()' => \&test__deleteProcessingFilesRec );
subtest( '_cleanDatabase()'            => \&test__cleanDatabase );

subtest( '_deleteFastqZipFile()' => \&test__deleteFastqZipFile );
subtest( '_deleteUploadDir()'    => \&test__deleteUploadDir );
subtest( '_cleanFileSystem()'    => \&test__cleanFileSystem );

subtest( 'doRerun()' => \&test_doRerun );

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
            'processing_files_id_1' => undef,
            'processing_files_id_2' => undef,
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
            'processing_files_id_1' => undef,
            'processing_files_id_2' => undef,
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

    # upload_file and file but no processing_files
    {
        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_files_id_1' => undef,
            'processing_files_id_2' => undef,
        };
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getFilePath = \&mock__getFilePath;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedProcessingFilesIds = \&mock__getAssociatedProcessingFilesIds_undef;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Retrieves expeted data record if no processing_files_1 record.";
            my $got = $obj->_getRerunData( $MOCK_DBH, \%MOCK_UPLOAD_REC );
            my $want = $rerunDataHR;
            is_deeply( $got, $want, $testThatThisSub);
        }
    }

    # upload_file, file processing_files_1 but no processing_files_2
    {
        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_files_id_1' => -201,
            'processing_files_id_2' => undef,
        };
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getFilePath = \&mock__getFilePath;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedProcessingFilesIds = \&mock__getAssociatedProcessingFilesIds_1of2;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Retrieves expeted data record if no processing_files_2 record.";
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
            'processing_files_id_1' => -201,
            'processing_files_id_2' => -202,
        };
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedFileId = \&mock__getAssociatedFileId;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getFilePath = \&mock__getFilePath;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_getAssociatedProcessingFilesIds = \&mock__getAssociatedProcessingFilesIds_2of2;
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

sub test__getAssociatedProcessingFilesIds {
    plan( tests => 9 );

    # Good run returning pair of processing_files ids
    {
        my $fileId   = -5;
        my $processingFilesId1 = -201;
        my $processingFilesId2 = -202;
        my @dbSession = (
            {
                'statement'    => qr/SELECT processing_files_id/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[ 'processing_files_id' ], [ $processingFilesId1 ], [$processingFilesId2]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct pair of processing_files records returned (given fixed DBIO output)";
            my @got = $obj->_getAssociatedProcessingFilesIds( $MOCK_DBH, $fileId );
            my @want = ($processingFilesId1, $processingFilesId2);
            is( @got, @want, $testThatThisSub);
        }
    }

    # Good run returning one processing_files id
    {
        my $fileId   = -5;
        my $processingFilesId1 = -201;
        my $processingFilesId2;
        my @dbSession = (
            {
                'statement'    => qr/SELECT processing_files_id/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[ 'processing_files_id' ], [ $processingFilesId1 ]],
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Correct single processing_files records returned (given fixed DBIO output)";
            my @got = $obj->_getAssociatedProcessingFilesIds( $MOCK_DBH, $fileId );
            my @want = ($processingFilesId1, $processingFilesId2);
            is( @got, @want, $testThatThisSub);
        }
    }

    # Good run returning no processing_files ids
    {
        my $fileId   = -5;
        my $processingFilesId1;
        my $processingFilesId2;
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
            my $testThatThisSub = "Correct no processing_files records returned (given fixed DBIO output)";
            my @got = $obj->_getAssociatedProcessingFilesIds( $MOCK_DBH, $fileId );
            my @want = ($processingFilesId1, $processingFilesId2);
            is( @got, @want, $testThatThisSub);
        }
    }

    # Error if returns more than two processing_files_ids
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
            $obj->_getAssociatedProcessingFilesIds( $MOCK_DBH, $fileId );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if more than two processing_files link records found";
            my $got = $error;
            my $want = qr/Found more than two processing records linked to fileId $fileId\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if more than two processing_files link records found";
            my $got = $obj->{'error'};
            my $want = 'too_many_processing_links';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getAssociatedProcessingFilesIds();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^_getAssociatedProcessingFilesIds\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getAssociatedProcessingFilesIds_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $fileId
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getAssociatedProcessingFilesIds( $MOCK_DBH );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no fileId parameter passed.";
            my $got = $@;
            my $want = qr/^_getAssociatedProcessingFilesIds\(\) missing \$fileId parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no fileId parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__getAssociatedProcessingFilesIds_fileId';
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
                'statement'    => qr/SELECT u\.\*/msi,
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
                'statement'    => qr/SELECT u\.\*/msi,
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
                'statement'    => qr/SELECT u\.\*/msi,
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
                'statement'    => qr/SELECT u\.\*/msi,
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
                'statement'    => qr/SELECT u\.\*/msi,
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
                'statement'    => qr/SELECT u\.\*/msi,
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

sub test__deleteUploadRec {

    plan( tests => 3 );

    # Good run
    {
        my $uploadId = -21;
        my @dbSession = (
            { 'statement'    => "DELETE FROM upload WHERE upload_id = ?",
              'bound_params' => [ $uploadId ],
              'results'  => [[ 'rows' ], []]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 1 when appears to work";
            ok( $obj->_deleteUploadRec( $MOCK_DBH, $uploadId ), $testThatThisSub );
        }
    }

    # Dies with error.
    {
        my $uploadId = -21;
        my @dbSession = (
            { 'statement'    => "DELETE FROM upload WHERE upload_id = ?",
              'bound_params' => [ $uploadId ],
              'results'  => [ [] ]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteUploadRec( $MOCK_DBH, $uploadId );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if no upload record deleted.";
            my $got = $error;
            my $want = qr/^Not 1 but 0 rows returned when attempting delete from upload\, id $uploadId\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no upload record deleted.";
            my $got = $obj->{'error'};
            my $want = 'delete_upload';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__deleteFileRec {

    plan( tests => 3 );

    # Good run
    {
        my $fileId = -5;
        my @dbSession = (
            { 'statement'    => "DELETE FROM file WHERE file_id = ?",
              'bound_params' => [ $fileId ],
              'results'  => [[ 'rows' ], []]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 1 when appears to work";
            ok( $obj->_deleteFileRec( $MOCK_DBH, $fileId ), $testThatThisSub );
        }
    }

    # Dies with error.
    {
        my $fileId = -5;
        my @dbSession = (
            { 'statement'    => "DELETE FROM file WHERE file_id = ?",
              'bound_params' => [ $fileId ],
              'results'  => [ [] ]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteFileRec( $MOCK_DBH, $fileId );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if no file record deleted.";
            my $got = $error;
            my $want = qr/^Not 1 but 0 rows returned when attempting delete from file\, id $fileId\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no file record deleted.";
            my $got = $obj->{'error'};
            my $want = 'delete_file';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__deleteUploadFileRec {

    plan( tests => 3 );

    # Good run
    {
        my $uploadId = -21;
        my $fileId = -5;
        my @dbSession = (
            { 'statement'    => "DELETE FROM upload_file WHERE upload_id = ? AND file_id = ?",
              'bound_params' => [ $uploadId, $fileId ],
              'results'  => [[ 'rows' ], []]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 1 when appears to work";
            ok( $obj->_deleteUploadFileRec( $MOCK_DBH, $uploadId, $fileId ), $testThatThisSub );
        }
    }

    # Dies with error.
    {
        my $uploadId = -21;
        my $fileId = -5;
        my @dbSession = (
            { 'statement'    => "DELETE FROM upload_file WHERE upload_id = ? AND file_id = ?",
              'bound_params' => [ $uploadId, $fileId ],
              'results'  => [ [] ]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteUploadFileRec( $MOCK_DBH, $uploadId, $fileId );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if no upload_file record deleted.";
            my $got = $error;
            my $want = qr/^Not 1 but 0 rows returned when attempting delete from upload_file\, upload $uploadId\, file $fileId\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no file record deleted.";
            my $got = $obj->{'error'};
            my $want = 'delete_upload_file';
            is( $got, $want, $testThatThisSub);
        }
    }
}

sub test__deleteProcessingFilesRec {

    plan( tests => 7 );

    # Good run with 2 Ids
    {
        my $processingFilesId1 = -201;
        my $processingFilesId2 = -202;
        my @dbSession = (
            {
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId1 ],
              'results'  => [[ 'rows' ], []]
            },{
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId2 ],
              'results'  => [[ 'rows' ], []]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 1 when appears to work";
            ok( $obj->_deleteProcessingFilesRec( $MOCK_DBH, $processingFilesId1, $processingFilesId2 ), $testThatThisSub );
        }
    }

    # Good run with first Id
    {
        my $processingFilesId1 = -201;
        my $processingFilesId2 = undef;
        my @dbSession = (
            {
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId1 ],
              'results'  => [[ 'rows' ], []]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 1 when appears to work";
            ok( $obj->_deleteProcessingFilesRec( $MOCK_DBH, $processingFilesId1, $processingFilesId2 ), $testThatThisSub );
        }
    }

    # Good run with second Id
    {
        my $processingFilesId1 = undef;
        my $processingFilesId2 = -202;
        my @dbSession = (
            {
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId2 ],
              'results'  => [[ 'rows' ], []]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 1 when appears to work";
            ok( $obj->_deleteProcessingFilesRec( $MOCK_DBH, $processingFilesId1, $processingFilesId2 ), $testThatThisSub );
        }
    }

    # Dies with error if bad second id.
    {
        my $processingFilesId1 = -201;
        my $processingFilesId2 = -202;
        my @dbSession = (
            {
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId1 ],
              'results'  => [[ 'rows' ], []]
            },{
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId2 ],
              'results'  => [[]]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteProcessingFilesRec( $MOCK_DBH, $processingFilesId1, $processingFilesId2 );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if no processing_files record 2 deleted.";
            my $got = $error;
            my $want = qr/^Not 1 but 0 rows returned when attempting delete from processing_files\, id $processingFilesId2\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no processing files record 2 deleted.";
            my $got = $obj->{'error'};
            my $want = 'delete_processing_files';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Dies with error if bad first id.
    {
        my $processingFilesId1 = -201;
        my $processingFilesId2 = -202;
        my @dbSession = (
            {
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId1 ],
              'results'  => [[]]
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteProcessingFilesRec( $MOCK_DBH, $processingFilesId1, $processingFilesId2 );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if no processing_files record 1 deleted.";
            my $got = $error;
            my $want = qr/^Not 1 but 0 rows returned when attempting delete from processing_files\, id $processingFilesId1\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no processing files record 1 deleted.";
            my $got = $obj->{'error'};
            my $want = 'delete_processing_files';
            is( $got, $want, $testThatThisSub);
        }
    }
}

sub test__cleanDatabase {
    plan( tests => 14 );

    # Smoke test, with everything
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );

        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_files_id_1' => -201,
            'processing_files_id_2' => -202,
        };

        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteProcessingFilesRec = \&mock__deleteProcessingFilesRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteUploadFileRec = \&mock__deleteUploadFileRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteFileRec = \&mock__deleteFileRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteUploadRec = \&mock__deleteUploadRec;

        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Succeds if all data provided.";
            ok( $obj->_cleanDatabase( $MOCK_DBH, $rerunDataHR ), $testThatThisSub);
        }
    }

    # Smoke test, everything, minus one processing record.
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );

        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_files_id_1' => -201,
            'processing_files_id_2' => undef,
        };

        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteProcessingFilesRec = \&mock__deleteProcessingFilesRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteUploadFileRec = \&mock__deleteUploadFileRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteFileRec = \&mock__deleteFileRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteUploadRec = \&mock__deleteUploadRec;

        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Succeds if all data provided.";
            ok( $obj->_cleanDatabase( $MOCK_DBH, $rerunDataHR ), $testThatThisSub);
        }
    }

    # Delete with no file record => no upload_file or processing_files by reference constraint.
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );

        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => undef,
            'file_path' => undef,
            'processing_files_id_1' => undef,
            'processing_files_id_2' => undef,
        };

        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteUploadRec = \&mock__deleteUploadRec;

        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Succeds if no data but upload_id provided.";
            ok( $obj->_cleanDatabase( $MOCK_DBH, $rerunDataHR ), $testThatThisSub);
        }
    }

    # Delete with no processing_file records.
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );

        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_files_id_1' => undef,
            'processing_files_id_2' => undef,
        };

        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteUploadFileRec = \&mock__deleteUploadFileRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteFileRec = \&mock__deleteFileRec;
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteUploadRec = \&mock__deleteUploadRec;

        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Succeds if no processing_files data provided.";
            ok( $obj->_cleanDatabase( $MOCK_DBH, $rerunDataHR ), $testThatThisSub);
        }
    }

    # Error propagation - from within this subroutine
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [],
            }, {
               'statement' => 'ROLLBACK',
                'results'  => [[]],
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );

        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_files_id_1' => -201,
            'processing_files_id_2' => -202,
        };

        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanDatabase( $MOCK_DBH, $rerunDataHR );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if transaction wrapping fails.";
            my $got = $error;
            my $want = qr/^_cleanDatabase failed to delete the upload record for upload $rerunDataHR->{'upload'}->{'upload_id'}\. ...+/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if transaction wrapping fails.";
            my $got = $obj->{'error'};
            my $want = 'unknown_rerun__cleanDatabase';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Error propagation - from within called subroutine
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
               'statement' => 'ROLLBACK',
                'results'  => [[]],
            }
        );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );

        my $rerunDataHR = {
            'upload' => \%MOCK_UPLOAD_REC,
            'file_id' => -5,
            'file_path' => $TEMP_FILE,
            'processing_files_id_1' => -201,
            'processing_files_id_2' => -202,
        };

        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_deleteProcessingFilesRec = \&mock__deleteProcessingFilesRec_die;
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanDatabase( $MOCK_DBH, $rerunDataHR );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if transaction wrapping fails.";
            my $got = $error;
            my $want = qr/^_cleanDatabase failed to delete the upload record for upload $rerunDataHR->{'upload'}->{'upload_id'}. Not 1 but 0 rows returned when attempting delete from processing_files\, id -201\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if transaction wrapping fails.";
            my $got = $obj->{'error'};
            my $want = 'delete_processing_files';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanDatabase();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^_cleanDatabase\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__cleanDatabase_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $rerunDataHR
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanDatabase( $MOCK_DBH );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no rerunDataHR parameter passed.";
            my $got = $@;
            my $want = qr/^_cleanDatabase\(\) missing \$rerunDataHR parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no rerunDataHR parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__cleanDatabase_rerunDataHR';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $rerunDataHr->upload->upload_id
    {
        my $rerunDataHR = {};
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanDatabase( $MOCK_DBH, $rerunDataHR );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no rerunDataHr->upload->upload_id parameter passed.";
            my $got = $@;
            my $want = qr/^_cleanDatabase\(\) missing \$rerunDataHR->upload->upload_id parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no rerunDataHR->upload->upload_id parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__cleanDatabase_rerunDataHR_upload_id';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__deleteFastqZipFile {
    plan( tests => 16 );

    # unlink fastq.tar.gz dummy file
    {
        my $file = makeTempZipFile();
        my $obj = $CLASS->new( $OPT_HR );
        {
            ok( -f $file, "zip file exists before deletion.");
        }
        {
            my $testThatThisSub = "Returns 1 when deletes file";
            my $got = $obj->_deleteFastqZipFile( $file );
            my $want = 1;
            is( $got, $want, $testThatThisSub);
        }
        {
            ok( ! -e $file, "zip file gone after deletion.");
        }
    }

    # Ok if no file name passed in
    {
        my $file;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 0 if no file to delete";
            my $got = $obj->_deleteFastqZipFile( $file );
            my $want = 0;
            is( $got, $want, $testThatThisSub);
        }
    }

    # Ok if no such file exists
    {
        my $file = "/no/such/fastq.tar.gz";
        {
            ok( ! -e $file, "Test requires no such file to exist");
        }
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 0 if no file to delete";
            my $got = $obj->_deleteFastqZipFile( $file );
            my $want = 0;
            is( $got, $want, $testThatThisSub);
        }
    }

    # raise errors from failed deletion
    {
        my $file = makeTempZipFile();
        `chmod -w $TEMP_DIR`;
        {
            ok( ! -w $TEMP_DIR, "test requires zip parent dir not writeable.");
        }
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_deleteFastqZipFile( $file );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if delete fails";
            my $got = $error;
            my $want = qr/^Not removed \- $file \- .+/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if delete fails";
            my $got = $obj->{'error'};
            my $want = 'rm_fastq';
            is( $got, $want, $testThatThisSub);
        }
        # cleanup, make file writeable again.
        `chmod +w $TEMP_DIR`;
        {
            ok( -w $TEMP_DIR, "Set zip parnet dir writeable again.");
        }
    }

    # fastq zipfile not zip file
    {
        my $fileName = "/dummy/file.1.fastq";
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteFastqZipFile( $fileName );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if filename not a gzip file.";
            my $got = $error;
            my $want = qr/^Not removed as filename doean\'t match \*fastq\*\.tar\.gz \- $fileName/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if filename not a gzip file.";
            my $got = $obj->{'error'};
            my $want = 'rm_fastq_bad_filename_ext';
            is( $got, $want, $testThatThisSub);
        }
    }

    # fastq zipfile not fastq
    {
        my $fileName = "/dummy/file.tar.gz";
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteFastqZipFile( $fileName );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if filename not a fastq file.";
            my $got = $error;
            my $want = qr/^Not removed as filename doean\'t match \*fastq\*\.tar\.gz \- $fileName/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if filename not a fastq file.";
            my $got = $obj->{'error'};
            my $want = 'rm_fastq_bad_filename_ext';
            is( $got, $want, $testThatThisSub);
        }
    }

    # fastq zipfile not absolute
    {
        my $fileName = "file.fastq.tar.gz";
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteFastqZipFile( $fileName );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if filename not an absolute path.";
            my $got = $error;
            my $want = qr/^Not removed as filename not absolute - $fileName/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if filename not an absolute path.";
            my $got = $obj->{'error'};
            my $want = 'rm_fastq_bad_filename_abs';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__deleteUploadDir {
    plan( tests => 13 );

    # Smoke test - delete upload dir
    {
        my $dir = makeTempUploadDir( $TEMP_DIR );
        my $baseDir = $TEMP_DIR;
        my $uuidDir = $MOCK_UPLOAD_REC{'cghub_analysis_id'};
        my $obj = $CLASS->new( $OPT_HR );
        {
            ok( -d $dir, "upload dir exists before deletion.");
        }
        {
            my $testThatThisSub = "Returns number of files deleted uplaod dir";
            my $got = $obj->_deleteUploadDir( $baseDir, $uuidDir );
            my $want = 4; # three files plus directory
            is( $got, $want, $testThatThisSub);
        }
        {
            ok( ! -d $dir, "uplaod dir gone after deletion.");
        }
    }


    # Ok if no $baseDir passed in
    {
        my $baseDir;
        my $uuidDir = $MOCK_UPLOAD_REC{'cghub_analysis_id'};
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 0 if no basedir to delete";
            my $got = $obj->_deleteUploadDir( $baseDir, $uuidDir );
            my $want = 0;
            is( $got, $want, $testThatThisSub);
        }
    }

    # Ok if no $uuidDir passed in
    {
        my $baseDir = $TEMP_DIR;
        my $uuidDir;
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 0 if no uuid to delete";
            my $got = $obj->_deleteUploadDir( $baseDir, $uuidDir );
            my $want = 0;
            is( $got, $want, $testThatThisSub);
        }
    }

    # Ok if no such upload directories exist.
    {
        my $baseDir = "/no/such/";
        my $uuidDir = $MOCK_UPLOAD_REC{'cghub_analysis_id'};
        {
            ok( ! -e $baseDir, "Test requires no such dir to exist");
        }
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Returns 0 if no file to delete";
            my $got = $obj->_deleteUploadDir($baseDir, $uuidDir );
            my $want = 0;
            is( $got, $want, $testThatThisSub);
        }
    }
    # Error when trying to delete upload dir
    {
        my $dir = makeTempUploadDir( $TEMP_DIR );
        `chmod -w $dir/analysis.xml`;
        if ( (! -f "$dir/analysis.xml") || ( -w "$dir/analysis.xml")) {
            die( "Prior to test, no-write file must exist" );
        }
        my $baseDir = $TEMP_DIR;
        my $uuidDir = $MOCK_UPLOAD_REC{'cghub_analysis_id'};
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteUploadDir( $baseDir, $uuidDir );
        };
        my $error = $@;

        {
            my $testThatThisSub = "Throws descriptive error if dir not deleted";
            my $got = $error;
            my $want = qr/^Not removed - $dir.*not empty/s;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if dir not deleted.";
            my $got = $obj->{'error'};
            my $want = 'rm_upload_dir';
            is( $got, $want, $testThatThisSub);
        }

        # cleanup
        `chmod +w $dir/analysis.xml`;
        if ( ! -w "$dir/analysis.xml" ) {
            die( "After test, file must be writeable." );
        }
    }

    # upload dir not absolute.
    {
        my $baseDir = 'relative/dir';
        my $uuidDir = $MOCK_UPLOAD_REC{'cghub_analysis_id'};
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteUploadDir( $baseDir, $uuidDir );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if filename not an absolute path.";
            my $got = $error;
            my $want = qr/^Not removed as dir not absolute \- relative/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if dir not an absolute path.";
            my $got = $obj->{'error'};
            my $want = 'rm_upload_dir_bad_filename_abs';
            is( $got, $want, $testThatThisSub);
        }
    }

    # upload dir not uuid
    {
        my $baseDir = $TEMP_DIR;
        my $uuidDir = "NotAUUID";
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_deleteUploadDir( $baseDir, $uuidDir );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if dir not formated as uuid";
            my $got = $error;
            my $want = qr/^Not removed as doesn\'t look like a uuid \- $TEMP_DIR.+/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if dir not formated as uuid.";
            my $got = $obj->{'error'};
            my $want = 'rm_upload_dir_bad_filename_format';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test__cleanFileSystem {
    plan( tests => 12 );

    # Smoke test, with everything
    {
        my $dir = makeTempUploadDir( $TEMP_DIR );
        my $file = makeTempZipFile();
        my $uploadHR = {
            'upload_id'         => -21,
            'sample_id'         => -19,
            'target'            => 'CGHUB_FASTQ',
            'status'            => 'zip_failed_dummy_error',
            'external_status'   => undef,
            'metadata_dir'      => $TEMP_DIR,
            'cghub_analysis_id' => '00000000-0000-0000-0000-000000000000',
        };
        my $rerunDataHR = {
            'upload' => $uploadHR,
            'file_id' => -5,
            'file_path' => $file,
            'processing_files_id_1' => -201,
            'processing_files_id_2' => -202,
        };

        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Succeeds if all data provided.";
            ok( -e $file, "File exists prior");
            ok( -d $dir, "Dir exists prior");
            ok( $obj->_cleanFileSystem( $rerunDataHR ), $testThatThisSub);
            ok( ! -e $file, "File does not exist after");
            ok( ! -d $dir, "Dir does not exist after");
            ok( -d $TEMP_DIR, "Parent dir DOES exists after");
        }
    }


    # Error propagation - from within called subroutine
    {
        my $dir = makeTempUploadDir( $TEMP_DIR );
        my $file = makeTempZipFile();
        my $uploadHR = {
            'upload_id'         => -21,
            'sample_id'         => -19,
            'target'            => 'CGHUB_FASTQ',
            'status'            => 'zip_failed_dummy_error',
            'external_status'   => undef,
            'metadata_dir'      => $TEMP_DIR,
            'cghub_analysis_id' => 'BadDirName',
        };
        my $rerunDataHR = {
            'upload' => $uploadHR,
            'file_id' => -5,
            'file_path' => $file,
            'processing_files_id_1' => -201,
            'processing_files_id_2' => -202,
        };

        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanFileSystem( $rerunDataHR );
        };
        my $error = $@;
        {
            my $testThatThisSub = "Throws descriptive error if filesystem delete fails.";
            my $got = $error;
            my $want = qr/^_cleanFileSystem failed to delete the file system data for upload $rerunDataHR->{'upload'}->{'upload_id'}\. Not removed as doesn\'t look like a uuid \- $TEMP_DIR.+/;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if filesystem delete fails.";
            my $got = $obj->{'error'};
            my $want = 'rm_upload_dir_bad_filename_format';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $rerunDataHR
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanFileSystem();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no rerunDataHR parameter passed.";
            my $got = $@;
            my $want = qr/^_cleanFileSystem\(\) missing \$rerunDataHR parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no rerunDataHR parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__cleanFileSystem_rerunDataHR';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $rerunDataHr->upload->upload_id
    {
        my $rerunDataHR = {};
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_cleanFileSystem( $rerunDataHR );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no rerunDataHr->upload->upload_id parameter passed.";
            my $got = $@;
            my $want = qr/^_cleanFileSystem\(\) missing \$rerunDataHR->upload->upload_id parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no rerunDataHR->upload->upload_id parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param__cleanFileSystem_rerunDataHR_upload_id';
            is( $got, $want, $testThatThisSub);
        }
    }

}

sub test_doRerun {
    plan( tests => 8 );

    # Full run
    {
        my $dir = makeTempUploadDir( $TEMP_DIR );
        my $file = makeTempZipFile();
        my $uploadRec = {
            'upload_id'         => -21,
            'sample_id'         => -19,
            'target'            => 'CGHUB_FASTQ',
            'status'            => 'zip_failed_dummy_error',
            'external_status'   => undef,
            'metadata_dir'      => $TEMP_DIR,
            'cghub_analysis_id' => '00000000-0000-0000-0000-000000000000',
        };

        my $uploadId = $uploadRec->{'upload_id'};
        my $fileId = -5;
        my $processingFilesId1 = -201;
        my $processingFilesId2 = -202;
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT u\.\*/msi,
                'results'  => [
                    [ 'upload_id',    'sample_id',
                      'status',       'external_status',
                      'metadata_dir', 'cghub_analysis_id',
                      'target'  ],
                    [ $uploadRec->{'upload_id'},    $uploadRec->{'sample_id'},
                      $uploadRec->{'status'},       $uploadRec->{'external_status'},
                      $uploadRec->{'metadata_dir'}, $uploadRec->{'cghub_analysis_id'},
                      $uploadRec->{'target'},
                    ],
                ]
            }, {
                'statement'    => qr/UPDATE upload/msi,
                'bound_params' => [ 'rerun_running', $uploadId ],
                'results'  => [[ 'rows' ], []]
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }, {
                'statement'    => qr/SELECT file_id/msi,
                'bound_params' => [ $uploadId ],
                'results'  => [[ 'file_id' ], [ $fileId ]],
            }, {
                'statement'    => qr/SELECT file_path/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[ 'file_path' ], [ $file ]],
            }, {
                'statement'    => qr/SELECT processing_files_id/msi,
                'bound_params' => [ $fileId ],
                'results'  => [[ 'processing_files_id' ], [ $processingFilesId1 ], [$processingFilesId2]],
            }, {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId1 ],
              'results'  => [[ 'rows' ], []]
            }, {
              'statement'    => "DELETE FROM processing_files WHERE processing_files_id = ?",
              'bound_params' => [ $processingFilesId2 ],
              'results'  => [[ 'rows' ], []]
            }, { 
              'statement'    => "DELETE FROM upload_file WHERE upload_id = ? AND file_id = ?",
              'bound_params' => [ $uploadId, $fileId ],
              'results'  => [[ 'rows' ], []]
            }, {
              'statement'    => "DELETE FROM file WHERE file_id = ?",
              'bound_params' => [ $fileId ],
              'results'  => [[ 'rows' ], []]
            }, {
              'statement'    => "DELETE FROM upload WHERE upload_id = ?",
              'bound_params' => [ $uploadId ],
              'results'  => [[ 'rows' ], []]
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            } 
        );


        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Full pass to delete upload record.";
            my $got = $obj->doRerun( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, $testThatThisSub);
            ok( ! -d $dir, "Upload dir deleted");
            ok( ! -f $file, "Zip file deleted");
        }
    }

    # Nothing to do run
    {
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT u\.\*/msi,
                'results'  => [ [], ]
            }, {
               'statement' => 'COMMIT',
                'results'  => [[]],
            }
        );


        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $testThatThisSub = "Nothing to delete.";
            my $got = $obj->doRerun( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, $testThatThisSub);
        }
    }

    # Error propogation
    {
        my $uploadRec = {
            'upload_id'         => -21,
            'sample_id'         => -19,
            'target'            => 'CGHUB_FASTQ',
            'status'            => 'zip_failed_dummy_error',
            'external_status'   => undef,
            'metadata_dir'      => $TEMP_DIR,
            'cghub_analysis_id' => '00000000-0000-0000-0000-000000000000',
        };

        my $uploadId = $uploadRec->{'upload_id'};
        my @dbSession = (
            {
                'statement' => 'BEGIN WORK',
                'results'  => [[]],
            }, {
                 'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                 'results'  => [[]],
            }, {
                'statement'    => qr/SELECT u\.\*/msi,
                'results'  => [
                    [ 'upload_id',    'sample_id',
                      'status',       'external_status',
                      'metadata_dir', 'cghub_analysis_id',
                      'target'  ],
                    [ $uploadRec->{'upload_id'},    $uploadRec->{'sample_id'},
                      $uploadRec->{'status'},       $uploadRec->{'external_status'},
                      $uploadRec->{'metadata_dir'}, $uploadRec->{'cghub_analysis_id'},
                      $uploadRec->{'target'},
                    ],
                ]
            }, {
                'statement'    => qr/UPDATE upload/msi,
                'bound_params' => [ 'rerun_running', $uploadId ],
                'results'  => [ [] ]
            }, {
               'statement' => 'ROLLBACK',
                'results'  => [[]]
            }
        );

        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doRerun( $MOCK_DBH );
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^DO_RERUN\: Failed trying to tag a \'fail\' upload record for rerun\: _changeUploadRerunStage failed to update rerun upload status for upload $uploadId\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'failed_rerun_unstored_lookup_error';
            is( $got, $want, $testThatThisSub);
        }
    }

    # Bad Parameters - $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doRerun();
        };
        {
            my $testThatThisSub = "Throws descriptive error if no dbh parameter passed.";
            my $got = $@;
            my $want = qr/^doRerun\(\) missing \$dbh parameter\./;
            like( $got, $want, $testThatThisSub);
        }{
            my $testThatThisSub = "Sets correct error tag if no dbh parameter passed.";
            my $got = $obj->{'error'};
            my $want = 'param_doRerun_dbh';
            is( $got, $want, $testThatThisSub);
        }
    }


}
