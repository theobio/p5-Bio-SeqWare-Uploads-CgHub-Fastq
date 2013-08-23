#! /usr/bin/env perl
use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages

use File::Temp;                      # Simple files for testing
use File::ShareDir qw(module_file);  # Access data files from install.

use Bio::SeqWare::Config 0.000003; # Get config data, with most recent keyset
use Bio::SeqWare::Uploads::CgHub::Fastq 0.000002; # Latest dev build.

use Test::Output;               # Test STDOUT and STDERR output.
use DBD::Mock;
use Test::More 'tests' => 2;    # Run this many Test::More compliant subtests

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $TEMP_DIR = File::Temp->newdir();  # Auto-delete self and contents when out of scope

my $CONFIG = Bio::SeqWare::Config->new();
my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode'            => 'meta',
    'uploadFastqBaseDir' => $TEMP_DIR,
    'myName'             => 'DELETE_ME-upload-cghub-fastq_0.0.3',
    'rerun'              => 2,
};

my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1 },
);

subtest( 'doMeta()' => \&test_doMeta );
subtest( '_changeUploadRunStage()' => \&test_changeUploadRunStage );


sub test_doMeta {
    plan( tests => 1 );

    my $obj = $CLASS->new( $OPT_HR );
    eval {
        $obj->doMeta( $MOCK_DBH );
    };
    like( $@, qr/^doMeta\(\) not implemented/, "Error when doMeta() Fails" );
}

sub test_changeUploadRunStage {
    plan( tests => 19 );

    my $oldStatus = "parent_stage_completed";
    my $newStatus = "child_stage_running";
    my $uploadId  = -21;
    my $sampleId  = -19;
    my $metaDataDir = 't';
    my $uuid      = 'Data';
    my $sqlTargetForFastqUpload = 'CGHUB_FASTQ';

    my $obj = $CLASS->new( $OPT_HR );

    my @dbSesssion = ({
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/SELECT upload_id.*/msi,
        'bound_params' => [ $sqlTargetForFastqUpload, $oldStatus ],
        'results'  => [[ 'upload_id', 'metadata_dir', 'cghub_analysis_id', 'sample_id' ],
                       [ $uploadId,   $metaDataDir,   $uuid,               $sampleId   ]]
    }, {
        'statement'    => qr/UPDATE upload.*/msi,
        'bound_params' => [ $newStatus,  $uploadId ],
        'results'  => [[ 'rows' ], []]
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    });

    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "smoke test", @dbSesssion );
        is( 1, $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus ), "Select upload appeard to work");
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "missing dbh parameter", @dbSesssion );
       eval {
           $obj->_changeUploadRunStage( undef, $oldStatus, $newStatus);
       };
       like($@, qr/^_changeUploadRunStage\(\) missing \$dbh parameter\./, "Bad param 1 - dbh");
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "missing oldStatus parameter", @dbSesssion );
       eval {
           $obj->_changeUploadRunStage( $MOCK_DBH, undef, $newStatus);
       };
       like($@, qr/^_changeUploadRunStage\(\) missing \$fromStatus parameter\./, "Bad param 2 - $oldStatus");
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "missing newStatus parameter", @dbSesssion );
       eval {
           $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, undef);
       };
       like($@, qr/^_changeUploadRunStage\(\) missing \$toStatus parameter\./, "Bad param 3 - $newStatus");
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose not", @dbSesssion );
        stdout_unlike {
            $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )
        } qr/SQL to find a lane/, 'No 1st messages if not verbose';
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose not 2", @dbSesssion );
        stdout_unlike {
             $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )
        } qr/SQL to set to state/, 'No 2nd messages if not verbose';
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose not 3", @dbSesssion );
        stdout_unlike {
             $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )
        } qr/\; UPLOAD_BASE_DIR/, 'No 3rd messages if not verbose';
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose", @dbSesssion );
        $obj->{'verbose'} = 1;
        stdout_is {
            $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )
        } "SQL to find a lane in state $oldStatus:\n"
        . "SELECT upload_id, metadata_dir, cghub_analysis_id, sample_id
        FROM upload
        WHERE u.target = ?
          AND u.status = ?
        ORDER by upload_id DESC limit 1"
        . "\n"
        . "SQL to set to state $newStatus:\n"
        . "UPDATE upload SET status = ? WHERE upload_id = ?"
        . "\n"
        ."Switching upload processing status from $oldStatus to $newStatus\n"
        . "; " . "SAMPLE: "           . $sampleId
        . "; " . "UPLOAD_ID: "        . $uploadId
        . "; " . "UPLOAD_BASE_DIR: "  . $metaDataDir
        . "; " . "UPLOAD_UUID: "      . $uuid
        . "\n",
        "verbose output";
    }
    {
        my $obj = $CLASS->new( $OPT_HR );

        my @dbSesssion = ({
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
            'statement'    => qr/SELECT upload_id.*/msi,
            'bound_params' => [ $sqlTargetForFastqUpload, $oldStatus ],
            'results'  => [[]]
        }, {
           'statement' => 'COMMIT',
           'results'   => [[]],
        });
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "select Nothing", @dbSesssion );

        is( 0, $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus ), "Select nothing upload appeard to work");
    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        my @dbSession = ({
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
            'statement'    => qr/SELECT upload_id.*/msi,
            'bound_params' => [ $sqlTargetForFastqUpload, $oldStatus ],
            'results'  => [[ 'upload_id', 'metadata_dir', 'cghub_analysis_id', 'sample_id' ],
                           [ $uploadId,   $metaDataDir,   $uuid,               $sampleId   ]]
        }, {
           'statement' => 'ROLLBACK',
            'results'  => [[]],
        });
        {
            $dbSession[1]->{'results'}->[1]->[0] = undef;
            $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( "Missing uploadId result", @dbSession );
            eval {
               $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus);
            };
            like($@, qr/Failed to retrieve upload data when switching from $oldStatus to $newStatus/, "Bad uploadId" );
            is( $obj->{'error'}, "upload_switch_data_" . $oldStatus . "_to_" . $newStatus , "error for bad uploadId" );
            $dbSession[1]->{'results'}->[1]->[0] = $uploadId;
        }
        {
            $dbSession[1]->{'results'}->[1]->[1] = undef;
            $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( "Missing metaDataDir result", @dbSession );
            eval {
               $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus);
            };
            like($@, qr/Failed to retrieve upload data when switching from $oldStatus to $newStatus/, "Bad metaDataDir" );
            is( $obj->{'error'}, "upload_switch_data_" . $oldStatus . "_to_" . $newStatus , "error for bad metaDataDir" );
            $dbSession[1]->{'results'}->[1]->[1] = $metaDataDir;
        }
        {
            $dbSession[1]->{'results'}->[1]->[2] = undef;
            $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( "Missing cghub_analysis_id result", @dbSession );
            eval {
               $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus);
            };
            like($@, qr/Failed to retrieve upload data when switching from $oldStatus to $newStatus/, "Bad cghub_analysis_id" );
            is( $obj->{'error'}, "upload_switch_data_" . $oldStatus . "_to_" . $newStatus , "error for bad cghub_analysis_id" );
            $dbSession[1]->{'results'}->[1]->[2] = $uuid;
        }
        {
            $dbSession[1]->{'results'}->[1]->[3] = undef;
            $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( "Missing sample_id result", @dbSession );
            eval {
               $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus);
            };
            like($@, qr/Failed to retrieve upload data when switching from $oldStatus to $newStatus/, "Bad sample_id" );
            is( $obj->{'error'}, "upload_switch_data_" . $oldStatus . "_to_" . $newStatus , "error for bad sample_id" );
            $dbSession[1]->{'results'}->[1]->[3] = $sampleId;
        }
        {
            $dbSession[1]->{'results'}->[1]->[2] = "NoSuchUuidDir";
            $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( "Die on missing fastq upload dir", @dbSession );
            eval {
               $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus);
            };
            like($@, qr/Failed to find the expected fastq upload dir\: ${metaDataDir}.*NoSuchUuidDir/, "Die on missing fastq upload dir" );
            is( $obj->{'error'}, "no_fastq_upload_dir_found" , "error for missing upload dir" );
            $dbSession[1]->{'results'}->[1]->[2] = $uploadId;
        }
    }

}