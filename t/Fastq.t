#! /usr/bin/env perl

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Temp;           # Simple files for testing

use Bio::SeqWare::Config; # Access SeqWare settings file as options
use Bio::SeqWare::Db::Connection;

use DBD::Mock;
use Test::Output;         # Tests what appears on stdout.
use Test::More 'tests' => 1 + 10;   # Main testing module; run this many subtests
                                     # in BEGIN + subtests (subroutines).


BEGIN {
	*CORE::GLOBAL::readpipe = \&mock_readpipe; # Must be before use
	use_ok( 'Bio::SeqWare::Uploads::CgHub::Fastq' );
}

my $mock_readpipe = { 'mock' => 0, 'exit' => 0, 'ret' => undef };

sub mock_readpipe {
    my $var = shift;
    my $retVal;
    if ( $mock_readpipe->{'mock'} && $mock_readpipe->{'exit'}) {
        $? = $mock_readpipe->{'exit'};
        $retVal = undef;  # Just to be definite
    }
    elsif ($mock_readpipe->{'mock'} && ! $mock_readpipe->{'exit'}) {
        $retVal = $mock_readpipe->{'ret'};
    }
    else {
        $retVal = CORE::readpipe($var);
    }
    return $retVal;
}

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';


my $CONFIG = Bio::SeqWare::Config->new();
my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode' => 'alL',
};

my $OBJ = $CLASS->new( $OPT_HR );

# Keeping in case enable test DB in future.
my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1, 'ShowErrorStatement' => 1 },
);

#
# if ( ! $ENV{'DB_TESTING'} ) {
# 	diag( 'skipping 2 test that requires DB_TESTING' );
# }
# else {
#     my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $CONFIG );
#     $DBH = $connectionBuilder->getConnection( {'RaiseError' => 1, 'AutoCommit' => 1} );
# }
#

# Class methods
subtest( 'new()'             => \&testNew              );
subtest( 'new(BAD)'          => \&testNewBad           );
subtest( 'getFileBaseName()' => \&test_getFileBaseName );
subtest( 'getUuid()'         => \&test_getUuid         );
subtest( 'getTimeStamp()'    => \&testgetTimeStamp );

# Object methods
subtest( 'getAll()'       => \&testGetAll       );
subtest( 'run()'          => \&testRun          );
subtest( 'sayVerbose()'   => \&test_sayVerbose  );

# Internal methods
subtest( '_changeUploadRunStage()' => \&test__changeUploadRunStage );
subtest( '_updateUploadStatus()'   => \&test__updateUploadStatus);

$MOCK_DBH->disconnect();

sub test__changeUploadRunStage {
    plan( tests => 25 );

    my $oldStatus = "parent_stage_completed";
    my $newStatus = "child_stage_running";
    my $uploadId  = -21;
    my $sampleId  = -19;
    my $metaDataDir = 't';
    my $uuid      = 'Data';

    my $obj = $CLASS->new( $OPT_HR );
    my $fakeUploadHR = {
        'upload_id'         => $uploadId,
        'sample_id'         => $sampleId,
        'cghub_analysis_id' => $uuid,
        'metadata_dir'      => $metaDataDir,
        'status'            => $newStatus,
    };

    my @dbSession = ({
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
         'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
         'results'  => [[]],
    }, {
        'statement'    => qr/SELECT \*/msi,
        'bound_params' => [ $oldStatus ],
        'results'  => [
            [ 'upload_id', 'status',   'metadata_dir', 'cghub_analysis_id', 'sample_id' ],
            [ $uploadId,   $oldStatus, $metaDataDir,   $uuid,               $sampleId  ],
        ]
    }, {
        'statement'    => qr/UPDATE upload/msi,
        'bound_params' => [ $newStatus,  $uploadId ],
        'results'  => [[ 'rows' ], []]
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    });

    {
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        is_deeply( $fakeUploadHR, $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus ), "Select upload appeard to work");
        is( $obj->{'_fastqUploadId'}, $uploadId, "Sets upload id");
    }

    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose not", @dbSession );
        stdout_unlike {
            $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )
        } qr/SQL to find a lane/, 'No 1st messages if not verbose';
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose not 2", @dbSession );
        stdout_unlike {
             $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )
        } qr/SQL to set to state/, 'No 2nd messages if not verbose';
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose not 3", @dbSession );
        stdout_unlike {
             $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )
        } qr/\; UPLOAD_BASE_DIR/, 'No 3rd messages if not verbose';
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose", @dbSession );
        $obj->{'verbose'} = 1;
        my $expectRE = qr/Looking for upload record with status "parent_stage_completed"\./s;
        stdout_like { $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )}
                     $expectRE, "Verbose info - looking for upload record message.";
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose", @dbSession );
        $obj->{'verbose'} = 1;
        my $expectRE = qr/Found upload record with status "parent_stage_completed" - sample id = -19 upload id = -21\./s;
        stdout_like { $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )}
                     $expectRE, "Verbose info - found upload record message." ;
    }
    {
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "verbose", @dbSession );
        $obj->{'verbose'} = 1;
        my $expectRE = qr/Changing status of upload record \(id = -21\) from "parent_stage_completed" to\s+"child_stage_running"\./s;
        stdout_like { $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus )}
                     $expectRE, "Verbose info - updated upload record message." ;
    }
    {
        my $obj = $CLASS->new( $OPT_HR );

        my @dbSession = ({
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
         'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
         'results'  => [[]],
        }, {
            'statement'    => qr/SELECT \*/msi,
            'bound_params' => [ $oldStatus ],
            'results'  => [[]]
        }, {
           'statement' => 'COMMIT',
           'results'   => [[]],
        });
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "select Nothing", @dbSession );

        is( undef, $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus ), "Select nothing upload appeard to work");
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
        my $selObj = $CLASS->new( $opt );
        my @dbSelectSession = ({
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
            'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
            'results'  => [[]],
        }, {
            'statement'    => qr/SELECT \*/msi,
            'bound_params' => [  ],
            'bound_params' => [ $oldStatus, $opt->{'sampleId'}, $opt->{'sampleAccession'},
                $opt->{'sampleAlias'}, $opt->{'sampleUuid'}, $opt->{'sampleTitle'}, $opt->{'sampleType'} ],
            'results'  => [
                [ 'upload_id', 'status',   'metadata_dir', 'cghub_analysis_id', 'sample_id' ],
                [ $uploadId,   $oldStatus, $metaDataDir,   $uuid,               $sampleId  ],
            ]
        }, {
            'statement'    => qr/UPDATE upload/msi,
            'bound_params' => [ $newStatus,  $uploadId ],
            'results'  => [[ 'rows' ], []]
        }, {
           'statement' => 'COMMIT',
            'results'  => [[]],
        });
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSelectSession );

        {
            my $got  = $selObj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus );
            my $want = $fakeUploadHR;
            is_deeply( $got, $want, "Select filter by sample appeard to work");
        }
        {
             my $got = $selObj->{'_fastqUploadId'};
             my $want = $uploadId;
            is( $got, $want, "Filtered by sample, sets upload id");
        }
    }

    {
        my @dbSession = ({
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
            'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
            'results'  => [[]],
        }, {
            'statement'    => qr/SELECT \*/msi,
            'bound_params' => [ $oldStatus ],
            'results'  => [[ 'upload_id' ], []]
        }, {
           'statement' => 'ROLLBACK',
            'results'  => [[]],
        });
        {
            my $obj = $CLASS->new( $OPT_HR );
            $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( "Missing uploadId result", @dbSession );
            eval {
               $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus);
            };
            like($@, qr/Error changing upload status from $oldStatus to $newStatus/, "Bad uploadId general" );
            like($@, qr/Failed to retrieve upload data\./, "Bad uploadId specific" );
            is( $obj->{'error'}, "status_query_" . $oldStatus . "_to_" . $newStatus , "error for bad uploadId" );
        }
    }
    {
        my @dbSession = ({
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
             'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
             'results'  => [[]],
        }, {
            'statement'    => qr/SELECT \*/msi,
            'bound_params' => [ $oldStatus ],
            'results'  => [
                [ 'upload_id', 'status',   'metadata_dir', 'cghub_analysis_id', 'sample_id' ],
                [ $uploadId,   $oldStatus, $metaDataDir,   $uuid,               $sampleId  ],
            ]
        }, {
            'statement'    => qr/UPDATE upload/msi,
            'bound_params' => [ $newStatus,  $uploadId ],
            'results'  => [[]]
        }, {
           'statement' => 'ROLLBACK',
            'results'  => [[]],
        });
        {
            my $obj = $CLASS->new( $OPT_HR );
            $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( "Missing update result", @dbSession );
            eval {
               $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus);
            };
            like($@, qr/Failed to update upload status\./, "Bad update, specific" );
            like($@, qr/Error changing upload status from $oldStatus to $newStatus/, "Bad update general" );
            is( $obj->{'error'}, "status_update_" . $oldStatus . "_to_" . $newStatus , "error for bad uploadId" );
        }
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_changeUploadRunStage();
        };
        {
          like( $@, qr/^_changeUploadRunStage\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__changeUploadRunStage_dbh', "Errror tag if no dbh param");
        }
    }

    # Bad param: $fromStatus
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_changeUploadRunStage( $MOCK_DBH );
        };
        {
          like( $@, qr/^_changeUploadRunStage\(\) missing \$fromStatus parameter\./, "Error if no fromStatus param");
          is( $obj->{'error'}, 'param__changeUploadRunStage_fromStatus', "Errror tag if no fromStatus param");
        }
    }

    # Bad param: $toStatus
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus );
        };
        {
          like( $@, qr/^_changeUploadRunStage\(\) missing \$toStatus parameter\./, "Error if no toStatus param");
          is( $obj->{'error'}, 'param__changeUploadRunStage_toStatus', "Errror tag if no toStatus param");
        }
    }

    # Error propagation
    {
        $MOCK_DBH->{mock_can_connect} = 0;
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_changeUploadRunStage( $MOCK_DBH, $oldStatus, $newStatus );
        };
        {
          like( $@, qr/^Error changing upload status from $oldStatus to $newStatus/, "Error propagaion");
          is( $obj->{'error'}, 'status_change_' . $oldStatus. '_to_' . $newStatus, "Error tag propagaion");
        }
        $MOCK_DBH->{mock_can_connect} = 1;

        # AutoCommit left as 0 after failure even though transaction never really started.
        # Doesn't matter what is provided as session, or if no session set.
        # Can't restore or reset though ahy Mock::DBI method that I can find. Just putting it
        # back manually after this failure seems required.
        $MOCK_DBH->{'AutoCommit'} = 1;
    }
}

sub test__updateUploadStatus {

    plan ( tests => 9 );

    my $newStatus  = 'test_ignore_not-real-status';
    my $uploadId= -21;

    my $obj = $CLASS->new( $OPT_HR );
    $obj->{'_fastqUploadId'} = $uploadId;

    my @dbSession = ({
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/UPDATE upload.*/msi,
        'bound_params' => [ $newStatus, $uploadId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    });

    {
        $MOCK_DBH->{mock_clear_history} = 1;
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        $MOCK_DBH->{'mock_session'}->reset();
        {
            is( 1, $obj->_updateUploadStatus( $MOCK_DBH, $uploadId, $newStatus ), "Updated upload status." );
        }
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_updateUploadStatus();
        };
        {
          like( $@, qr/^_updateUploadStatus\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__updateUploadStatus_dbh', "Errror tag if no dbh param");
        }
    }

    # Bad param: $uploadId
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_updateUploadStatus( $MOCK_DBH );
        };
        {
          like( $@, qr/^_updateUploadStatus\(\) missing \$uploadId parameter\./, "Error if no uploadId param");
          is( $obj->{'error'}, 'param__updateUploadStatus_uploadId', "Errror tag if no uploadId param");
        }
    }

    # Bad param: $newStatus
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_updateUploadStatus( $MOCK_DBH, $uploadId );
        };
        {
          like( $@, qr/^_updateUploadStatus\(\) missing \$newStatus parameter\./, "Error if no newStatus param");
          is( $obj->{'error'}, 'param__updateUploadStatus_newStatus', "Errror tag if no newStatus param");
        }
    }

    # Error propagation
    {
        $MOCK_DBH->{mock_can_connect} = 0;
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_updateUploadStatus( $MOCK_DBH, $uploadId, $newStatus );
        };
        {
          like( $@, qr/^Failed to update status of upload record upload_id=$uploadId to $newStatus/, "Error propagaion");
          is( $obj->{'error'}, 'update_upload', "Error tag propagaion");
        }
        $MOCK_DBH->{mock_can_connect} = 1;

        # AutoCommit left as 0 after failure even though transaction never really started.
        # Doesn't matter what is provided as session, or if no session set.
        # Can't restore or reset though ahy Mock::DBI method that I can find. Just putting it
        # back manually after this failure seems required.
        $MOCK_DBH->{'AutoCommit'} = 1;
    }

}


sub testNew {
    plan( tests => 2 );
    {
	    ok($OBJ, "Default object created ok");
	}
    {
        my $opt = {
            'runMode' => 'ALL',
        };
        my $obj = $CLASS->new( $opt );
        $opt->{'runMode'} = "OOPS";
        my $got = $obj->getAll();
        my $want = {
            'runMode' => 'ALL',
            'myName'  => 'upload-cghub-fastq_0.0.1',
            'error'   => undef,
        };
	    is_deeply($got, $want, "Default object created saftley");
	}
}

sub testNewBad {
	plan( tests => 2 );
    {
        eval{ $CLASS->new(); };
        my $got = $@;
        my $want = qr/^A hash-ref parameter is required\./;
        like( $got, $want, "error with no param");
    }
    {
        eval{ $CLASS->new( "BAD_PARAM"); };
        my $got = $@;
        my $want = qr/^A hash-ref parameter is required\./;
        like( $got, $want, "error with non hash-ref param");
    }
}

sub test_getFileBaseName {
	plan( tests => 35 );

    # Filename parsing
    is_deeply( [$CLASS->getFileBaseName( "base.ext"      )], ["base", "ext"        ], "base and extension"         );
    is_deeply( [$CLASS->getFileBaseName( "base.ext.more" )], ["base",    "ext.more"], "base and extra extension"   );
    is_deeply( [$CLASS->getFileBaseName( "baseOnly"      )], ["baseOnly", undef    ], "base only"                  );
    is_deeply( [$CLASS->getFileBaseName( "base."         )], ["base",     ""       ], "base and dot, no extension" );
    is_deeply( [$CLASS->getFileBaseName( ".ext"          )], ["",         "ext"    ], "hidden = extension only"    );
    is_deeply( [$CLASS->getFileBaseName( "."             )], ["",          ""      ], "just dot"                   );
    is_deeply( [$CLASS->getFileBaseName( ""              )], ["",          undef   ], "empty"                      );

    # Path parsing
    is_deeply( [$CLASS->getFileBaseName( "dir/to/base.ext"       )], ["base", "ext" ], "relative dir to file"   );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/to/base.ext"  )], ["base", "ext" ], "abssolute dir to file"  );
    is_deeply( [$CLASS->getFileBaseName( "is/dir/base.ext/"      )], ["",     undef ], "relative dir, not file" );
    is_deeply( [$CLASS->getFileBaseName( "/is/abs/dir/base.ext/" )], ["",     undef ], "absolute dir, not file" );
    is_deeply( [$CLASS->getFileBaseName( "is/dir/base.ext/."     )], ["",     undef ], "relative dir, ends with /." );
    is_deeply( [$CLASS->getFileBaseName( "/is/abs/dir/base.ext/.")], ["",     undef ], "absolute dir, ends with /." );

    # Undefined input
    eval {
         $CLASS->getFileBaseName();
    };
    like( $@, qr/^ERROR: Undefined parmaeter, getFileBaseName\(\)\./, "Undefined param");

    # Filename parsing with spaces
    is_deeply( [$CLASS->getFileBaseName( " . "   )], [" ", " "  ], "base and extension are space"            );
    is_deeply( [$CLASS->getFileBaseName( " . . " )], [" ", " . "], "base and each extra extension are space" );
    is_deeply( [$CLASS->getFileBaseName( " "     )], [" ", undef], "base only, is space"                     );
    is_deeply( [$CLASS->getFileBaseName( " ."    )], [" ", ""   ], "base as space and dot, no extension"     );
    is_deeply( [$CLASS->getFileBaseName( ". "    )], ["",  " "  ], "hidden space = extension only"           );

    # Path parsing
    is_deeply( [$CLASS->getFileBaseName( "dir/to/ . "           )], [" ",    " "   ], "relative path, files are space"  );
    is_deeply( [$CLASS->getFileBaseName( "dir/ /base.ext"       )], ["base", "ext" ], "relative path with space"        );
    is_deeply( [$CLASS->getFileBaseName( " /to/base.ext"        )], ["base", "ext" ], "relative path start with space"  );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/to/ . "      )], [" ",    " "   ], "absolute path, files are spacee" );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/ /base.ext"  )], ["base", "ext" ], "absolute path with sapce"        );
    is_deeply( [$CLASS->getFileBaseName( "dir/ /base.ext/"      )], ["",     undef ], "relative dir with space"         );
    is_deeply( [$CLASS->getFileBaseName( " /to/base.ext/"       )], ["",     undef ], "relative dir starts with space"  );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/ /base.ext/" )], ["",     undef ], "absolute dir with space"         );

    # Extra dots
    is_deeply( [$CLASS->getFileBaseName( "base..ext" )], ["base", ".ext" ], "base .. extension"           );
    is_deeply( [$CLASS->getFileBaseName( "base.."    )], ["base", "."    ], "base and .., no extension"   );
    is_deeply( [$CLASS->getFileBaseName( "..ext"     )], ["",     ".ext" ], "no base, .., extension only" );
    is_deeply( [$CLASS->getFileBaseName( ".."        )], ["",     "."    ], "just .."                     );

    # Path parsing with double dots
    is_deeply( [$CLASS->getFileBaseName( "dir/to/.."       )], ["", undef ], "relative path, .. file"  );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/to/.."  )], ["", undef ], "absolute path, .. file"  );
    is_deeply( [$CLASS->getFileBaseName( "is/dir/../"      )], ["", undef ], "relative dir, .. as dir" );
    is_deeply( [$CLASS->getFileBaseName( "/is/abs/dir/../" )], ["", undef ], "absolute dir, .. as dir" );

    # Multiple Spaces

}

sub test_getUuid {
	plan( tests => 3 );

    {
        $mock_readpipe->{'mock'} = 1;
        eval {
            $CLASS->getUuid();
        };
        like( $@, qr/ERROR: `uuidgen` failed silently/, "Error on uuidgen returning nothing");
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'exit'} = 4;
        eval {
            $CLASS->getUuid();
        };
        like( $@, qr/ERROR: `uuidgen` exited with error, exit value was: 4/, "Error on uuidgen returning error status.");
        $mock_readpipe->{'mock'} = 0;
    }
    {
        my $got = $CLASS->getUuid();
        # Note: \A absolute multiline string start, \z absolute end, AFTER last \n, if any.
        like($got, qr/\A[\dA-F]{8}-[\dA-F]{4}-[\dA-F]{4}-[\dA-F]{4}-[\dA-F]{12}\z/i, "Match uuid format, NO TRAILING LINE BREAK")
    }
}

sub testgetTimeStamp {

    plan( tests => 1 );

    {
        my $want = "2013-09-08_15:16:17";
        my $got = $CLASS->getTimeStamp( 1378667777 );
        is( $got, $want, "timestamp from unix time." );
    }

}
sub test_sayVerbose {
	plan( tests => 5 );

    # Output with Uuid tag
    {
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_fastqUploadUuid'} = '12345678-1234-1234-1234-1234567890AB';
        $obj->{'verbose'} = 1;
        my $text = 'Say something';
        my $expectRE = qr/^567890AB: \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d - $text$/;
        {
            stdout_like { $obj->sayVerbose( $text ); } $expectRE, "Verbose output with uuid";
        }
    }

    # Output with undefined message
    {
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_fastqUploadUuid'} = '12345678-1234-1234-1234-1234567890AB';
        $obj->{'verbose'} = 1;
        my $text = undef;
        my $expectRE = qr/^567890AB: \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d - \( undef \)$/;
        {
            stdout_like { $obj->sayVerbose( $text ); } $expectRE, "Verbose output with no message";
        }
    }

    # Output with No Uuid tag
    {
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_fastqUploadUuid'} = undef;
        $obj->{'verbose'} = 1;
        my $text = 'Say something';
        my $expectRE = qr/^00000000: \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d - $text$/;
        {
            stdout_like { $obj->sayVerbose( $text ); } $expectRE, "Verbose output with no uuid";
        }
    }

    # Output with Bad Uuid tag
    {
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_fastqUploadUuid'} = "1234";
        $obj->{'verbose'} = 1;
        my $text = 'Say something';
        my $expectRE = qr/^00000000: \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d - $text$/;
        {
            stdout_like { $obj->sayVerbose( $text ); } $expectRE, "Verbose output with bad uuid";
        }
    }

    # Output with Bad message
    {
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_fastqUploadUuid'} = "1234";
        $obj->{'verbose'} = 1;
        my $text = 'Say something';
        my $expectRE = qr/^00000000: \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d - $text$/;
        {
            stdout_like { $obj->sayVerbose( $text ); } $expectRE, "Verbose output with bad uuid";
        }
    }

}

sub testGetAll {
	plan( tests => 2 );
    {
        my $got = $OBJ->getAll();
        my $want = $OPT_HR;
        $want->{'myName'}  = 'upload-cghub-fastq_0.0.1';
        $want->{'error'}   = undef,

        is_deeply( $got, $want, "Get everything expected");
    }
    {
        my $got1 = $OBJ->getAll();
        my $got2 = $OBJ->getAll();
        $got2->{"runMode"} = "OOPS";
        isnt( $got1->{"runMode"}, $got2->{"runMode"}, "Retrieves separate hashs");
    }
}

sub testRun {

	plan( tests => 5 );
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run() };
       my $got = $@;
       my $want = qr/^Can\'t run unless specify a runMode\./;
       like( $got, $want, "error if runMode undefined");
    }
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run( [1,2] ) };
       my $got = $@;
       my $want = qr/^Can\'t run unless specify a runMode\./;
       like( $got, $want, "error if runMode is hash");
    }
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run( "", $MOCK_DBH ) };
       my $got = $@;
       my $want = qr/^Illegal runMode \"\" specified\./;
       like( $got, $want, "error if runMode is empty string");
    }
    {
	   my $obj = $CLASS->new( {} );
       $obj->{'dbh'} = $MOCK_DBH;
	   eval{ $obj->run( "BOB" ) };
       my $got = $@;
       my $want = qr/^Illegal runMode \"BOB\" specified\./;
       like( $got, $want, "error if runMode is unknown");
    }
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run( "BOB" ) };
       my $got = $@;
       my $want = qr/^Failed to connect to the database/;
       like( $got, $want, "error if no dbh provided and can't be created from input");
    }
}
