use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Temp;           # Simple files for testing

use Bio::SeqWare::Config; # Read the seqware config file
use DBD::Mock;
use Test::More 'tests' => 2;    # Run this many Test::More compliant subtests

### Allow mocking of qx// commands

BEGIN {
	*CORE::GLOBAL::readpipe = \&mock_readpipe; # Must be before use
	require Bio::SeqWare::Uploads::CgHub::Fastq;
}

my $mock_readpipe = { 'mock' => 0, 'ret' => undef , 'exit' => 0 };

sub mock_readpipe {
    my $var = shift;
    my $retVal;
    if ( $mock_readpipe->{'mock'} ) {
        $retVal = $mock_readpipe->{'ret'};
        $? = $mock_readpipe->{'exit'};
    }
    else {
        $retVal = CORE::readpipe($var);
    }
    return $retVal;
}

###

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $TEMP_DIR = File::Temp->newdir();  # Auto-delete self and contents when out of scope
my $CONFIG = Bio::SeqWare::Config->new();

my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode'            => 'live',
};

my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1 },
);

#
# TESTING
#

subtest( 'doLive()' => \&test_doLive );
subtest( '_live()' => \&test__live );

sub test_doLive {
 
    plan( tests => 6
     );

    my $oldStatus = "submit-fastq_completed";
    my $newStatus = "live_running";
    my $finalStatus = "live_completed";
    my $externalStatus = "live";
    my $sampleId    = -19;

    my $uploadId       = -21;
    my $uploadUuid     = "test_uuid";

    my $mockQxRetGood = "***FIX ME***";

    my @dbSessionGood = ({
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
            [ $uploadId,   $oldStatus, $TEMP_DIR,      $uploadUuid,         $sampleId  ],
        ]
    }, {
        'statement'    => qr/UPDATE upload/msi,
        'bound_params' => [ $newStatus,  $uploadId ],
        'results'  => [[ 'rows' ], []]
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/UPDATE upload.*/msi,
        'bound_params' => [ $finalStatus, $externalStatus, $uploadId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    });

    my @dbSessionEmpty = ({
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
        'results'  => [[]],
    });

    # Test ok whem doLive finds sample to run and processes it.
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $mockQxRetGood;
	    my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'dolive good', @dbSessionGood );
	    {
            my $shows = "doLive returns 1 when succesful";
            my $got = $obj->doLive( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }

    #Test ok when doLive finds no sample to run (nothing to do).
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $mockQxRetGood;
	    my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'dolive empty', @dbSessionEmpty );
	    {
            my $shows = "doLive returns 1 when nothing to do";
            my $got = $obj->doLive( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doLive();
        };
        {
          like( $@, qr/^doLive\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'failed_live_param_doLive_dbh', "Errror tag if no dbh param");
        }
    }

    # Error propagation on error.
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doLive( $MOCK_DBH );
        };
        {
          like( $@, qr/^Error changing upload status from submit-fastq_completed to live_running/, "Error propogates out");
          is( $obj->{'error'}, 'failed_live_status_change_submit-fastq_completed_to_live_running', "Errror tag propogates out");
        }
    }

}

sub test__live {
    my $analysisUuid = shift;

    plan( tests => 2 );

    # Bad input handling
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_live( undef );
        };
        my $error = $@;
        {
            my $got = $error;
            my $want = qr/_live\(\) missing \$uploadHR parameter\./;
            like( $got, $want, "Error if no uploadHR param");
        }
        {
            my $got = $obj->{'error'};
            my $want = 'param_live_uploadHR';
            is( $got, $want, "Errror tag if no uploadHR param");
        }
    }

}