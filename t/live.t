use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Temp;           # Simple files for testing
use IO::File;             # Reading data from text files

use Bio::SeqWare::Config; # Read the seqware config file
use DBD::Mock;
use Test::More 'tests' => 7;    # Run this many Test::More compliant subtests

use lib 't';
use Test::Utils qw( error_tag_ok
    dbMockStep_Begin    dbMockStep_Commit
    dbMockStep_Rollback dbMockStep_SetTransactionLevel
);

use Bio::SeqWare::Uploads::CgHub::Fastq;

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $SAMPLE_DOWLOADED_XML_FILE = File::Spec->catfile( $DATA_DIR, 'sampleAnalysisQuery.xml');
my $MOCK_XML_RETURN_STRING = 

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

sub mock_goodPullXmlFromUrl {
    my $self = shift;

    my $fh = new IO::File "$SAMPLE_DOWLOADED_XML_FILE", "r";
    if (! $fh) {
        die "Unable to read test data file \"$SAMPLE_DOWLOADED_XML_FILE\": $!";
    }
    return join("", <$fh>);
    # Close file explicitly
}

sub mock_badPullXmlFromUrl { return undef; }

sub mock__live_undef { return undef; }
sub mock__live_live { return 'live'; }
sub mock__live_recheck { return 'recheck-waiting'; }

#
# TESTING
#

subtest( '_makeCghubAnalysisQueryUrl()' => \&test__makeCghubAnalysisQueryUrl );
subtest( '_pullXmlFromUrl()'            => \&test__pullXmlFromUrl );
subtest( '_xmlToHashRef()'              => \&test__xmlToHashRef );
subtest( '_evaluateExternalStatus()'    => \&test__evaluateExternalStatus );
subtest( '_statusFromExternal()'        => \&test__statusFromExternal );

subtest( 'doLive()' => \&test_doLive );
subtest( '_live()'  => \&test__live );

sub test__makeCghubAnalysisQueryUrl {

    plan( tests => 3 );

    my $obj = $CLASS->new( $OPT_HR );

    my $uploadHR->{'cghub_analysis_id'} = '21912089-1e42-4bcc-9ad9-fe9a9b88fb09';
    {
        # This url verified online 
        my $want = 'https://cghub.ucsc.edu/cghub/metadata/analysisAttributes?analysis_id=21912089-1e42-4bcc-9ad9-fe9a9b88fb09';
        my $got = $obj->_makeCghubAnalysisQueryUrl( $uploadHR );
        is($got, $want, "Assemble correct url");
    }

    # Bad param: $uploadHR
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_makeCghubAnalysisQueryUrl();
        };
        {
          like( $@, qr/^_makeCghubAnalysisQueryUrl\(\) missing \$uploadHR parameter\./, "Error if no uploadHR param");
          is( $obj->{'error'}, 'param__makeCghubAnalysisQueryUrl_uploadHR', "Errror tag if no uploadHR param");
        }
    }

}

sub test__pullXmlFromUrl {
 
    plan( tests => 3 );

    # Bad param: $queryUrl
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_pullXmlFromUrl();
        };
        {
          like( $@, qr/^_pullXmlFromUrl\(\) missing \$queryUrl parameter\./, "Error if no queryUrl param");
          is( $obj->{'error'}, 'param__pullXmlFromUrl_queryUrl', "Errror tag if no queryUrl param");
        }
    }

    # Nothing retrieved
    {
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $got = $obj->_pullXmlFromUrl( 'ht://' );
            isnt( $got, "Nothing retrieved" );
        }
    }

}

sub test__xmlToHashRef {
 
    plan( tests => 8 );

    {
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_pullXmlFromUrl = \&mock_goodPullXmlFromUrl;
        my $obj = $CLASS->new( $OPT_HR );
        my $xmlString = $obj->_pullXmlFromUrl();
        my $xmlHR = $obj->_xmlToHashRef( $xmlString );
        {
              is( scalar (keys $xmlHR) , 5, "Found top level keys" );
              is( $xmlHR->{'Hits'} , 1, "Found hit count value" );
              is( scalar (keys $xmlHR->{'ResultSummary'}) , 3, "Found ResultSummary keys" );
              is( $xmlHR->{'ResultSummary'}->{'state_count'}->{'live'}, 1, "Found live count value" );
        }
    }


    # Bad xml parsing
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_xmlToHashRef( "NOT XML STRING" );
        };
        {
          like( $@, qr/^Uncaught error parsing retrieved cghub analysis xml\; error was/, "Error if parsing dies");
          is( $obj->{'error'}, 'cghub_xml_parsing', "Error tag if parsing dies");
        }

    }

    # Bad param: $cgHubXml
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_xmlToHashRef();
        };
        {
          like( $@, qr/^_xmlToHashRef\(\) missing \$cgHubXml parameter\./, "Error if no cgHubXml param");
          is( $obj->{'error'}, 'param__xmlToHashRef_cgHubXml', "Error tag if no cgHubXml param");
        }
    }

}

sub test__evaluateExternalStatus {
 
    plan( tests => 7 );

    # external status from xml with no top level <Hits> element.
    {
        my $obj = $CLASS->new( $OPT_HR );
        my $parsedXmlHR->{'ResultSummary'}->{'state_count'}->{'live'} = 1;
        {
            my $want = 'bad-hit-undef';
            my $got = $obj->_evaluateExternalStatus( $parsedXmlHR );
            is($got, $want, "Undefined hit count");
        }
    }

    # external status from xml with no live-count element.
    {
        my $obj = $CLASS->new( $OPT_HR );
        my $parsedXmlHR->{'Hits'} = 1;
        {
            my $want = 'bad-live-count-undef';
            my $got = $obj->_evaluateExternalStatus( $parsedXmlHR );
            is($got, $want, "Undefined live count");
        }
    }

    # external status from xml with bad value for top level <Hits> element.
    {
        my $obj = $CLASS->new( $OPT_HR );
        my $parsedXmlHR->{'Hits'} = 2;
        $parsedXmlHR->{'ResultSummary'}->{'state_count'}->{'live'} = 1;
        {
            my $want = 'bad-hit-2';
            my $got = $obj->_evaluateExternalStatus( $parsedXmlHR );
            is($got, $want, "Invalid value for hit count");
        }
    }

    # external status from xml with bad value for live count.
    {
        my $obj = $CLASS->new( $OPT_HR );
        my $parsedXmlHR->{'Hits'} = 1;
        $parsedXmlHR->{'ResultSummary'}->{'state_count'}->{'live'} = 2;
        {
            my $want = 'bad-live-count-2';
            my $got = $obj->_evaluateExternalStatus( $parsedXmlHR );
            is($got, $want, "Invalid value for live count");
        }
    }

    # Good run.
    {
        my $obj = $CLASS->new( $OPT_HR );
        my $parsedXmlHR->{'Hits'} = 1;
        $parsedXmlHR->{'ResultSummary'}->{'state_count'}->{'live'} = 1;
        {
            my $want = 'live';
            my $got = $obj->_evaluateExternalStatus( $parsedXmlHR );
            is($got, $want, "Invalid value for live count");
        }
    }

    # Bad param: $dataHR
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_evaluateExternalStatus();
        };
        {
          like( $@, qr/^_evaluateExternalStatus\(\) missing \$dataHR parameter\./, "Error if no dataHR param");
          is( $obj->{'error'}, 'param__evaluateExternalStatus_dataHR', "Errror tag if no dataHR param");
        }
    }

}

sub test__statusFromExternal {
 
    plan( tests => 5 );

    # Bad param: $externalStatus
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_statusFromExternal();
        };
        {
          like( $@, qr/^_statusFromExternal\(\) missing \$externalStatus parameter\./, "Error if no externalStatus param");
          is( $obj->{'error'}, 'param__statusFromExternal_externalStatus', "Errror tag if no externalStatus param");
        }
    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        is( $obj->_statusFromExternal('live'), 'live_completed', "status given live" );
        is( $obj->_statusFromExternal('recheck-waiting'), 'live_waiting', "status given waiting" );
        is( $obj->_statusFromExternal('other'), 'failed_live_other', "status given odd return" );
    }
}

sub test_doLive {
 
    plan( tests => 6 );

    my $oldStatus = "submit-fastq_completed";
    my $newStatus = "live_running";
    my $finalStatus = "live_completed";
    my $externalStatus = "live";
    my $sampleId    = -19;

    my $uploadId       = -21;
    my $uploadUuid     = "21912089-1e42-4bcc-9ad9-fe9a9b88fb09";

    my @dbSessionGood = (
        dbMockStep_Begin(),
        dbMockStep_SetTransactionLevel(),
        {
            'statement'    => qr/SELECT u\.\*/msi,
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
        },
        dbMockStep_Commit(),
    );

    my @dbSessionEmpty = (
        dbMockStep_Begin(),
        dbMockStep_SetTransactionLevel(),
        {
            'statement'    => qr/SELECT u\.\*/msi,
            'bound_params' => [ $oldStatus ],
            'results'  => [[]]
        }, {
           'statement' => 'COMMIT',
            'results'  => [[]],
        },
        dbMockStep_Commit(),
    );

    # Test ok when doLive finds sample to run.
    {
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_live = \&mock__live_live;
        my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSessionGood );
        {
            ok( $obj->doLive( $MOCK_DBH ), "smoke check");
        }
    }

    # Test ok when doLive does not find a sample to run.
    {
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_live = \&mock__live_undef;
        my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSessionEmpty );
        {
            ok( $obj->doLive( $MOCK_DBH ), "nothing to do is ok");
        }
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
    # Test ok when doLive does not find a sample to run.
    {
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_live = \&mock__live_undef;
        my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( @dbSessionGood );
        eval {
             $obj->doLive( $MOCK_DBH );
        };
        {
          like( $@, qr/^_statusFromExternal\(\) missing \$externalStatus parameter\./, "Error if bad externalStatus");
          is( $obj->{'error'}, 'failed_live_param__statusFromExternal_externalStatus', "Errror tag if bad externalStatus");
        }
    }

}

sub test__live {
    my $analysisUuid = shift;

    plan( tests => 4 );

    {
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_pullXmlFromUrl = \&mock_goodPullXmlFromUrl;
        my $uploadHR->{'cghub_analysis_id'} = '21912089-1e42-4bcc-9ad9-fe9a9b88fb09';
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $want = 'live';
            my $got = $obj->_live( $uploadHR );
            is ($got, $want, 'Correct handling using mock (good) xml return')
        }
    }

    {
        no warnings 'redefine';
        local *Bio::SeqWare::Uploads::CgHub::Fastq::_pullXmlFromUrl = \&mock_badPullXmlFromUrl;
        my $uploadHR->{'cghub_analysis_id'} = '21912089-1e42-4bcc-9ad9-fe9a9b88fb09';
        my $obj = $CLASS->new( $OPT_HR );
        {
            my $want = 'recheck-waiting';
            my $got = $obj->_live( $uploadHR );
            is ($got, $want, 'Correct handling using mock (bad) xml return')
        }
    }

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