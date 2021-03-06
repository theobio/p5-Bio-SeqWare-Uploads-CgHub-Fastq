use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages

use Cwd;                          # Get current working directory.
use File::ShareDir qw(dist_dir);  # Access data files from install.
use File::Temp;                   # Temporary directory and contents, auto-removed
                                  #    when out of scope.

use Bio::SeqWare::Config; # Read the seqware config file

use DBD::Mock;
use Test::More 'tests' => 2;    # Run this many Test::More compliant subtests

use lib 't';
use Test::Utils qw( error_tag_ok
    dbMockStep_Begin    dbMockStep_Commit
    dbMockStep_Rollback dbMockStep_SetTransactionLevel
);

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

my $CLASS    = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $TEMP_DIR = File::Temp->newdir();  # Auto-delete self and contents when out of scope
my $CONFIG   = Bio::SeqWare::Config->new();

my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode'            => 'SUBMIT_FASTQ',
    'uploadFastqBaseDir' => "$TEMP_DIR",
    'myName'             => 'DELETE_ME-upload-cghub-fastq_0.0.3',
    'rerun'              => 2,
    'xmlSchema'          => 'SRA_1-5',
    'templateBaseDir'    => dist_dir('Bio-SeqWare-Uploads-CgHub-Fastq'),
        'cghubSubmitExec'  => '/usr/bin/cgsubmit',
        'cghubUploadExec'  => '/usr/bin/gtupload',
        'cghubSubmitUrl'   => 'https://cghub.ucsc.edu/',
        'chghubSubmitCert' => "/datastore/alldata/tcga/CGHUB/Key.20140221/cghub.key",
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

subtest( '_submitFastq()'   => \&test__submitFastq   );
subtest( 'doSubmitFastq()'  => \&test_doSubmitFastq  );

sub test_doSubmitFastq {
     plan( tests => 5 );

    my $oldStatus = "submit-meta_completed";
    my $newStatus = "submit-fastq_running";
    my $finalStatus = "submit-fastq_completed";
    my $sampleId = -21;

    my $uploadId       = 7851;
    my $uploadUuid     = "notReallyTheFastqUploadUuid";

    my $uploadDir = File::Spec->catdir( "$TEMP_DIR", $uploadUuid );
    mkdir($uploadDir);

    my $fakeValidMessage = "Fake (GOOD) Submission Return\n"
                          . "100.000.\n";

    my $fakeErrorMessage = "Fake (BAD) Submission Return\n"
                          . "Error    : Your are attempting to upload to a uuid"
                          . " which already exists within the system and is not"
                          . "in the submitted or uploading state. This is not allowed.\n";

    my $fakeUnknowMessage = "Fake (UNKNOWN) Submission Return\n"
                          . "This is not the result you are looking for.\n";

    my @dbSession = (
        dbMockStep_Begin(),
        dbMockStep_SetTransactionLevel(),
        {
            'statement'    => qr/SELECT u\.\*/msi,
            'bound_params' => [ $oldStatus ],
            'results'  => [
                [ 'upload_id', 'status',   'metadata_dir', 'cghub_analysis_id', 'sample_id' ],
                [ $uploadId,   $oldStatus, $TEMP_DIR,      $uploadUuid,         $sampleId ],
            ]
        }, {
            'statement'    => qr/UPDATE upload/msi,
            'bound_params' => [ $newStatus,  $uploadId ],
            'results'  => [[ 'rows' ], []]
        },
        dbMockStep_Commit(),
        dbMockStep_Begin(),
        {
            'statement'    => qr/UPDATE upload.*/msi,
            'bound_params' => [ $finalStatus, $uploadId ],
            'results'  => [[ 'rows' ], []],
        },
        dbMockStep_Commit(),
    );

    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $fakeValidMessage;
	    my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( "doSubmitFastqOk", @dbSession );
	    {
            my $shows = "doSubmitFastq returns 1 when succesful";
            my $got = $obj->doSubmitFastq( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doSubmitFastq();
        };
        {
          like( $@, qr/^doSubmitFastq\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'failed_submit-fastq_param_doSubmitFastq_dbh', "Errror tag if no dbh param");
        }
    }

    # Error propagation.
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doSubmitFastq( $MOCK_DBH );
        };
        {
          like( $@, qr/^Error changing upload status from submit-meta_completed to submit-fastq_running/, "Error propogates out");
          is( $obj->{'error'}, 'failed_submit-fastq_status_change_submit-meta_completed_to_submit-fastq_running', "Errror tag propogates out");
        }
    }

}

sub test__submitFastq {

    plan( tests => 22 );
    my $fakeValidMessage = "Fake (GOOD) Submission Return\n"
                          . "100.000.\n";

    my $fakeKnownErrorMessage =
        "Error    : Your are attempting to upload to a uuid which already exists within the system and is not in the submitted or uploading state. This is not allowed.";

    my $fakeUnknowMessage = "Fake (UNKNOWN) Submission Return\n"
                          . "Oops.\n";

    my $uploadHR = {
        'metadata_dir' => "t",
        'cghub_analysis_id' => 'Data',
    };
    my $opts_HR = { %$OPT_HR, '_gtuploadExecutable' => '/usr/bin/gtupload' };

    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $fakeValidMessage;
	    my $obj = $CLASS->new( $opts_HR );
	    {
            my $shows = "Success when returns valid message";
            my $got = $obj->_submitFastq( $uploadHR );
            my $want = 1;
            is( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $fakeValidMessage;
	    my $obj = $CLASS->new( $opts_HR );
	    {
            my $shows = "Directory doesn't change";
            my $want = getcwd();
            $obj->_submitFastq( $uploadHR );
            my $got = getcwd();
            is( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $fakeKnownErrorMessage;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_submitFastq( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error if return known bad no error";
            my $want = qr/Submit fastq error: Already submitted\./s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes return (known bad, no error)";
            my $escapedMessage = quotemeta( $fakeKnownErrorMessage );
            my $want = qr/Actual submit fastq result was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command (known bad, no error)";
            my $escapedMessage = quotemeta( $obj->{'_gtuploadExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'exit'} = 1;
        $mock_readpipe->{'ret'} = $fakeUnknowMessage;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_submitFastq( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error describes exit value when command exits with error and ret value";
            my $want = qr/Submit fastq error: exited with error value \"$mock_readpipe->{'exit'}\"\./;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes returned value when command exits with error and ret value";
            my $escapedMessage = quotemeta( $fakeUnknowMessage );
            my $want = qr/Output was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command when command exits with error and ret value";
            my $escapedMessage = quotemeta( $obj->{'_gtuploadExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
        $mock_readpipe->{'exit'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'exit'} = 1;
        $mock_readpipe->{'ret'} = undef;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_submitFastq( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error describes exit value when command exits with error but no ret value";
            my $want = qr/Submit fastq error: exited with error value \"$mock_readpipe->{'exit'}\"\./;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes returned value when command exits with error but no ret value";
            my $want = qr/No output was generated.{1,2}/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command when command exits with error";
            my $escapedMessage = quotemeta( $obj->{'_gtuploadExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
        $mock_readpipe->{'exit'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = undef;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_submitFastq( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error if no error but no return text.";
            my $want = qr/Submit fastq error: neither error nor result generated\. Strange\./;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command (no error, but no return)";
            my $escapedMessage = quotemeta( $obj->{'_gtuploadExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $fakeUnknowMessage;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_submitFastq( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error if return value indicates invalid";
            my $want = qr/Submit fastq error: Apparently failed to submit\..{1,2}/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes return (invalid return)";
            my $escapedMessage = quotemeta( $fakeUnknowMessage );
            my $want = qr/Actual submit fastq result was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command (invalid returnn)";
            my $escapedMessage = quotemeta( $obj->{'_gtuploadExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }
        {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'exit'} = 27;
        $mock_readpipe->{'ret'} = $fakeKnownErrorMessage;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_submitFastq( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error if return known bad with error";
            my $want = qr/Submit fastq error: Already submitted\./s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error if return known bad with error";
            my $want = qr/Exited with error value \"$mock_readpipe->{'exit'}\"\./s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes return (known bad, with error)";
            my $escapedMessage = quotemeta( $fakeKnownErrorMessage );
            my $want = qr/Actual submit fastq result was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command (known bad, with error)";
            my $escapedMessage = quotemeta( $obj->{'_gtuploadExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
        $mock_readpipe->{'exit'} = 0;
    }

    # Bad input handling
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_submitFastq( undef );
        };
        my $error = $@;
        {
            my $got = $error;
            my $want = qr/_submitFastq\(\) missing \$uploadHR parameter\./;
            like( $got, $want, "Error if no uploadHR param");
        }
        {
            my $got = $obj->{'error'};
            my $want = 'param_submitFastq_uploadHR';
            is( $got, $want, "Errror tag if no uploadHR param");
        }
    }


}