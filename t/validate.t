use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages

use Cwd;                          # Get current working directory.
use File::ShareDir qw(dist_dir);  # Access data files from install.
use File::Temp;           # Temporary directory and contents, auto-removed
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

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $DATA_DIR = File::Spec->catdir( "t", "Data" );
my $TEMP_DIR = File::Temp->newdir();  # Auto-delete self and contents when out of scope
my $CONFIG = Bio::SeqWare::Config->new();

my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode'            => 'meta',
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

subtest( 'doValidate()'    => \&test_doValidate   );
subtest( '_validateMeta()' => \&test__validateMeta );

sub test_doValidate {
    plan( tests => 5 );

    my $oldStatus = "meta_completed";
    my $newStatus = "validate_running";
    my $finalStatus = "validate_completed";
    my $sampleId    = -21;

    my $uploadId       = 7851;
    my $uploadUuid     = "notReallyTheFastqUploadUuid";

    my $uploadDir = File::Spec->catdir( "$TEMP_DIR", $uploadUuid );
    mkdir($uploadDir);

    my $fakeValidMessage = "Fake (GOOD) Validate Return\n"
                          . "Metadata Validation Succeeded.\n";

    my @dbSession = (
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
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( "doValidateOk", @dbSession );
	    {
            my $shows = "doValidate returns 1 when succesful";
            my $got = $obj->doValidate( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doValidate();
        };
        {
          like( $@, qr/^doValidate\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'failed_validate_param_doValidate_dbh', "Errror tag if no dbh param");
        }
    }

    # Error propagation on error.
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doValidate( $MOCK_DBH );
        };
        {
          like( $@, qr/^Error changing upload status from meta_completed to validate_running/, "Error propogates out");
          is( $obj->{'error'}, 'failed_validate_status_change_meta_completed_to_validate_running', "Errror tag propogates out");
        }
    }

}

sub test__validateMeta {

    plan( tests => 15 );
    my $fakeValidMessage = "Fake (GOOD) Validate Return\n"
                          . "Metadata Validation Succeeded.\n";
    my $fakeNotValidMessage   = "Fake (BAD) Validate Return\n"
                          . "Error: oops.\n";
    my $uploadHR = {
        'metadata_dir' => "t",
        'cghub_analysis_id' => 'Data',
    };
    my $opts_HR = { %$OPT_HR, '_cgSubmitExecutable' => '/usr/bin/cgsubmit' };

    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $fakeValidMessage;
	    my $obj = $CLASS->new( $opts_HR );
	    {
            my $shows = "Success when returns valid message";
            my $got = $obj->_validateMeta( $uploadHR );
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
            $obj->_validateMeta( $uploadHR );
            my $got = getcwd();
            is( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'exit'} = 1;
        $mock_readpipe->{'ret'} = $fakeNotValidMessage;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_validateMeta( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error describes exit value when command exits with error and ret value";
            my $want = qr/Validation error: exited with error value \"$mock_readpipe->{'exit'}\"\./;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes returned value when command exits with error and ret value";
            my $escapedMessage = quotemeta( $fakeNotValidMessage );
            my $want = qr/Output was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command when command exits with error and ret value";
            my $escapedMessage = quotemeta( $obj->{'_cgSubmitExecutable'} );
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
            $retval = $obj->_validateMeta( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error describes exit value when command exits with error but no ret value";
            my $want = qr/Validation error: exited with error value \"$mock_readpipe->{'exit'}\"\./;
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
            my $escapedMessage = quotemeta( $obj->{'_cgSubmitExecutable'} );
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
            $retval = $obj->_validateMeta( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error if no error but no return text.";
            my $want = qr/Validation error: neither error nor result generated\. Strange\./;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command (no error, but no return)";
            my $escapedMessage = quotemeta( $obj->{'_cgSubmitExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = $fakeNotValidMessage;
	    my $obj = $CLASS->new( $opts_HR );
        my $retval;
        eval {
            $retval = $obj->_validateMeta( $uploadHR );
        };
        my $error = $@;
	    {
            my $shows = "Error if return value indicates invalid";
            my $want = qr/Validation error: Apparently failed to validate\..{1,2}/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes return (invalid return)";
            my $escapedMessage = quotemeta( $fakeNotValidMessage );
            my $want = qr/Actual validation result was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
	    {
            my $shows = "Error describes command (invalid returnn)";
            my $escapedMessage = quotemeta( $obj->{'_cgSubmitExecutable'} );
            my $want = qr/Original command was:.{1,2}$escapedMessage/s;
            my $got = $error;
            like( $got, $want, $shows);
	    }
        $mock_readpipe->{'mock'} = 0;
    }

    # Bad input handling
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_validateMeta( undef );
        };
        my $error = $@;
        {
            my $got = $error;
            my $want = qr/_validateMeta\(\) missing \$uploadHR parameter\./;
            like( $got, $want, "Error if no uploadHR param");
        }
        {
            my $got = $obj->{'error'};
            my $want = 'param_validateMeta_uploadHR';
            is( $got, $want, "Errror tag if no dbh param");
        }
    }
}