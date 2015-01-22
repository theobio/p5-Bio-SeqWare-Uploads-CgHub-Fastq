#!/usr/bin/env perl

use strict;
use warnings;

use Data::Dumper;   # Easy stringification of objects.

# Needed for testing
use DBD::Mock;                   # Fake database results
use Test::MockModule;            # Fake one module while testing another.
use Test::MockObject::Extends;   # Fake one method while testing another.
use Test::Output;                # Test what appears on stdout.
use Test::Exception;             # Test exception handling code.

use Test::More 'tests' => 1;     # Main testing module; run this many subtests

# Code needed only for testing.
use lib 't';
use Test::Utils qw( dbMockStep_Begin dbMockStep_Commit );


# Class to test
use Bio::SeqWare::Uploads::CgHub::Bam;

# Convient shortcut for refereing to class being tested.
my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Bam';


# Class method tests
# subtest( 'new()'     => \&testNew     );

# Instance methods
# subtest( 'DESTROY()' => \&testDESTROY );
# subtest( 'sayVerbose()' => \&testSayVerbose );
# subtest( 'test_defaultIfUndef()' => \&testdefaultIfUndef );
subtest( '_withError() and _getExceptionName()' => \&testWithErrorAndGetExceptionName );


# subtest( 'testMockRunNothingToDo'   => \&testMockRunNothingToDo );

# subtest( 'run()'        => \&testRun        );
# subtest( 'testRunBadParameters()'  => \&testRunBadParameters );
# subtest( 'run()-BadSelectWorkflowRun' => \&testRunBadSelectWorkflowRun );

# Internal methods
# subtest( '_dbh()' => \&test_dbh );
# subtest( '_parameterDefinedOrCroak()' => \&test_parameterDefinedOrCroak );
# subtest( '_updateUploadStatus()' => \&test__updateUploadStatus );

sub testNew {

    plan( tests => 4 );

    # Need fake database connection handle.
    my $dbh = makeMockDbh();

    {
        my $failMessage = "Create with minimal parameters";
        my $OPT_HR = { 'dbh' => $dbh };
        ok( 'Bio::SeqWare::Uploads::CgHub::Bam'->new( $OPT_HR ), $failMessage);
    }
    {
        my $failMessage = "Create with real parameters, except \$dbh";
        my $CONFIG = Bio::SeqWare::Config->new();
        my $OPT = $CONFIG->getKnown();
        my $OPT_HR = {
            %$OPT,
            'dbh' => $dbh,
        };
        ok( 'Bio::SeqWare::Uploads::CgHub::Bam'->new( $OPT_HR ), $failMessage);
    }
    {
       my $error1 = 'ClassInitalizationException: Failed to init class Bio::SeqWare::Uploads::CgHub::Bam\.';
       my $error2 = 'Error was: BadParameterException: Parameter "\$paramHR" has invalid value "<undef>"\.';
       my $expectError = qr/^$error1\n$error2\n\s+at.*/s;
       throws_ok( sub {$CLASS->new();}, $expectError );
    }
    {
       my $error1 = 'ClassInitalizationException: Failed to init class Bio::SeqWare::Uploads::CgHub::Bam\.';
       my $error2 = 'Error was: BadParameterException: Parameter "\$paramHR" has invalid value "BAD_PARAM"\.';
       my $expectError = qr/^$error1\n$error2\n\s+at.*/s;
       throws_ok( sub {$CLASS->new("BAD_PARAM");}, $expectError );
    }
}

sub test_init {

}

sub testDESTROY {
    plan( tests => 6 );
    {
        my $message = "DESTROY dbh - mine + closed";
        my $obj;
        ok( $obj = makeBamObj(), $message);
        $obj->{'_isMyDbh'} = 1;
        $obj->{'_dbh'}->disconnect();
        $obj->{'_dbh'}->{mock_can_connect} = 0;
        undef $obj;  # Invokes DESTROY.
    }
    {
        my $message = "DESTROY dbh - not mine + closed";
        my $obj;
        ok( $obj = makeBamObj(), $message);
        $obj->{'_isMyDbh'} = 0;
        $obj->{'_dbh'}->disconnect();
        $obj->{'_dbh'}->{mock_can_connect} = 0;
        undef $obj;  # Invokes DESTROY.
    }
    {
        my $message = "DESTROY dbh - mine + open";
        my $obj;
        ok( $obj = makeBamObj(), $message);
        $obj->{'_isMyDbh'} = 1;
        undef $obj;  # Invokes DESTROY.
    }
    {
        my $message = "DESTROY dbh - not mine + open";
        my $obj;
        ok( $obj = makeBamObj(), $message);
        $obj->{'_isMyDbh'} = 0;
        undef $obj;  # Invokes DESTROY.
    }
    {
        my $message = "DESTROY dbh - mine + open + transaction";
        my $obj;
        ok( $obj = makeBamObj(), $message);
        $obj->{'_isMyDbh'} = 1;
        $obj->{'_dbh'}->begin_work();
        undef $obj;  # Invokes DESTROY.
    }
    {
        my $message = "DESTROY dbh - not mine + open + transaction";
        my $obj;
        ok( $obj = makeBamObj(), $message);
        $obj->{'_isMyDbh'} = 0;
        $obj->{'_dbh'}->begin_work();
        undef $obj;  # Invokes DESTROY.
    }
}

sub testWithErrorAndGetExceptionName {
    plan( tests => 2 );

    my $obj = makeBamObj();
    {
        my $message = "Exception, no detail, no trace";
        my $text1 = 'CrashedException: Aborting\. Cleanup is probably needed\.';
        my $expect = qr/^$text1$/;
        my $got = $obj->_withError ('CrashedException');
        like( $got, $expect, $message );
        is( 'CrashedException', $obj->_getExceptionName($got), "Name for: $message" );
    
    }
#        {
#            my $message = "BadParameterException string";
#            my $expect = qr/^BadParameterException: Parameter "theParam" has invalid value "<undef>"\.\n$/s;
#            my $got = $obj->_withError (
#                    'BadParameterException',
#                    { paramName => 'theParam', paramValue => undef }
#            );
#            like( $got, $expect, $message );
#        }
#        {
#            my $message = "UnknownStepException string";
#            my $expect = qr/^UnknownStepException: No such step "BAD_STEP". Note: match is case sensitive\.$/s;
#            my $got = $obj->_withError (
#                    'UnknownStepException',
#                    { step => 'BAD_STEP' }
#            );
#            like( $got, $expect, $message );
#            is( 'UnknownStepException', $obj->_getExceptionName($got), $message . " name" );
#        }
#        {
#            my $message = "UnknownStepException string";
#            my $expect = qr/^UnknownStepException: No such step "<undef>". Note: match is case sensitive\.$/s;
#            my $got = $obj->_withError (
#                    'UnknownStepException',
#                    { step => undef }
#            );
#            like( $got, $expect, $message );
#        }
#        {
#            my $message = "CrashedException no runHR string";
#            my $line1 = 'CrashedException: Aborting\. Cleanup is probably needed\.';
#            my $line2 = '\(This occured in the context of another exception: OopsException: Oops\.\)';
#            my $expect = qr/^$line1\n$line2$/s;
#            my $got = $obj->_withError (
#                    'CrashedException',
#                    { exception => 'OopsException: Oops.' }
#            );
#            like( $got, $expect, $message );
#            is( 'CrashedException', $obj->_getExceptionName($got), $message . " name" );
#        }
#        {
#            my $message = "RunStatusChangeException with no details";
#            my $expect = qr/^RunStatusChangeException: Failed changing run state to \"failed\". Cleanup is probably needed\.$/s;
#            my $got = $obj->_withError ( 'RunStatusChangeException',
#                    { toState => 'failed' });
#            like( $got, $expect, $message );
#            is( 'RunStatusChangeException', $obj->_getExceptionName($got), $message . " name" );
#        }
#        {
#            my $message = "ClassInitalizationException";
#            my $expect = qr/^ClassInitalizationException: Failed to init class My::Class\.\nError was: Oops$/s;
#            my $got = $obj->_withError ( 'ClassInitalizationException',
#                    { 'class' => 'My::Class', 'caught' => "Oops\n\n\n" });
#            like( $got, $expect, $message );
#            is( 'ClassInitalizationException', $obj->_getExceptionName($got), $message . " name" );
#        }
#        {
#            my $message = "Generic exception";
#            my $expect = qr/^SomeException: Some error.\n$/s;
#            my $got = $obj->_withError ( 'SomeException', "Some error." );
#            like( $got, $expect, $message );
#            is( 'SomeException', $obj->_getExceptionName($got), $message . " name" );
#        }
#        {
#            is( $obj->_getExceptionName("Not an Exception"), undef, "Unknown exception name is undef")
#        }
#    }
#    {
#        # Mocking a value for getRunDescription in errors reporting runHR.
#        my $obj = makeBamObj();
#        my $cyberObj = Test::MockObject::Extends->new( $obj );
#        $cyberObj->mock( 'getRunDescription', sub { return "Mocked run info"; } );
#        my $runHR = { shouldNotLookInsideHere => "runInfo" };
#        {
#            my $message = "CrashedException with runHR string";
#            my $expect = qr/^CrashedException: Crashed. Cleanup is probably needed.\nDied while running Mocked run info\nError was: Oops$/s;
#            my $got = $cyberObj->_withError ( 'CrashedException',
#                    { caught => 'Oops', runHR  => $runHR });
#            like( $got, $expect, $message );
#        }
#        {
#            my $message = "RunStatusChangeException with runHR";
#            my $expect1 = "RunStatusChangeException: Failed changing run state to \"failed\". Cleanup is probably needed.";
#            my $expect2 = "Trying to change run: Mocked run info";
#            my $expect = qr/$expect1\n$expect2$/s;
#            my $got = $obj->_withError ( 'RunStatusChangeException',
#                    { toState => 'failed', runHR  => $runHR  });
#            like( $got, $expect, $message );
#        }
#        {
#            my $message = "RunStatusChangeException with runHR and detail";
#            my $expect1 = "RunStatusChangeException: Failed changing run state to \"failed\". Cleanup is probably needed.";
#            my $expect2 = "Trying to change run: Mocked run info";
#            my $expect3 = "Error preventing status change was: DbException: some error...";
#            my $expect = qr/$expect1\n$expect2\n$expect3$/s;
#            my $got = $obj->_withError ( 'RunStatusChangeException',
#                    { toState => 'failed', runHR  => $runHR, detail => "DbException: some error..."  });
#            like( $got, $expect, $message );
#        }
#        {
#            my $message = "RunStatusChangeException with runHR, detail and trigger";
#            my $expect1 = "RunStatusChangeException: Failed changing run state to \"failed\". Cleanup is probably needed.";
#            my $expect2 = "Trying to change run: Mocked run info";
#            my $expect3 = "Error preventing status change was: DbException: some error...";
#            my $expect4 = "Error that could not be logged was: UnknownException: Oops!";
#            my $expect = qr/$expect1\n$expect2\n$expect3\n$expect4$/s;
#            my $got = $obj->_withError ( 'RunStatusChangeException',
#                    { toState => 'failed', runHR  => $runHR, trigger=> 'UnknownException: Oops!', detail => "DbException: some error..."  });
#            like( $got, $expect, $message );
#        }
#    }
}

sub testMockRunNothingToDo {
    plan( tests => 1 );

    {
       my $elem1 = 'CrashedException: Aborted without recording failure\. Cleanup is probably needed\.';
       my $elem2 = 'Abort caused by: \[RunStatusChangeException: Failed changing run state\. No run data available\.';
       my $elem3 = 'Failure driving status change: \[BadParameterException: Parameter "\$runHR" has invalid value "<undef>"\.\]';
       my $expectError = qr/^$elem1\n$elem2\n$elem3\s+at.*\]\s+at.*/s;
       my $obj = makeBamObj();
       throws_ok( sub {$obj->run();}, $expectError );
    }
#    {
#       my $elem1 = 'CrashedException\: Crashed\. Cleanup is probably needed\.';
#       my $elem2 = 'Error was: BadParameterException: Parameter "\$paramHR" has invalid value "BAD_VAL"\.';
#       my $expectError = qr/^$elem1\n$elem2\s+at.*/s;
#       my $obj = makeBamObj();
#       throws_ok( sub {$obj->run("BAD_VAL");}, $expectError );
#    }
#    {
#        # Mock the internal function _getNextStepToRun to simulate nothing to run.
#        # (signalled by returning \undef).
#        my $obj = makeBamObj();
#        my $cyberObj = Test::MockObject::Extends->new( $obj );
#        $cyberObj->mock( '_getNextRun', sub { return; } );
#
#        {
#            my $message = "Successful";
#            my $got = $cyberObj->run();
#            my $expect = 1;
#            is ($got, $expect, $message);
#        }
#        {
#            my $message = "Verbose messages can be trrned off";
#            $cyberObj->{'verbose'} = 0;
#            my $expectRE = qr/^$/;
#            stdout_like { $cyberObj->run(); } $expectRE, $message;
#        }
#        {
#            my $message = "Local verbose messages (not from subs)";
#            $obj->{'verbose'} = 1;
#            my $prefix = '00000000 \(\d+\) \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d -';
#            my $line1RE = "$prefix LOOKING FOR RUNNABLE STEP\.";
#            my $line2RE = "$prefix NOTHING TO DO\.";
#            my $expectAllRE = qr/^$line1RE\n$line2RE\n$/m;
#            stdout_like { $cyberObj->run(); } $expectAllRE, $message;
#        }
#    }
#    {
#        # Mock the internal function _getNextStepToRun to throw an error
#        my $obj = makeBamObj();
#        my $cyberObj = Test::MockObject::Extends->new( $obj );
#        $cyberObj->mock( '_getNextRun', sub { die "Not that!" } );
#
#        {
#            my $expectError = qr/^UnknownStepException: Exact spelling and case required.. With: paramName = "\$step"; paramValue = "NoSuchStep"\n/s;
#            my $obj = makeBamObj();
#            throws_ok( sub {$obj->run({ 'step' => 'NoSuchStep' });}, $expectError );
#        
#        }
#    }

}

sub testRunBadSelectWorkflowRun {
    plan( tests => 1 );

    my $obj = makeBamObj();

    # Mock the internal function _getWorkflowRun to simulate it dieing with
    # error.
    my $cyberObj = Test::MockObject::Extends->new( $obj );
    my $fauxErrorMessage = "Oops";
    $cyberObj->mock( '_selectWorkflowRun', sub { die $fauxErrorMessage; } );

    {
        my $message = "Expect error if selecting a workflowRun throws error.";
        eval {
            $cyberObj->run();
        };
        my $gotError = $@;
        my $line1RE = "Can't retrieve any workflow record\.";
        my $line2RE = "This may cause every attempt to run this workflow to fail\.";
        my $line3RE = "Error was: \t$fauxErrorMessage";
        my $matchStartRE = qr/^$line1RE\n$line2RE\n$line3RE/s;
        like ($gotError, $matchStartRE, $message);
    }
}

sub testRunBadUpdateWorkflowRun {

}

sub testRun {
    plan( tests => 8 );

    my $aRunRecord = {
        'sample_id' => 1234,
        'upload_id' => 2121,
        'uuid_id'   => '12345678-1234-1234-1234-1234567890AB',
        'step'      => 'START',
    };
    my $run_id =  'sample_id: 1234, upload_id: 2121, uuid: 12345678-1234-1234-1234-1234567890AB, step: START';


    {
       my $obj = makeBamObj();
       $obj->{'verbose'} = 1;
       my $cyberObj = Test::MockObject::Extends->new( $obj );
       my $fauxRunRecord = undef;
       $cyberObj->mock( '_selectWorkflowRun', sub { return $fauxRunRecord; } );
       my $stepVal = 'START';
       my $runParamHR = {'step' => $stepVal};
       {
           my $message = "Expect correct messages if verbose (step specifed)";
           my $prefix = '00000000 \(\d+\) \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d -';
           my $text1 = "$prefix Searching for runs ready for step \"$stepVal\"\.";
           my $text2 = "$prefix Nothing ready for step \"$stepVal\"\.";
           my $expectRE = qr/^$text1\n$text2\n$/m;
           stdout_like { $cyberObj->run($runParamHR); } $expectRE, $message;;
        }
       {
           $cyberObj->{'verbose'} = 0;
           my $message = "Expect no messages if not verbose (step specifed)";
           my $expectRE = qr/^$/;
           stdout_like { $cyberObj->run($runParamHR); } $expectRE, $message;
       }
    }

    {
        my $obj = makeBamObj();
        my $cyberObj = Test::MockObject::Extends->new( $obj );
        $cyberObj->{'verbose'} = 1;
        $cyberObj->mock( '_selectWorkflowRun', sub { return $aRunRecord; } );
        $cyberObj->mock( '_updateRunStatus', sub { die "Oops"; } );
        my $stepVal = 'START';
        my $runParamHR = {'step' => $stepVal};
        {
            my $message = "Expect error if fails to mark as running.";
            eval {
                $cyberObj->run();
            };
            my $gotError = $@;
            my $matchRE = qr/^Can't change status to running for $run_id\nThis may cause every attempt to run this workflow to fail\.\nError was: \tOops/s;
            like ($gotError, $matchRE, $message);
        }
        {
           my $message = "Expect correct messages if verbose (step specifed)";
           my $prefix = '567890AB \(\d+\) \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d -';
           my $text1 = "$prefix Searching for runs ready for step \"$stepVal\"\.";
           my $text2 = "$prefix Nothing ready for step \"$stepVal\"\.";
           my $expectRE = qr/^$text1\n$text2\n$/m;

           stdout_like { eval {$cyberObj->run($runParamHR); }; } $expectRE, $message;;
        }
        
    }

    {
        my $obj = makeBamObj();
        my $cyberObj = Test::MockObject::Extends->new( $obj );
        $cyberObj->mock( '_selectWorkflowRun', sub { return $aRunRecord; } );
        $cyberObj->mock( '_updateRunStatus', sub { return 1; } );
        $cyberObj->{_error} = 'faux';
        $cyberObj->mock( '_run', sub { die "Oops"; } );
        {
            my $message = "Expect error if fails during step run.";
            eval {
                $cyberObj->run();
            };
            my $gotError = $@;
            my $matchRE = qr/^Run failed for $run_id\nError was "faux": \tOops/s;
            like ($gotError, $matchRE, $message);
        }
    }
    {
        my $obj = makeBamObj();
        my $cyberObj = Test::MockObject::Extends->new( $obj );
        $cyberObj->mock( '_selectWorkflowRun', sub { return $aRunRecord; } );
        $cyberObj->mock(
            '_updateRunStatus',
            sub {
                my $self = shift;
                my $run = shift;
                my $status = shift;
                if ($status eq 'running') {
                    return 1;
                }
                if ($status eq 'failed') {
                    die "Oops again";
                }
            }
        );
        $cyberObj->{_error} = 'faux';
        $cyberObj->mock( '_run', sub { die "Oops"; } );
        {
            my $message = "Expect error if fails during step run and during update.";
            eval {
                $cyberObj->run();
            };
            my $gotError = $@;
            my $line1 = "Can't change status to failed for $run_id";
            my $line2 = "This may cause perpetual attempts to run this workflow\.";
            my $line3 = "Error causing run to fail was: \tOops";
            my $line4 = "Error updating status was: \tOops again";
            my $matchRE = qr/^$line1\n$line2\n$line3.*$line4/s;
            like ($gotError, $matchRE, $message);
        }
    }
    {
        my $obj = makeBamObj();
        my $cyberObj = Test::MockObject::Extends->new( $obj );
        $cyberObj->mock( '_selectWorkflowRun', sub { return $aRunRecord; } );
        $cyberObj->mock(
            '_updateRunStatus',
            sub {
                my $self = shift;
                my $run = shift;
                my $status = shift;
                if ($status eq 'running') {
                    return 1;
                }
                if ($status eq 'completed') {
                    die "Oops";
                }
            }
        );
        $cyberObj->mock( '_run', sub { return 1; } );
        {
            my $message = "Expect error if fails during step run and during update.";
            eval {
                $cyberObj->run();
            };
            my $gotError = $@;
            my $line1 = "Can't set status to comleted for $run_id";
            my $line2 = "This workflow will need to be resolved manually\.";
            my $line3 = "Error was: \tOops";
            my $matchRE = qr/^$line1\n$line2\n$line3/s;
            like ($gotError, $matchRE, $message);
        }
    }
    {
        my $obj = makeBamObj();
        my $cyberObj = Test::MockObject::Extends->new( $obj );
        $cyberObj->mock( '_selectWorkflowRun', sub { return $aRunRecord; } );
        $cyberObj->mock( '_updateRunStatus', sub { return 1; } );
        $cyberObj->mock( '_run', sub { return 1; } );
        {
            my $message = "Return 1 if run succeeds";
            my $got = $cyberObj->run();
            my $expect = 1;
            is ($got, $expect, $message);
        }
    }
    # TODO - verbose messages up to each failure point above.
    # TODO - ensure not connected if my dbh
    # TODO - ensure connected if not my dbh and was connected before
    # TODO - ensure no error after done if "worked".
}

sub testSayVerbose {
    plan( tests => 4 );

    my $prefix = '00000000 \(\d+\) \[INFO\] \d\d\d\d-\d\d-\d\d_\d\d:\d\d:\d\d -';
    my $obj = makeBamObj();

    {
        my $message = "No output when 'verbose' not set";
        my $text = 'Say something';
        my $expectRE = qr/^$/;
        stdout_like { $obj->sayVerbose( $text ); } $expectRE, "Verbose output turned off";
    }

    $obj->{'verbose'} = 1;

    {
        my $message = "Verbose output when 'verbose' is set";
        my $text = 'Say something';
        my $expectRE = qr/^$prefix $text$/;
        stdout_like { $obj->sayVerbose( $text ); } $expectRE, $message;
    }
    {
        my $message = "Default message if needed.";
        my $text = undef;
        my $expectRE = qr/^$prefix \( No message specified\. \)$/;
        stdout_like { $obj->sayVerbose( $text ); } $expectRE, $message;
    }
    {
        my $message = "New lines handled as expected";
        my $text = "Say\nsomething\n\n\n\n";
        my $textWrapped = "Say\n\tsomething\n";
        my $expectRE = qr/^$prefix $textWrapped/m;
        stdout_like { $obj->sayVerbose( $text ); } $expectRE, $message;
    }

    # Need test for line wrapping
}

sub test_defaultIfUndef {
    plan( tests => 9 );

    my $obj = makeBamObj();

    is( $obj->_defaultIfUndef( "ok",  "okToo" ), "ok",    "Case true,  true"  );
    is( $obj->_defaultIfUndef( "ok",  0       ), "ok",    "Case true,  false" );
    is( $obj->_defaultIfUndef( "ok",  undef   ), "ok",    "Case true,  undef" );
    is( $obj->_defaultIfUndef( 0,     "okToo" ), 0,       "Case false, true"  );
    is( $obj->_defaultIfUndef( 0,     0       ), 0,       "Case false, false" );
    is( $obj->_defaultIfUndef( 0,     undef   ), 0,       "Case false, undef" );
    is( $obj->_defaultIfUndef( undef, "okToo" ), "okToo", "Case undef, true"  );
    is( $obj->_defaultIfUndef( undef, 0       ), 0,       "Case undef, false" );
    is( $obj->_defaultIfUndef( undef, undef   ), undef,   "Case undef, undef" );
}

sub test_parameterDefinedOrCroak {

    plan( tests => 4 );

    {
        my $obj;
        my $aParam = "notNull";
        eval{ $obj = wrapperFor_parameterDefinedOrCroak($aParam) };
        my $error = $@;
        {
            my $message = 'Expect no error when parameter is defined.';
            is( $error, "",  $message);
        }
        {
            my $message = 'Expect no error tag when parameter is defined.';
            ok( exists $obj->{'_error'} && ! defined $obj->{'_error'}, );
        }
        {
            my $message = 'Expect checked parameter to be returned as value';
            is( $obj->{'aParam'}, $aParam, $message );
        }
    }
    {
        my $obj;
        my $aParam = undef;
        eval{ $obj = wrapperFor_parameterDefinedOrCroak($aParam) };
        my $error = $@;
        my $pattern = qr/wrapperFor_parameterDefinedOrCroak\(\) missing \$aParam parameter\./;
        like( $error, $pattern, 'Expect error when parameter undefined.');
    }
}

sub wrapperFor_parameterDefinedOrCroak {
    my $obj = makeBamObj();
    my $aParam = $obj->_parameterDefinedOrCroak( shift, "aParam" );
    $obj->{'aParam'}=$aParam;
    return $obj;
}

sub test__updateUploadStatus {

    plan ( tests => 6 );

    my $newStatus  = 'test_ignore_not-real-status';
    my $uploadId= -21;

    {
        my $obj = makeBamObj();
        $obj->{'_uploadId'} = $uploadId;

        my @dbSession = (
            dbMockStep_Begin(),
            {
                'statement'    => qr/UPDATE upload.*/msi,
                'bound_params' => [ $newStatus, $uploadId ],
                'results'  => [[ 'rows' ], []],
            },
            dbMockStep_Commit(),
        );

        my $mockDbh = makeMockDbh();
        $mockDbh->{mock_clear_history} = 1;
        $mockDbh->{'mock_session'} = DBD::Mock::Session->new( @dbSession );
        $mockDbh->{'mock_session'}->reset();
        {
            is( 1, $obj->_updateUploadStatus( $mockDbh, $newStatus ), "Updated upload status." );
        }
    }

    # Bad param: $dbh
    {
        my $obj = makeBamObj();
        eval { $obj->_updateUploadStatus(); };
        is( $obj->{'error'}, 'param__updateUploadStatus_dbh', "Error tag, no dbh param");
    }

    # Bad param: $newStatus
    {
        my $obj = makeBamObj();
        my $mockDbh = makeMockDbh();
        eval { $obj->_updateUploadStatus( $mockDbh ); };
        is( $obj->{'error'}, 'param__updateUploadStatus_newStatus', "Error tag no newStatus param");
    }

    # Bad option: $uploadId
    {
        my $obj = makeBamObj();
        delete $obj->{'_uploadId'};
        my $mockDbh = makeMockDbh();
        eval { $obj->_updateUploadStatus( $mockDbh, $newStatus); };
        is( $obj->{'error'}, 'opt__updateUploadStatus__uploadId', "Error tag no uploadId option" );
    }

    # Error propagation
    {
        my $obj = makeBamObj();
        $obj->{'_uploadId'} = $uploadId;
        my $mockDbh = makeMockDbh();
        $mockDbh->{mock_can_connect} = 0;
        eval {
             $obj->_updateUploadStatus( $mockDbh, $newStatus );
        };
        {
          like( $@, qr/^Failed to update status of upload record upload_id=$uploadId to $newStatus/, "Error propagaion");
          is( $obj->{'error'}, 'update_upload', "Error tag propagaion");
        }
        $mockDbh->{mock_can_connect} = 1;
    }
}

sub test_dbh {

     plan ( tests => 9 );
     my $mockDbh = makeMockDbh();
     my $obj = $CLASS->new({ 'dbh' => $mockDbh });
     {
         my $message = "Get dbh handle from object";
         my $want = $mockDbh;
         my $got = $obj->_dbh();
         is($want, $got, $message);
     }

     my $newMockDbh = makeMockDbh();
     {
         my $message = "Expect: Two mock dbh handles are not equal";
         isnt( $newMockDbh, $mockDbh, $message);
     }
     {
         my $message = "Expect returns old _dbh value";
         my $want = $mockDbh;
         my $got = $obj->_dbh($newMockDbh);
         is($want, $got, $message);
     }
     {
         my $message = "Expect _dbh to be changed";
         my $want = $newMockDbh;
         my $got = $obj->{'_dbh'};
         is($want, $got, $message);
     }
     {
         my $message = "Creating new dbh connection fails";
         my $badDbhObj = makeBamObj();
         $badDbhObj->{'_dbh'} = undef;
         eval {
             $badDbhObj->_dbh();
         };
         my $error = $@;
         {
             my $message = "Expect error message if fail to create new dbh";
             my $matchRE = qr/^Failed to create a new connection to the database/;
             like( $error, $matchRE, $message );
        }
        {
            my $message = "Expect error tag if fail to create new dbh";
            my $want = 'db_not_connecting';
            my $got = $badDbhObj->{'_error'};
            is( $got, $want, $message );
        }
    }
    {
         my $obj = makeBamObj();
         $obj->{'_dbh'} = undef;
         $obj->{'_isMyDbh'} = 0;
         my $module = new Test::MockModule( 'Bio::SeqWare::Db::Connection' );
         my $expectDbh = { 'class' => "Not a real dbh" };
         $module->mock( 'getConnection', sub { return $expectDbh; } );
         {
             my $message = "Expect looks like succesful new dbh";
             my $got = $obj->_dbh();
             is( $got, $expectDbh, $message );
         }
         {
             my $message = "Expect isMyDbh set";
             my $got = $obj->{'_isMyDbh'};
             my $expect = 1;
             is( $got, $expect, $message );
         }
    }
    {
         my $badDbhObj = makeBamObj();
         $badDbhObj->{'_dbh'} = undef;
         my $module = new Test::MockModule( 'Bio::SeqWare::Db::Connection' );
         $module->mock( 'getConnection', sub { return undef; } );
         eval {
             $badDbhObj->_dbh();
         };
         my $error = $@;
         {
             my $message = "Expect error if no dbh actually returned";
             my $matchRE = qr/^Failed to create a new connection to the database/;
             like( $error, $matchRE, $message );
        }
    }

}

sub makeBamObj {

    my $cofigObj = Bio::SeqWare::Config->new();
    my $configOptions = $cofigObj->getKnown();
    my $paramHR = {
        %$configOptions,
        'dbh' => makeMockDbh(),
    };

    return $CLASS->new( $paramHR );
}

sub makeMockDbh {
    my $mockDbh = DBI->connect(
        'DBI:Mock:',
        '',
        '',
        { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1, 'ShowErrorStatement' => 1 },
    );
    return $mockDbh;
}