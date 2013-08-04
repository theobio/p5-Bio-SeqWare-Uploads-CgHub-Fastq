#! /usr/bin/env perl

use Carp;                 # Caller-relative error messages

use Test::More 'tests' => 1 + 4;   # Main testing module; run this many subtests
                                   # in BEGIN + subtests (subroutines).

BEGIN {
	use_ok( 'Bio::SeqWare::Uploads::CgHub::Fastq' );
}

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';


my $OPT_HR = {
    'runMode' => 'alL',
};

my $OBJ = $CLASS->new( $OPT_HR );

subtest( 'new()' => \&testNew );
subtest( 'new(BAD)' => \&testNewBad );
subtest( 'getAll()' => \&testGetAll );
subtest( 'run()' => \&testRun );

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
        };
	    is_deeply($got, $want, "Default object created saftley");
	}

}

sub testNewBad {
	plan( tests => 2 );
    {
        eval{ $CLASS->new(); };
        $got = $@;
        $want = qr/^A hash-ref parameter is required\./;
        like( $got, $want, "error with no param");
    }
    {
        eval{ $CLASS->new( "BAD_PARAM"); };
        $got = $@;
        $want = qr/^A hash-ref parameter is required\./;
        like( $got, $want, "error with non hash-ref param");
    }
}

sub testGetAll {
	plan( tests => 2 );
    {
        $got = $OBJ->getAll();
        $want = $OPT_HR;
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
	plan( tests => 4 );
	{
	   my $opt = {};
	   my $obj = $CLASS->new( $opt );
	   eval{ $obj->run() };
       $got = $@;
       $want = qr/^Can\'t run unless specify a run mode\./;
       like( $got, $want, "error if runMode undefined");
    }
    {
	   eval{ $OBJ->run( [1,2] ) };
       $got = $@;
       $want = qr/^Can\'t run unless specify a run mode\./;
       like( $got, $want, "error if runMode is hash");
    }
    {
	   eval{ $OBJ->run( "" ) };
       $got = $@;
       $want = qr/^Illegal runMode of \"\" specified\./;
       like( $got, $want, "error if runMode is empty string");
    }
    {
	   eval{ $OBJ->run( "BOB" ) };
       $got = $@;
       $want = qr/^Illegal runMode of \"BOB\" specified\./;
       like( $got, $want, "error if runMode is unknown");
    }
}