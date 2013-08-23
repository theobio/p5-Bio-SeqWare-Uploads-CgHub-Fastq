#! /usr/bin/env perl

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Temp;           # Simple files for testing

use Bio::SeqWare::Config; # Access SeqWare settings file as options
use Bio::SeqWare::Db::Connection;

use DBD::Mock;
use Test::More 'tests' => 1 + 5;   # Main testing module; run this many subtests
                                     # in BEGIN + subtests (subroutines).

BEGIN {
	use_ok( 'Bio::SeqWare::Uploads::CgHub::Fastq' );
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
    { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1 },
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

subtest( 'new()' => \&testNew );
subtest( 'new(BAD)' => \&testNewBad );
subtest( 'getAll()' => \&testGetAll );
subtest( 'run()' => \&testRun );
subtest( 'runNotImplemented()' => \&testRunNotImplemented );

$MOCK_DBH->disconnect();

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

sub testRunNotImplemented {
	plan( tests => 2 );
	{
	   my $obj = $CLASS->new( {} );
       $obj->{'dbh'} = $MOCK_DBH;
	   eval{ $obj->run( "VALIDATE" ) };
       my $got = $@;
       my $want = qr/^doValidate\(\) not implemented/;
       like( $got, $want, "error if runMode is VALIDATE");
    }
	{
	   my $obj = $CLASS->new( {} );
       $obj->{'dbh'} = $MOCK_DBH;
	   eval{ $obj->run( "UPLOAD" ) };
       my $got = $@;
       my $want = qr/^doUpload\(\) not implemented/;
       like( $got, $want, "error if runMode is UPLOAD");
    }
}