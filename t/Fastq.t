#! /usr/bin/env perl

use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages

use Bio::SeqWare::Config; # Access SeqWare settings file as options

use Test::More 'tests' => 1 + 6;   # Main testing module; run this many subtests
                                   # in BEGIN + subtests (subroutines).

BEGIN {
	use_ok( 'Bio::SeqWare::Uploads::CgHub::Fastq' );
}

if ( ! $ENV{'DB_TESTING'} ) {
	diag( 'skipping 1 test that requires DB_TESTING' );
}

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';
my $opt = Bio::SeqWare::Config->new()->getKnown();

$OPT_HR = { %$opt,
    'runMode' => 'alL',
};

my $OBJ = $CLASS->new( $OPT_HR );

subtest( 'new()' => \&testNew );
subtest( 'new(BAD)' => \&testNewBad );
subtest( 'getAll()' => \&testGetAll );
subtest( 'run()' => \&testRun );
subtest( 'doZip()' => \&testDoZip );
subtest( '_tagLaneForZipping()' => \&test_TagLaneForZipping );

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

sub testDoZip {
	plan( tests => 1 );
	{
	    pass("testDoZip not implemented");
    }
}

sub test_TagLaneForZipping {
    if ( $ENV{'DB_TESTING'} ) {
	    plan( tests => 4 );
    }
    else {
        plan( skip_all => 'Skip: requires DB_TESTING' );
    }
    my $configObj = Bio::SeqWare::Config->new();
    my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $configObj );
    my $dbh = $connectionBuilder->getConnection( {'RaiseError' => 1, 'AutoCommit' => 1} );

    # Do record insert!
    {
        my $result = $OBJ->_tagLaneforZipping( $dbh );
        if (! defined $result) {
            fail("Failed to tag for zip")
        } 
        elsif ($result == 0) {
            diag("NOTHING TO ZIP");
            pass("Failed to tag for zip")
        }
        else {
            ok( $result, "reported successful tagging" );
        }
    }
    {
        ok( $OBJ->{'_zipUploadId'}, "Inserted an upload record");
    }

    # Retrieve record inserted to verify correct
    my $selectSQL =
        "SELECT * from upload WHERE upload_id = $OBJ->{_zipUploadId}";
    my $selectSTH = $dbh->prepare($selectSQL)
            or die $dbh->errstr();
    $selectSTH->execute()
            or die $selectSTH->errstr();
    $row_HR = $selectSTH->fetchrow_hashref();
    if (! defined $row_HR) {
        die $selectSTH->errstr();
    }
    {
        is($row_HR->{'target'}, 'CGHUB_FASTQ', "Correct target inserted" );
    }
    {
        is($row_HR->{'status'}, 'zip_candidate', "Correct status inserted" );
    }

    # Cleanup inserted record
    $selectSTH->finish();
    $dbh->begin_work()
            or die $dbh->errstr();  # Autocommit should be on.
    my $deleteSQL =
        "DELETE from upload WHERE upload_id = $OBJ->{_zipUploadId}";
    my $deleteSTH = $dbh->prepare($deleteSQL)
            or die $dbh->errstr();
    $deleteSTH->execute()
            or die $selectSTH->errstr();
    my $rowsAffected = $deleteSTH->rows();
    if (! defined $rowsAffected || $rowsAffected != 1) {
        $dbh->rollback();
        die "Failed to delete test record - need to fix manually\n"
                . Dumper($OBJ);
    }
    $dbh->commit();
    $deleteSTH->finish();
    $dbh->disconnect();
}
