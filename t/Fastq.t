#! /usr/bin/env perl

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Temp;           # Simple files for testing

use Bio::SeqWare::Config; # Access SeqWare settings file as options
use Bio::SeqWare::Db::Connection;

use Test::More 'skip_all' => 'Skip: temporaryily disabled.';

# use Test::More 'tests' => 1 + 8;   # Main testing module; run this many subtests
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

my $DBH;
if ( ! $ENV{'DB_TESTING'} ) {
	diag( 'skipping 2 test that requires DB_TESTING' );
}
else {
    my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $CONFIG );
    $DBH = $connectionBuilder->getConnection( {'RaiseError' => 1, 'AutoCommit' => 1} );
}

subtest( 'new()' => \&testNew );
subtest( 'new(BAD)' => \&testNewBad );
subtest( 'getAll()' => \&testGetAll );
subtest( 'run()' => \&testRun );
subtest( 'doZip()' => \&testDoZip );
subtest( '_tagLaneForZippingAnd_updateUploadStatus()' => \&test_TagLaneForZippingAndTest_UpdateUploadStatus );
subtest( '_getFilesToZip()' => \&test_getFilesToZip );
subtest( '_zip()' => \&test_zip );

if ($DBH) {
    $DBH->disconnect();
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
       my $got = $@;
       my $want = qr/^Can\'t run unless specify a run mode\./;
       like( $got, $want, "error if runMode undefined");
    }
    {
	   eval{ $OBJ->run( [1,2] ) };
       my $got = $@;
       my $want = qr/^Can\'t run unless specify a run mode\./;
       like( $got, $want, "error if runMode is hash");
    }
    {
	   eval{ $OBJ->run( "" ) };
       my $got = $@;
       my $want = qr/^Illegal runMode of \"\" specified\./;
       like( $got, $want, "error if runMode is empty string");
    }
    {
	   eval{ $OBJ->run( "BOB" ) };
       my $got = $@;
       my $want = qr/^Illegal runMode of \"BOB\" specified\./;
       like( $got, $want, "error if runMode is unknown");
    }
}

sub testDoZip {
	plan( tests => 1 );
	{
	    pass("testDoZip not implemented");
    }
}

sub test_TagLaneForZippingAndTest_UpdateUploadStatus {
    if ( $ENV{'DB_TESTING'} ) {
	    plan( tests => 6 );
    }
    else {
        plan( skip_all => 'Skip: requires DB_TESTING' );
    }

    # Do record insert!
    {
        my $result = $OBJ->_tagLaneforZipping( $DBH );
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
    my $selectSTH = $DBH->prepare($selectSQL)
            or die $DBH->errstr();
    $selectSTH->execute()
            or die $selectSTH->errstr();
    my $row_HR = $selectSTH->fetchrow_hashref();
    if (! defined $row_HR) {
        die $selectSTH->errstr();
    }
    {
        is($row_HR->{'target'}, 'CGHUB_FASTQ', "Correct target inserted" );
    }
    {
        is($row_HR->{'status'}, 'zip_candidate', "Correct status inserted" );
    }

    # check update here too
    {
         ok($OBJ->_updateUploadStatus( $DBH, "TESTING" ), "Updated ok");
    }
    $selectSTH->execute()
            or die $selectSTH->errstr();
    $row_HR = $selectSTH->fetchrow_hashref();
    if (! defined $row_HR) {
        die $selectSTH->errstr();
    }
    {
        is($row_HR->{'status'}, 'TESTING', "Correct status after update" );
    }

    # Cleanup inserted record
    $selectSTH->finish();
    $DBH->begin_work()
            or die $DBH->errstr();  # Autocommit should be on.
    my $deleteSQL =
        "DELETE from upload WHERE upload_id = $OBJ->{_zipUploadId}";
    my $deleteSTH = $DBH->prepare($deleteSQL)
            or die $DBH->errstr();
    $deleteSTH->execute()
            or die $selectSTH->errstr();
    my $rowsAffected = $deleteSTH->rows();
    if (! defined $rowsAffected || $rowsAffected != 1) {
        $DBH->rollback();
        die "Failed to delete test record - need to fix manually\n"
                . Dumper($OBJ);
    }
    $DBH->commit();
    $deleteSTH->finish();
}

sub test_getFilesToZip {
    plan( tests => 6 );
    my $localOpt = {
        %$OPT_HR,
        '_sampleId'  => 13846,
        '_laneId'    => 13401,
    };

    my $localObj = $CLASS->new( $localOpt );
    my $selectHR = {
         'workflowAccession' => 613863,
         'algorithm'         => 'FinalizeCasava'
    };

    {
        ok( $localObj->_getFilesToZip( $DBH, $selectHR ), "Can get files to zip" );
    }
    {
        my $got = $localObj->{'_fastqs'};
        my $want = [{
            'filePath' => '/datastore/nextgenout2/seqware-analysis/illumina/130702_UNC9-SN296_0380_BC24VKACXX/seqware-0.7.0_FinalizeCasava_0.7.0/130702_UNC9-SN296_0380_BC24VKACXX_GATCAG_L002_1.fastq',
            'md5sum'   => '9ac03737c9c0389a37ba5b6737703ed1',
        }, {
            'filePath' => '/datastore/nextgenout2/seqware-analysis/illumina/130702_UNC9-SN296_0380_BC24VKACXX/seqware-0.7.0_FinalizeCasava_0.7.0/130702_UNC9-SN296_0380_BC24VKACXX_GATCAG_L002_2.fastq',
            'md5sum'   => '9962c5f135d9c428d2090ac3bdb7a3a6',
        }];
        is_deeply( $got, $want, "Correct file data retrieved");
    }
    {
        my $got = $localObj->{'_workflowRunId'};
        my $want = 98312;
        is ($got, $want, "Sets workfow run id property");
    }
    {
        my $got = $localObj->{'_flowcell'};
        my $want = '98312;130702_UNC9-SN296_0380_BC24VKACXX'
        is ($got, $want, "Sets flowcell property");
    }
    {
        my $got = $localObj->{'_laneIndex'};
        my $want = 1;
        is ($got, $want, "Sets lane index property");
    }
    {
        my $got = $localObj->{'_barcode'};
        my $want = 'GATCAG'
        is ($got, $want, "Sets barcode property");
    }
}

sub test_zip {
    plan( tests => 1 );
    
    # Setupt files for testing:
    my $tmp = File::Temp->new();
    $tmp
    my $self = {
        %$OPT_HR,
        '_sampleId'  => 13846,
        '_laneId'    => 13401,
    };
    {
        fail("Not tested")
    }
}
