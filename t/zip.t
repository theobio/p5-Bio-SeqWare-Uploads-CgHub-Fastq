#! /usr/bin/env perl

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Temp;           # Simple files for testing
use File::Spec;
use DBI;
use DBD::Mock;
use Test::Exception;
use DBD::Mock::Session;

use Bio::SeqWare::Config; # Access SeqWare settings file as options
use Bio::SeqWare::Db::Connection;
use Bio::SeqWare::Uploads::CgHub::Fastq;

use Test::More 'tests' => 8;   # Run this many Test::More compliant subtests.

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';

#
# Set up fastq files to test on
#

my $fastq1Text = <<'FQ1';
@UNC9-SN296:380:C24VKACXX:2:1101:1240:1035 1:N:0:GATCAG
CCANGGTCTTCAAGAAATGGTGATGTCAAAAGTCATCATAGTCCATAN
+
@CC#4AADHHHHFHJGIJHIEHIGJIIFIJIIFHIIIJJIIEIJJJJ#
@UNC9-SN296:380:C24VKACXX:2:1101:1479:1017 1:N:0:GATCAG
NACCCACCTGTCAGCCTCACCTGCCACCTCTCTCCATGTTGGCTTGTT
+
#1=DDFFFHHHHHJIJIJJIIIJJJJIIIIIIJJJJGGFIEIJJJIDE
FQ1

my $FASTQ1 = File::Temp->new();
print $FASTQ1 "$fastq1Text";
$FASTQ1->seek( 0, SEEK_END );

my $FASTQ1_MD5 = '1c93d2e4c1c0b65e59c8a8923c7d2b7d';

my $fastq2Text = <<'FQ2';
@UNC9-SN296:380:C24VKACXX:2:1101:1240:1035 2:N:0:GATCAG
TTTATAACCATTTTGAGCTAATAAGCTATACCTCACTCTCTGCTTTCN
+
CCCFFFFFHHHHGJFHIIIIIGIHJJJIIJJJIJJJJJJJJJJJIJI#
@UNC9-SN296:380:C24VKACXX:2:1101:1479:1017 2:N:0:GATCAG
GACCAAAGCGACCTAAACACTTGAAGCTAAAAGCAAGTGCTGATGATG
+
@CCFFFFFHHHHHIJJIJJJJIHIJIJIIJIJGHHIJGIIJJHIIIJJ
FQ2

my $FASTQ2 = File::Temp->new();
print $FASTQ2 "$fastq2Text";
$FASTQ2->seek( 0, SEEK_END );

my $FASTQ2_MD5 = 'a285bcffb5dcf00433f113c191b7829b';

my $fastqBadText = <<'FQ_BAD';
TTTATAACCATTTTGAGCTAATAAGCTATACCTCACTCTCTGCTTTCN
+
CCCFFFFFHHHHGJFHIIIIIGIHJJJIIJJJIJJJJJJJJJJJIJI#
@UNC9-SN296:380:C24VKACXX:2:1101:1479:1017 2:N:0:GATCAG
GACCAAAGCGACCTAAACACTTGAAGCTAAAAGCAAGTGCTGATGATG
+
@CCFFFFFHHHHHIJJIJJJJIHIJIJIIJIJGHHIJGIIJJHIIIJJ
FQ_BAD

my $FASTQ_BAD_MD5 = '7643c31b47fa345c0ebc88a90a1315d0';

my $FASTQ_BAD = File::Temp->new();
print $FASTQ_BAD "$fastqBadText";
$FASTQ_BAD->seek( 0, SEEK_END );

my $fastqMismatchTxt = <<'FQ2_MISMATCH';
@UNC9-SN296:380:C24VKACXX:2:1101:1240:1035 2:N:0:GATCAG
TTTATAACCATTTTGAGCTAATAAGCTATACCTCACTCTCTGCTTTCN
+
CCCFFFFFHHHHGJFHIIIIIGIHJJJIIJJJIJJJJJJJJJJJIJI#
FQ2_MISMATCH

my $FASTQ_MISMATCH = File::Temp->new();
print $FASTQ_MISMATCH "$fastqMismatchTxt";
$FASTQ_MISMATCH->seek( 0, SEEK_END );

my $FASTQ_MISMATCH_MD5 = '9e758d73df84c21d1d23ef985f07f8f8';

#
# Set up object
#
my $OBJ;
my $CONFIG = Bio::SeqWare::Config->new();
my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode' => 'alL',
};
$OBJ = $CLASS->new( $OPT_HR );

$OBJ->{'minFastqSize'} = 10;
$OBJ->{'_fastqs'} = [
    {
        'filePath' => $FASTQ1,
        'md5sum'   => $FASTQ1_MD5,
    }, {
        'filePath' => $FASTQ2,
        'md5sum'   => $FASTQ2_MD5,
    },
];
$OBJ->{'_flowcell'} = '130702_UNC9-SN296_0380_BC24VKACXX';
$OBJ->{'_laneIndex'} = '1';
$OBJ->{'_barcode'} = 'GATCAG';
$OBJ->{'_uploadId'} = -21;
$OBJ->{'rerun'} = 1;


my $EXPECT_OUTFILE = File::Spec->catdir(
     $OBJ->{'dataRoot'}, $OBJ->{'_flowcell'}, $OBJ->{'myName'}
);
$EXPECT_OUTFILE = File::Spec->catfile( $EXPECT_OUTFILE,
     $OBJ->{'_flowcell'} . "_" . ($OBJ->{'_laneIndex'} + 1) . "_" . $OBJ->{'_barcode'} . ".tar.gz"
);

my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0 },
);

#
# Run tests
#

subtest( '_findNewLaneToZip()'         => \&test__findNewLaneToZip );
subtest( '_insertNewZipUploadRecord()' => \&test__insertNewZipUploadRecord );
subtest( '_getLaneToZip()'             => \&test__getLaneToZip );
subtest( '_getFilesToZip()'            => \&test__getFilesToZip );
subtest( '_fastqFilesSqlSubSelect()'   => \&test__fastqFilesSqlSubSelect);
subtest( '_updateUploadStatus()'       => \&test__UpdateUploadStatus);
subtest( '_zip()'                      => \&test__zip );
subtest( '_insertNewFileRecord()' => \&test_insertNewFileRecord);

#
# Cleanup
#

#
# Subtests
#

sub test__findNewLaneToZip {
    plan( tests => 12 );

    my $sampleId    = -19;
    my $laneId      = -12;
    my $uploadId    = -21;
    my $metaDataDir = "/some/root/dir";
    my $uuidDir     = "B2F72FC3-2B9C-4448-B0C2-FE288C3C200C";
    
    # Test when good data returned
    {
        my @dbEventsOk = ({
            'statement'   => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
            'boundParams' => [ 'CGHUB', 'live', 'CGHUB_FASTQ' ],
            'results'     => [
                [ 'lane_id', 'sample_id', 'upload_id', 'metadata_dir', 'cghub_analysis_id' ],
                [ $laneId,    $sampleId,   $uploadId,   $metaDataDir,   $uuidDir           ],
            ],
        });
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'getLanesToZip', @dbEventsOk );
    
        # Object will be modified, so need local
        my $objToModify = $CLASS->new( $OPT_HR );
        
        {
            my $got  = $objToModify->_findNewLaneToZip( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, "Return 1 if found candidate to zip" );
        }
        {
            my $got  = $objToModify->{'_laneId'};
            my $want = $laneId;
            is( $got, $want, "Lane id stored in object" );
        }
        {
            my $got  = $objToModify->{'_sampleId'};
            my $want = $sampleId;
            is( $got, $want, "Sample id stored in object" );
        }
        {
            my $got  = $objToModify->{'_mapSpliceUploadId'};
            my $want = $uploadId;
            is( $got, $want, "Mapsplice upload id stored in object" );
        }
        {
            my $got  = $objToModify->{'_mapSpliceUploadBaseDir'};
            my $want = $metaDataDir;
            is( $got, $want, "Mapsplice upload meta data dir is stored in object" );
        }
        {
            my $got  = $objToModify->{'_mapSpliceUploadUuidDir'};
            my $want = $uuidDir;
            is( $got, $want, "Mapsplice upload UUID stored in object" );
        }
    }

    # Test when no lane to zip is found.
    {
        my @dbEventsNone = ({
            'statement' => qr/SELECT vwf\.lane_id, u\.sample_id.*/msi,
            'boundParams' => [ 'CGHUB', 'live', 'CGHUB_FASTQ' ],
            'results'   => [[]],
        });
        $MOCK_DBH->{'mock_session'} =
                DBD::Mock::Session->new( 'noLanesToZip', @dbEventsNone );

        # Object may be modified, so need local
        my $objToModify = $CLASS->new( $OPT_HR );

        {
            my $got  = $OBJ->_findNewLaneToZip( $MOCK_DBH );
            my $want = 1;
            is( $got, $want, "Return 1 if found no candidate to zip" );
        }
        {
            my $got  = $objToModify->{'_laneId'};
            my $want = undef;
            is( $got, $want, "Lane id not retrieved for object" );
        }
        {
            my $got  = $objToModify->{'_sampleId'};
            my $want = undef;
            is( $got, $want, "Sample id not retrieved for object" );
        }
        {
            my $got  = $objToModify->{'_mapSpliceUploadId'};
            my $want = undef;
            is( $got, $want, "Mapsplice upload id not retrieved for object" );
        }
        {
            my $got  = $objToModify->{'_mapSpliceUploadBaseDir'};
            my $want = undef;
            is( $got, $want, "Mapsplice upload meta data dir not retrieved for object" );
        }
        {
            my $got  = $objToModify->{'_mapSpliceUploadUuidDir'};
            my $want = undef;
            is( $got, $want, "Mapsplice upload UUID not retrieved for object" );
        }
    }
}

sub test__insertNewZipUploadRecord {
    plan( tests => 2 );

    # Object will be modified, so need local
    my $sampleId    = -19;
    my $laneId      = -12;
    my $uploadId    = -21;
    my $metaDataDir = "/some/root/dir";
    my $uuidDir     = "B2F72FC3-2B9C-4448-B0C2-FE288C3C200C";

    my $objToModify = $CLASS->new( $OPT_HR );
    $objToModify->{'_laneId'} = $laneId;
    $objToModify->{'_sampleId'} = $sampleId;

    my @dbEventsOk = ({
        'statement'   => qr/INSERT INTO upload.*/msi,
        'bound_params' => [ $sampleId ],
        'results'  => [[ 'upload_id' ], [ $uploadId ]],
    });
    $MOCK_DBH->{'mock_session'} =
        DBD::Mock::Session->new( 'newUploadRecord', @dbEventsOk );

    {
        my $got = $objToModify->_insertNewZipUploadRecord( $MOCK_DBH );
        my $want = 1;
        is( $got, $want, "Return 1 if inserted upload record to zip" );
    }
    {
       my $got = $objToModify->{'_uploadId'};
       my $want = $uploadId;
       is( $got, $want, "Upload id stored in object" );
    }
}

sub test__fastqFilesSqlSubSelect {
    plan( tests => 7 );

    {
        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_workflowAccession'} = 613863;

        {
            my $got = $objToModify->_fastqFilesSqlSubSelect();
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 613863 AND vw_files.algorithm = 'FinalizeCasava'";
            is ($got, $want, "Got expected sub select string from internal.");
        }{
            my $got = $objToModify->_fastqFilesSqlSubSelect($objToModify->{'_workflowAccession'});
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 613863 AND vw_files.algorithm = 'FinalizeCasava'";
            is ($got, $want, "Got expected sub select string from explict.");
        }
    }{
        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_workflowAccession'} = 851553;

        {
            my $got = $objToModify->_fastqFilesSqlSubSelect();
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 851553 AND vw_files.algorithm = 'srf2fastq'";
            is ($got, $want, "Got alternate expected sub select string from internal.");
        }{
            my $got = $objToModify->_fastqFilesSqlSubSelect($objToModify->{'_workflowAccession'});
            my $want = "SELECT vw_files.file_id FROM vw_files WHERE vw_files.workflow_accession = 851553 AND vw_files.algorithm = 'srf2fastq'";
            is ($got, $want, "Got alternate expected sub select string from explict.");
        }
    }{
        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_workflowAccession'} = -1;

        {
            my $got = $objToModify->_fastqFilesSqlSubSelect();
            my $want = undef;
            is ($got, $want, "Got undefined from unknown internal" );
        }{
            my $got = $objToModify->_fastqFilesSqlSubSelect($objToModify->{'_workflowAccession'});
            my $want = undef;
            is ( $got, $want, "Got undefined from unknown expicit" );
        }
    }{
        my $objToModify = $CLASS->new( $OPT_HR );
        {
            eval {
                 $objToModify->_fastqFilesSqlSubSelect();
            };
            my $got = $@;
            my $want = qr/^Fastq workflow accession not specified and not set internally\./;
            like( $got, $want, "Fatal if don't have a workflow_accession" );
        }
    }
}

sub test__getLaneToZip {
    plan( tests => 2 );

    my $sampleId = -19;
    my $laneId = -12;
    my $uploadId = -21;

    my $objToModify = $CLASS->new( $OPT_HR );
    $objToModify->{'_laneId'} = $laneId;
    $objToModify->{'_sampleId'} = $sampleId;

    my @dbEventsOk = ({
        'statement'   => qr/INSERT INTO upload.*/msi,
        'bound_params' => [ $sampleId ],
        'results'  => [[ 'upload_id' ], [ $uploadId ]],
    });
    $MOCK_DBH->{'mock_session'} =
        DBD::Mock::Session->new( 'newUploadRecord', @dbEventsOk );

    {
        my $got = $objToModify->_insertNewZipUploadRecord( $MOCK_DBH );
        my $want = 1;
        is( $got, $want, "Return 1 if inserted upload record to zip" );
    }
    {
       my $got = $objToModify->{'_uploadId'};
       my $want = $uploadId;
       is( $got, $want, "Upload id stored in object" );
    }

}

sub test__getFilesToZip {
    plan( tests => 50 );
    my $sampleId = -19;
    my $laneId = -12;
    my $uploadId = -21;
    my $fastqWorkflowRunId = -2315;
    my $flowcell = $OBJ->{'_flowcell'};
    my $laneIndex = $OBJ->{'_laneIndex'};
    my $barcode = $OBJ->{'_barcode'};
    my $processingId = -20;
    my $fastq1 = $FASTQ1;
    my $fastq2 = $FASTQ2;
    my $fileId1 = -6;
    my $fileId2 = -66;
    my $fastq1Md5 = $FASTQ1_MD5;
    my $fastq2Md5 = $FASTQ1_MD5;
    my $wf1 = 613863;
    my $wf2 = 851553;

    {
        my @dbEvents = ({
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [
                [ 'file_path', 'md5sum', 'workflow_run_id',
                  'flowcell',  'lane_index', 'barcode', 'processing_id' ],
                [ $fastq1, $fastq1Md5, $fastqWorkflowRunId,
                  $flowcell,  $laneIndex, $barcode, $processingId ],
            ]
        });

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'one613863', @dbEvents );

        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_laneId'} = $laneId;
        $objToModify->{'_sampleId'} = $sampleId;
        {
            is( $objToModify->_getFilesToZip( $MOCK_DBH ), 1, "File one613863 retrieval ok." );
            is( $objToModify->{'_workflowAccession'}, $wf1, "ok one613863 _workflowAccession" );
            is( $objToModify->{'_fastqProcessingId'}, $processingId, "ok one613863 _flowcell" );
            is( $objToModify->{'_workflowRunId'}, $fastqWorkflowRunId, "ok one613863 _flowcell" );
            is( $objToModify->{'_flowcell'}, $flowcell, "ok one613863 _flowcell" );
            is( $objToModify->{'_laneIndex'}, $laneIndex, "ok one613863 _laneIndex" );
            is( $objToModify->{'_barcode'}, $barcode, "ok one613863 _barcode" );

            is( $objToModify->{'_fastqs'}->[0]->{'filePath'}, $fastq1, "ok one613863 filePath0" );
            is( $objToModify->{'_fastqs'}->[0]->{'md5sum'}, $fastq1Md5, "ok one613863 md5sum0" );
            is( $objToModify->{'_fastqs'}->[1], undef );
        }
    }{
        my @dbEvents = ({
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [
                [ 'file_path', 'md5sum', 'workflow_run_id',
                  'flowcell',  'lane_index', 'barcode', 'processing_id' ],
                [ $fastq1, $fastq1Md5, $fastqWorkflowRunId,
                  $flowcell,  $laneIndex, $barcode, $processingId ],
                [ $fastq2, $fastq2Md5, $fastqWorkflowRunId,
                  $flowcell,  $laneIndex, $barcode, $processingId ],
            ]
        });

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'two613863', @dbEvents );

        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_laneId'} = $laneId;
        $objToModify->{'_sampleId'} = $sampleId;
        {
            is( $objToModify->_getFilesToZip( $MOCK_DBH ), 1, "File two613863 retrieval ok." );
            is( $objToModify->{'_workflowAccession'}, $wf1, "ok two613863 _workflowAccession" );
            is( $objToModify->{'_fastqProcessingId'}, $processingId, "ok two613863 _flowcell" );
            is( $objToModify->{'_workflowRunId'}, $fastqWorkflowRunId, "ok two613863 _flowcell" );
            is( $objToModify->{'_flowcell'}, $flowcell, "ok two613863 _flowcell" );
            is( $objToModify->{'_laneIndex'}, $laneIndex, "ok two613863 _laneIndex" );
            is( $objToModify->{'_barcode'}, $barcode, "ok two613863 _barcode" );

            is( $objToModify->{'_fastqs'}->[0]->{'filePath'}, $fastq1, "ok two613863 filePath0" );
            is( $objToModify->{'_fastqs'}->[0]->{'md5sum'}, $fastq1Md5, "ok two613863 md5sum0" );
            is( $objToModify->{'_fastqs'}->[1]->{'filePath'}, $fastq2, "ok two613863 filePath0" );
            is( $objToModify->{'_fastqs'}->[1]->{'md5sum'}, $fastq2Md5, "ok two613863 md5sum0" );
        }
    }{
        my @dbEvents = ({
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [[]],
        },
        {
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'srf2fastq\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [
                [ 'file_path', 'md5sum', 'workflow_run_id',
                  'flowcell',  'lane_index', 'barcode', 'processing_id' ],
                [ $fastq1, $fastq1Md5, $fastqWorkflowRunId,
                  $flowcell,  $laneIndex, $barcode, $processingId ],
            ]
        });

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'one851553', @dbEvents );

        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_laneId'} = $laneId;
        $objToModify->{'_sampleId'} = $sampleId;
        {
            is( $objToModify->_getFilesToZip( $MOCK_DBH ), 1, "File one851553 retrieval ok." );
            is( $objToModify->{'_workflowAccession'}, $wf2, "ok one851553 _workflowAccession" );
            is( $objToModify->{'_fastqProcessingId'}, $processingId, "ok one851553 _flowcell" );
            is( $objToModify->{'_workflowRunId'}, $fastqWorkflowRunId, "ok one851553 _flowcell" );
            is( $objToModify->{'_flowcell'}, $flowcell, "ok one851553 _flowcell" );
            is( $objToModify->{'_laneIndex'}, $laneIndex, "ok one851553 _laneIndex" );
            is( $objToModify->{'_barcode'}, $barcode, "ok one851553 _barcode" );

            is( $objToModify->{'_fastqs'}->[0]->{'filePath'}, $fastq1, "ok one851553 filePath0" );
            is( $objToModify->{'_fastqs'}->[0]->{'md5sum'}, $fastq1Md5, "ok one851553 md5sum0" );
            is( $objToModify->{'_fastqs'}->[1], undef );
        }
    }{
        my @dbEvents = ({
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [[]],
        },
        {
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'srf2fastq\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [
                [ 'file_path', 'md5sum', 'workflow_run_id',
                  'flowcell',  'lane_index', 'barcode', 'processing_id' ],
                [ $fastq1, $fastq1Md5, $fastqWorkflowRunId,
                  $flowcell,  $laneIndex, $barcode, $processingId ],
                [ $fastq2, $fastq2Md5, $fastqWorkflowRunId,
                  $flowcell,  $laneIndex, $barcode, $processingId ],
            ]
        });

        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'two851553', @dbEvents );

        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_laneId'} = $laneId;
        $objToModify->{'_sampleId'} = $sampleId;
        {
            is( $objToModify->_getFilesToZip( $MOCK_DBH ), 1, "File two851553 retrieval ok." );
            is( $objToModify->{'_workflowAccession'}, $wf2, "ok two851553 _workflowAccession" );
            is( $objToModify->{'_fastqProcessingId'}, $processingId, "ok two851553 _flowcell" );
            is( $objToModify->{'_workflowRunId'}, $fastqWorkflowRunId, "ok two851553 _flowcell" );
            is( $objToModify->{'_flowcell'}, $flowcell, "ok two851553 _flowcell" );
            is( $objToModify->{'_laneIndex'}, $laneIndex, "ok two851553 _laneIndex" );
            is( $objToModify->{'_barcode'}, $barcode, "ok two851553 _barcode" );

            is( $objToModify->{'_fastqs'}->[0]->{'filePath'}, $fastq1, "ok two851553 filePath0" );
            is( $objToModify->{'_fastqs'}->[0]->{'md5sum'}, $fastq1Md5, "ok two851553 md5sum0" );
            is( $objToModify->{'_fastqs'}->[1]->{'filePath'}, $fastq2, "ok two851553 filePath0" );
            is( $objToModify->{'_fastqs'}->[1]->{'md5sum'}, $fastq2Md5, "ok two851553 md5sum0" );
        }
    }{
        my $newStatus = "error_zip_no-db-fastq-files";
        my @dbEvents = (({
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'FinalizeCasava\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [[]],
        },
        {
            'statement'   => qr/SELECT vwf\.file_path.*AND vw_files\.algorithm \= \'srf2fastq\'/msi,
            'bound_params' => [ $sampleId, $laneId],
            'results'  => [[]],
        }), makeUploadDbEvents($newStatus, $uploadId));


        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( 'noFiles', @dbEvents );

        my $objToModify = $CLASS->new( $OPT_HR );
        $objToModify->{'_laneId'} = $laneId;
        $objToModify->{'_sampleId'} = $sampleId;
        $objToModify->{'_uploadId'} = $uploadId;
        {
            is( $objToModify->_getFilesToZip( $MOCK_DBH ), 0, "File noFiles retrieval ok." );
            is( $objToModify->{'_workflowAccession'}, undef, "ok noFiles _workflowAccession" );
            is( $objToModify->{'_fastqProcessingId'}, undef, "ok noFiles _flowcell" );
            is( $objToModify->{'_workflowRunId'}, undef, "ok noFiles _flowcell" );
            is( $objToModify->{'_flowcell'}, undef, "ok noFiles _flowcell" );
            is( $objToModify->{'_laneIndex'}, undef, "ok noFiles _laneIndex" );
            is( $objToModify->{'_barcode'}, undef, "ok noFiles _barcode" );

            is( $objToModify->{'_fastqs'}, undef, "ok noFiles _fastqs" );
        }
    }

}

sub test__zip {
    plan( tests => 21 );
    
    my $oneFastq = [{
        'filePath' => $FASTQ1,
        'md5sum'   => $FASTQ1_MD5,
    }];
    my $twoFastq = [{
        'filePath' => $FASTQ1,
        'md5sum'   => $FASTQ1_MD5,
    }, {
        'filePath' => $FASTQ2,
        'md5sum'   => $FASTQ2_MD5,
    }];

    my $obj1File = $CLASS->new( $OPT_HR );
    $obj1File->{'minFastqSize'} = 10;
    $obj1File->{'_fastqs'}      = $oneFastq;
    $obj1File->{'_flowcell'}    = '130702_UNC9-SN296_0380_BC24VKACXX';
    $obj1File->{'_laneIndex'}   = '1';
    $obj1File->{'_barcode'}     = 'GATCAG';
    $obj1File->{'_uploadId'}    = -21;
    $obj1File->{'rerun'}        = 1;

    my $obj2File = $CLASS->new( $OPT_HR );
    $obj2File->{'minFastqSize'} = 10;
    $obj2File->{'_fastqs'}      = $twoFastq;
    $obj2File->{'_flowcell'}    = '130702_UNC9-SN296_0380_BC24VKACXX';
    $obj2File->{'_laneIndex'}   = '1';
    $obj2File->{'_barcode'}     = 'GATCAG';
    $obj2File->{'_uploadId'}    = -21;
    $obj2File->{'rerun'}        = 1;

    {
         ok( $obj1File->_zip(), "Zip seemed to work for one fastq");
         ok( $obj2File->_zip(), "Zip seemed to work for two fastq");
    }
    {
         ok( (-f $EXPECT_OUTFILE), "Created zip file");
    }
    {
         my $old = $obj1File->{'_fastqs'}->[0]->{'filePath'};
         $obj1File->{'_fastqs'}->[0]->{'filePath'} = "NOsuchPATHiHOPE";
         is( $obj1File->_zip(), 0, "Zip1 fail with bad file 0 path");
         is( $obj1File->{'_error'}, "error_zip_fastq-not-found", "Error message, zip1 fail with bad file 0 path");
         $obj1File->{'_error'} = undef;
         $obj1File->{'_fastqs'}->[0]->{'filePath'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[0]->{'filePath'};
         $obj2File->{'_fastqs'}->[0]->{'filePath'} = "NOsuchPATHiHOPE";
         is( $obj2File->_zip(), 0, "Zip2 fail with bad file 0 path");
         is( $obj2File->{'_error'}, "error_zip_fastq-not-found", "Error message, zip2 fail with bad file 0 path");
         $obj2File->{'_error'} = undef;
         $obj2File->{'_fastqs'}->[0]->{'filePath'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[1]->{'filePath'};
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = "NOsuchPATHiHOPE";
         is( $obj2File->_zip(), 0, "Zip2 fail with bad file 1 path");
         is( $obj2File->{'_error'}, "error_zip_fastq-not-found", "Error message, zip2 fail with bad file 1 path");
         $obj2File->{'_error'} = undef;
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = $old;
    }
    {
         my $old = $obj1File->{'_fastqs'}->[0]->{'md5sum'};
         $obj1File->{'_fastqs'}->[0]->{'md5sum'} = "123";
         is( $obj1File->_zip(), 0, "Zip1 fail with bad file 0 md5sum");
         is( $obj1File->{'_error'}, "error_zip_fastq-md5-failed", "Error message, zip1 fail with bad file 0 md5sum");
         $obj1File->{'_error'} = undef;
         $obj1File->{'_fastqs'}->[0]->{'md5sum'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[0]->{'md5sum'};
         $obj2File->{'_fastqs'}->[0]->{'md5sum'} = "123";
         is( $obj2File->_zip(), 0, "Zip2 fail with bad file 0 md5sum");
         is( $obj2File->{'_error'}, "error_zip_fastq-md5-failed", "Error message, zip2 fail with bad file 0 md5sum");
         $obj2File->{'_error'} = undef;
         $obj2File->{'_fastqs'}->[0]->{'md5sum'} = $old;
    }
    {
         my $old = $obj2File->{'_fastqs'}->[1]->{'md5sum'};
         $obj2File->{'_fastqs'}->[1]->{'md5sum'} = "123";
         is( $obj2File->_zip(), 0, "Zip2 fail with bad file 1 md5sum");
         is( $obj2File->{'_error'}, "error_zip_fastq-md5-failed", "Error message, zip2 fail with bad file 1 md5sum");
         $obj2File->{'_error'} = undef;
         $obj2File->{'_fastqs'}->[1]->{'md5sum'} = $old;
    }
    {
         my $old = $obj2File->{'minFastqSize'};
         $obj2File->{'minFastqSize'} = 1000000;
         is( $obj2File->_zip(), 0, "Zip fail with to small fastq file");
         is( $obj2File->{'_error'}, "error_zip_fastq-too-small", "Error message, Zip fail with to small fastq file");
         $obj2File->{'_error'} = undef;
         $obj2File->{'minFastqSize'} = $old;
    }
    {
         my $old1       = $obj2File->{'_fastqs'}->[1]->{'filePath'};
         my $oldmd5sum2 = $obj2File->{'_fastqs'}->[1]->{'md5sum'};
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = $FASTQ_BAD;
         $obj2File->{'_fastqs'}->[1]->{'md5sum'}   = $FASTQ_BAD_MD5;
         is( $obj2File->_zip(), 0, "Zip2 fail with bad file wc count");
         is( $obj2File->{'_error'}, "error_zip_fastq-not-lines-mod-4", "Error message, Zip2 fail with bad file wc count");
         $obj2File->{'_error'} = undef;
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = $old1;
         $obj2File->{'_fastqs'}->[1]->{'md5sum'} = $oldmd5sum2;
    }
    {
         my $old1       = $obj2File->{'_fastqs'}->[1]->{'filePath'};
         my $oldmd5sum2 = $obj2File->{'_fastqs'}->[1]->{'md5sum'};
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = $FASTQ_MISMATCH;
         $obj2File->{'_fastqs'}->[1]->{'md5sum'}   = $FASTQ_MISMATCH_MD5;
         is( $obj2File->_zip(), 0, "Zip2 fail with mismatched fastq line counts");
         is( $obj2File->{'_error'}, "error_zip_fastq-mismatched-line-count", "Error message, Zip2 fail with mismatched fastq line counts");
         $obj2File->{'_error'} = undef;
         $obj2File->{'_fastqs'}->[1]->{'filePath'} = $old1;
         $obj2File->{'_fastqs'}->[1]->{'md5sum'} = $oldmd5sum2;
    }
}

sub test__UpdateUploadStatus {

    plan ( tests => 1 );

    my $newStatus  = 'test_ignore_not-real-status';
    my @dbSesssion = makeUploadDbEvents($newStatus, $OBJ->{'_uploadId'});
    $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( $newStatus, @dbSesssion );
    {
        my $got;
        lives_ok {
             $got = $OBJ->_updateUploadStatus( $MOCK_DBH, $newStatus )
        }, "DB session worked.";
    }

}

sub test_insertNewFileRecord {

    plan ( tests => 1 );

    my $type  = 'fastq-by-end-tar-bundled-gz-compressed';
    my $metaType  = 'application/tar-gz';
    my $description = "fastq files, tarred and gzipped, one file per end.";
    my $zipFile = $EXPECT_OUTFILE;
    my $zipFileMd5 = 'f586508aaae41811e1ed491f762442d9';
    my $fileId = -6;
    my $processingId = -20;

    $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( 'insertFileRec', ({
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/INSERT INTO file.*/msi,
        'bound_params' => [ $zipFile, $zipFileMd5 ],
        'results'  => [[ 'file_id' ], [ $fileId ]],
    }, {
        'statement'    => qr/INSERT INTO processing_file.*/msi,
        'bound_params' => [ $processingId, $fileId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    } ));

    my $uploaderObj = $CLASS->new( $OPT_HR );
    $uploaderObj->{'_zipFileMd5'} = $zipFileMd5;
    $uploaderObj->{'_zipFile'} = $zipFile;
    $uploaderObj->{'_fastqProcessingId'} = $processingId;
    $uploaderObj->{'_zipFileId'} = $fileId; # Set during run, preset to allow testing

    {
        my $got;
        lives_ok {
             $got = $uploaderObj->_insertNewFileRecord( $MOCK_DBH )
        }, "Insert new file and processing_file records worked.";
    }
}

sub makeUploadDbEvents {

    my $newStatus = shift;
    my $uploadId = shift;

    my @dbEvents = ( {
            'statement' => 'BEGIN WORK',
            'results'  => [[]],
        }, {
            'statement'    => qr/UPDATE upload.*/msi,
            'bound_params' => [ $newStatus, $uploadId ],
            'results'  => [[ 'rows' ], []],
        }, {
           'statement' => 'COMMIT',
           'results'  => [[]],
        }
    );

    return @dbEvents;
}
