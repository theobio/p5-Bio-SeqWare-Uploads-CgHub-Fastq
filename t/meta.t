#! /usr/bin/env perl
use 5.014;  # Safe $@ eval exception handling

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages

use File::Temp;                      # Simple files for testing
use File::ShareDir qw(dist_dir);     # Access data files from install.
use File::Copy qw(cp);               # Copy a file

use Bio::SeqWare::Config 0.000003; # Get config data, with most recent keyset

use Test::Output;               # Test STDOUT and STDERR output.
use DBD::Mock;
use Test::File::Contents;
use Test::More 'tests' => 5;    # Run this many Test::More compliant subtests

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
};

my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1 },
);

my $MOCK_READ_LENGTH_SAMTOOLS_RETURN = 
"TTAGATAAAGGATACTG
AAAAGATAAGGATA
GCCTAAGCTAA
ATAGCTTCAGC
TAGGC
CAGCGGCAT
";


subtest( '_getTemplateDataReadLength()' => \&test__getTemplateDataReadLength );
subtest( '_getTemplateDataReadEnds()'   => \&test__getTemplateDataReadEnds   );
subtest( '_getTemplateData()'      => \&test__getTemplateData      );
subtest( '_makeFileFromTemplate'   => \&test__makeFileFromTemplate );
subtest( 'doMeta()'                => \&test_doMeta );


sub test_doMeta {
    plan( tests => 5 );

    my $oldStatus = "zip_completed";
    my $newStatus = "meta_running";
    my $finalStatus = "meta_completed";

    my $uploadId       = 7851;
    my $fileTimestamp  = "2013-08-14 12:20:42.703867";
    my $sampleTcgaUuid = "66770b06-2cd6-4773-b8e8-5b38faa4f5a4";
    my $laneAccession  = 2090626;
    my $fileAccession  = 2149605;
    my $sampleId  = -19;
    my $experimentId  = -5;
    my $fileMd5sum     = "4181ac122b0a09f28cde79a9c3d5af39";
    my $filePath       = "/path/to/file/130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA.fastq.tar.gz";
    my $uploadUuid     = "notReallyTheFastqUploadUuid";
    my $experimentAccession = 975937;
    my $sampleAccession     = 2090625;
    my $instrumentModel = 'Illumina HiSeq 2000';
    my $experimentDescription = 'TCGA RNA-Seq Paired-End Experiment';
    my $readEnds       = 2;
    my $readLength     = 17;

    my $uploadDir = File::Spec->catdir( "$TEMP_DIR", $uploadUuid );
    mkdir($uploadDir);

    my @dbSession = ({
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
            [ $uploadId,   $oldStatus, $TEMP_DIR,      $uploadUuid,          $sampleId  ],
        ]
    }, {
        'statement'    => qr/UPDATE upload/msi,
        'bound_params' => [ $newStatus,  $uploadId ],
        'results'  => [[ 'rows' ], []]
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    }, {
        'statement'    => qr/SELECT.*/msi,
        'bound_params' => [ $uploadId ],
        'results'  => [
            [
                'file_timestamp',       'sample_tcga_uuid',  'lane_accession',
                'file_accession',       'file_md5sum',       'file_path',
                'fastq_upload_basedir', 'fastq_upload_uuid', 'experiment_accession',
                'sample_accession',     'experiment_description', 'experiment_id',
                'instrument_model',     'sample_id'
            ], [
                $fileTimestamp,   $sampleTcgaUuid,     $laneAccession,
                $fileAccession,   $fileMd5sum,         $filePath,
                "$TEMP_DIR",      $uploadUuid,         $experimentAccession,
                $sampleAccession, $experimentDescription, $experimentId,
                $instrumentModel, $sampleId,
            ]
        ]
    }, {
        'statement'    => qr/SELECT count\(\*\) as read_ends.*/msi,
        'bound_params' => [ $experimentId ],
        'results'  => [ ['read_ends'], [$readEnds], ]
    }, {
        'statement'    => qr/SELECT f\.file_path.*/msi,
        'bound_params' => [ $sampleId ],
        'results'  => [ ['file_path'], ["t/Data/toy.bam"], ]
    }, {
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/UPDATE upload.*/msi,
        'bound_params' => [ $finalStatus, $uploadId ],
        'results'  => [[ 'rows' ], []],
    }, {
       'statement' => 'COMMIT',
        'results'  => [[]],
    });

    {
        my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( "doMetaOk", @dbSession );
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = "$MOCK_READ_LENGTH_SAMTOOLS_RETURN";
        {
            is(1, $obj->doMeta( $MOCK_DBH ), "SmokeTest doMeta" );
        }
        $mock_readpipe->{'mock'} = 0;
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doMeta();
        };
        {
          like( $@, qr/^doMeta\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'failed_meta_param_doMeta_dbh', "Errror tag if no dbh param");
        }
    }
    
    # Error propagation on error.
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->doMeta( $MOCK_DBH );
        };
        {
          like( $@, qr/^Error changing upload status from zip_completed to meta_running/, "Error propogates out");
          is( $obj->{'error'}, 'failed_meta_status_change_zip_completed_to_meta_running', "Errror tag propogates out");
        }
    }

}


sub test__getTemplateData {
    plan( tests => 15 );

    # Chosen to match analysis.xml data file
    my $uploadId       = 7851;
    my $fileTimestamp  = "2013-08-14 12:20:42.703867";
    my $sampleTcgaUuid = "66770b06-2cd6-4773-b8e8-5b38faa4f5a4";
    my $laneAccession  = 2090626;
    my $fileAccession  = 2149605;
    my $fileMd5sum     = "4181ac122b0a09f28cde79a9c3d5af39";
    my $filePath       = "/path/to/file/130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA.fastq.tar.gz";
    my $uploadUuid     = "notReallyTheFastqUploadUuid";

    my $localFileLink  = "UNCID_2149605.66770b06-2cd6-4773-b8e8-5b38faa4f5a4.130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA.fastq.tar.gz";
    my $fileBase       = "130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA";
    my $uploadIdAlias  = "upload $uploadId";
    my $xmlTimestamp  = "2013-08-14T12:20:42.703867";

    # Additional for run.xml data file
    my $experimentAccession = 975937;
    my $sampleAccession    = 2090625;
    my $experimentId = -5;
    my $sampleId = -19;
    
    # Additional for experiment.xml
    my $instrumentModel = 'Illumina HiSeq 2000';
    my $experimentDescription = 'TCGA RNA-Seq Paired-End Experiment';
    my $readEnds       = 2;
    my $baseCoord     = 16;

    my $uploadDir = File::Spec->catdir( "$TEMP_DIR", $uploadUuid );
    mkdir($uploadDir);

    my $expectData = {
        'program_version'    => $Bio::SeqWare::Uploads::CgHub::Fastq::VERSION,
        'sample_tcga_uuid'   => $sampleTcgaUuid,
        'lane_accession'     => $laneAccession,
        'file_md5sum'        => $fileMd5sum,
        'file_accession'     => $fileAccession,
        'upload_file_name'   => $localFileLink,
        'uploadIdAlias'      => $uploadIdAlias,
        'file_path_base'     => $fileBase,
        'analysis_date'      => $xmlTimestamp,
        'experiment_accession' => $experimentAccession,
        'sample_accession'   => $sampleAccession,
        'experiment_description' => $experimentDescription,
        'instrument_model'   => $instrumentModel,
        'read_ends'          => $readEnds,
        'library_layout'     => 'PAIRED',
        'base_coord'         => $baseCoord,
    };

    my @dbSession = ({
        'statement'    => qr/SELECT.*/msi,
        'bound_params' => [ $uploadId ],
        'results'  => [
            [
                'file_timestamp',       'sample_tcga_uuid',  'lane_accession',
                'file_accession',       'file_md5sum',       'file_path',
                'fastq_upload_basedir', 'fastq_upload_uuid', 'experiment_accession',
                'sample_accession',     'experiment_description', 'experiment_id',
                'instrument_model',     'sample_id'
            ], [
                $fileTimestamp,   $sampleTcgaUuid,     $laneAccession,
                $fileAccession,   $fileMd5sum,         $filePath,
                "$TEMP_DIR",      $uploadUuid,         $experimentAccession,
                $sampleAccession, $experimentDescription, $experimentId,
                $instrumentModel, $sampleId,
            ]
        ]
    }, {
        'statement'    => qr/SELECT count\(\*\) as read_ends.*/msi,
        'bound_params' => [ $experimentId ],
        'results'  => [ ['read_ends'], [$readEnds], ]
    }, {
        'statement'    => qr/SELECT f\.file_path.*/msi,
        'bound_params' => [ $sampleId ],
        'results'  => [ ['file_path'], ["t/Data/toy.bam"], ]
    });

    {

        my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "Good run", @dbSession );
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = "$MOCK_READ_LENGTH_SAMTOOLS_RETURN";
        my $got = $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        my $want = $expectData;
        {
            is_deeply($got, $want, "Return value correct");
            is( $obj->{'_fastqUploadDir'}, $uploadDir, "Set upload directory");
        }
        $mock_readpipe->{'mock'} = 0;
    }
    {
        my $realFile = File::Spec->catfile( $TEMP_DIR, "single_end.fastq");
        {
            ok( cp("t/Data/single_end.fastq", $TEMP_DIR), "Setup for link check ok" );
            ok( (-f $realFile), "File copied" );
        }
        $dbSession[0]->{'results'}->[1]->[5] = $realFile;
        my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "SingleEndFastQ", @dbSession );
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = "$MOCK_READ_LENGTH_SAMTOOLS_RETURN";
        my $data = $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        my $link = File::Spec->catfile( $obj->{'_fastqUploadDir'} , $data->{'upload_file_name'} );
        {
            ok( (-f $link ), "file exists: $link" );
            ok( (-l $link ), "link exists: $link" );
            files_eq( $link, $realFile, "Link points to real file");

        }
        $dbSession[0]->{'results'}->[1]->[5] = "$filePath";
        $mock_readpipe->{'mock'} = 0;
    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        $dbSession[0]->{'results'}->[1]->[5] = ".fastq.tar.gz";
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "Die on empty value (file_path)", @dbSession );
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = "$MOCK_READ_LENGTH_SAMTOOLS_RETURN";
        eval {
            $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        };
        {
            like ($@, qr/No value obtained for template data element \'file_path_base\'/, "fatal with empty file_path_base.");
            is ($obj->{'error'}, "bad_tempalte_datq", "error type for missing data");
        }
        $dbSession[0]->{'results'}->[1]->[5] = $filePath;
        $mock_readpipe->{'mock'} = 0;
    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        $dbSession[0]->{'results'}->[1]->[4] = undef;
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "Die on undefined data value (md5Sum)", @dbSession );
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'ret'} = "$MOCK_READ_LENGTH_SAMTOOLS_RETURN";
        eval {
            $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        };
        {
            like ($@, qr/No value obtained for template data element \'file_md5sum\'/, "fatal with undefined md5Sum.");
            is ($obj->{'error'}, "bad_tempalte_datq", "error type for empty data");
        }
        $dbSession[0]->{'results'}->[1]->[4] = $fileMd5sum;
        $mock_readpipe->{'mock'} = 0;
    }

    # Bad param: $dbh
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getTemplateData();
        };
        {
          like( $@, qr/^_getTemplateData\(\) missing \$dbh parameter\./, "Error if no dbh param");
          is( $obj->{'error'}, 'param__getTemplateData_dbh', "Errror tag if no dbh param");
        }
    }

    # Bad param: $uploadId
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
             $obj->_getTemplateData( $MOCK_DBH );
        };
        {
          like( $@, qr/^_getTemplateData\(\) missing \$uploadId parameter\./, "Error if no uploadId param");
          is( $obj->{'error'}, 'param__getTemplateData_uploadId', "Errror tag if no uploadId param");
        }
    }


}

sub test__makeFileFromTemplate {
    plan( tests => 25 );

    # Chosen to match actual xml data files
    my $uploadId       = 7851;
    my $expectData = {
        'program_version'    => $Bio::SeqWare::Uploads::CgHub::Fastq::VERSION,
        'sample_tcga_uuid'   => "66770b06-2cd6-4773-b8e8-5b38faa4f5a4",
        'lane_accession'     => 2090626,
        'file_md5sum'        => '4181ac122b0a09f28cde79a9c3d5af39',
        'file_accession'     => 2149605,
        'uploadIdAlias'      => "upload $uploadId",
        'file_path_base'     => "130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA",
        'analysis_date'      => "2013-08-14T12:20:42.703867",
        'upload_file_name'   => "UNCID_2149605.66770b06-2cd6-4773-b8e8-5b38faa4f5a4.130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA.fastq.tar.gz",
        'instrument_model'    => 'Illumina HiSeq 2000',
        'experiment_description' => 'TCGA RNA-Seq Paired-End Experiment',
        'experiment_accession'   => 975937,
        'sample_accession'       => 2090625,
        'experiment_description' => 'TCGA RNA-Seq Paired-End Experiment',
        'instrument_model'   => 'Illumina HiSeq 2000',
        'read_ends'          => 2,
        'base_coord'         => 16,
        'library_layout'     => 'PAIRED',
    };

    # Tests for analysis.xml
    {
        my $sampleFileName = File::Spec->catfile( "t", "Data", "analysis.xml" );
        my $outFileName = File::Spec->catfile( "$TEMP_DIR", "analysis.xml" );
        my $templateFileName = File::Spec->catfile($OPT_HR->{'templateBaseDir'}, $OPT_HR->{'xmlSchema'}, "analysis_fastq.xml.template" );

        # Files for use by test are available
        {
            ok( (-f $templateFileName), "Can find analysis.xml template file");
            ok( (-f $sampleFileName), "Can find analysis.xml example file");
        }

        # Absolute paths
        {
            my $obj = $CLASS->new( $OPT_HR );
            my $analysisXml = $obj->_makeFileFromTemplate( $expectData, $outFileName, $templateFileName );
            {
                is ( $analysisXml, $outFileName, "Appeared to create analysis.xml file");
                ok( (-f $analysisXml),   "Can find analysis.xml file");
                files_eq_or_diff( $analysisXml, $sampleFileName, "analysis.xml file generated correctly." );
            }
        }

        # Default paths
        {
            my $obj = $CLASS->new( $OPT_HR );
            $obj->{'_fastqUploadDir'} = "$TEMP_DIR";
            my $analysisXml = $obj->_makeFileFromTemplate( $expectData, "analysis.xml", "analysis_fastq.xml.template" );
            {
                is ( $analysisXml, $outFileName, "Appeared to create analysis.xml file");
                ok( (-f $analysisXml),   "Can find analysis.xml file");
                files_eq_or_diff( $analysisXml, $sampleFileName, "analysis.xml file generated correctly." );
            }
        }
    }

    # Tests for run.xml
    {
        my $sampleFileName = File::Spec->catfile( "t", "Data", "run.xml" );
        my $outFileName = File::Spec->catfile( "$TEMP_DIR", "run.xml" );
        my $templateFileName = File::Spec->catfile($OPT_HR->{'templateBaseDir'}, $OPT_HR->{'xmlSchema'}, "run_fastq.xml.template" );

        # Files for use by test are available
        {
            ok( (-f $templateFileName), "Can find run.xml template file");
            ok( (-f $sampleFileName), "Can find run.xml example file");
        }

        # Absolute paths
        {
            my $obj = $CLASS->new( $OPT_HR );
            my $runXml = $obj->_makeFileFromTemplate( $expectData, $outFileName, $templateFileName );
            {
                is ( $runXml, $outFileName, "Appeared to create run.xml file");
                ok( (-f $runXml),   "Can find run.xml file");
                files_eq_or_diff( $runXml, $sampleFileName, "run.xml file generated correctly." );
            }
        }
 
         # Default paths
        {
            my $obj = $CLASS->new( $OPT_HR );
            $obj->{'_fastqUploadDir'} = "$TEMP_DIR";
            my $runXml = $obj->_makeFileFromTemplate( $expectData, "run.xml", "run_fastq.xml.template" );
            {
                is ( $runXml, $outFileName, "Appeared to create run.xml file");
                ok( (-f $runXml),   "Can find run.xml file");
                files_eq_or_diff( $runXml, $sampleFileName, "run.xml file generated correctly." );
            }
        }
    }

    # Tests for experiment.xml
    {
        my $sampleFileName = File::Spec->catfile( "t", "Data", "experiment.xml" );
        my $outFileName = File::Spec->catfile( "$TEMP_DIR", "experiment.xml" );
        my $templateFileName = File::Spec->catfile($OPT_HR->{'templateBaseDir'}, $OPT_HR->{'xmlSchema'}, "experiment_fastq.xml.template" );

        # Files for use by test are available
        {
            ok( (-f $templateFileName), "Can find experiment.xml template file");
            ok( (-f $sampleFileName), "Can find experiment.xml example file");
        }

        # Absolute paths
        {
            my $obj = $CLASS->new( $OPT_HR );
            my $resultFile = $obj->_makeFileFromTemplate( $expectData, $outFileName, $templateFileName );
            {
                is ( $resultFile, $outFileName, "Appeared to create experiment.xml file");
                ok( (-f $resultFile),   "Can find experiment.xml file");
                files_eq_or_diff( $resultFile, $sampleFileName, "experiment.xml file generated correctly." );
            }
        }
    }

    # Tests for parameters
    {
        # Bad param: $dataHR
        {
            my $obj = $CLASS->new( $OPT_HR );
            eval {
                 $obj->_makeFileFromTemplate();
            };
            {
              like( $@, qr/^_makeFileFromTemplate\(\) missing \$dataHR parameter\./, "Error if no dataHR param");
              is( $obj->{'error'}, 'param__makeFileFromTemplate_dataHR', "Errror tag if no dataHR param");
            }
        }

        # Bad param: $outFile
        {
            my $obj = $CLASS->new( $OPT_HR );
            eval {
                 $obj->_makeFileFromTemplate( $MOCK_DBH );
            };
            {
              like( $@, qr/^_makeFileFromTemplate\(\) missing \$outFile parameter\./, "Error if no outFile param");
              is( $obj->{'error'}, 'param__makeFileFromTemplate_outFile', "Errror tag if no outFile param");
            }
        }
    }

}

sub test__getTemplateDataReadLength {
    plan( tests => 5 );

    my $sampleId = -19;

    my @readLengthDbSession = ({
        'statement'    => qr/SELECT f\.file_path.*/msi,
        'bound_params' => [ $sampleId ],
        'results'  => [ ['file_path'], ["t/Data/toy.bam"], ]
    });

    # Good test
    {
        {
            my $obj = $CLASS->new( $OPT_HR );
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( "ok_getTemplateDataReadLength", @readLengthDbSession );
            $mock_readpipe->{'mock'} = 1;
            $mock_readpipe->{'ret'} = "$MOCK_READ_LENGTH_SAMTOOLS_RETURN";
            {
                my $want = 17; # Length of longest line in $MOCK_READ_LENGTH_SAMTOOLS_RETURN
                my $got = $obj->_getTemplateDataReadLength( $MOCK_DBH, $sampleId );
                my $testName = "smokeTest for _getTemplateDataReadLength";
                is( $got, $want, $testName);
            }
            $mock_readpipe->{'mock'} = 1;
        }

    }

    # Tests for parameters
    {
        # Bad param: $dbh
        {
            my $obj = $CLASS->new( $OPT_HR );
            eval {
                 $obj->_getTemplateDataReadLength();
            };
            {
              like( $@, qr/^_getTemplateDataReadLength\(\) missing \$dbh parameter\./, "Error if no dbh param");
              is( $obj->{'error'}, 'param__getTemplateDataReadLength_dbh', "Errror tag if no dbh param");
            }
        }

        # Bad param: $sampleId
        {
            my $obj = $CLASS->new( $OPT_HR );
            eval {
                 $obj->_getTemplateDataReadLength( $MOCK_DBH );
            };
            {
              like( $@, qr/^_getTemplateDataReadLength\(\) missing \$sampleId parameter\./, "Error if no sampleId param");
              is( $obj->{'error'}, 'param__getTemplateDataReadLength_sampleId', "Errror tag if no sampleId param");
            }
        }
    }

}

sub test__getTemplateDataReadEnds {
    plan( tests => 5 );

    my $experimentId = -5;
    my $readEnds = 2;

    my @readEndsDbSession = ({
        'statement'    => qr/SELECT count\(\*\) as read_ends.*/msi,
        'bound_params' => [ $experimentId ],
        'results'  => [ ['read_ends'], [$readEnds], ]
    });

    # Good test
    {
        {
            my $obj = $CLASS->new( $OPT_HR );
            $MOCK_DBH->{'mock_session'} = DBD::Mock::Session->new( "ok_getTemplateDataReadEnds", @readEndsDbSession );
            {
                my $want = $readEnds;
                my $got = $obj->_getTemplateDataReadEnds( $MOCK_DBH, $experimentId );
                my $testName = "smokeTest for _getTemplateDataReadEnds";
                is( $got, $want, $testName);
            }
        }

    }

    # Tests for parameters
    {
        # Bad param: $dbh
        {
            my $obj = $CLASS->new( $OPT_HR );
            eval {
                 $obj->_getTemplateDataReadEnds();
            };
            {
              like( $@, qr/^_getTemplateDataReadEnds\(\) missing \$dbh parameter\./, "Error if no dbh param");
              is( $obj->{'error'}, 'param__getTemplateDataReadEnds_dbh', "Errror tag if no dbh param");
            }
        }

        # Bad param: $experimentId
        {
            my $obj = $CLASS->new( $OPT_HR );
            eval {
                 $obj->_getTemplateDataReadEnds( $MOCK_DBH );
            };
            {
              like( $@, qr/^_getTemplateDataReadEnds\(\) missing \$experimentId parameter\./, "Error if no experimentId param");
              is( $obj->{'error'}, 'param__getTemplateDataReadEnds_experimentId', "Errror tag if no experimentId param");
            }
        }
    }

}