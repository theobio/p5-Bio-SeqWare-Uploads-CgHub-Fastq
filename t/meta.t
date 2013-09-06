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
use Bio::SeqWare::Uploads::CgHub::Fastq 0.000002; # Latest dev build.

use Test::Output;               # Test STDOUT and STDERR output.
use DBD::Mock;
use Test::File::Contents;
use Test::More 'tests' => 3;    # Run this many Test::More compliant subtests

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

subtest( 'doMeta()'                => \&test_doMeta );
subtest( '_getTemplateData()'      => \&test__getTemplateData      );
subtest( '_makeFileFromTemplate'   => \&test__makeFileFromTemplate );


sub test_doMeta {
    plan( tests => 5 );

    my $sqlTargetForFastqUpload = 'CGHUB_FASTQ';
    my $oldStatus = "zip_completed";
    my $newStatus = "meta_running";
    my $finalStatus = "meta_completed";

    my $uploadId       = 7851;
    my $fileTimestamp  = "2013-08-14 12:20:42.703867";
    my $sampleTcgaUuid = "66770b06-2cd6-4773-b8e8-5b38faa4f5a4";
    my $laneAccession  = 2090626;
    my $fileAccession  = 2149605;
    my $sampleId  = -19;
    my $fileMd5sum     = "4181ac122b0a09f28cde79a9c3d5af39";
    my $filePath       = "/path/to/file/130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA.fastq.tar.gz";
    my $uploadUuid     = "notReallyTheFastqUploadUuid";

    my $uploadDir = File::Spec->catdir( "$TEMP_DIR", $uploadUuid );
    mkdir($uploadDir);

    my @dbSession = ({
        'statement' => 'BEGIN WORK',
        'results'  => [[]],
    }, {
        'statement'    => qr/SELECT \*/msi,
        'bound_params' => [ $sqlTargetForFastqUpload, $oldStatus ],
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
                'file_timestamp',       'sample_tcga_uuid', 'lane_accession',
                'file_accession',       'file_md5sum',     'file_path',
                'fastq_upload_basedir', 'fastq_upload_uuid',
            ], [
                $fileTimestamp,   $sampleTcgaUuid,     $laneAccession,
                $fileAccession,   $fileMd5sum,         $filePath,
                "$TEMP_DIR",      $uploadUuid
            ]
        ]
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
        {
            is(1, $obj->doMeta( $MOCK_DBH ), "SmokeTest doMeta" );
        }
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
    plan( tests => 13 );

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
    };

    my @dbSession = ({
            'statement'    => qr/SELECT.*/msi,
            'bound_params' => [ $uploadId ],
            'results'  => [
                [
                    'file_timestamp',       'sample_tcga_uuid', 'lane_accession',
                    'file_accession',       'file_md5sum',     'file_path',
                    'fastq_upload_basedir', 'fastq_upload_uuid',
                ], [
                    $fileTimestamp,   $sampleTcgaUuid,     $laneAccession,
                    $fileAccession,   $fileMd5sum,         $filePath,
                    "$TEMP_DIR",      $uploadUuid
                ]
            ]
        });


    {
        my $obj = $CLASS->new( $OPT_HR );
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "Die on missing fastq upload dir", @dbSession );
        my $got = $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        my $want = $expectData;
        {
            is_deeply($got, $want, "Return value correct");
            is( $obj->{'_fastqUploadDir'}, $uploadDir, "Set upload directory");
        }
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
            DBD::Mock::Session->new( "Die on missing fastq upload dir", @dbSession );
        my $data = $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        my $link = File::Spec->catfile( $obj->{'_fastqUploadDir'} , $data->{'upload_file_name'} );
        {
            ok( (-f $link ), "file exists: $link" );
            ok( (-l $link ), "link exists: $link" );
            files_eq( $link, $realFile, "Link points to real file");

        }
        $dbSession[0]->{'results'}->[1]->[5] = "$filePath";
    }
#    {
#         my $obj = $CLASS->new( $OPT_HR );
#         $obj->{'verbose'} = 1;
#         $MOCK_DBH->{'mock_session'} =
#            DBD::Mock::Session->new( "Verbose check", @dbSession );
#        {
#        stdout_is {
#            $obj->_getTemplateData( $MOCK_DBH, $uploadId )
#        } "SQL to get template data:\n"
#     . "SELECT vf.tstmp             as file_timestamp,
#               vf.tcga_uuid         as sample_tcga_uuid,
#               l.sw_accession       as lane_accession,
#               vf.file_sw_accession as file_accession,
#               vf.md5sum            as file_md5sum,
#               vf.file_path,
#               u.metadata_dir       as fastq_upload_basedir,
#               u.cghub_analysis_id  as fastq_upload_uuid
#        FROM upload u, upload_file uf, vw_files vf, lane l
#        WHERE u.upload_id = ?
#          AND u.upload_id = uf.upload_id
#          AND uf.file_id = vf.file_id
#          AND vf.lane_id = l.lane_id"
#        . "\n"
#        . "Template Data:\n"
#        . "\t\"analysis_date\" = \"$xmlTimestamp\"\n"
#        . "\t\"file_accession\" = \"$fileAccession\"\n"
#        . "\t\"file_md5sum\" = \"$fileMd5sum\"\n"
#        . "\t\"file_path_base\" = \"$fileBase\"\n"
#        . "\t\"lane_accession\" = \"$laneAccession\"\n"
#        . "\t\"program_version\" = \"$Bio::SeqWare::Uploads::CgHub::Fastq::VERSION\"\n"
#        . "\t\"sample_tcga_uuid\" = \"$sampleTcgaUuid\"\n"
#        . "\t\"uploadIdAlias\" = \"$uploadIdAlias\"\n"
#        . "\t\"upload_file_name\" = \"$localFileLink\"\n"
#        ,
#        "verbose output";
#        }
#    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        eval {
            $obj->_getTemplateData( $MOCK_DBH, undef );
        };
        {
            like ($@, qr/ERROR: Missing \$uploadId parameter for _getTemplateData/, "fatal without param");
            is ($obj->{'error'}, "bad_get_data_param", "error type for missing parameter");
        }
    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        $dbSession[0]->{'results'}->[1]->[5] = ".fastq.tar.gz";
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "Die on empty value (file_path)", @dbSession );
        eval {
            $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        };
        {
            like ($@, qr/No value obtained for template data element \'file_path_base\'/, "fatal with empty file_path_base.");
            is ($obj->{'error'}, "bad_tempalte_datq", "error type for missing data");
        }
        $dbSession[0]->{'results'}->[1]->[5] = $filePath;
    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        $dbSession[0]->{'results'}->[1]->[4] = undef;
        $MOCK_DBH->{'mock_session'} =
            DBD::Mock::Session->new( "Die on undefined data value (md5Sum)", @dbSession );
        eval {
            $obj->_getTemplateData( $MOCK_DBH, $uploadId );
        };
        {
            like ($@, qr/No value obtained for template data element \'file_md5sum\'/, "fatal with undefined md5Sum.");
            is ($obj->{'error'}, "bad_tempalte_datq", "error type for empty data");
        }
        $dbSession[0]->{'results'}->[1]->[4] = $fileMd5sum;
    }
}

sub test__makeFileFromTemplate {
    plan( tests => 8 );

    # Chosen to match analysis.xml data file
    my $uploadId       = 7851;
    my $fileTimestamp  = "2013-08-14 12:20:42.703867";
    my $sampleTcgaUuid = "66770b06-2cd6-4773-b8e8-5b38faa4f5a4";
    my $laneAccession  = 2090626;
    my $fileAccession  = 2149605;
    my $fileMd5sum     = "4181ac122b0a09f28cde79a9c3d5af39";
    my $filePath       = "/path/to/file/130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA.fastq.tar.gz";

    my $localFileLink  = "UNCID_2149605.66770b06-2cd6-4773-b8e8-5b38faa4f5a4.130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA.fastq.tar.gz";
    my $fileBase       = "130702_UNC9-SN296_0379_AC25KWACXX_6_ACTTGA";
    my $uploadIdAlias  = "upload $uploadId";
    my $xmlTimestamp  = "2013-08-14T12:20:42.703867";

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
    };

    my $sampleFileName = File::Spec->catfile( "t", "Data", "analysis.xml" );
    my $outFileName = File::Spec->catfile( "$TEMP_DIR", "analysis.xml" );
    my $templateFileName = File::Spec->catfile($OPT_HR->{'templateBaseDir'}, $OPT_HR->{'xmlSchema'}, "analysis_fastq.xml.template" );

    {
        ok( (-f $templateFileName), "Can find default template file");
        ok( (-f $sampleFileName), "Can find expected output sample file");
    }

    {
        my $obj = $CLASS->new( $OPT_HR );
        my $analysisXml = $obj->_makeFileFromTemplate( $expectData, $outFileName, $templateFileName );
        {
            is ( $analysisXml, $outFileName, "Appeared to create output file");
            ok( (-f $analysisXml),   "Can find output file");
            files_eq( $analysisXml, $outFileName, "analysis file generated correctly." );
        }
    }
    {
        my $obj = $CLASS->new( $OPT_HR );
        $obj->{'_fastqUploadDir'} = "$TEMP_DIR";
        my $analysisXml = $obj->_makeFileFromTemplate( $expectData, "analysis.xml", "analysis_fastq.xml.template" );
        {
            is ( $analysisXml, $outFileName, "Appeared to create output file");
            ok( (-f $analysisXml),   "Can find output file");
            files_eq( $analysisXml, $outFileName, "analysis file generated correctly." );
        }
    }
}