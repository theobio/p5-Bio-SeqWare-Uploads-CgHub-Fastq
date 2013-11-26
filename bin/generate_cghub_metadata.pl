#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;
use Template;
use Data::Dumper;
use File::Path;
use POSIX;

#
# CGHub xml generation script originally based upon analysis_hack.pl
# This is currently only tested with V1 output.
#
# The following changes will be needed for V2 (at a minimum):
#   Double check SEQ_LABEL in analysis.xml (chrM at least will change)
#   Change Read group label 

my ( $template_dir,   # Filename for the template.
     $output_dir,     # Full path filename to write out to.
     $error_file,     # File in which to record problem samples
     $samtools_exec,  # Samtools executable
     $algo,           # Algorithm
     $v1_paired_transcriptome, # Is this v1 paired transcriptome?
     $no_prior_check,
     $v2_single_end,  # Is this v2 single end?
);

# Required: database to read info from
my ( $username,         # Database user
     $password,         # Database password
     $dbhost,           # Database host
     $seqware_meta_db,  # Database name with meta_db information
);

my ( $workflow_accession,  # Seqware accession id for this workflow (version specific)  
);

# Optional: Various controls.
my ( $help,     # Flag to indicate help and quit
     $quiet,    # Print nothing, just do.
     $dummy_aliquot_id, # If specified, generate a "dummy" aliquot uuid
     $skip_file_check, # If specified, skip check for file existence
);

my $argSize      = scalar( @ARGV );

my $getOptResult = GetOptions(
    'template-dir=s'        => \$template_dir,
    'output-dir=s'          => \$output_dir,
    'username=s' => \$username,
    'password=s' => \$password,
    'dbhost=s'   => \$dbhost,
    'db=s'       => \$seqware_meta_db,
    'workflow_accession=s' => \$workflow_accession,
    'help'     => \$help,
    'dummy-aliquot-id'    => \$dummy_aliquot_id,
    'skip-file-check' => \$skip_file_check,
    'error-file=s' => \$error_file,
    'samtools=s' => \$samtools_exec,    
    'algo=s' => \$algo,
    'v1_paired_transcriptome' => \$v1_paired_transcriptome,
    'no_prior_check' => \$no_prior_check,
    'v2-single-end' => \$v2_single_end,
);

if ( !(defined($template_dir)) ||
     !(defined($output_dir)) ||
     !(defined($username)) ||
     !(defined($password)) ||
     !(defined($dbhost)) ||
     !(defined($seqware_meta_db)) ||
     !(defined($workflow_accession)) ||
     !(defined($error_file)) ||
     !(defined($samtools_exec)) ||
     !(defined($algo)) ||
     $help ) {
    usage();
}

# Make sure output dir is absolute
if (!($output_dir =~ m/^\//)) {
    print "output-dir must be an absolute path.\n";
    exit(1);
}

# Connect to db
my $dbn = "DBI:Pg:dbname=$seqware_meta_db;host=$dbhost";
my $database=DBI->connect( $dbn, $username, $password, {RaiseError => 1} );

# Get workflow info
my $workflow_sth = $database->prepare(
    "SELECT
        name, version
    FROM
        workflow
    WHERE
        sw_accession = ?");

$workflow_sth->execute($workflow_accession);
my ($workflow, $workflow_version) = $workflow_sth->fetchrow_array;
$workflow_sth->finish();

my $isV2 = 0;

if ($v1_paired_transcriptome) {
	$template_dir = "$template_dir/v1_paired_transcriptome";
} elsif ($v2_single_end) {
    $template_dir = "$template_dir/v2_single_end";
} elsif ($workflow eq "MapspliceRSEM") {
    $isV2 = 1;
    $template_dir = "$template_dir/v2";
} else {
    $template_dir = "$template_dir/v1";
}

# Get sample / lane info
my $sth1 = $database->prepare(
    "SELECT
        s.sample_id, l.sw_accession, s.tcga_uuid
    FROM
        sample AS s, lane AS l, sequencer_run AS sr
    WHERE
        sr.name = ?
        AND sr.sequencer_run_id = l.sequencer_run_id
        AND l.lane_index = ?
        AND l.sample_id = s.sample_id
        AND s.title = ?
        AND (l.barcode = ? or l.barcode is null)"
);

# Check for prior upload
my $upload_sth = $database->prepare("SELECT count(*) from upload where sample_id = ? and target = 'CGHUB'");

# Bam file info
my $sth2 = $database->prepare(
    "SELECT
        file_path, file_sw_accession, tstmp, md5sum, file_id
     FROM
        vw_files
     WHERE 
        algorithm = '$algo' 
        AND meta_type = 'application/bam'
        AND flowcell = ?
        AND (lane_index + 1) = ?
        AND (barcode = ? or barcode is null) 
        AND workflow_accession = $workflow_accession
     ORDER BY tstmp ");

# Globally unique id.
my $sth3 = $database->prepare(
    "SELECT nextval('sw_accession_seq');"
);

# Upload sequence
my $upload_seq_sth = $database->prepare(
    "SELECT nextval('upload_upload_id_seq');"
);

# Upload table insert
my $upload_insert_sth = $database->prepare(
    "INSERT INTO upload (upload_id, sample_id, target, status, cghub_analysis_id, metadata_dir) VALUES (
     ?,
     ?,
     'CGHUB',
     'METADATA_GENERATED',
     ?,
     '$output_dir')");

# upload_file table insert
my $upload_file_insert_sth = $database->prepare("INSERT INTO upload_file (upload_id, file_id) VALUES (?, ?)");

my $tstmp = strftime("%Y_%m_%d_%H_%M_%S", localtime);

my @invalid_samples;

# Loop over STDIN input                  
while(<STDIN>) {
  chomp;
  next if (/^#/);
  my ($sample, $flowcell, $lane, $barcode) = split( /\s+/ );

  if (!(defined($barcode))) {
    $barcode = "";
  }

  my $sample_descriptor = "$sample\t$flowcell\t$lane\t$barcode";

  print "\nProcessing: $sample_descriptor\n";

  my $lane_index = $lane - 1;

  # Get sample information
  $sth1->execute($flowcell, $lane_index, $sample, $barcode);
  my ($sample_id, $lane_accession, $sample_uuid) = $sth1->fetchrow_array;

  if (!defined($sample_id) || "" eq $sample_id) {
    print "Unable to locate sample in db: $sample_descriptor\n";
    push(@invalid_samples, $sample_descriptor);
    $sth1->finish();
    next;
  }

  if ($sth1->fetchrow_array) {
    print "ERROR!  Multiple samples returned for: $sample_descriptor\n";
    exit(1);
  }
  $sth1->finish();

  if (!$no_prior_check) {
	  $upload_sth->execute($sample_id);
	  my ($upload_count) = $upload_sth->fetchrow_array;
	  $upload_sth->finish();
	 #George test for kirc 
	  #if ($upload_count > 0) {
	  #  print "Prior upload record exists for: $sample_descriptor\n";
	  #  push(@invalid_samples, $sample_descriptor);
	  #  next;
	  #}
  }

  if ($dummy_aliquot_id) {
    $sample_uuid = getUuid();
  } else {
    if (!defined($sample_uuid)) {
        print "UUID missing for: $sample_descriptor\n";
        push(@invalid_samples, $sample_descriptor);
        next;
    }
  }

  my $analysis_id = getUuid();

  my $sample_dir = "$output_dir/$analysis_id";

  mkpath($sample_dir, { mode => 0775 }) or die "$! Unable to create $sample_dir";

  print "Sample ID: $sample_id\n";

  # Now find the BAM file and get its xml data

  # algo: 'ConvertBAMTranscript2Genome' or 'samtools-sort-genome'
  $sth2->execute($flowcell, $lane, $barcode);

  my ($path, $swid, $date, $md5sum, $fileId) = $sth2->fetchrow_array;

  if (!defined($path)) {
    print "No file in db for $sample_descriptor\n";
    push(@invalid_samples, $sample_descriptor);
    $sth2->finish();
    next;
  }

  if ($sth2->fetchrow_array) {
    print "ERROR!  Multiple files returned for: $sample_descriptor\n";
#    exit(1);
    next;
  }

  $sth2->finish();

  if (!($skip_file_check) && (!(-e $path))) {
    print "File does not exist for $sample_descriptor.  [$path]\n";
    push(@invalid_samples, $sample_descriptor);
    next;
  }

  $path =~ /([^\/]+)$/;
  my $file = $1;
  my $file_base = $1;
  $file_base =~ /(.*)\.[^.]+$/;
  $file_base = $1;

  # Convert date time to xs:dataTime, needs T separating date and time, not space.
  $date =~ s/ /T/;

  # Get unique key number
  $sth3->execute();
  my ( $upload_accession ) = $sth3->fetchrow_array;
  $sth3->finish();

  my $tcga_file_name = "UNCID_$swid.$sample_uuid.$file";

  my $sample_link = "$sample_dir/$tcga_file_name";

  system("ln -s $path $sample_link");

  # Get read group from bam file (assumes 1 read group defined)
  my $read_group = "UNDEFINED";

  my $read_len = 0;

  if (!$skip_file_check) {
      $read_group = `$samtools_exec view -H $sample_link | grep \@RG | cut -f 2 | cut -d : -f 2` or die $!;
      chomp($read_group);

      # If a newline is in the output, we may have multiple read groups
      my $containsNewline = $read_group =~ m/\n/;

      if (($containsNewline) || ($read_group eq "")) {
        print "Invalid read group for: $sample_descriptor\n";
        push(@invalid_samples, $sample_descriptor);
        next;
      }

      $read_len = getReadLength($samtools_exec, $sample_link);

      if ($read_len < 1) {
        print "Invalid read length for: $sample_descriptor\n";
        push(@invalid_samples, $sample_descriptor);
        next;
      }
  }

  # Add hash with all data onto the array
  my @analysis_set;
  push( @analysis_set, {
    run => $flowcell,
    read_group => $read_group,
    lane => $lane,
    TCGA_id => $sample_uuid,
    lane_sw_accession => $lane_accession,
    upload_accession => $upload_accession,
    file => $file,
    file_no_ext => $file_base,
    file_sw_accession => $swid,
    analysis_date => $date,
    workflow_accession => $workflow_accession,
    workflow => $workflow,
    workflow_version => $workflow_version,
    workflow_algorithm => $algo,
    checksum => $md5sum, });

  buildAnalysisXml($sample_dir, @analysis_set);

  my $experiment_ref = buildExperimentXml($sample_id, $sample_uuid, $sample_dir, $isV2, $sample_descriptor, $read_len);

  buildRunXml($experiment_ref, $lane_accession, $sample_dir);

  if (!($skip_file_check) && (!($dummy_aliquot_id))) {
    updateUploadInfo($sample_id, $analysis_id, $fileId);
  }

  # Grant read/write access to sample dir for group.
  system ("chmod -R 0770 $sample_dir");

} # End loop over input file

# Grant read/write access to output dir
system ("chmod 0770 $output_dir");

recordErrorSamples();

$database->disconnect();

sub usage {
  print "usage: cat [LIST OF SAMPLE TITLES] | $0 [--username USERNAME] [--password PASSWORD] [--dbhost DBHOST] [--db SEQWARE_META_DB] [--template-dir <path_to_template_dir>] [--output-dir <path_to_output_dir>] [--workflow_accession <workflow accession>] [--error-file <path to error sample file output>] [--samtools <path to samtools>] [--algo <file algorithm>] [--dummy-aliquot-id] [--skip-file-check]\n";
  exit 0;
}

sub getUuid {
    my $uuid = `uuidgen`;
    chomp($uuid);
    return $uuid;
}

sub buildAnalysisXml {
    my ($sample_dir, @analysis_set) = @_;

    my $data = { analysis_set => \@analysis_set };

#       print( Dumper( $data ));

    my $templater = Template->new({
        ABSOLUTE => 1,
    });

    $templater->process( "$template_dir/analysis.xml", $data, "$sample_dir/analysis.xml" )
        || die $templater->error(), "\n";
}

sub buildRunXml {
    my ($experiment_ref, $lane_accession, $sample_dir) = @_;

    my @run_set;

    my $run_templater = Template->new({
        ABSOLUTE => 1,
    });

    # Add hash with all data onto the array (one element for each input file)
    push( @run_set, {
        lane_accession => $lane_accession,
        experiment_ref => $experiment_ref,
    });

    my $run_data = { run_set => \@run_set };

    # Merge the template and the data
    $run_templater->process( "$template_dir/run.xml", $run_data, "$sample_dir/run.xml" )
        || die $run_templater->error(), "\n";
}

sub buildExperimentXml {
    my ($sampleId, $sample_uuid, $sample_dir, $isV2, $sample_descriptor, $read_len) = @_;

    # Retrieve experiment / sample / platform info  
    my $exp_sth = $database->prepare(
        "SELECT
            e.experiment_id,
            e.description,
            e.sw_accession as experiment_accession,
            s.sw_accession as sample_accession,
            p.instrument_model
         FROM
            experiment e, sample s, platform p
         WHERE
            s.sample_id = ? and
            e.experiment_id = s.experiment_id and
            e.platform_id = p.platform_id");

    $exp_sth->execute($sampleId);

    my ($experiment_id, $description, $experiment_accession, $sample_accession, $instrument_model) = $exp_sth->fetchrow_array;

    # Identify library layout (paired or single)
    my $num_reads_sth = $database->prepare("
        select
            count(*) 
        from 
            experiment_spot_design_read_spec spec, experiment_spot_design design, experiment
        where 
            experiment.experiment_id = ? and
            experiment.experiment_spot_design_id = design.experiment_spot_design_id and
            spec.experiment_spot_design_id = design.experiment_spot_design_id and
            read_class = 'Application Read' and read_type != 'BarCode'");

    $num_reads_sth->execute($experiment_id);

    my $library_layout;
    my ($num_reads) = $num_reads_sth->fetchrow_array;

    if ($num_reads == 1) {
        if ($isV2) {
            print "\n\nERROR!  Invalid attempt to generate single end metadata for V2 pipeline: $sample_descriptor.\n";
            exit(1);
        }
        $library_layout = "SINGLE";
    } elsif ($num_reads == 2) {
        if (!$isV2 && !$v1_paired_transcriptome) {
            print "\n\nERROR!  Invalid attempt to generate paired end metadata for V1 pipeline: $sample_descriptor.\n";
            exit(1);
        }
        $library_layout = "PAIRED";
    } else {
        print "Error!  Invalid number of reads: $num_reads for sample id: $sampleId\n";
        exit;
    }

    my @experiment_set;

    my $exp_templater = Template->new({
        ABSOLUTE => 1,
    });

    my $experiment_ref = "UNCID:$experiment_accession-$sample_accession"; 

    # Add hash with all data onto the array
    push( @experiment_set, {
        description => $description,
        experiment_ref => $experiment_ref,
        sample_uuid => $sample_uuid,
        library_layout => $library_layout,
        instrument_model => $instrument_model,
        read2_base_coord => $read_len+1,
    });

    my $exp_data = { experiment_set => \@experiment_set };

    # Merge the template and the data
    $exp_templater->process( "$template_dir/experiment.xml", $exp_data, "$sample_dir/experiment.xml" )
        || die $exp_templater->error(), "\n";

    return $experiment_ref;
}

# Write error samples to a file
sub recordErrorSamples {
    print "\nWriting error samples to: $error_file\n";

    open ERRORS, ">>$error_file" or die "can't open '$error_file': $!";
    foreach (@invalid_samples) {
        print ERRORS $_ . "\n";
    }
    close ERRORS; 
}

# Insert tracking information into upload and upload_file tables.
sub updateUploadInfo {
    my ($sampleId, $analysis_uuid, $fileId) = @_;

    $upload_seq_sth->execute();
    my ($uploadId) = $upload_seq_sth->fetchrow_array;
    $upload_seq_sth->finish();

    $upload_insert_sth->execute($uploadId, $sampleId, $analysis_uuid);
    $upload_insert_sth->finish();

    $upload_file_insert_sth->execute($uploadId, $fileId);
    $upload_file_insert_sth->finish();
}

# Examine first 1000 lines of BAM file looking for max read length.
sub getReadLength {
    my ($samtools_exec, $sample_link) = @_;

    my $read_str = `$samtools_exec view $sample_link | head -1000 | cut -f 10` or die $!;

    my @reads = split (/\n/, $read_str);

    my $max_length = 0;
    foreach my $read (@reads) {
        my $length = length($read);
        if ($length > $max_length) {
            $max_length = $length;
        }
    }

    print "Calculated read length of: $max_length\n";

    return $max_length;
}