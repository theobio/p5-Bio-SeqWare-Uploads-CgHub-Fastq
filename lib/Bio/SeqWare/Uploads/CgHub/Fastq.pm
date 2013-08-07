package Bio::SeqWare::Uploads::CgHub::Fastq;

use 5.008;         # No reason, just being specific. Update your perl.
use strict;        # Don't allow unsafe perl constructs.
use warnings;      # Enable all optional warnings.
use Carp;          # Base the locations of reported errors on caller's code.
use Bio::SeqWare::Config;   # Read the seqware config file
use Bio::SeqWare::Db::Connection 0.000002; # Dbi connection, with parameters
use Data::Dumper;

=head1 NAME

Bio::SeqWare::Uploads::CgHub::Fastq - Support uploads of fastq files to cghub

=cut

=head1 VERSION

Version 0.000.001   # PRE-RELEASE

=cut

our $VERSION = '0.000001';   # PRE-RELEASE

=head1 SYNOPSIS

    use Bio::SeqWare::Uploads::CgHub::Fastq;

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new();

=cut

=head1 DESCRIPTION

Supports the upload of zipped fastq file sets for samples to cghub. Includes
db interactions, zip command line convienience functions, and meta-data
generation control. The meta-data uploads are a hack on top of a current
implementation, just generates the current version, then after-the-fact
modifies it to do a fastq upload.

=cut

=head1 CLASS METHODS

=cut

=head2 new()

    my $obj = Bio::SeqWare::Uploads::CgHub::Fastq->new();

Creates and returns a Bio::SeqWare::Uploads::CgHub::Fastq object. Takes
no parameters, providing one is a fatal error.

=cut

sub new {
    my $class = shift;
    my $param = shift;
    unless (defined $param && ref( $param ) eq 'HASH') {
        croak( "A hash-ref parameter is required." );
    }
    my %copy = %$param;
    my $self = {
        %copy,
        '_laneId' => undef,
        '_sampleId'    => undef,
        '_zipUploadId' => undef,
    };
    bless $self, $class;
    return $self;
}

=head1 INSTANCE METHODS

=cut

=head2 run()

  $obj->run();
  my @allowedModes = qw( ZIP META VALIDATE UPLOAD ALL ); # Case unimportant
  $obj->run( "all" );

This is the "main" program loop, associated with running C<upload-cghub-fastq>
This method can be called with or without a parameter. If called without a
parameter, it uses the value of the instances runMode property, all allowed
values for that parameter are supported (case insenistive "ZIP", "META",
"VALIDATE", "UPLOAD", "ALL"). Each parameter causes the associated "do..."
method to be invoked, although "ALL"" causes each of the 4 do... methods to be
invoked in order.

This method will either succeed and return 1, or will trigger a fatal exit.

=cut

sub run {
    my $self = shift;
    my $runMode = shift;
    if (! defined $runMode) {
        $runMode = $self->{'runMode'};
    }
    if (! defined $runMode || ref $runMode ) {
        croak("Can't run unless specify a run mode.");
    }
    else {
       $runMode = uc $runMode;
    }
    if ( $runMode eq "ALL" ) {
        $self->run('ZIP');
        $self->run('META');
        $self->run('VALIDATE');
        $self->run('UPLOAD');
    }
    elsif ($runMode eq "ZIP") {
        $self->doZip();
    }
    elsif ($runMode eq "META") {
        $self->doMeta();
    }
    elsif ($runMode eq "VALIDATE") {
        $self->doValidate();
    }
    elsif ($runMode eq "UPLOAD") {
        $self->doUpload();
    }
    else {
        croak("Illegal runMode of \"$runMode\" specified.");
    }
    return 1;
}

=head2 = doZip()

 $obj->doZip();

Performs three steps:

=over

=item 1

Identify a potential lane to zip and insert a new upload record, status =
'zip_candidate' and set internal _zipUploadId value. If none found, return 0.
To be tagged there must be an existing CGHUB upload record with an
external_status = 'live' that is linked to a file record which shows up in
vw_files, and that lane may not be linked to any existing CGHUB_FASTQ record 

=item 2

For the lane zipping, find corresponding file/files on system and zip them. If
zip fails update upload record status = 'zip_error_no_wfID', 'zip_error_missing_fastq'
'zip_error_tiny_fastq', 'zip_error_fastq_md5', 'zip_error_zip_failed',
'zip_error_unknown', return undef.

=item 3

When done, validate output, calculate md5 sum, insert new file record
(that points to workflow_run that generated fastq), and update upload
status to zip_completed. If fails, update upload status=
'zipval_error_missing_zip', 'zipval_error_tiny_zip', 'zipval_error_md5',
'zipval_error_file_insert' 'zipval_error_unknown' and return undef.

=back

Database changes are done as transaction to allow parallel runs of this step.

Upload records are inserted with target = 'CGHUB-zip', external-status of "",
and status = "zip-started"

File records are inserted with links via workflow_run_files and 

=cut


sub doZip {
    my $self = shift;
    my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $self );
    my $dbh = $connectionBuilder->getConnection( {'RaiseError' => 0, 'AutoCommit' => 1} );

    $self->_tagLaneforZipping();
    $self->_startZipping();

    $dbh->disconnect();

    return 1;
}

=head2 _tagLaneforZipping()

Internal method that takes no parmaeters. It returns 0 if no records exist to
update, 1 if successeds. Dies on a bunch of database errors.

Sets private data fields _zipUploadId, _laneId, _sampleId

Inserts a new upload table record for CGHUB_FASTQ, for the same sample
as an existing upload record for CGHUB, when the CGHUB record is for a live
mapsplice upload and no CGHUB_FASTQ exists for that sample.

=cut

sub _tagLaneforZipping {
    my $self = shift;
    my $dbh = shift;

    # First half of transaction, get lane id for possible update.

    my $selectionSQL =
        "-- select lanes with bam files uploaded to cghub.
        SELECT vwf.lane_id, u.sample_id
        FROM vw_files AS vwf, upload_file AS uf, upload AS u
        WHERE vwf.file_id       = uf.file_id
          AND uf.upload_id      = u.upload_id
          AND u.target          = 'CGHUB'
          AND u.external_status = 'live'
          AND vwf.sample_id NOT IN (
              -- Sample not already processed.
              SELECT u.sample_id
              FROM upload AS u
              WHERE u.target      = 'CGHUB_FASTQ'
          ) order by vwf.lane_id DESC limit 1";

    $dbh->begin_work()
            or die $dbh->errstr();  # Autocommit should be on.
    my $selectionSTH = $dbh->prepare($selectionSQL)
            or die $dbh->errstr();
    $selectionSTH->execute()
            or die $selectionSTH->errstr();
    my $row_HR = $selectionSTH->fetchrow_hashref();
    if (! defined $row_HR) {
        if ($selectionSTH->err()) {
            die $selectionSTH->errstr();
        }
        else {
            return 0;  # RETURN - nothing to update.
        }
    }
    $self->{'_laneId'} = $row_HR->{'lane_id'};
    $self->{'_sampleId'} = $row_HR->{'sample_id'};

    # Second half of transaction

    my $insertUploadSQL = 
        "--Inserting a new upload table record and catching the new id.
         --Using postgres-specific extension to get id of inserted record.
         INSERT INTO upload ( sample_id, target, status )
         VALUES ( $self->{'_sampleId'}, 'CGHUB_FASTQ', 'zip_candidate' )
         RETURNING upload_id";

    my $insertSTH = $dbh->prepare($insertUploadSQL)
        or die $dbh->errstr();
    $insertSTH->execute()
        or die $insertSTH->errstr();
    $row_HR = $insertSTH->fetchrow_hashref();
    if (! defined $row_HR) {
        die $insertSTH->errstr();
    }
    $self->{'_zipUploadId'} = $row_HR->{'upload_id'};

    # Done with transaction
    $dbh->commit();

    # Cleanup
    $selectionSTH->finish();
    $insertSTH->finish();

    # Success
    return 1;
}

=head2 = doMeta()

 $obj->doMeta();

=cut

sub doMeta() {
    my $self = shift;
    return 1;

}

=head2 = doValidate()

 $obj->doValidate();

=cut

sub doValidate() {
    my $self = shift;
    return 1;

}

=head2 = doUpload()

 $obj->doUpload();

=cut

sub doUpload() {
    my $self = shift;
    return 1;

}

=head2 = getAll()

  my $settingsHR = $obj->getAll();
  
Retrieve a copy of the properties assoiciated with this object.
=cut

sub getAll() {
    my $self = shift;
    my $copy;
    for my $key (keys %$self) {
        # Skip internal only (begin with "_") properties
        if ($key !~ /^_/) {
            $copy->{$key} = $self->{$key};
        }
    }
    return $copy;
}

=head1 INTERNAL METHODS

NOTE: These methods are for I<internal use only>. They are documented here
mainly due to the effort needed to separate user and developer documentation.
Pay no attention to code behind the curtain; these are not the methods you are
looking for. If you use these function I<you are doing something wrong.>

    NONE

=cut

=head1 AUTHOR

Stuart R. Jefferys, C<< <srjefferys (at) gmail (dot) com> >>

=cut

=head1 DEVELOPMENT

This module is developed and hosted on GitHub, at
L<p5-Bio-SeqWare-Config https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq>.
It is not currently on CPAN, and I don't have any immediate plans to post it
there unless requested by core SeqWare developers (It is not my place to
set out a module name hierarchy for the project as a whole :)

=cut

=head1 INSTALLATION

You can install a version of this module directly from github using

    $ cpanm git://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq.git@v0.000.001

Any version can be specified by modifying the tag name, following the @;
the above installs the latest I<released> version. If you leave off the @version
part of the link, you can install the bleading edge pre-release, if you don't
care about bugs...

You can select and download any package for any released version of this module
directly from L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/releases>.
Installing is then a matter of unzipping it, changing into the unzipped
directory, and then executing the normal (C>Module::Build>) incantation:

     perl Build.PL
     ./Build
     ./Build test
     ./Build install

=cut

=head1 BUGS AND SUPPORT

No known bugs are present in this release. Unknown bugs are a virtual
certainty. Please report bugs (and feature requests) though the
Github issue tracker associated with the development repository, at:

L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Fastq/issues>

Note: you must have a GitHub account to submit issues.

=cut

=head1 ACKNOWLEDGEMENTS

This module was developed for use with L<SegWare | http://seqware.github.io>.

=cut

=head1 LICENSE AND COPYRIGHT

Copyright 2013 Stuart R. Jefferys.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut

1; # End of Bio::SeqWare::Uploads::CgHub::Fastq
