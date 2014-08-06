package Bio::SeqWare::Uploads::CgHub::Bam;

use 5.014;         # Eval $@ safe to use.
use strict;        # Don't allow unsafe perl constructs.
use warnings;      # Enable all optional warnings.
use Carp;          # Base the locations of reported errors on caller's code.

# $Carp::Verbose = 1;
use Data::Dumper;  # Quick data structure printing

use Bio::SeqWare::Db::Connection 0.000002; # Dbi connection, with parameters
use Bio::SeqWare::Uploads::CgHub::Fastq 0.000031; # Uuid generation.

=head1 NAME

Bio::SeqWare::Uploads::CgHub::Bam - Support uploads of bam files to cghub

=cut

=head1 VERSION

Version 0.000.031

=cut

our $VERSION = '0.000031';

=head1 SYNOPSIS

    use Bio::SeqWare::Uploads::CgHub::Bam;

    my $obj = Bio::SeqWare::Uploads::CgHub::Bam->new( $paramHR );
    $obj->run();
    $obj->run( "INIT" );

=cut

=head1 DESCRIPTION

Supports the upload of bam file sets for samples to cghub. Includes
db interactions, and meta-data generation control. The meta-data uploads are a
hack on top of a current implementation.

=head2 Conventions

Errors are reported via setting $self->{'error} and returning undef.

Any run mode can be repeated; they should be self-protecting by persisting
approriate text to the upload record status as <runMode>_<running|completed|failed_<message>>.

Each runmode should support the --rerun flag, eventually. That probably
requires separating the selection and the processing logic, with --rerun only
supported by the processing logic.

=cut

=head1 CLASS METHODS

=cut

=head2 new()

    my $obj = Bio::SeqWare::Uploads::CgHub::Bam->new( $paramHR );

Creates and returns a Bio::SeqWare::Uploads::CgHub::Bam object. Takes
a hash-ref of parameters, each of which is made avaialble to the object.
Don't use parameters beging with a _ (underscore). These may be overwritten.
The parameter 'error' is cleared automatically, 'myName' is set to
"upload-cghub-bam_$VERSION" where version is the version of this module,
like 0.000031"

=cut

sub new {
    my $class = shift;
    my $param = shift;
    unless (defined $param && ref( $param ) eq 'HASH') {
        croak( "A hash-ref parameter is required." );
    }
    my %copy = %$param;
    my $self = {
        'error'  => undef,
        'myName' => 'upload-cghub-bam_' . $VERSION,

        %copy,

        '_uploadUuid' => undef,
        '_uploadId'   => undef,

    };

    bless $self, $class;

    return $self;

}

=head1 INSTANCE METHODS

=cut

=head2 run()

    $obj->run();
  # or
    $obj->run( $runMode );
    # $runMode one of: ZIP META VALIDATE SUBMIT_META SUBMIT_FASTQ LIVE ALL
  # or
    $obj->run( $runMode, $dbh );
  # or
    $obj->run( undef, $dbh );

=cut

sub run {
    my $self = shift;
    my $runMode = shift;
    my $dbh = shift;

    # Validate runMode parameter
    if (! defined $runMode) {
        $runMode = $self->{'runMode'};
    }
    if (! defined $runMode || ref $runMode ) {
        $self->{'error'} = "failed_run_param_mode";
        croak "Can't run unless specify a runMode.";
    }
    $runMode = uc $runMode;

    # Database connection = from param, or else from self, or else get new one.
    if (! defined $dbh) {
        $dbh = $self->{'dbh'};
    }
    if (! defined $dbh ) {
        eval {
            my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $self );
            if (! defined $connectionBuilder) {
                $self->{'error'} = "failed_run_constructing_connection";
                croak "Failed to create Bio::SeqWare::Db::Connection.\n";
            }

            $dbh = $connectionBuilder->getConnection(
                 {'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1, 'ShowErrorStatement' => 1}
            );
        };
        if ($@ || ! $dbh) {
            $self->{'error'} = "failed_run_db_connection";
            croak "Failed to connect to the database $@\n$!\n";
        }

    }

    # Allow UUID to be provided, basically for testing as this is a random value.
    if (! $self->{'_uploadUuid'}) {
        $self->{'_uploadUuid'} = Bio::SeqWare::Uploads::CgHub::Fastq->getUuid();
    }
    if (! $self->{'_uploadUuid'} =~ /[\dA-f]{8}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{12}/i) {
         $self->{'error'} = 'bad_uuid';
         croak( "Not a valid uuid: $self->{'_uploadUuid'}" );
    }
    $self->sayVerbose("Starting run for $runMode.");
    $self->sayVerbose("Analysis UUID = $self->{'_uploadUuid'}.");


    # Run as selected.
    eval {
        if ( $runMode eq "ALL" ) {
            $self->doInit( $dbh );
        }
        elsif ($runMode eq "INIT" ) {
            $self->doInit( $dbh );
        }
        else {
            $self->{'error'} = "failed_run_unknown_run_mode";
            croak "Illegal runMode \"$runMode\" specified.\n";
        }
    };

    if ($@) {
        my $error = $@;
        if ( $self->{'_uploadId'})  {
            if (! $self->{'error'}) {
                $self->{'error'} = 'failed_run_unknown_error';
            }
            eval {
                $self->_updateUploadStatus( $dbh, $self->{'error'} );
            };
            if ($@) {
                $error .= " ALSO: Did not update UPLOAD: $self->{'_uploadId'}\n";
            }
        }
        eval {
            $dbh->disconnect();
        };
        if ($@) {
            $error .= " ALSO: error disconnecting from database: $@\n";
        }
        if (! $self->{'error'}) {
            $self->{'error'} = 'failed_run_unknown_error';
        }
        croak $error;
    }
    else {
        $dbh->disconnect();
        if ($@) {
            my $error .= "$@";
            warn "Problem encountered disconnecting from the database - Likely ok: $error\n";
        }
        $self->sayVerbose("Finishing run for $runMode.");
        return 1;
    }
}

=head2 doInit()

 $obj->doInit( $dbh );

=cut

sub doInit {
    my $self = shift;
    my $dbh = shift;

    unless ($dbh) {
        $self->{'error'} = 'failed_init_param_doInit_dbh';
        croak ("doInit() missing \$dbh parameter.");
    }

    eval {
        $self->_getSelectedBam( $dbh );
    };

    if ($@) {
        my $error = $@;
        if (! $self->{'error'}) {
            $self->{'error'} = 'unknown_error';
        }
        $self->{'error'} = 'failed_init_' . $self->{'error'};
        croak $error;
    }

    return 1;
}



=head2 _updateUploadStatus( ... )

    $self->_updateUploadStatus( $dbh, $uploadId, $newStatus );

Set the status of the internally referenced upload record to the specified
$newStatus string.

=cut

sub _updateUploadStatus {

    my $self = shift;
    my $dbh = shift;
    my $newStatus = shift;

    $self->_parameterDefinedOrCroak($dbh,       'dbh',       '_updateUploadStatus');
    $self->_parameterDefinedOrCroak($newStatus, 'newStatus', '_updateUploadStatus');

    $self->_optionExistsOrCroak( '_uploadId', '_getSelectedBam' );
    my $uploadId = $self->{'_uploadId'};

    my $updateSQL =
        "UPDATE upload
         SET status = ?
         WHERE upload_id = ?";

    eval {
        $dbh->begin_work();
        my $updateSTH = $dbh->prepare($updateSQL); 
        $updateSTH->execute( $newStatus, $uploadId );
        my $rowsAffected = $updateSTH->rows();
        $updateSTH->finish();

        if (! defined $rowsAffected || $rowsAffected != 1) {
            $self->{'error'} = 'update_upload';
            croak "Update appeared to fail.";
        }
        $dbh->commit();
    };
    if ($@) {
        my $error = $@;
        eval {
            $dbh->rollback();
        };
        if ($@) {
            $error .= " ALSO: error rolling back _updateUploadStatus transaction: $@\n";
        }
        if (! $self->{'error'}) {
            $self->{'error'} = 'update_upload'
        }
        croak "Failed to update status of upload record upload_id=$uploadId to $newStatus: $error\n";
    }

    $self->sayVerbose("Set upload status for upload_id $uploadId to \"$newStatus\".");
    return 1;
}

sub _getSelectedBam {

    my $self = shift;
    my $dbh = shift;

    $self->_parameterDefinedOrCroak($dbh, 'dbh', '_getSelectedBam');

    $self->_optionExistsOrCroak( 'sample',     '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'flowcell',   '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'lane',       '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'barcode',    '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'workflowId', '_getSelectedBam' );

    my $sample     = $self->{'sample'};
    my $flowcell   = $self->{'flowcell'};
    my $lane       = $self->{'lane'};
    my $barcode    = $self->{'barcode'};
    my $workflowId = $self->{'workflowId'};

}

sub _optionExistsOrCroak {
    my $self = shift;
    my $option = shift;
    my $subName = shift;  # Optional

    if (! exists $self->{$option}) {
         my $message;
         if (! defined $subName) {
              $message = "Option must exist: \'$option\'.";
              $self->{'error'} = 'missing_option_' . $option;
         }
         else {
              $message = "Sub $subName requires existand of option: \'$option\'.";
              $self->{'error'} = 'sub_' . $subName . '_missing_option_' . $option;
         }
         croak $message;
    }

}

sub _parameterDefinedOrCroak {
    my $self = shift;
    my $param = shift;
    my $paramName = shift;
    my $subName = shift;

    if (! defined $param) {
        $self->{'error'} = 'param_' . $subName . "_" . $paramName;
        croak ($subName . '() missing $' . $paramName . 'parameter.');
    }
}

=head1 AUTHOR

Stuart R. Jefferys, C<< <srjefferys (at) gmail (dot) com> >>

Contributors:
  Lisle Mose (get_sample.pl and generate_cghub_metadata.pl)
  Brian O'Conner

=cut

=head1 DEVELOPMENT

This module is developed and hosted on GitHub, at
L<p5-Bio-SeqWare-Config https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam>.
It is not currently on CPAN, and I don't have any immediate plans to post it
there unless requested by core SeqWare developers (It is not my place to
set out a module name hierarchy for the project as a whole :)

=cut

=head1 INSTALLATION

You can install a version of this module directly from github using

   $ cpanm https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam/archive/v0.000.031.tar.gz

The above installs the latest I<released> version. To install the bleading edge
pre-release, if you don't care about bugs...

   $ cpanm https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam

You can select and download any package for any released version of this module
directly from L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam/releases>.
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

L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam/issues>

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

1; # End of Bio::SeqWare::Uploads::CgHub::Bam
