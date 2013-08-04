package Bio::SeqWare::Uploads::CgHub::Fastq;

use 5.008;         # No reason, just being specific. Update your perl.
use strict;        # Don't allow unsafe perl constructs.
use warnings;      # Enable all optional warnings.
use Carp;          # Base the locations of reported errors on caller's code.
use Bio::SeqWare::Config;   # Read the seqware config file

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
    my $self = \%copy;
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

Looks through the database for fastqs that have not been zipped yet, makes
sure nothing else is in the process of zipping them already, then zips them.
Uses db transactions and marker status to ensure not treading on itself.
'fastq-zip-start', fastq-zip-end'.


=cut

  # BEGIN;
  # SELECT hits FROM webpages WHERE url = '...' FOR UPDATE;
  # -- client internally computes $newval = $hits + 1
  # UPDATE webpages SET hits = $newval WHERE url = '...';
  # COMMIT;

sub doZip() {
    my $self = shift;
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
    my %copy = %$self;
    return \%copy;
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
