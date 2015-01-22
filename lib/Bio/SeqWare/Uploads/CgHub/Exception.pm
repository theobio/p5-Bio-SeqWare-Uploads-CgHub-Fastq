package Bio::SeqWare::Uploads::CgHub::Exception;

use 5.014;         # Eval $@ safe to use.
use strict;        # Don't allow unsafe perl constructs.
use warnings;      # Enable all optional warnings.
use autodie;       # Make core perl die on errors instead of returning undef.

use Data::Dumper;  # Quick data structure printing

use Try::Tiny;

# Non-CPAN modules

=head1 NAME

Bio::SeqWare::Uploads::CgHub::Exception - Data based exceptions

=cut

=head1 VERSION

Version 0.000.031

=cut

our $VERSION = '0.000031';

=head1 SYNOPSIS

    my $exceptionManager = "Bio::SeqWare::Uploads::CgHub::Exception->new()";
    $exceptionMangager->addDefault();
    my myExtraExceptionsHR = {
        "MyException"    => "The message.",
        "BadException\n" => "Message followed by trace info.",
        "Ahhh!\n"        => "It's on FIRE!",
    }
    $exceptionManager->add( myExtraExceptionsHR )
    my $allExceptionsHR = $exceptionManager->getExceptions();
    my $exception = 
    $exceptionManager->die('MyException')