#! /usr/bin/env perl

use Carp;                 # Caller-relative error messages

use Test::More 'tests' => 1 + 2;   # Main testing module; run this many subtests
                                   # in BEGIN + subtests (subroutines).

BEGIN {
	use_ok( 'Bio::SeqWare::Uploads::CgHub::Fastq' );
}

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';

# Create default object for testing
my $FILE_OBJ = $CLASS->new();

subtest( 'new()' => \&testNew );
subtest( 'new(BAD)' => \&testNewBAD );

sub testNew {
    plan( tests => 1 );
    {
	    ok($FILE_OBJ, "New object created ok");
	}
}

sub testNewBAD {
	plan( tests => 1 );
    {
        eval{ $CLASS->new( "BAD_PARAM"); };
        $got = $@;
        $want = qr/^No parameter is allowed\./;
        like( $got, $want, "error with bad param");
    }
}