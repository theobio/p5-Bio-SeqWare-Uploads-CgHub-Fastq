#!perl -T
use 5.008;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Bio::SeqWare::Uploads::CgHub::Fastq' ) || print "Bail out!\n";
}

diag( "Testing Bio::SeqWare::Uploads::CgHub::Fastq $Bio::SeqWare::Uploads::CgHub::Fastq::VERSION, Perl $], $^X" );
