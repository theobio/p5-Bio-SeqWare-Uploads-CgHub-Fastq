#! /usr/bin/env perl

use 5.014;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Bio::SeqWare::Uploads::CgHub::Bam' ) || print "Bail out!\n";
}

diag( "Testing Bio::SeqWare::Uploads::CgHub::Bam $Bio::SeqWare::Uploads::CgHub::Bam::VERSION, Perl $], $^X" );
