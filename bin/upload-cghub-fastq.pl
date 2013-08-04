#! /usr/bin/env perl

use warnings;
use strict;

use Bio::SeqWare::Uploads::CgHub::Fastq;

# TODO: consider allow pre-parsing cli parameters for config file name
my $configParser = Bio::SeqWare::Config->new();
my $configOptions = $configParser->getAll();

my $opt = $class->_process_command_line( $configOptions );
my $instance = Bio::SeqWare::Uploads::CgHub::Fastq->new( $opt );

$instance->run();

