use 5.008;
use strict;
use warnings FATAL => 'all';
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'Bio::SeqWare::Uploads::CgHub::Fastq',
    license             => 'gpl',
    dist_author         => q{Stuart R. Jefferys <srjefferys (at) gmail (dot) com>},
    dist_version_from   => 'lib/Bio/SeqWare/Uploads/CgHub/Fastq.pm',
    release_status      => 'stable',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'perl'=> '0.008',
    },
    add_to_cleanup     => [ 'p5-Bio-SeqWare-Uploads-CgHub-Fastq-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
