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
    build_requires             => {
        'Test::More'           => 0,
        'DBD::Mock'            => '0.018',  # Session objects required
        'Test::File::Contents' => 0,
        'Test::Output'         => 0,
    },
    requires => {
        'perl' => '0.014',        # Safe eval/$@ as throw/catch for exception handling.
        'Bio::SeqWare::Db::Connection'=> '0.000004', # Using corrected compatible object feature.
        'Bio::SeqWare::Config'        => '0.000003', # Added needed keys.
        'DBD::Pg' => 0,
    },
    add_to_cleanup     => [ 'p5-Bio-SeqWare-Uploads-CgHub-Fastq-*' ],
    create_makefile_pl => 'traditional',
    share_dir => 'share',
);

$builder->create_build_script();
