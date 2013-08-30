#! /usr/bin/env perl

use strict;
use warnings;
use Carp;                 # Caller-relative error messages
use Data::Dumper;         # Quick error messages
use File::Temp;           # Simple files for testing

use Bio::SeqWare::Config; # Access SeqWare settings file as options
use Bio::SeqWare::Db::Connection;

use DBD::Mock;
use Test::More 'tests' => 1 + 6;   # Main testing module; run this many subtests
                                     # in BEGIN + subtests (subroutines).


BEGIN {
	*CORE::GLOBAL::readpipe = \&mock_readpipe; # Must be before use
	use_ok( 'Bio::SeqWare::Uploads::CgHub::Fastq' );
}

my $mock_readpipe = { 'mock' => 0, 'exit' => 0, 'ret' => undef };

sub mock_readpipe {
    my $var = shift;
    my $retVal;
    if ( $mock_readpipe->{'mock'} && $mock_readpipe->{'exit'}) {
        $? = $mock_readpipe->{'exit'};
        $retVal = undef;  # Just to be definite
    }
    elsif ($mock_readpipe->{'mock'} && ! $mock_readpipe->{'exit'}) {
        $retVal = $mock_readpipe->{'ret'};
    }
    else {
        $retVal = CORE::readpipe($var);
    }
    return $retVal;
}

my $CLASS = 'Bio::SeqWare::Uploads::CgHub::Fastq';


my $CONFIG = Bio::SeqWare::Config->new();
my $OPT = $CONFIG->getKnown();
my $OPT_HR = { %$OPT,
    'runMode' => 'alL',
};

my $OBJ = $CLASS->new( $OPT_HR );

# Keeping in case enable test DB in future.
my $MOCK_DBH = DBI->connect(
    'DBI:Mock:',
    '',
    '',
    { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1 },
);

#
# if ( ! $ENV{'DB_TESTING'} ) {
# 	diag( 'skipping 2 test that requires DB_TESTING' );
# }
# else {
#     my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $CONFIG );
#     $DBH = $connectionBuilder->getConnection( {'RaiseError' => 1, 'AutoCommit' => 1} );
# }
#

# Class methods
subtest( 'new()'             => \&testNew              );
subtest( 'new(BAD)'          => \&testNewBad           );
subtest( 'getFileBaseName()' => \&test_getFileBaseName );
subtest( 'getUuid()'         => \&test_getUuid         );

# Object methods
subtest( 'getAll()'            => \&testGetAll );
subtest( 'run()'               => \&testRun );

$MOCK_DBH->disconnect();

sub testNew {
    plan( tests => 2 );
    {
	    ok($OBJ, "Default object created ok");
	}
    {
        my $opt = {
            'runMode' => 'ALL',
        };
        my $obj = $CLASS->new( $opt );
        $opt->{'runMode'} = "OOPS";
        my $got = $obj->getAll();
        my $want = {
            'runMode' => 'ALL',
            'myName'  => 'upload-cghub-fastq_0.0.1',
            'error'   => undef,
        };
	    is_deeply($got, $want, "Default object created saftley");
	}
}

sub testNewBad {
	plan( tests => 2 );
    {
        eval{ $CLASS->new(); };
        my $got = $@;
        my $want = qr/^A hash-ref parameter is required\./;
        like( $got, $want, "error with no param");
    }
    {
        eval{ $CLASS->new( "BAD_PARAM"); };
        my $got = $@;
        my $want = qr/^A hash-ref parameter is required\./;
        like( $got, $want, "error with non hash-ref param");
    }
}

sub test_getFileBaseName {
	plan( tests => 35 );

    # Filename parsing
    is_deeply( [$CLASS->getFileBaseName( "base.ext"      )], ["base", "ext"        ], "base and extension"         );
    is_deeply( [$CLASS->getFileBaseName( "base.ext.more" )], ["base",    "ext.more"], "base and extra extension"   );
    is_deeply( [$CLASS->getFileBaseName( "baseOnly"      )], ["baseOnly", undef    ], "base only"                  );
    is_deeply( [$CLASS->getFileBaseName( "base."         )], ["base",     ""       ], "base and dot, no extension" );
    is_deeply( [$CLASS->getFileBaseName( ".ext"          )], ["",         "ext"    ], "hidden = extension only"    );
    is_deeply( [$CLASS->getFileBaseName( "."             )], ["",          ""      ], "just dot"                   );
    is_deeply( [$CLASS->getFileBaseName( ""              )], ["",          undef   ], "empty"                      );

    # Path parsing
    is_deeply( [$CLASS->getFileBaseName( "dir/to/base.ext"       )], ["base", "ext" ], "relative dir to file"   );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/to/base.ext"  )], ["base", "ext" ], "abssolute dir to file"  );
    is_deeply( [$CLASS->getFileBaseName( "is/dir/base.ext/"      )], ["",     undef ], "relative dir, not file" );
    is_deeply( [$CLASS->getFileBaseName( "/is/abs/dir/base.ext/" )], ["",     undef ], "absolute dir, not file" );
    is_deeply( [$CLASS->getFileBaseName( "is/dir/base.ext/."     )], ["",     undef ], "relative dir, ends with /." );
    is_deeply( [$CLASS->getFileBaseName( "/is/abs/dir/base.ext/.")], ["",     undef ], "absolute dir, ends with /." );

    # Undefined input
    eval {
         $CLASS->getFileBaseName();
    };
    like( $@, qr/^ERROR: Undefined parmaeter, getFileBaseName\(\)\./, "Undefined param");

    # Filename parsing with spaces
    is_deeply( [$CLASS->getFileBaseName( " . "   )], [" ", " "  ], "base and extension are space"            );
    is_deeply( [$CLASS->getFileBaseName( " . . " )], [" ", " . "], "base and each extra extension are space" );
    is_deeply( [$CLASS->getFileBaseName( " "     )], [" ", undef], "base only, is space"                     );
    is_deeply( [$CLASS->getFileBaseName( " ."    )], [" ", ""   ], "base as space and dot, no extension"     );
    is_deeply( [$CLASS->getFileBaseName( ". "    )], ["",  " "  ], "hidden space = extension only"           );

    # Path parsing
    is_deeply( [$CLASS->getFileBaseName( "dir/to/ . "           )], [" ",    " "   ], "relative path, files are space"  );
    is_deeply( [$CLASS->getFileBaseName( "dir/ /base.ext"       )], ["base", "ext" ], "relative path with space"        );
    is_deeply( [$CLASS->getFileBaseName( " /to/base.ext"        )], ["base", "ext" ], "relative path start with space"  );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/to/ . "      )], [" ",    " "   ], "absolute path, files are spacee" );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/ /base.ext"  )], ["base", "ext" ], "absolute path with sapce"        );
    is_deeply( [$CLASS->getFileBaseName( "dir/ /base.ext/"      )], ["",     undef ], "relative dir with space"         );
    is_deeply( [$CLASS->getFileBaseName( " /to/base.ext/"       )], ["",     undef ], "relative dir starts with space"  );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/ /base.ext/" )], ["",     undef ], "absolute dir with space"         );

    # Extra dots
    is_deeply( [$CLASS->getFileBaseName( "base..ext" )], ["base", ".ext" ], "base .. extension"           );
    is_deeply( [$CLASS->getFileBaseName( "base.."    )], ["base", "."    ], "base and .., no extension"   );
    is_deeply( [$CLASS->getFileBaseName( "..ext"     )], ["",     ".ext" ], "no base, .., extension only" );
    is_deeply( [$CLASS->getFileBaseName( ".."        )], ["",     "."    ], "just .."                     );

    # Path parsing with double dots
    is_deeply( [$CLASS->getFileBaseName( "dir/to/.."       )], ["", undef ], "relative path, .. file"  );
    is_deeply( [$CLASS->getFileBaseName( "/abs/dir/to/.."  )], ["", undef ], "absolute path, .. file"  );
    is_deeply( [$CLASS->getFileBaseName( "is/dir/../"      )], ["", undef ], "relative dir, .. as dir" );
    is_deeply( [$CLASS->getFileBaseName( "/is/abs/dir/../" )], ["", undef ], "absolute dir, .. as dir" );

    # Multiple Spaces

}

sub test_getUuid {
	plan( tests => 3 );

    {
        $mock_readpipe->{'mock'} = 1;
        eval {
            $CLASS->getUuid();
        };
        like( $@, qr/ERROR: `uuidgen` failed silently/, "Error on uuidgen returning nothing");
        $mock_readpipe->{'mock'} = 0;
    }
    {
        $mock_readpipe->{'mock'} = 1;
        $mock_readpipe->{'exit'} = 4;
        eval {
            $CLASS->getUuid();
        };
        like( $@, qr/ERROR: `uuidgen` exited with error, exit value was: 4/, "Error on uuidgen returning error status.");
        $mock_readpipe->{'mock'} = 0;
    }
    {
        my $got = $CLASS->getUuid();
        # Note: \A absolute multiline string start, \z absolute end, AFTER last \n, if any.
        like($got, qr/\A[\dA-F]{8}-[\dA-F]{4}-[\dA-F]{4}-[\dA-F]{4}-[\dA-F]{12}\z/i, "Match uuid format, NO TRAILING LINE BREAK")
    }
}

sub testGetAll {
	plan( tests => 2 );
    {
        my $got = $OBJ->getAll();
        my $want = $OPT_HR;
        $want->{'myName'}  = 'upload-cghub-fastq_0.0.1';
        $want->{'error'}   = undef,

        is_deeply( $got, $want, "Get everything expected");
    }
    {
        my $got1 = $OBJ->getAll();
        my $got2 = $OBJ->getAll();
        $got2->{"runMode"} = "OOPS";
        isnt( $got1->{"runMode"}, $got2->{"runMode"}, "Retrieves separate hashs");
    }
}

sub testRun {
	plan( tests => 5 );
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run() };
       my $got = $@;
       my $want = qr/^Can\'t run unless specify a runMode\./;
       like( $got, $want, "error if runMode undefined");
    }
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run( [1,2] ) };
       my $got = $@;
       my $want = qr/^Can\'t run unless specify a runMode\./;
       like( $got, $want, "error if runMode is hash");
    }
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run( "", $MOCK_DBH ) };
       my $got = $@;
       my $want = qr/^Illegal runMode \"\" specified\./;
       like( $got, $want, "error if runMode is empty string");
    }
    {
	   my $obj = $CLASS->new( {} );
       $obj->{'dbh'} = $MOCK_DBH;
	   eval{ $obj->run( "BOB" ) };
       my $got = $@;
       my $want = qr/^Illegal runMode \"BOB\" specified\./;
       like( $got, $want, "error if runMode is unknown");
    }
    {
	   my $obj = $CLASS->new( {} );
	   eval{ $obj->run( "BOB" ) };
       my $got = $@;
       my $want = qr/^Failed to connect to the database/;
       like( $got, $want, "error if no dbh provided and can't be created from input");
    }
}
