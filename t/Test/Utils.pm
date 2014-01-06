package Test::Utils;

use strict;
use warnings;

use Test::Builder;
use Sub::Exporter -setup => { exports => [
     'error_tag_ok',
     'dbMockStep_Begin',
     'dbMockStep_Commit',
     'dbMockStep_Rollback',
     'dbMockStep_SetTransactionLevel',
] };

my $Test = Test::Builder->new();

#
# TESTS
#

sub error_tag_ok {
    my $ok;
    my ($obj, $expectError, $why) = @_;
    my $objType = ref($obj);
    if (! $objType eq 'Bio::SeqWare::Uploads::CgHub::Fastq') {
        diag( "\$obj must be of type Bio::SeqWare::Uploads::CgHub::Fastq, not $objType" );
        return $ok;
    }
    $objType = ref($expectError);
    if ($objType && $objType ne ref qr//) {
        diag( "\$expectError must be a string, or a regular expression like qr/.../." );
        return $ok;
    }

    $why = "Incorrect error tag when $why";
    # Checking string:
    if (! ref($expectError)) {
        $ok = $Test->is_eq( $obj->{'error'}, $expectError, $why);
    }
    # Checking regexp
    else {
        $ok = $Test->like( $obj->{'error'}, $expectError, $why);
    }

    return $ok;
}

#
# Mock DB Helpers
#

sub dbMockStep_Begin {
    return {
        'statement' => 'BEGIN WORK',
        'results'   => [ [] ],
    };
}

sub dbMockStep_SetTransactionLevel {
    return {
        'statement' => 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
        'results'  => [ [] ],
    };
}

sub dbMockStep_Commit {
    return {
        'statement' => 'COMMIT',
        'results'   => [ [] ],
    };
}

sub dbMockStep_Rollback {
    return {
        'statement' => 'ROLLBACK',
        'results'   => [ [] ],
    };
}
