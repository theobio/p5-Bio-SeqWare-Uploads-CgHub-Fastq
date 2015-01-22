use strict;
use warnings;

use Exception::Class(
  'MyException',
  'AnotherException' => {
      isa => 'MyException',
      description => "Default error message\n",
      fields => ["Bob", "BOBO"],
  },
);

sub MyException::full_message {
    my $self = shift;
    my $msg = $self->message();
    my $exceptionName = blessed $self;
    if (! length $msg) {
        $msg = $exceptionName->description();
    }
    chomp $msg;
    my @keys = $exceptionName->Fields();
    if (@keys){
        my @fields;
        foreach my $key (@keys) {
            my $val = '(undef)';
            if ($self->field_hash()->{$key}) {
                 $val = $self->field_hash()->{$key};
            }
            if (ref $val) {
                $val = Dumper($val);
            }
            my $field = $key . ' = ' . '"' . $val . '"';
            push @fields, $field;
        }
        $msg .= ' - ' . join("; ", @fields);
    }
    return "Error \"$exceptionName\": $msg\n";
}

eval {
    AnotherException->throw();
};

die $@;
