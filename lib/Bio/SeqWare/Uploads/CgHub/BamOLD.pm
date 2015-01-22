package Bio::SeqWare::Uploads::CgHub::Bam;

use 5.014;         # Eval $@ safe to use.
use strict;        # Don't allow unsafe perl constructs.
use warnings;      # Enable all optional warnings.
use autodie;       # Make core perl die on errors instead of returning undef.

use Carp;          # Allow errors from caller's perspective.

use Scalar::Util qw( blessed ); # Get object type

# Logging and error reporting

# $Carp::Verbose = 1;  # Enabling makes carp append stacktraces to all message.
use Data::Dumper;  # Quick data structure printing

use Text::Wrap qw( wrap );       # Wrapping of text in paragraphs.
local $Text::Wrap::columns = 132;      #    Wrap at column 132
local $Text::Wrap::huge = 'overflow';  #    Don't break very long words >= 132 characters

use Try::Tiny;

# Non-CPAN modules
use Bio::SeqWare::Db::Connection 0.000002; # Dbi connection, with parameters
use Bio::SeqWare::Uploads::CgHub::Fastq 0.000031; # Uuid generation.

# Configure sayVerbose(). Prepare for migrating to package.
my $VERBOSE_flag_field = 'verbose';
my $VERBOSE_id_sub   = \&getRunUuidTag;
my $VERBOSE_default_id = '';
my $VERBOSE_seq  = 0; # Increased with evey call to sayVerbose;
my $VERBOSE_default_message = "( No message specified. )";

=head1 NAME

Bio::SeqWare::Uploads::CgHub::Bam - Upload a bam file to cghub

=cut

=head1 VERSION

Version 0.000.031

=cut

our $VERSION = '0.000031';

=head1 SYNOPSIS

    use Bio::SeqWare::Uploads::CgHub::Bam;

    my $obj = Bio::SeqWare::Uploads::CgHub::Bam->new( $paramHR );
    $obj->run();
    $obj->run( "INIT" );

=cut

=head1 DESCRIPTION

This is a hard-coded implementation of a "workflow" runner. It codes for just
one workflow: Uploading a bam file to cghub. It uses the "upload" table to
initiate and manage processing. Each workflow run corresponding to one upload
record, with the upload.target field specifying the workflow name. This workflow
is named "CGHUB_BAM".

Each sample will have one bam uploaded, which is one run of this workflow and
hence one record in the upload table. Processing is initiated by inserting an
upload record with appropriate information signalling the start of the workflow.
See the run() method, step START.

A workflow consistis of a linear (non-branching) sequence of steps, each of
which must be completed before the next can run, and none of which interact with
a run of a different workflow (except possibly the first, START step) and none
of which interact with any step of a parallel workflow (expcept possibly for
resource contention). The sequence of steps is hardcoded into the run() method.

Each successive call to this workflows run() method processes one step of one
workflow-run (and hence one record, which also means one sample). run() is called
repeatedly until all necessary processing is complete. For example, by a cron
script. At each call, run() scans the upload table for workflow runs associated
with this workflow (upload records where upload.target is 'CGHUB_BAM'). The
uplaod.status field of the workflow run is used to record the current
state of the workflow (upload) as <STEP>_<status>. As the run
method knows the STEP dependencies for this workflow, it
1. Selects the first workflow_run it finds where <status> = "<PARENT_STEP>_completed"
(and <PARENT_STEP> is not "END", the last step.
2. Changes the upload.status of the the selected run to "<CHILD_STEP>_running".
(The run() method knows the sequence).
3. Runs this "CHILD_STEP" in the workflow and waits for it to complete.
4. Notice if "CHILD_STEP" fails with an error or if it completes succesfully.
5. Changes the uplaod.status of the workflow-run to "<CHILD_STEP>_failed_<ERROR>"
if the step failed, or "<CHILD_STEP>_completed" if it succeeded.

TODO: Provide for archiving workflow runs so they don't have to be inspected
at every run for status once "END" or irrecoverably "failed".

The run method does not "submit" a step to run, but runs it inline, waiting
for it to finsih. The step is hard-coded as a function in this module, by
convention named do<STEP>. When a record is selected to run a step in the
workflow, the run method calls the associated do<STEP> method. It is the success
or failure of this method that determines the resulting status. The do<STEP>
subroutine is responsible for actually executing STEP. It must exit
without error ONLY if the step completed properly, and must exit with error ONLY
if the STEP did not complete properly, hopefully also setting an error tag and
providing a useful error message. The run() subroutine will catch any errors
and change the status of the running record from <CHILD_STEP>_running to
<CHILD_STEP>_failed_<error_tag>. If the error tag is not set by the do<STEP>
function, it will be "?". The run function will then exit, reporing the error
message.

TODO: allow submitting jobs, although require wait for finish.
TODO: Improve submission protocol to remove wait for finish. Allow run to exit
after succesful submission; probably requiring an extension of the process
tracking and a new status.

It is possible to specify the "<CHILD_STEP>" to run(), which will limit its
processing to just workflow-run records with status "<PARENT_STEP>_completed".
This is useful for handling errors and reruns.

It is important that each run step can overlap with a parallel run
step on a different sample. I.E. No step should lock shared resources. Also,
since a step may run for a long time, no step should block the database.
It is not possible for the same record to be selected simultaneously by
multiple run() scans, the database is used to prevent this. Note, however, that
nothing prevents the record from being altered or changed by something else
that doesn't respect the status fields.

Records with status <STEP>_failed_<error_name> will need to be manually
corrected, they will never be selected for running again until either the
upload and upload_file records are deleted, or its status is set to
<STEP>_completed, usually <PARENT_STEP>_completed.

TODO: Implement rerun protocols, requires each step to have a clean<STEP>
function to back it out; assumes backed out in reverse order, must succeed to
advance to previous step. Essentially a backwards workflow.

It is also possible for run() to exit without error but without finishing a
task. (i.e. killed at the process level, the machine it is running on hangs or
crashes, etc). It will leave the record it was working onwith a status
<STEP>_running. As with <STEP>_failed, this will never run again and must be
manually resolved.

TODO: Persist process id's so can auto-fail jobs stuck at running.

Note that this run methodology does not allow branching processes as their is
only one entry per workflow. All branching workflows can be converted to linear
ones by declaring additional dependencies. This has the drawback that failure
of some steps might stop processing of others that don't care about that
failure, but even branching workflows must fail the workflow as a whole if any
branch fails, regardless of those that succeed.

=head2 Conventions

Tasks or runmodes are named in all capital letters, words separated by
underscores.

The first step of a workflow is always "START". The final step is always "END".
Neither of these steps should do any processing. START is used to indicate that
the appropriate parent criteria have been met for this workflow to be started.
For instance, manual QC has been done and upload can start. This workflow will
never create a record with status START, but may change the status of an existing
workflow to start due to a requested rerun. If a step with an invalid or unknown
name is requested, or if the child step to run can not be determined, the
status will be changed to "???"_failed_<ERROR> where <ERROR> is something like:
"unknown_step_<STEP>" or "bad_child_of_<STEP>"

TODO: Implement rerun.

The only statuses for steps currently allowed are "running", "completed", "failed",
and "strange". The "strange" status is to be used in the event that the status
of a workflow_run can not be determined.

Errors are reported from subroutines by setting $self->{'error'} to a short name
(like 'bad_md5') and calling die. THe error handler at the top (run()) level
will try to log a status based on the run mode and the error name (i.e.
INIT_failed_bad_md5 ).

No fields are intented for external use; data is made available or changed by
calling methods.

=head2 Database

This module interacts with a database, and caches the database connection
within itself. If a database connection is provided, it will use that, otherwise
it will create its own. See "new() Parameters" below for details.

=cut


=head1 CLASS METHODS

=cut

=head2 new()

    my $obj = Bio::SeqWare::Uploads::CgHub::Bam->new( $paramHR );

Creates and returns a Bio::SeqWare::Uploads::CgHub::Bam object. Takes
a hash-ref of parameters, each of which is made avaialble to the object.
Parameters are copied by value into this object so creation is
heavy-weight. Copy is not deep, however, so objects pointed to by parameters
should not be changed.

This object provides no fields, only input parameters. Don't access the data
components of the object. Data for use by external programs are made available
by methods.

Don't pass parameters beging with a _ (underscore). These are for internal use
only; they are documented here for developers.

Parameters all have defaults (possibly undefined). If a parameter has a default
of "CFG" that means it is 1. read from the config file by default; 2. overlayed
with any default provided by Bio::SeqWare::Config, and then 3. overlayed with
any parameter passed in to new(). If a parameter has a default of *, that means
the default is complicated and will be described in the following text.

=cut

=head2 new() Parameters

=over 3

=item dbh = *

A DBI database handle object. If provided will be used for all database
activity, otherwise a new connection handle will be obtained by calling $self->_dbh().
The connection handle (passed in or created) will be cached in the object until
this object is DESTROYED(). Upon destruction, any transaction in process will be
rolled back. Any database connection created by _dbh() will be closed.

=item dbUser = CFG

The user for authenticating a connection to the database. Ignored if dbh is
specified.

=item dbPassword = CFG

The password for authenticating a connection to the database (as user dbUser).
Ignored if dbh is specified.

=item dbHost = CFG

Will try to connect to a database server running on this host. Ignored if dbh
is specified.

=item dbSchema = CFG

Will try to connect to this database on the database server. Ignored if dbh is
specified.

=item verbose = undef

If set, will increase details reported while processing.

=back

=cut

=head2 Internal data fields

The following data fields are used for maintaining state information about the
workflow between method calls. They are internal  and are described for developer
useonly. These are not part of the API and may change without notice.

=over 3

=item _uploadId

Identifies the workflow run in progress. It is used as the primary key into the
upload table, records of which correspond to workflow runs. This is saved as the
upload.id field in the db.

No other instance of this module should be running anything against this record.
It is expected that nothing else changes or deletes the corresponding upload
record until processing is done.

=item _uploadUuid

Corresponds to the upload.cghub_analysis_id field in the db. May be created
by this program. Used as an id for reporting associated with the processing
of this record, as part of the directory where metadata id generated into, and as the analysis id as part of the uploaded
data.

=item _workflowName = "CGHUB_BAM"

Corresponds to the upload.target field. The "name" of this workflow. Only
records with where the target field has this value will be inspected or updated.

=item _dbConnectFlags = *

Defaults to { 'RaiseError' => 1, 'PrintError' => 0, 'AutoCommit' => 1,
'ShowErrorStatement' => 1 }. Will be used as the default settings for any
database handle created by this object. Cached database connection will not
be changed.

=item _isMyDbh = undef

Set true (to 1) if $dbh was created here. Set to 0 if $dbh was passed in.

=item _error = undef

The name of the error that caused (or will shortly cause) the run() method to
abort. Setting this has no functional value, it is only used for reporting. It
is cleared every time run() is called.

=item _myName = "upload-cghub-bam_$VERSION"

Copy of the name and version of this module.

=back

=cut

sub new {
    my $class = shift;
    my $paramHR = shift;

    my $self = {};
    bless $self, $class;
    try {
        $self->_initBam($paramHR);
    }
    catch {
        my $caught = $_;
        croak $self->_withError( "ClassInitalizationException", { 'class' => blessed($self), 'exception' => $caught } );
    };
    return $self;
}

sub _initBam {
    my $self = shift;
    my $paramHR = shift;
 
    try {
        unless (defined $paramHR && ref( $paramHR ) eq 'HASH') {
            die $self->_withError('BadParameterException', { 'paramName' => "\$paramHR", 'paramValue' => $paramHR } );
        }
        $self->{'_myName'} = 'upload-cghub-bam_' . $VERSION;
        $self->{'_error'}  = undef;

        $self->{'_dbConnectFlags'} = {
             'RaiseError'         => 1,
             'PrintError'         => 0,
             'AutoCommit'         => 1,
             'ShowErrorStatement' => 1,
        };
        $self->{'_stepAR'} = [
            'START',
            'GENERATE_META', 'VALDIATE_META', 'UPLOAD_META',
            'UPLOAD_BAM', 'CONFIRM_LIVE',
            'END',
        ];
        for my $key (keys %{$paramHR}) {
            $self->{"$key"} = $paramHR->{"$key"};
        }

        $self->_initStepHR();
        $self->_dbh( $self->{'dbh'} );
        $self->_setRunUuid( '00000000-0000-0000-0000-000000000000' );
    }
    catch {
        croak $_;
    };

    return 1;
}


=head1 INSTANCE METHODS

=cut

=head2 DESTROY()

Called automatically upon destruction of this object. Should close the
database handle if opened by this class. Only really matters for error
exits. Planned exists do this manually.

=cut

sub DESTROY {
    my $self = shift;
    if ($self->{'_dbh'}->{'Active'}) {
        unless ($self->{'_dbh'}->{'AutoCommit'}) {
            $self->{'_dbh'}->rollback();
        }
        if ( $self->{'_isMyDbh'} ) {
            $self->{'_dbh'}->disconnect();
        }
    }
}

=head2 sayVerbose()

    my $copy = $self->sayVerbose( $message ).

=over 3

=item DESC
Prints the given message preceeded with a prefix formatted like:

"$id ($seq) [INFO] $timestamp - "

and wrapped to 132 columns with all lines after the first indented with a
"\t". The timestamp is generated by C<Bio::SeqWare::Uploads::CgHub::Fastq->getTimeStamp()>.

=item PARAM $message
The message to print. Will have any trailing whitespace, including newlines,
removed and a single newline will be suffixed before printing. Internal new
lines will be preserved, but following text will be indented by \t.

=back

=cut

sub sayVerbose {
    my $self = shift;

    # Skip unless $verbose flag is set.
    # Delay everything else to lighten if output not wanted.
    unless ( $self->{$VERBOSE_flag_field} ) {
        return;
    }

    # Message $prefix with id, sequence, severity, and timestamp.
    my $id = $VERBOSE_id_sub->($self);
    $VERBOSE_seq += 1;
    my $timestamp = Bio::SeqWare::Uploads::CgHub::Fastq->getTimeStamp();
    my $prefix = "$id ($VERBOSE_seq) [INFO] $timestamp - ";

    # $message; should end in a newline. No need to manage multiple as that
    # is handled on output by wrap.
    my $message = $self->_defaultIfUndef( shift, $VERBOSE_default_message);
    $message .= "\n";
    my $wrappedMessage = wrap( $prefix, "\t", $message );
    print( $wrappedMessage );
    return $wrappedMessage;
}

=head2 _defaultIfUndef

    my $valOrDefault = $self->_defaultIfUndef( $val, $default);

=over 3

=item DESC
Used to provide a default for a value that may be undefined. (Not the same
thing as being false. Returns either the checked value, or the default if the
checked value was undefined. This is equivalent to the new perl // operator.
Can be set to provide cascading defaults by replacing $defaultValue with
another call, i.e. like:

    $self->_defaultIfUndef( $val,
            $self->_defaultIfUndef( $default, $defaultForDefault));

This can be nested for multiple overlapping defaults, returning the first
defined value, possibly false, and, if never getting defined value, possibly
undefined. This will, however, check for defaults from the bottom up, even
when then original value is defined and no default will be needed.

=item PARAM $val
The value that may be undefined and needs a default.

=item PARAM $default
The value that will be used if the checked value is undefined, but NOT if it
is false. It may itself be undefined, in whch case the returned value will
be undefined.

=item RETURNS
If $val is defined, returns $val, otherwise returns $default. I.e returns

   $val // $default

=back

=cut

sub _defaultIfUndef {
    my $self = shift;
    
    my $val = shift;
    if (defined $val) {
        return $val;
    }
    return shift;
}

=head2 run()

=over 3

=item USAGE

    $obj->run();
  # or
    $obj->run( $STEP );
    # $runMode one of: INIT, ALL

=item DESCRIPTION

Runs one step of one workflow run instance. To do this it does:

=over 3

=item 1
Selects a workflow run instance step with status "completed", marking it as
"running" so only this program instance tries to run it.

This is implemented
using the upload record to represent a workflow run instance, with the
upload.target field naming the workflow (i.e. "CGHUB_FASTQ") and the
upload.status field giving the step;status;tag

=item 2
Runs the required code (subroutine) to process this step.

This is implemented as a hash (ref) specifying the function to run for each
step named in the workflow. Defaults to do<StepName>, or as specified in
the passed in workflow description (not implemented yet).

=item 3
Determines if the processing completed succesfully or failed.

This is implemented as a simple error trap. It is up to the code to die with
a fatal error if the generated results are not ok. Note that validation can
be implemented as a spearate workflow step anywhere warranted.

=item 4
Marks the selected workflow run instance step as completed or failed.

Implemented as a change to the selected workflow run instance (upload record)
step/status.

=back

=item PARAM $paramHR
Uses named parameters. If multiple parameters are set, will select only those
that match ALL settings to run.

=over 3

=item step = undef (STRING)
The step to run. Limits selection records to those where the parentOf(step)
has been completed. If not set (undef), then will choose the most advanced
workflow_run instance to run, and the latest one of those by default

=item ignoreCompleteness = 0
Set this to true (1) to make date more important for selecting workfow than
progress (step number). By default the workflow with the highest step number
is preferenctially selected as better to have several workflows finsihed than
many done.

=item oldestFirst = 0
Set this to true (1) to make the oldest workflow the first selected. By default
the newest workflow is selected. This prevents crashing workflow_runs from
holding up all production (i.e. newere, correct workflows will get processed
before again trying to run the failing one.

Note that this is by default secondary to workflow progress, only when there
are more than one workflow tied for farthest along will new or old be
considered.

=item id = undef (INT)
The record to try and run, based in upload_id. It is this record or nothing.

=item uuid = undef (STRING)
The record to try and run, based on the uuid of the run instance. It is this
record or nothing.

=item workflow = "CGHUB_FASTQ" (STRING)
The workflow to select workflow_run instances for. There is no versioning
currently implemented. Change the name if needed. This should probably not
currently change as the workflow is not really generalized yet.

=back

=back


=cut

sub run {
    my $self = shift;
    my $paramHR = shift;

    my $runHR;

    try {
        unless (defined $self && blessed $self) {
            die $self->_withError( 'BadParameterException', { paramName => '$self', paramValue => $self } );
        }
        unless (defined $paramHR && ref( $paramHR ) eq 'HASH') {
            die $self->_withError( 'BadParameterException', { paramName => '$paramHR', paramValue => $paramHR } );
        }

        $self->sayVerbose( "LOOKING FOR RUNNABLE STEP.\n" );
        # Retrieves next run, ensures $runHR is either valid, or undef.
        $runHR = $self->_getNextRun( $paramHR );
        if (! $runHR) {
            $self->sayVerbose( "DONE - Nothing to do.\n" );
        }
        else {
            $self->sayVerbose( 'RUNNING: ' . $self->_getRunDescription($runHR) );
            $runHR = $self->_updateRunStatus( $runHR, { status => "running" } );
            $self->_run( $runHR, $paramHR );
            $runHR = $self->_updateRunStatus( $runHR, { status => "completed" } );
            $self->sayVerbose( 'DONE - COMPLETED: ' . $self->_getRunDescription($runHR) );
        }
        if ($self->{'_myDbh'}) {
            $self->{'_dbh'}->disconnect();
        }
    }
    catch {
        my $caught = $_;
        my $exceptionName = $self->_getExceptionName( $caught );

        # Something died without throwing an error I know how to handle.
        if ( ! $exceptionName ) {
            $self->sayVerbose( "CRASHED - \"$caught\"" );
            die $self->_withError( 'CrashedException',
                    {exception => $caught, runHR => $runHR} );
        }

        # Something died with an error I know I can't log.
        if ( $exceptionName eq 'RunStatusChangeException' ) {
            $self->sayVerbose( "CRASHED - \"$caught\"" );
            die $caught;
        }
        if ( $exceptionName eq 'RunSelectException' ) {
            $self->sayVerbose( "CRASHED - \"$caught\"" );
            die $caught;
        }
        if ( $exceptionName eq 'CrashedException' ) {
            $self->sayVerbose( "CRASHED - \"$caught\"" );
            die $caught;
        }

        try {
            # Trying to handle exception by logging error against run record
            # This is considered a NORMAL EXIT from the runner, although this
            # run failed. This may, of course, also die.
            my $exceptionName = $self->_defaultIfUndef( $exceptionName, "???");
            $runHR = $self->_updateRunStatus({ run => $runHR, status => "failed", error => $caught });
            $self->sayVerbose( 'DONE - FAILED: ' . $self->_getRunDescription($runHR) );
        }
        catch {
            # Nothing left to do, just die whatever error happened.
            my $caughtAgain = $_;
            $self->sayVerbose( "CRASHED - \"$caughtAgain\"" );
            die $self->_withError( 'CrashedException',
                    {exception => $caughtAgain, runHR => $runHR} );
        };
    };

    return 1;
}

sub _completeRun {
    my $self = shift;
    my $runHR = shift;

    $self->sayVerbose( "DONE: " . $self->_getRunDescription() );
}

sub _failRun {
    my $self = shift;
    my $runHr = shift;
    my $errorName = "oops";

    $self->sayVerbose( "FAILED: failed_$errorName" . $self->_getRunDescription() );
}

sub _getNextRun {
    my $self = shift;
    my $paramHR = shift;

    my $step = $paramHR->{'step'} // "";
    unless ( exists $self->{'_stepHR'}->{"$step"} ) {
        UnknownStepException->throw( step => $step );
    }
    my $selectMessage = "Selecting on step to run: \"$step\"\n";
    $self->sayVerbose( "$selectMessage");

    my $runHR;
    return $runHR;
}

sub _getRunDescription {

    my $self = shift;
    my $runHR = shift;

    return "sample_id: $runHR->{'sample_id'}"
                    . ", upload_id: $runHR->{'upload_id'}"
                    . ", uuid: $runHR->{'uuid_id'}"
                    . ", step: $runHR->{'step'}";
}

sub _initRun {
    my $self = shift;
    my $runHR = shift;

    $self->_setRunUuid( $runHR->{'uuid_id'} );
    my $runDescription = $self->_getRunDescription( $runHR );
    $self->sayVerbose( "STARTING: " . $runDescription );
    $self->_updateRunInfo( $runHR, { status => "running"} );
    return 1;
}

sub _logException {
    my $self = shift;
    my $caught = shift;
    my $runHR  = shift;


    return 1;
}


=head2 doInit()

 $obj->doInit( $dbh );

=cut

sub doInit {
    my $self = shift;
    my $dbh = $self->getOption('dbh');

    eval {
        $self->_getSelectedBam( $dbh );
    };

    if ($@) {
        $self->_tagAndDie( 'init', "From Init: " . $@ );
    }
    return 1;
}

=head2 getRunUuid()

    my $uuidForRun = $self->getRunUuid();

Returns the uuid assigned to this workflow_run. As this is persisted across
steps, this is usually read from wherever the workflow_run information is
persisted to. During early parts of a workflow step run, a default
'00000000-0000-0000-0000-000000000000' value is briefly used. This may be used,
for example, in an error message when it is impossible to read the workflow_run
information from the database.

=cut

sub getRunUuid {
    my $self = shift;
    return $self->{'_runUuid'};
}

=head2 getRunUuidTag()

    my $idTag = getRunUuidTag();

Provides a short version of the uuid tag for use in limited scope contexts, such
as error logs. Returns the last 8 characters of the run uuid. Failure to find a
run uuid with 8 or more characters is a fatal error.

=over 3

=item RETURN: $idTag

The last 8 characters of the $runUuid.

=item ERROR: runUuid_invalid

      "Can't get a runUuidTag from: $runUuid"

It is a fatal error if the $runUuid fails to match /(.{8})$/

=back

=cut

sub getRunUuidTag {
    my $self = shift;
    $self->{'_runUuid'} =~ /(.{8})$/
       or $self->_die( 'runUuid_invalid', "Can't get a runUuidTag from: " . $self->{'_runUuid'} );
    return $1;
}

=head1 INTERNAL METHODS

=cut

=head2 _updateRunStatus()

    $self->_updateRunStatus( $runHR, $newStatus );

Set the status of the internally referenced upload record to the specified
$newStatus string. Returns the new (updated) $runHR reflecting this status.

Update is done inside a transaction. If the update fails, will try to roll it
back and then throw a RunUpdateException (including information about any
failed rollback).

=cut

sub _updateRunStatus {

    my $self = shift;
    my $paramHR = shift;

    my $newStatus = $paramHR->{toStatus};
    my $runHR     = $paramHR->{runHR};
    my $error     = $paramHR->{error};

    try {

        if ($newStatus eq 'failed') {
             unless (defined $error) {
                 $error = $self->_withError('UnspecifiedFailureException', "Failed without giving a reason.");
             }
             my $errorName = $self->_defaultIfUndef( $self->_getExceptionName( $error, "???" ));
             $errorName =~ s/Exception$//;
             $newStatus = $newStatus . '_' . $errorName;
        }
        else {
            # Can't have error if not "failed" status.
            $error = "";
        }

        my $dbh      = $self->{'_dbh'};
        my $uploadId = $runHR->{'upload_id'};
        my $updateSQL = "UPDATE upload SET status = ? WHERE upload_id = ?";

        $dbh->begin_work();
        my $updateSTH = $dbh->prepare( $updateSQL ); 
        $updateSTH->execute( $newStatus, $uploadId );
        my $rowsAffected = $updateSTH->rows();
        $updateSTH->finish();

        if (! $rowsAffected || $rowsAffected != 1) {
            $rowsAffected = $self->_defaultIfUndef( $rowsAffected, "<undef>");
            die $self->_withError( "DbResultCountException", {expected=>"1", found=>"$rowsAffected", exception=>$error} );
        }
        $dbh->commit();
    }
    catch {
        my $caught = $_;

        try {
            $self->{'_dbh'}->rollback();
        }
        catch{
            my $rollbackError = $_;
            $rollbackError = $self->_withError( "DbRollbackException", { detail=>$rollbackError, exception=>$caught });
            die $self->_withError( "RunStatusChangeException",
                { toStatus=>$newStatus, runHR => $runHR, exception => $rollbackError } );
        };

        die $self->_withError( "RunStatusChangeException",
               { toStatus=>$newStatus, runHR => $runHR, exception => $caught} );
    };

    $self->sayVerbose("Set upload status for upload_id " . $runHR->{upload_id} . " to \"$newStatus\".");
    $runHR->status = $newStatus;
    return $runHR;
}

sub _getSelectedBam {

    my $self = shift;
    my $dbh = shift;

    $self->_parameterDefinedOrCroak($dbh, 'dbh', '_getSelectedBam');

    $self->_optionExistsOrCroak( 'sample',     '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'flowcell',   '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'lane',       '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'barcode',    '_getSelectedBam' );
    $self->_optionExistsOrCroak( 'workflowId', '_getSelectedBam' );

    my $sample     = $self->{'sample'};
    my $flowcell   = $self->{'flowcell'};
    my $lane       = $self->{'lane'};
    my $barcode    = $self->{'barcode'};
    my $workflowId = $self->{'workflowId'};

    if ($@) {
       my $caghtErrorMessage = $@;
       my $message = "Error retrieving workflow record.\n";
       $message .= "This may cause every attempt to run this workflow to fail.\n";
       $message .= "Error was: \t$caghtErrorMessage" . "\n";
       die ("$message". "\n");
    }

}

=head2 _parameterDefinedOrCroak()

    my $paramName = $self->_parameterDefinedOrCroak( shift, 'paramName');

Checks to see if a parameter value is defined, croaking with error if not.
Note: This reports error from user perspective. Don't validate subroutine
parmaeters passed purely internally except as double check against programmer
errors. Try to catch the programmer errors in the tests instead.

=cut

sub _parameterDefinedOrCroak {
    my $self = shift;
    my $paramVal = shift;
    my $paramName = shift;

    if (! defined $paramVal) {
        my $subName = (caller(1))[3];
        $self->{'error'} = 'param_' . $subName . "_" . $paramName;
        croak ($subName . '() missing $' . $paramName . ' parameter.');
    }

    return $paramVal;
}

sub _dbh {
    my $self = shift; # Object method
    my $dbh = shift;  # Optional

    # If passed in explicit parameter, this is a setter method. Returns the
    # previous value of $self->{'_dbh'}.
    if ($dbh) {
        my $old = $self->{'_dbh'};
        $self->{'_dbh'} = $dbh;
        return $old;
    }

    # If defined as object property already, this is a getter method.
    if ($self->{'_dbh'}) {
        return $self->{'_dbh'};
    }

    # Otherwise, not already defined, this is an init method. Create a
    # new database object and mark it as mine.
    eval {
        my $connectionBuilder = Bio::SeqWare::Db::Connection->new( $self );
        $self->{'_dbh'} = $connectionBuilder->getConnection( $self->{'_dbConnectFlags'} );
    };
    # Should not be possible to get nothing back from getConnection without
    # error, but easy to check for and this is critical for everything 
    if ($@ || ! $self->{'_dbh'}) {
        my $error = $@;
        $self->{'_error'} = "db_not_connecting";
        croak "Failed to create a new connection to the database.\n$error\n$!\n";
    }

    # Track my ownership of DBH, prevent returning it when done.
    $self->{'_isMyDbh'} = 1;
    return $self->{'_dbh'};
}

=head2 _setRunUuid()

    $self->_setRunUuid();
        or
    my $oldUuid = $self->_setRunUuid( $newUuid );

Internal function to manage changing the _runUuid field. Changes the _runUuid
to a valid uuid or dies. Returns the old value on success. (possibly returns
undef).

=over 3

=item PARAM: $newUuid - A valid uuid | undef.

Given a valid uuid, the runUUid field will be set to this value and the
old value will be returned. If this is not formatted correctly, will
die with error (uuid_format).

If $newUuid is undefined, a new uuid will generated by calling:
Bio::SeqWare::Uploads::CgHub::Fastq->getUuid(). This may fail, which
is a fatal error (uuid_gen).

=item RETURN: $oldUuid | undef

Will return undef if there was no previous uuid. This is independent
of how the uuid is being changed (i.e. doen't matter if new uuid was provided
or generated).

=item ERROR: uuid_format

"Not a valid uuid: $newUuid" ...

It is a fatal error if the $newUuid param is defined but not formated like
/[\dA-f]{8}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{12}/i

=item ERROR: uuid_gen

"Failed to generate a new uuid.\n\tError was: " ...

It is a fatal error if generation of a new uuid triggered an error.

=back

=cut

sub _setRunUuid {
    my $self = shift;
    my $uuid = shift;

    # Save old value
    my $oldUuid = $self->{'_runUuid'};

    # Try to Use provided uuid, if any
    if ($uuid) {
        if ( $uuid !~ /[\dA-f]{8}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{4}-[\dA-f]{12}/i ) {
            my $errorMessage = "Not a valid uuid: $uuid";
            $self->_die( 'uuid_format', $errorMessage );
        }
        $self->{'_runUuid'} = $uuid;
    }
    # Try to generate a uuid value if not provided.
    else {
        eval {
            # Contract: return valid uuid or die with error.
            $self->{'_runUuid'} = Bio::SeqWare::Uploads::CgHub::Fastq->getUuid();
        };
        if ($@) {
            my $trappedError = $@;
            my $errorMessage = "Failed to generate a new uuid.\n\tError was: $trappedError";
            $self->_die( 'uuid_gen', $errorMessage);
        }
    }

    return $oldUuid;
}

=head2 _initStepHR()

    _initStepHR();

Initiates the internal data field _stepHR using the data field _stepAR. Makes
finding parent and child steps easier. Returns 1 to be specific.

=cut

sub _initStepHR {
    my $self = shift;
    for (my $pos = 0; $pos < scalar (@{$self->{'_stepAR'}}); ++$pos) {
        my $step = $self->{'_stepAR'}->[$pos];
        $self->{'_stepHR'}->{"$step"} = $pos;
    }
    return 1;
}

=head2 _parentStepOf( $step )

    my $parent = $self->_parentStepOf( "someStep" );

Returns the name of the parent step of a specified $step, or undefined if $step
is the first step (i.e. if step is "START").

=cut

sub _parentStepOf {
    my $self = shift;
    my $step = shift;

    if ($step eq "START") {
         return undef;
    }
    else {
        my $stepPos = $self->{'_stepHR'}->{"$step"};
        return $self->{'_stepAR'}->[$stepPos - 1];
    }
}

=head2 _childStepOf( $step )

    my $child = $self->_childStepOf( "someStep" );

Returns the name of the child step of a specified $step, or undefined if $step
is the last step (i.e. if step is "END").

=cut

sub _childStepOf {
    my $self = shift;
    my $step = shift;

    if ($step eq "END") {
         return undef;
    }
    else {
        my $stepPos = $self->{'_stepHR'}->{"$step"};
        return $self->{'_stepAR'}->[$stepPos + 1];
    }
}

# Essentially a nested exception handling class
{
    my %ERRORS = (
        '???Exception' =>
            'Oops.',
        'BadParameterException' =>
            'Subroutine parameter value was invalid.',
        'UnknownStepException' =>
            'I don\'t know how to do this. Note: step name is case sensitive.',
        'CrashedException' =>
            'Aborting. Cleanup is probably needed.',
        'RunStatusChangeException' =>
            'Failed changing run state. Database may need manual cleanup.',
        'ClassInitalizationException' =>
            'Something went wrong setting up an object\'s internal data.',
        'DbResultCountException' =>
            'Wrong number of results returned from database operation.',
        'DbRollbackException' =>
            'Error during db rollback. Database may need manual cleanup.',
    );

    my $CONTEXT_ITEM_DELIM = ' - ';
    my $CONTEXT_STRING_DELIM = "\n";
    my $CONTEXT_LEADER_TEXT = " Some details ...\n";

=head2 withError

    my $e = $self->_withError('Name');
    my $e = $self->_withError("Name\n");
    my $detailsHR = { 'SomeKey' => "SomeValue",
     'Some complex description' => "The value described" };
    my $e = $self->_withError( 'Name', $detailsHR, });
    my $e = $self->_withError( "Name\n", $detailsHR, });

=over 3

=item DESC
Generate strings suitable for dieing with. Exceptions are just strings. The
first word in the exception string will always be the given name, ending in
"Exception". You can leave off the "Exception" when giving the name, it will
be added for you. (Be carefull of spelling. Nobody wants a
SomeExceptonException)

Any exception name can be used. If the excption is unknown, a default error
message "Well that wasn't supposed to happen." will be used. If a known
parameter needs to be a string. If the exception is pre-defined, then
the second parameter should be a hash-ref of key => values. The defined
exceptions and their allowed parameters are listed below.

To create a chained exception, a third parameter, $exception can be added to
the generic exception, or an exception => $exception parmeter can be passed
to a known exception. This will add at the end of the exception string:

   "\n(This occured in the context of another exception: $exception)";

Terminal "\n" are used to trigger the appending of trace information. For
generic, non-chaining messages, the original line ending status is preserved.
For generic chaining messages, a "\n" will always be appended to the original
message. For known error messages, if trace is printed it will be stated.

=over 3

=item BadParameterException
Die with this error to signal a suborutine parameter has a bad value. Message
ends with trace information 

=over 3

=item paramName
The name of the parameter with a bad value

=item paramValue
The value of the bad parameter. If undefined will display as <undef>.

=back

=item UnknownStepException
Die with this error to signal a run was attempted when the step was not in the
list of approvied steps.

=over 3

=item step
The unrecognized step name. If undefined, will display as <undef>.

=back

=item CrashedException
Die with this error to signal an unexpected crash, usually something not part
of the exception hierarchy. This probably means some cleanup will be neeeded
as the crash need not have updated the run state correctly. The error
triggering this message should be specified with {exception=>$exception}

=over 3

=item runHR (optional)
The run in progress when the crash occured.

=back

=item RunStatusChangeException
Die with this error to signal that a change to the run status could not be
made. This usually means some cleanup will be needed as the run will be in
an incorrect state. The error triggering this message should be specified
with {exception=>$exception}

=over 3

=item toState
The state trying to change the run to, like 'failure' or 'running'. (No error)
tag should be applied, the other parameters will set this.

=item runHR (optional)
The run whose state is trying to be changed. Optional as there may be no run
(that may be the problem...).

=back

=item ClassInitalizationException
This error signals that the initialization of a new class object failed. This
is mostly used for complex classes where the new() constructor calls internal
methods to initialize an object. The error triggering this message should be
specified with {exception=>$exception}

=over 3

=item class
The name of the class whose initialization has failed.

=back

=item DbResultCountException
This error signals a database select returned an unexpected number of rows, or
an update or insert affected an unexpected number of rows. Trace information
is appended

=over 3

=item expected
The number of database items that were expected to be found. Is just used as a
string so any description is allowed.

=item found
The number of database items that were actually returned.

=back

=item DbRollbackException
This error signals the failure of a database rollback attempt. The exception
triggering the rollback should be attached as {exception=>$exception}. Trace
information is attached.

=item details
The details about the exception as provided by the db.

=back

=back

=item PARAM $exceptionName
The name of the exception. Should be only letters and numbers and end in
"Exception", although nothing enforces that. May be a known exception or an
unknown exception. Spelling errors for known exceptions become unknown
exceptions!.

=item PARAM $paramHR | $errorString
If $exceptionName is an unknown eception, then this parameter will be
treated as the error string. If $exceptionName is a known, pre-defined
exception (as above), then this will be a hash-ref of key=value parameters to
pass to the exception for converting into an error string.

=item PARAM $exception
If $exceptionName is an unknown eception, then this parameter will be
treated as a cascading error. If $exceptionName is a known, this will be
ignored

=item RETURNS
A string representing the error, starting with "$exceptionName: " (The name
of the exception followed by a colon and a space.)

=back

=cut

    sub _withError {
    
        # Making every effort to not fail when this is called.
        my $self = shift;
        my $name = shift;
        my $contextHR = shift;
    
        my $message;
    
        try {
            if (! defined $name) {
                $name = '???Exception';
            }
     
            my $withTrace = 0;
            if ($name =~ /\n$/ ) {
                $withTrace = 1;
            }
            chomp $name;
    
            my @contextItems;
            for my $key (keys(%{$contextHR})) {
                my $value = $contextHR->{"$key"};
                # Value Should be defined and scalar, but we'll try to do something
                # with it if not.
                if (ref $value) {
                    $value = '[' . Dumper($value). ']';
                }
                elsif (! defined $value) {
                    $value = '<undef>';
                }
                push @contextItems, join($CONTEXT_ITEM_DELIM, ("$key", "$value"));
            }
            my $contextString = join($CONTEXT_STRING_DELIM, @contextItems);
    
            $message = $ERRORS{"$name"};
            if (! defined $message) {
                $message = "Well that wasn't supposed to happen.";
            }
    
            $message = "$name: $message";
            if ($contextString) {
                $message .= $CONTEXT_LEADER_TEXT . $contextString;
            }
            chomp $message;
            if ($withTrace) {
                $message .= "\n";
            }
        }
        catch {
           die "Died while trying to die! Error was: $_\n";
        };
    
        return $message;
    }

    sub _getExceptionName {
        my $self = shift;
        my $exception = shift;

        if ($exception =~ /^([^\s]+Exception): /) {
            return $1;
        }
        else {
            return;
        }
    }
}
=head1 AUTHOR

Stuart R. Jefferys, C<< <srjefferys (at) gmail (dot) com> >>

Contributors:
  Lisle Mose (get_sample.pl and generate_cghub_metadata.pl)
  Brian O'Conner

=cut

=head1 DEVELOPMENT

This module is developed and hosted on GitHub, at
L<p5-Bio-SeqWare-Config https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam>.
It is not currently on CPAN, and I don't have any immediate plans to post it
there unless requested by core SeqWare developers (It is not my place to
set out a module name hierarchy for the project as a whole :)

=cut

=head1 INSTALLATION

You can install a version of this module directly from github using

   $ cpanm https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam/archive/v0.000.031.tar.gz

The above installs the latest I<released> version. To install the bleading edge
pre-release, if you don't care about bugs...

   $ cpanm https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam

You can select and download any package for any released version of this module
directly from L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam/releases>.
Installing is then a matter of unzipping it, changing into the unzipped
directory, and then executing the normal (C>Module::Build>) incantation:

     perl Build.PL
     ./Build
     ./Build test
     ./Build install

=cut

=head1 BUGS AND SUPPORT

No known bugs are present in this release. Unknown bugs are a virtual
certainty. Please report bugs (and feature requests) though the
Github issue tracker associated with the development repository, at:

L<https://github.com/theobio/p5-Bio-SeqWare-Uploads-CgHub-Bam/issues>

Note: you must have a GitHub account to submit issues.

=cut

=head1 ACKNOWLEDGEMENTS

This module was developed for use with L<SegWare | http://seqware.github.io>.

=cut

=head1 LICENSE AND COPYRIGHT

Copyright 2014 Stuart R. Jefferys.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut

1; # End of Bio::SeqWare::Uploads::CgHub::Bam
