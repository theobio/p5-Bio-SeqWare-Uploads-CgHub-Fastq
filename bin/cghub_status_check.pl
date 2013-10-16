use strict;

use DBI;
use Getopt::Long;
use XML::Parser;
use LWP::Simple qw($ua get);

print "CGHUB status check starting - " . scalar(localtime) . "\n";

my ($username, $password, $dbhost, $seqware_meta_db);

my $getOptResult = GetOptions(
    'username=s' => \$username,
    'password=s' => \$password,
    'dbhost=s'   => \$dbhost,
    'db=s'       => \$seqware_meta_db,
);

my $state = "";
my $hits = "";
my $isInState = 0;
my $isInHits = 0;

# Connect to db
my $dbn = "DBI:Pg:dbname=$seqware_meta_db;host=$dbhost";
my $database=DBI->connect( $dbn, $username, $password, {RaiseError => 1} );

my $sth_update = $database->prepare("update upload set external_status = ? where cghub_analysis_id = ?");

print "Retrieving analysis ids\n";
my @analysisIds = get_analysis_ids();

print "Updating " . scalar(@analysisIds) . " records.\n";

my $cnt = 0;

foreach (@analysisIds) {
	my $analysisId = $_;
	my $status = get_status($analysisId);
	if (!($status eq "")) {
		update_external_status($analysisId, $status);
	}
	
	$cnt = $cnt + 1;
	if (($cnt % 100) == 0) {
		print "Processed $cnt records\n";
	}
	
#	if ($cnt >= 5) {
#		last;
#	}
}

print "CGHUB status check done - " . scalar(localtime) . "\n";

sub update_external_status {
	my ($analysisId, $status) = @_;
	
	$sth_update->execute($status, $analysisId);
	$sth_update->finish();
}

sub get_analysis_ids {
	my @analysis_ids;
	my $sth1 = $database->prepare("select cghub_analysis_id from upload where target = 'CGHUB' and (external_status != 'live' or external_status is null)");
	$sth1->execute();
	
	while(my @row = $sth1->fetchrow_array) {
        push (@analysis_ids, $row[0]);
	}
	
	$sth1->finish();
	
	return @analysis_ids;
}

sub get_status {
	my ($analysisId) = @_;
	
	$state = "";
    $hits = "";
    $isInState = 0;
    $isInHits = 0;
	
	my $content = get("https://cghub.ucsc.edu/cghub/metadata/analysisAttributes?analysis_id=$analysisId");
	
	if (!($content eq "")) {
	    my $parser = new XML::Parser(ErrorContext => 2);
	    $parser->setHandlers(Start => \&start_handler,
	                  End   => \&end_handler,
	                  Char  => \&char_handler);
	      
	    $parser->parse($content);
	
#	    print "Hits: [$hits]\n";
	    if ($hits eq 1) {
#	        print "Status: [$status]\n";
	    } else {
	    	print "[$hits] Hits found for [$analysisId]\n";
	    }
	} else {
		print "No content received for [$analysisId]\n";
	}

    return $state;
}

sub start_handler {
    my ($p, $elt, %atts) = @_;
    
    if ($elt eq "state") {
      $isInState = 1;
      $state = "";
    } elsif ($elt eq "Hits") {
      $isInHits = 1;
      $hits = "";
    }
}

sub char_handler {
    my ($p, $str) = @_;
    
    if ($isInState) {
        $state = $state . $str;
    } elsif ($isInHits) {
        $hits = $hits . $str;
    }
}

sub end_handler {
    $isInState = 0;
    $isInHits  = 0;
}