#use warnings;
use JSON qw( decode_json );
use WWW::Curl::Easy;
use URI::URL;
use YAML;
use Date::Parse;
use Statistics::Descriptive;
use Getopt::Std;
use Math::Round;

#instantiate the curl handle object
my $curl = WWW::Curl::Easy->new;

# load and propcess command line argument
my %options = ();
getopts("u:p:d:c:t:hfsq", \%options);

if ($options{h}) {
    print "Usage: find_bw_by_app -d {databasename} -c {command} -t {time range e.g. 1h or 10d}\n";
    print "                      -f (use Freedman-Diaconis histogram bin size)\n";
    print "                      -s (unified stats using max of in/out bw oper flow)\n";
    print "                      -q only print histogram data\n";
    exit;
}
if (!defined $options{d}) {
    print "Please provide a host database name\n";
    exit;
}
if (!defined $options{c}) {
    print "Please provide a command to search on\n";
    exit;
}
if (!defined $options{t}) {
    print "please provide a time range";
    exit;
}
if (!defined $options{u}) {
    print "please provide a username for the database";
    exit;
}
if (!defined $options{p}) {
    print "please provide a password for the database user";
    exit;
}

#we are including a shortcut for br033 and br034 to make
#it less annoying
my $db_name = $options{d};
if ($db_name eq "br033") {
    $db_name =  "ALL_PSC_br033.dmz.bridges.psc.edu";
}
if ($db_name eq "br034") {
    $db_name =  "ALL_PSC_br034.dmz.bridges.psc.edu";
}

my $base_url = "https://hotel.psc.edu:8086/";
my $command = $options{c};
my $time = $options{t};

#create the query
my $query = "query?pretty=true&db=" . $db_name . "&q=select flow,value from command WHERE value = '$command' AND time > now() - $time";

#set the url for the curl handle
my $url = URI::URL->new($query, $base_url);

if (!$options{q}) {
    print $url->abs . "\n";
}

#create the curl handle
$curl->setopt(CURLOPT_HEADER,0);
$curl->setopt(CURLOPT_URL, $url->abs);
$curl->setopt(CURLOPT_HTTPAUTH, CURLAUTH_ANY);
$curl->setopt(CURLOPT_USERPWD, "$options{u}:$options{p}");

# A filehandle, reference to a scalar or reference to a typeglob can be used here.
my $response_body;
$curl->setopt(CURLOPT_WRITEDATA, \$response_body);
 
# Starts the actual request
my $retcode = $curl->perform;
 
# Looking at the results...
if ($retcode == 0) {
        my $response_code = $curl->getinfo(CURLINFO_HTTP_CODE);
} else {
        # Error code, type of error, error message
        print("An error happened: $retcode ".$curl->strerror($retcode)." ".$curl->errbuf."\n");
}

# decode the json text into a more usable form
my $decoded = decode_json($response_body);

#this returns a list of flowids asscoiated with the command in question
@values =  @{$decoded->{'results'}->[0]->{series}->[0]->{values}};

if (!$options{q}) {
    print "We have $#values results\n";
}

my $i = 1;
#load the flows has with appropriate data. 
foreach $value (@values) {
    my $time = 0;
    my $src_port = 0;
    my $dest_port = 0;
    my $msecs = 0;
    my $secs = 0;
    my $inbytes = 0;
    my $outbytes = 0;
    $flowid = @{$value}[1];
    print STDERR "$i $flowid\n";

    # if we are searching on gridftp command we want to remove the control channel
    # since this takes time we want to only do it for gridftp

    $query = "query?pretty=true&db=" . $db_name . "&q=select last(value) from HCDataOctetsIn, HCDataOctetsOut, ElapsedSecs, \
                                                                              ElapsedMicroSecs, dest_port, src_port \
                                                                              WHERE flow='$flowid'";
 
    ($msecs, $secs, $inbytes, $outbytes, $src_port, $dest_port) = &runQuery($query);
 
    # if we are searching on gridftp command we want to remove the control channel
    # since this takes time we want to only do it for gridftp
    if ($command eq "globus-gridftp-") {
	if ($dest_port == 2811) {
	    next;
	}
	if ($src_port == 2811) {
	    next;
	}
    }
    
    $flows{$flowid}{Time} = $time = $secs + $msecs/1000000;
    $flows{$flowid}{InBW} = int($inbytes / $time);
    $flows{$flowid}{OutBW} = int($outbytes / $time);
    $i++;
}

my $stats = Statistics::Descriptive::Full->new();
my $in_stats = Statistics::Descriptive::Full->new();
my $out_stats = Statistics::Descriptive::Full->new();

#load the relevant data into the stats data struct
#note: we are being lazy here because we may be adding
#data from a single flow twice (in and out)
#this can screw up the stats accuracy
#may want to break this down into inbound and outbound
my ($inbw, $outbw, $intime, $outtime);
foreach $flowid (keys %flows) {
    if ($options{s}) {
	if ($flows{$flowid}{InBW} > $flows{$flowid}{OutBW}) {
	    $flows{$flowid}{unified} = $flows{$flowid}{InBW};
	    $stats->add_data($flows{$flowid}{InBW});
	} else {
	    $flows{$flowid}{unified} = $flows{$flowid}{OutBW};
	    $stats->add_data($flows{$flowid}{OutBW});
	}
    } else {
	$in_stats->add_data($flows{$flowid}{InBW});
	$out_stats->add_data($flows{$flowid}{OutBW});
    }
}

#making these global for ease of laziness
my $bin_size, $bin_count, $altbin_size, $altbin_count;
if ($options{s}) {
    print "\nUnified Stats\n";
    &printStats($stats);
    &printResults("unified");
} else { 
    print "\nInbound Stats\n";
    if ($in_stats->count() > 0) {
	&printStats($in_stats);
	&printResults("InBW");
    } else {
	print "No inbound results\n";
    }
    if ($out_stats->count() > 0) {
	print "\nOutbound Stats\n";
	&printStats($out_stats);
	&printResults("OutBW");
    } else {
	print "No outbound results\n";
    }
}

print "\n\nend\n";
exit;

sub printResults () {
    my $direction = shift @_;
    my @results = ();
    my @altresults = ();
    my $bw;
    my $bin;
    foreach $flowid (sort { $flows{$a}{$direction} <=> $flows{$b}{$direction}} keys %flows) {
	$bw = $flows{$flowid}{$direction};
	$bin = int ($bw/$bin_size);
	$bintotal[$bin]++;
	push (@{$results[$bin]}, $flowid);
    }
    if ($options{f}) {
	foreach $flowid (sort { $flows{$a}{$direction} <=> $flows{$b}{$direction}} keys %flows) {
	    $bw = $flows{$flowid}{$direction};
	    if ($altbin_count != 0) {
		$altbin = int ($bw/$altbin_size);
	    } else {
		$altbin = 0;
	    }
	    $altbintotal[$altbin]++;
	    push (@{$altresults[$altbin]}, $flowid);
	}
    }
    
    print "Bin Count (Scott's rule)\n";
    if ($options{s}) {
	print "bin:flow:inbw:intime:outbw:outtime\n";
    } else {
	print "bin:flow:$direction:Time\n";
    }
    for ($i = 0; $i <= $bin_count; $i++) {
	my @bindata = @{$results[$i]};
	for (my $j = 0; $j <= $#bindata; $j++) {
	    if ($options{s}) {
		print "$i:$bindata[$j]:$flows{$bindata[$j]}{InBW}:$flows{$bindata[$j]}{Time}:$flows{$bindata[$j]}{OutBW}:$flows{$bindata[$j]}{Time}\n";
	    } else {
		print "$i:$bindata[$j]:$flows{$bindata[$j]}{$direction}:$flows{$bindata[$j]}{Time}\n";
	    }
	}
    }
        
    if ($options{f}) {   
	if ($options{s}) {
	    print "bin:flow:inbw:intime:outbw:outtime\n";
	} else {
	    print "bin:flow:$direction:Time\n";
	}
	print "Bin Count (Freedman–Diaconis' choice)\n";
	for ($i = 0; $i <= $altbin_count; $i++) {
	    my @bindata = @{$altresults[$i]};
	    for (my $j = 0; $j <= $#bindata; $j++) {
		if (($flows{$bindata[$j]}{inbw} > 0) || ($flows{$bindata[$j]}{outbw} > 0)) {
		    if ($options{s}) {
			print "$i:$bindata[$j]:$flows{$bindata[$j]}{InBW}:$flows{$bindata[$j]}{Time}:$flows{$bindata[$j]}{OutBW}:$flows{$bindata[$j]}{Time}\n";
		    } else {
			print "$i:$bindata[$j]:$flows{$bindata[$j]}{$direction}:$flows{$bindata[$j]}{Time}\n";
		    }
		}
	    }
	}
    }
}

sub printStats () {
    my $stats = shift @_;
    # basic stats and histogram bin size
    $bin_size = ((1/($stats->count()**(1/3)) * $stats->standard_deviation()) * 3.49);
    $bin_count = int($stats->max() / $bin_size);
    if (!$options{q}) {
	print "Count = " . $stats->count() . "\n";
	print "Min = " . $stats->min() ."\n";
	print "Mean = " . $stats->mean() ."\n";
	print "Max = " . $stats->max() . "\n";
	print "Stddev = " . $stats->standard_deviation() ."\n";
	print "Median = " . $stats->median() . "\n";
	print "Mode = " . $stats->mode() . "\n";
	print "Bin size = $bin_size\n";
	print "Bins = $bin_count\n";
    }
    
    #alt bin size
    if ($options{f}) {
	my $IQR = $stats->quantile(3) - $stats->quantile(1);
	$altbin_size = 2 * ($IQR/($stats->count()**(1/3)));
	if ($altbin_size == 0) {
	    $altbin_count = 0;
	} else {
	    $altbin_count = int($stats->max() / $altbin_size);
	}
	if (!$options{q}) {
	    print "quintile 1 =" . $stats->quantile(1) . "\n";
	    print "quintile 3 =" . $stats->quantile(3)  . "\n";
	    print "IQR = $IQR\n";
	    print "altbin = $altbin_size\n";
	    print "alt bin count = $altbin_count\n";
	}
    }
}

sub runQuery () {
    $query = shift @_;
    $url = URI::URL->new($query, $base_url);
    $curl->setopt(CURLOPT_URL, $url->abs);
    my $returned_data;
    $curl->setopt(CURLOPT_WRITEDATA,\$returned_data);
    my $retcode = $curl->perform;
    if ($retcode != 0) {
        # Error code, type of error, error message
        print("An error happened: $retcode ".$curl->strerror($retcode)." ".$curl->errbuf."\n");
    }
    my $decoded = decode_json($returned_data);
    my @series = $decoded->{'results'}->[0]->{series};
    my $msecs;
    my $secs;
    my $in;
    my $out;
    my $dport;
    my $sport;
    for (my $i = 0; $i < 6; $i++) {
	$name = $series[0][$i]{name};
	if ($name eq "ElapsedMicroSecs") {
	    $msecs = $series[0][$i]{values}[0][1];
	}
	if ($name eq "ElapsedSecs") {
	    $secs = $series[0][$i]{values}[0][1];
	}
	if ($name eq "HCDataOctetsIn") {
	    $in = $series[0][$i]{values}[0][1];
	}
	if ($name eq "HCDataOctetsOut") {
	    $out = $series[0][$i]{values}[0][1];
	}	
	if ($name eq "src_port") {
	    $sport = $series[0][$i]{values}[0][1];
	}
	if ($name eq "dest_port") {
	    $dport = $series[0][$i]{values}[0][1];
	}
    }
    return ($msecs, $secs, $in, $out, $sport, $dport);
}
