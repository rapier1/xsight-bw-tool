#use warnings;
use JSON qw( decode_json );
use WWW::Curl::Easy;
use URI::URL;
use YAML;
use Date::Parse;
use Statistics::Descriptive;
use Getopt::Std;

#instantiate the curl handle object
my $curl = WWW::Curl::Easy->new;

# load and propcess command line argument
my %options = ();
getopts("d:c:t:hfuq", \%options);

if ($options{h}) {
    print "Usage: find_bw_by_app -d {databasename} -c {command} -t {time range e.g. 1h or 10d}\n";
    print "                      -f (use Freedman-Diaconis histogram bin size)\n";
    print "                      -u (unified stats using max of in/out bw oper flow)\n";
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
$curl->setopt(CURLOPT_USERPWD, "user:pwd");

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
my $port = 0;
#load the flows has with appropriate data. 
foreach $value (@values) {
    $flowid = @{$value}[1];
    print STDERR "$i $flowid\n";
    # if we are searching on gridftp command we want to remove the control channel
    # since this takes time we want to only do it for gridftp
    if ($command eq "globus-gridftp-") {
	$query = "query?pretty=true&db=" . $db_name . "&q=select first(value) from dest_port WHERE flow='$flowid'";
	(my $tmp, my $port) = &runquery($query);
	if ($port == 2811) {
	    next;
	}
	$query = "query?pretty=true&db=" . $db_name . "&q=select first(value) from src_port WHERE flow='$flowid'";
	($tmp, $port) = &runquery($query);
	if ($port == 2811) {
	    next;
	}
    }
    #get the first and last hcdata octets in both directions and the associated timestamps
    $query = "query?pretty=true&db=" . $db_name . "&q=select first(value) from HCDataOctetsIn WHERE flow='$flowid'";
    ($flows{$flowid}{hcdatain_starttime}, $flows{$flowid}{hcdatain_firstval}) = &runquery($query);
    $query = "query?pretty=true&db=" . $db_name . "&q=select last(value) from HCDataOctetsIn WHERE flow='$flowid'";
    ($flows{$flowid}{hcdatain_endtime}, $flows{$flowid}{hcdatain_lastval}) = &runquery($query);
    $query = "query?pretty=true&db=" . $db_name . "&q=select first(value) from HCDataOctetsOut WHERE flow='$flowid'";
    ($flows{$flowid}{hcdataout_starttime}, $flows{$flowid}{hcdataout_firstval}) = &runquery($query);
    $query = "query?pretty=true&db=" . $db_name . "&q=select last(value) from HCDataOctetsOut WHERE flow='$flowid'";
    ($flows{$flowid}{hcdataout_endtime}, $flows{$flowid}{hcdataout_lastval}) = &runquery($query);
    $i++;
}

my $deltatime;
my $deltabytes;
my $bw;
my %bwresults;

# go through each of the flows and see if there is enough data to generate
# bandwidth data
foreach $flowid (keys %flows) {
    $deltatime = str2time($flows{$flowid}{hcdatain_endtime}) - str2time($flows{$flowid}{hcdatain_starttime});
    $deltabytes =  $flows{$flowid}{hcdatain_lastval} -  $flows{$flowid}{hcdatain_firstval};
    if ($deltatime != 0) {
	$bw = int(($deltabytes*8)/$deltatime);
	if ($bw > 0) {
	    $bwresults{$flowid}{inbw} = $bw;
	    $bwresults{$flowid}{intime} = $deltatime;
	}
    }
    $deltatime = str2time($flows{$flowid}{hcdataout_endtime}) - str2time($flows{$flowid}{hcdataout_starttime});
    $deltabytes =  $flows{$flowid}{hcdataout_lastval} -  $flows{$flowid}{hcdataout_firstval};
    if ($deltatime != 0) {
	$bw = int(($deltabytes*8)/$deltatime);
	if ($bw > 0) {
	    $bwresults{$flowid}{outbw} = $bw;
	    $bwresults{$flowid}{outtime} = $deltatime;
	}
    }
    if ($options{u}) {
	if ($bwresults{$flowid}{inbw} > $bwresults{$flowid}{outbw}) {
	    $bwresults{$flowid}{unified} = $bwresults{$flowid}{inbw};
	} else {
	    $bwresults{$flowid}{unified} = $bwresults{$flowid}{outbw};
	}
    }
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
foreach $flowid (keys %bwresults) {
    if ($options{u}) {
	if (($bwresults{$flowid}{inbw} > 0) || ($bwresults{$flowid}{outbw} > 0)) {
	    if ($bwresults{$flowid}{inbw} > $bwresults{$flowid}{outbw}) {
		$stats->add_data($bwresults{$flowid}{inbw});
	    } else {
		$stats->add_data($bwresults{$flowid}{outbw});
	    }
	}
    } else {
	$in_stats->add_data($bwresults{$flowid}{inbw});
	$out_stats->add_data($bwresults{$flowid}{outbw});
    }
}

#making these global for ease of laziness
my $bin_size, $bin_count, $altbin_size, $altbin_count;
if ($options{u}) {
    print "\nUnified Stats\n";
    &printStats($stats);
    &printResults("unified");
} else { 
    print "\nInbound Stats\n";
    if ($in_stats->count() > 0) {
	&printStats($in_stats);
	&printResults("inbw");
    } else {
	print "No inbound results\n";
    }
    if ($out_stats->count() > 0) {
	print "\nOutbound Stats\n";
	&printStats($out_stats);
	&printResults("outbw");
    } else {
	print "No outbound results\n";
    }
}

print "\n\nend\n";
exit;

sub printResults () {
    my $direction = shift @_;
    my $dirtime = "outtime";
    if ($direction eq "inbw") {
	$dirtime = "intime";
    }    
    my @results = ();
    my @altresults = ();
    my $bw;
    my $bin;
    foreach $flowid (sort { $bwresults{$a}{$direction} <=> $bwresults{$b}{$direction}} keys %bwresults) {
	$bw = $bwresults{$flowid}{$direction};
	$bin = int ($bw/$bin_size);
	$bintotal[$bin]++;
	push (@{$results[$bin]}, $flowid);
    }
    if ($options{f}) {
	foreach $flowid (sort { $bwresults{$a}{$direction} <=> $bwresults{$b}{$direction}} keys %bwresults) {
	    $bw = $bwresults{$flowid}{$direction};
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
    #for ($i = 0; $i <= $#bintotal; $i++) {
    #    print "$i\t$bintotal[$i]\n";
    #}
    if ($options{u}) {
	print "bin:flow:inbw:intime:outbw:outtime\n";
    } else {
	print "bin:flow:$direction:$dirtime\n";
    }
    for ($i = 0; $i <= $bin_count; $i++) {
	my @bindata = @{$results[$i]};
	for (my $j = 0; $j <= $#bindata; $j++) {
	    if (($bwresults{$bindata[$j]}{inbw} > 0) || ($bwresults{$bindata[$j]}{outbw} > 0)) {
		if ($options{u}) {
		    print "$i:$bindata[$j]:$bwresults{$bindata[$j]}{inbw}:$bwresults{$bindata[$j]}{intime}:$bwresults{$bindata[$j]}{outbw}:$bwresults{$bindata[$j]}{outtime}\n";
		} else {
		    print "$i:$bindata[$j]:$bwresults{$bindata[$j]}{$direction}:$bwresults{$bindata[$j]}{$dirtime}\n";
		}
	    }
	}
    }
        
    if ($options{f}) {   
	print "Bin Count (Freedman–Diaconis' choice)\n";
	#for ($i = 0; $i <= $#altbintotal; $i++) {
	#    print "$i\t$altbintotal[$i]\n";
	#}
	print "bin:flow:$direction:$dirtime\n";
	for ($i = 0; $i <= $altbin_count; $i++) {
	    my @bindata = @{$altresults[$i]};
	    for (my $j = 0; $j <= $#bindata; $j++) {
		if (($bwresults{$bindata[$j]}{inbw} > 0) || ($bwresults{$bindata[$j]}{outbw} > 0)) {
		    if ($options{u}) {
			print "$i:$bindata[$j]:$bwresults{$bindata[$j]}{inbw}:$bwresults{$bindata[$j]}{intime}:$bwresults{$bindata[$j]}{outbw}:$bwresults{$bindata[$j]}{outtime}\n";
		    } else {
			print "$i:$bindata[$j]:$bwresults{$bindata[$j]}{$direction}:$bwresults{$bindata[$j]}{$dirtime}\n";
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

sub runquery () {
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
    my $time = $decoded->{'results'}->[0]->{'series'}->[0]->{'values'}[0][0];
    my $hcdataoctets = $decoded->{'results'}->[0]->{'series'}->[0]->{'values'}[0][1];
    if (!$time) {
	#no time stamp means no data
	$time = 0;
	$hcdataoctets = 0;
    }
    return ($time, $hcdataoctets);
}
