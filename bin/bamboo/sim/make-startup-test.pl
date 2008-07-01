#!/usr/bin/perl -w
#
# Copyright (c) 2001-2003 Regents of the University of California.
# All rights reserved.
#
# See the file LICENSE included in this distribution for details.

use strict;


my $node_count = 30;
my $seed = 1;
my $end_time = 60*1000*2;

srand $seed;

open (EXP, ">/tmp/startup-test.exp") or die;

my $pwd = `pwd`;
chomp $pwd;
print EXP "bamboo.sim.KingNetworkModel $pwd/king128.top\n";

my $port = 3660;
my @ips;
my %ipset;
for (my $i = 1; $i < $node_count; ++$i) {
    my $ip;
    while (1) {
        my $graph_index = int (rand () * 128) + 1;
        $ip = "10.0.0.$graph_index";
        if (! (defined $ipset{$ip})) {
            $ips[$i] = $ip;
            $ipset{$ip} = $ip;
            last;
        }
    }
    my $gateway;
    if ($i == 1) {
        $gateway = 1;
    }
    else {
        $gateway = int (rand () * ($i-1)) + 1;
    }
    my $gateway_ip = $ips[$gateway];
    my $cfg = "/tmp/node-$i.cfg";
    printf EXP "$ip:$port\t%19s %8d %10d\n", $cfg, $i*1000, $end_time;
    open (CFG, ">$cfg");
    print CFG<<EOF;
<sandstorm>
    <global>
	<initargs>
EOF
    print CFG "	    node_id $ip:$port\n";
    print CFG<<EOF;
	</initargs>
    </global>

    <stages>
	<Router>
	    class bamboo.router.Router
	    <initargs>
EOF
    print CFG "		gateway $gateway_ip:$port\n";
    print CFG<<EOF;
                debug_level 0
	    </initargs>
	</Router>
    </stages>
</sandstorm>
EOF
    close (CFG);
}
close (EXP);

