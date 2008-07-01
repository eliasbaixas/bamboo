#!/usr/bin/perl -w
use strict;

my %putsum;
my %getsum;
my %putcnt;
my %getcnt;
my %putlat;
my %getlat;

while (<STDIN>) {
    if (m/Put successful: size=(\d+) key=0x[0-9a-f]+ ttl_sec=\d+ lat=(\d+) ms/){
        my ($size, $lat) = ($1, $2);
        if (defined $putsum{$size}) {
            $putsum{$size} += $lat;
            $putcnt{$size} += 1;
            my $aryref = $putlat{$size};
            push @$aryref, $lat;
        }
        else {
            $putsum{$size} = $lat;
            $putcnt{$size} = 1;
            my @ary = ($lat);
            $putlat{$size} = \@ary;
        }
    }
    elsif (m/Get successful: key=0x[0-9a-f]+ size=(\d+) lat=(\d+) ms/) {
        my ($size, $lat) = ($1, $2);
        if (defined $getsum{$size}) {
            $getsum{$size} += $lat;
            $getcnt{$size} += 1;
            my $aryref = $getlat{$size};
            push @$aryref, $lat;
        }
        else {
            $getsum{$size} = $lat;
            $getcnt{$size} = 1;
            my @ary = ($lat);
            $getlat{$size} = \@ary;
        }
     }
}

print "put/get size    count   low   5th    med     95th     high     mean\n";
print "------- ----    -----   ---   ---    ---     ----     ----     ----\n";

my $size;
foreach $size (sort { $a <=> $b } keys %putsum) {
    my $aryref = $putlat{$size};
    my @sorted = sort { $a <=> $b } @$aryref;
    my $med = $sorted[int ($#sorted/2)];
    printf "put %8d %8d %5d %5d %6d %8d %8d %8.1f\n", $size, $putcnt{$size}, 
           $sorted[0],
           $sorted[int($#sorted*0.05)],
           $sorted[int($#sorted*0.50)],
           $sorted[int($#sorted*0.95)],
           $sorted[$#sorted],
           $putsum{$size} / $putcnt{$size}; 
}
foreach $size (sort { $a <=> $b } keys %getsum) {
    my $aryref = $getlat{$size};
    my @sorted = sort { $a <=> $b } @$aryref;
    my $med = $sorted[int ($#sorted/2)];
    printf "get %8d %8d %5d %5d %6d %8d %8d %8.1f\n", $size, $getcnt{$size}, 
           $sorted[0],
           $sorted[int($#sorted*0.05)],
           $sorted[int($#sorted*0.50)],
           $sorted[int($#sorted*0.95)],
           $sorted[$#sorted],
           $getsum{$size} / $getcnt{$size}; 
}

