#!/usr/bin/perl
use strict;
use warnings;
use Time::HiRes;

my $reporting_interval = 10.0; # seconds
my $bytes_this_interval = 0;
my $start_time = [Time::HiRes::gettimeofday()];

STDOUT->autoflush(1);

while (<>) {
  if (/ length (\d+):/) {
    $bytes_this_interval += $1;
    my $elapsed_seconds = Time::HiRes::tv_interval($start_time);
    if ($elapsed_seconds > $reporting_interval) {
       my $bps = $bytes_this_interval / $elapsed_seconds;
       printf "%02d:%02d:%02d %10.2f Bps\n", (localtime())[2,1,0],$bps;
       $start_time = [Time::HiRes::gettimeofday()];
       $bytes_this_interval = 0;
    }
  }
}    