#!/usr/bin/perl

use strict;
use warnings;
use Readonly;
use LWP::UserAgent;
use XML::Simple;
use File::Basename;
use File::Path qw(make_path);
use Cwd 'abs_path';
use Log::Log4perl;

# Constants
Readonly my $SCRIPT_PATH => dirname(abs_path($0));
Readonly my $BASE_DIR    => "$SCRIPT_PATH/../data/sota";
Readonly my $SITEMAP_DIR => "$BASE_DIR/sitemap";
Readonly my %DIRS        => (
    'areas'    => "$BASE_DIR/areas",
    'datasets' => "$BASE_DIR/datasets",
    'tasks'    => "$BASE_DIR/tasks",
);
Readonly my $LOG_DIR    => "$SCRIPT_PATH/../logs";
Readonly my $LOG_FILE   => "$LOG_DIR/sota.log";
Readonly my $MAX_RETRIES => 5;

# Ensure the log directory exists
make_path($LOG_DIR) unless -d $LOG_DIR;

# Initialize logging configuration
my $log_config = qq(
    log4perl.rootLogger=DEBUG, Screen, File

    log4perl.appender.Screen=Log::Log4perl::Appender::Screen
    log4perl.appender.Screen.stderr=0
    log4perl.appender.Screen.layout=Log::Log4perl::Layout::PatternLayout
    log4perl.appender.Screen.layout.ConversionPattern=%d %p %m%n

    log4perl.appender.File=Log::Log4perl::Appender::File
    log4perl.appender.File.filename=$LOG_FILE
    log4perl.appender.File.mode=append
    log4perl.appender.File.layout=Log::Log4perl::Layout::PatternLayout
    log4perl.appender.File.layout.ConversionPattern=%d %p %m%n
);

Log::Log4perl::init(\$log_config);
my $logger = Log::Log4perl->get_logger();

# Ensure base directories exist
for my $dir (values %DIRS) {
    make_path($dir) unless -d $dir;
    $logger->info("Directory created: $dir");
}

# Setup UserAgent
my $ua = LWP::UserAgent->new;
$ua->agent('Mozilla/5.0');
$logger->info("UserAgent created");

# Function to process each XML file
sub process_file {
    my ($file_path, $category) = @_;
    my $xml = XML::Simple->new;
    my $data = $xml->XMLin($file_path);

    # Process each URL found within <loc> tags
    for my $url (@{$data->{url}}) {
        my $loc = $url->{loc};
        download_html($loc, $DIRS{$category});
    }
}

# Download HTML function with retry and backoff
sub download_html {
    my ($url, $output_dir) = @_;
    my $retry = 0;
    my $success = 0;

    while (!$success && $retry < $MAX_RETRIES) {
        my $response = $ua->get($url);
        if ($response->is_success) {
            my $filename = (split '/', $url)[-1];
            $filename .= ".html" unless $filename =~ /\.html$/;
						my $file_path =  "$output_dir/$filename";
						open my $fh, '>:encoding(UTF8)', $file_path or $logger->logdie("Cannot open file: $!");
            print $fh $response->decoded_content;
            close $fh;
            $logger->info("Downloaded and saved: $filename to $output_dir");
            $success = 1;
        } else {
            $logger->warn("Failed to download $url. Retry $retry. HTTP Status: " . $response->status_line);
            sleep(2 ** $retry);  # Exponential backoff
            $retry++;
        }
    }
    $logger->error("Failed to download after $MAX_RETRIES retries: $url") unless $success;
}

# Identify each XML file in the sitemap directory and process
for my $file (glob("$SITEMAP_DIR/*.xml")) {
    if ($file =~ /sitemap-areas(-\d+)?\.xml$/) {
        process_file($file, 'areas');
    } elsif ($file =~ /sitemap-tasks(-\d+)?\.xml$/) {
        process_file($file, 'tasks');
    } elsif ($file =~ /sitemap-datasets(-\d+)?\.xml$/) {
        process_file($file, 'datasets');
    }
}
