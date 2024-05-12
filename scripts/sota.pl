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
use Parallel::ForkManager;
use Try::Tiny;

# Constants
Readonly my $SCRIPT_PATH => dirname(abs_path($0));
Readonly my $BASE_DIR    => "$SCRIPT_PATH/../data/sota";
Readonly my %DIRS        => (
    'areas'    => "$BASE_DIR/areas",
    'datasets' => "$BASE_DIR/datasets",
    'tasks'    => "$BASE_DIR/tasks",
);
Readonly my $LOG_DIR    => "$SCRIPT_PATH/../logs";
Readonly my $LOG_FILE   => "$LOG_DIR/sota.log";
Readonly my $MAX_RETRIES => 5;
Readonly my $MAX_PROCESSES => 5; # Adjust as needed

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

# URLs for sitemap XMLs
my @sitemap_urls = (
    'https://paperswithcode.com/sitemap-areas.xml',
    map { "https://paperswithcode.com/sitemap-tasks.xml?p=$_" } (1..5),
    map { "https://paperswithcode.com/sitemap-datasets.xml?p=$_" } (1..10),
);

# Function to process XML data from a URL
sub fetch_and_process {
    my ($url, $category) = @_;

    try {
        my $response = $ua->get($url);
        if ($response->is_success) {
            my $xml = XML::Simple->new;
            my $data = $xml->XMLin($response->decoded_content);

            # Process each URL found within <loc> tags
            for my $url (@{$data->{url}}) {
                my $loc = $url->{loc};
                download_html($loc, $DIRS{$category});
            }
        } else {
            $logger->error("Failed to fetch XML: $url with error " . $response->status_line);
        }
    } catch {
        $logger->error("An error occurred while processing $url: $_");
    };
}

# Function to download HTML with retry and backoff
sub download_html {
    my ($url, $output_dir) = @_;
    my $retry = 0;
    my $success = 0;

    while (!$success && $retry < $MAX_RETRIES) {
        try {
            my $response = $ua->get($url);
            if ($response->is_success) {
                my $filename = (split '/', $url)[-1];
                $filename .= ".html" unless $filename =~ /\.html$/;
                my $file_path = "$output_dir/$filename";
                open my $fh, '>:encoding(UTF8)', $file_path or $logger->logdie("Cannot open file: $file_path");
                print $fh $response->decoded_content;
                close $fh;
                $logger->info("Downloaded and saved: $filename to $output_dir");
                $success = 1;
            } else {
                $logger->warn("Failed to download $url. Retry $retry. HTTP Status: " . $response->status_line);
                sleep(2 ** $retry);  # Exponential backoff
                $retry++;
            }
        } catch {
            $logger->error("An error occurred while downloading $url: $_");
            sleep(2 ** $retry);  # Exponential backoff
            $retry++;
        };
    }
    $logger->error("Failed to download after $MAX_RETRIES retries: $url") unless $success;
}

# Main function that dispatches processing based on category
sub main {
    my $pm = Parallel::ForkManager->new($MAX_PROCESSES);

    for my $url (@sitemap_urls) {
        $pm->start and next;

        my $category = $url =~ /sitemap-areas/ ? 'areas' :
                       $url =~ /sitemap-tasks/ ? 'tasks' :
                       'datasets';
        fetch_and_process($url, $category);

        $pm->finish;
    }

    $pm->wait_all_children;
}

# Call the main function
main();
