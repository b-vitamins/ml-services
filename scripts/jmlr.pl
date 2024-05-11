#!/usr/bin/perl

use strict;
use warnings;
use Readonly;
use LWP::UserAgent;
use Text::BibTeX;
use HTML::TreeBuilder;
use Cwd 'abs_path';
use File::Path qw(make_path);
use File::Glob ':glob';
use File::Basename;
use File::Spec;
use Carp qw(croak);
use Parallel::ForkManager;
use Log::Log4perl;

# Define constants
Readonly my $SCRIPT_DIR      => dirname(abs_path($0));
Readonly my $BASE_URL => 'https://jmlr.org';
Readonly my $PAGE_URL => 'https://jmlr.org/tmlr/papers';
Readonly my $BASE_DIR        => File::Spec->catdir($SCRIPT_DIR, '..', 'data', 'jmlr');
Readonly my $BIB_DIR         => File::Spec->catdir($BASE_DIR, 'bibliography');
Readonly my $LOG_DIR         => File::Spec->catdir($SCRIPT_DIR, '..', 'logs');
Readonly my $LOG_FILE        => File::Spec->catfile($LOG_DIR, 'jmlr.log');
Readonly my $MAX_PARALLEL    => 50;
Readonly my @VOLUMES = (1..25);
Readonly my @SPECIAL_ISSUES = qw(kernel01 shallow_parsing02 colt02 icml01 text_images03 feature03 kdfusion03 ilp03 learning_theory03 ica03 chervonenkis15);
Readonly my @TOPICAL_ISSUES = qw(learning_theory inductive_programming ml_opt ml_sec COLT2005 model_selection causality language graphs_relations large_scale_learning gesture_recognition 2016-Learning-from-Electronic-Health-Data bayesian_optimization);

# Create the log directory if it doesn't exist
unless ( -d $LOG_DIR ) {
    make_path($LOG_DIR) or die "Failed to create log directory: $LOG_DIR";
}

# Initialize the Log4perl configuration
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

# Initialize logging
Log::Log4perl::init( \$log_config );
my $logger = Log::Log4perl->get_logger();

# Prepare user agent
my $ua = LWP::UserAgent->new;
$ua->agent("Mozilla/5.0");

# Create directories if they do not exist
eval {
		make_path($BIB_DIR) unless -d $BIB_DIR;
		1;
} or $logger->fatal("Failed to create directory: $BIB_DIR");

# Helper subroutine to download files
sub download_file {
    my ($url, $dir, $file_name) = @_;
    my $response = $ua->get($url);
    if ($response->is_success) {
        my $file_path = "$dir/$file_name";
        open my $fh, '>:encoding(UTF-8)', $file_path or croak "Could not open file '$file_path': $!";
        print $fh $response->decoded_content;
        close $fh;
        return 1;
    } else {
        $logger->error("Failed to download $url");
        return 0;
    }
}

# Process each volume and issue with appropriate naming
sub process_volume {
    my ($url, $folder_label, $prefix) = @_;
    my $dir = "$BIB_DIR/$folder_label";
    make_path($dir) unless -d $dir;
    my $response = $ua->get($url);
    if ($response->is_success) {
        my $tree = HTML::TreeBuilder->new_from_content($response->decoded_content);
        my $count = 1;
        for my $link ($tree->look_down(_tag => 'a')) {
            if (my $href = $link->attr('href')) {
                if ($href =~ /\.bib$/) {
                    my $file_name = "${prefix}-${count}.bib";
                    my $bib_url = "$BASE_URL$href";
                    $count++;
										if ( !-d $dir ) {
												make_path($dir)
														or croak "Cannot create volume directory: $!";
										}
                    download_file($bib_url, $dir, $file_name);
                $logger->info("Successfully processed $bib_url");
                }
            }
        }
        $tree->delete;
    } else {
				$logger->error("Failed to download $url");
    }
}

# Function to concatenate all bib files for a specific year into one
sub concatenate_bibs {
    my ($volume)            = @_;
    my $volume_dir          = "$BIB_DIR/$volume";
    my $combined_bib_file = "$BIB_DIR/$volume.bib";

    # Open the combined file for writing
    open my $combined_fh, '>:encoding(UTF8)', $combined_bib_file
      or croak "Cannot open file: $combined_bib_file";

    # Iterate over all bib files in the year directory
    opendir my $dir, $volume_dir or croak "Cannot open directory: $volume_dir";
    while ( my $file = readdir $dir ) {
        next if $file =~ /^\./;    # Skip special directories . and ..

        my $file_path = "$volume_dir/$file";
        next unless -f $file_path;    # Skip if not a file

      # Open each individual bib file and write its content to the combined file
        open my $fh, '<:encoding(UTF8)', $file_path
          or croak "Cannot open file: $file_path";
        while ( my $line = <$fh> ) {
            print $combined_fh $line;
        }
        close $fh;
    }
    closedir $dir;

    # Close the combined bib file
    close $combined_fh;

    $logger->info(
"Successfully concatenated BibTeX files for volume: $volume into $combined_bib_file"
    );
}

# Function to clean up the individual bib files and year directory
sub cleanup {
    my ($volume) = @_;
    my $volume_dir = "$BIB_DIR/$volume";

    # Remove individual bib files and the year directory
    unlink glob "$volume_dir/*";
    rmdir $volume_dir or croak "Failed to remove directory: $volume_dir";

    $logger->info(
        "Successfully cleaned up BibTeX files and directory for year: $volume");
}

# Main routine
sub main {
    my $pm = Parallel::ForkManager->new($MAX_PARALLEL);
    foreach my $volume (@VOLUMES) {
        $pm->start and next;    # Fork a child process
        process_volume("$BASE_URL/papers/v$volume", "v$volume", "v$volume");
				concatenate_bibs("v$volume");
        cleanup("v$volume");
        $pm->finish;                # End the child process
    }
    foreach my $index (0 .. $#SPECIAL_ISSUES) {
        process_volume("$BASE_URL/papers/special/$SPECIAL_ISSUES[$index].html", "s$index", "s$index");
				concatenate_bibs("s$index");
        cleanup("s$index");
        $pm->finish;                # End the child process
    }
    foreach my $index (0 .. $#TOPICAL_ISSUES) {
        process_volume("$BASE_URL/papers/topic/$TOPICAL_ISSUES[$index].html", "t$index", "t$index");
				concatenate_bibs("t$index");
        cleanup("t$index");
        $pm->finish;                # End the child process
    }
}

main();
