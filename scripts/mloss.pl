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
Readonly my $PAGE_URL => 'https://jmlr.org/mloss';
Readonly my $BASE_DIR        => File::Spec->catdir($SCRIPT_DIR, '..', 'data', 'mloss');
Readonly my $BIB_DIR         => File::Spec->catdir($BASE_DIR, 'bibliography');
Readonly my $LOG_DIR         => File::Spec->catdir($SCRIPT_DIR, '..', 'logs');
Readonly my $LOG_FILE        => File::Spec->catfile($LOG_DIR, 'mloss.log');

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


# Function to download .bib files
sub download_bib_file {
    my ($bib_url) = @_;
    my $response = $ua->get($bib_url);
    return $response->is_success ? $response->decoded_content : undef;
}


# Function to modify .bib entry
sub modify_bib_entry {
    my ($bib_content, $pdf_link, $code_link, $file_name) = @_;

    # Write the bib content to a temporary file to read with Text::BibTeX
    my $temp_bib_path = "$BIB_DIR/temp.bib";
		open my $temp_fh, '>:encoding(UTF-8)', $temp_bib_path or croak "Could not create temp file: $!";
    print $temp_fh $bib_content;
    close $temp_fh;

    # Read the BibTeX file
    my $bib_file = Text::BibTeX::File->new($temp_bib_path);
    my $entry = Text::BibTeX::Entry->new;
    $entry->read($bib_file);

    # Set PDF and code links if provided
    $entry->set('pdf', $pdf_link) if $pdf_link;
    $entry->set('code', $code_link) if $code_link;

    # Save to new .bib file
    my $output_bib_path = File::Spec->catfile($BIB_DIR, $file_name);
    open my $fh, '>:encoding(UTF-8)', $output_bib_path or croak "Could not open file '$output_bib_path': $!";
    print $fh $entry->print_s;
    close $fh;

    # Clean up the temporary file
    unlink $temp_bib_path;
}

# Main parser function
sub parse_page {
    my $url = shift;
    my $response = $ua->get($url);
    $logger->error("Failed to access $url") unless $response->is_success;

    my $tree = HTML::TreeBuilder->new_from_content($response->decoded_content);
    my $file_counter = 1;  # Initialize a counter to name files sequentially
    foreach my $dl ($tree->look_down(_tag => 'dl')) {
        my $bib_link = $dl->look_down(_tag => 'a', sub { $_[0]->attr('href') && $_[0]->attr('href') =~ /\.bib$/ });
        my $pdf_link = $dl->look_down(_tag => 'a', sub { $_[0]->attr('href') && $_[0]->attr('href') =~ /\.pdf$/ });
        my $code_link = $dl->look_down(_tag => 'a', sub { $_[0]->as_text eq 'code' });

        if ($bib_link) {
            my $bib_url = $BASE_URL . $bib_link->attr('href');
            my $bib_content = download_bib_file($bib_url);
            if ($bib_content) {
                my $file_name = "$file_counter.bib";
                $file_counter++;  # Increment the counter for each file
                modify_bib_entry(
                    $bib_content,
                    $pdf_link ? $pdf_link->attr('href') : undef,
                    $code_link ? $code_link->attr('href') : undef,
                    $file_name
                );
                $logger->info("Successfully processed $bib_url");
            } else {
								$logger->error("Failed to download $bib_url");
            }
        } else {
            $logger->error("No .bib link found in this item\n");
        }
    }
    $tree->delete; # Clean up
}

# Function to concatenate all bib files for a specific year into one
sub concatenate_bibs {
    my $combined_bib_file = "$BIB_DIR/mloss.bib";

    # Open the combined file for writing
    open my $combined_fh, '>:encoding(UTF8)', $combined_bib_file
      or $logger->error("Cannot open file: $combined_bib_file");

    # Iterate over all bib files in the year directory
    opendir my $dir, $BIB_DIR or croak "Cannot open directory: $BIB_DIR";
    while ( my $file = readdir $dir ) {
        next if $file =~ /^\./;    # Skip special directories . and ..
        next if $file eq 'mloss.bib';    # Skip the combined .bib file

        my $file_path = "$BIB_DIR/$file";
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
				"Successfully concatenated BibTeX files  into $combined_bib_file"
    );
}

# Function to clean up the individual bib files
sub cleanup {
    opendir my $dir, $BIB_DIR or croak "Cannot open directory: $BIB_DIR";
    while (my $file = readdir $dir) {
        next unless $file =~ /\.bib$/;  # Only delete .bib files
        next if $file eq 'mloss.bib';    # Skip the combined .bib file

        my $file_path = "$BIB_DIR/$file";
        unlink $file_path or croak "Failed to delete file: $file_path";
    }
    closedir $dir;

    $logger->info("Successfully cleaned up individual BibTeX files");
}

# Main routine
sub main {
    parse_page($PAGE_URL);
		concatenate_bibs();
		cleanup();
}

main();
