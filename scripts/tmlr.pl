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
Readonly my $BASE_DIR        => File::Spec->catdir($SCRIPT_DIR, '..', 'data', 'tmlr');
Readonly my $BIB_DIR         => File::Spec->catdir($BASE_DIR, 'bibliography');
Readonly my $LOG_DIR         => File::Spec->catdir($SCRIPT_DIR, '..', 'logs');
Readonly my $LOG_FILE        => File::Spec->catfile($LOG_DIR, 'tmlr.log');

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

# Function to modify .bib entry with improved error handling
sub modify_bib_entry {
    my ($bib_content, $pdf_link, $review_link, $code_link, $badge, $file_name, $bib_url) = @_;
    my $temp_bib_path = File::Spec->catfile($BIB_DIR, 'temp.bib');

    # Write the bib content to a temporary file using UTF-8 encoding
    open my $temp_fh, '>:encoding(UTF-8)', $temp_bib_path or croak "Could not create temp file: $!";
    print $temp_fh $bib_content;
    close $temp_fh;

    # Read the BibTeX file
    my $bib_file = Text::BibTeX::File->new($temp_bib_path);
    my $entry = Text::BibTeX::Entry->new;
    eval {
        $entry->read($bib_file) or die "Syntax error in BibTeX file";
    };
    if ($@) {
        $logger->error("Syntax error when processing $bib_url: $@");
        unlink $temp_bib_path;
        return;  # Skip processing this entry
    }

    # Set optional links and badge if provided
    $entry->set('pdf', $pdf_link) if $pdf_link;
    $entry->set('review', $review_link) if $review_link;
    $entry->set('code', $code_link) if $code_link;
    $entry->set('badge', $badge) if $badge;

    # Write the modified BibTeX entry to the target file using UTF-8 encoding
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
    foreach my $item ($tree->look_down(_tag => 'li', class => qr/item/)) {
        my $bib_link = $item->look_down(_tag => 'a', sub { $_[0]->attr('href') && $_[0]->attr('href') =~ /bib$/ });
        my $pdf_link = $item->look_down(_tag => 'a', sub { $_[0]->as_text eq 'pdf' });
        my $review_link = $item->look_down(_tag => 'a', sub { $_[0]->as_text eq 'openreview' });
        my $code_link = $item->look_down(_tag => 'a', sub { $_[0]->as_text eq 'code' });
        my $badge_link = $item->look_down(
            _tag => 'a',
            sub {
                my $text = $_[0]->as_text;
                return $text =~ /^(Featured|Outstanding|Survey|Reproducibility|Written by Expert Reviewer|Event)/;
            }
        );

        my $badge = $badge_link ? $badge_link->as_text : 'None';

        if ($bib_link) {
            my $bib_url = $BASE_URL . $bib_link->attr('href');
            my $bib_content = download_bib_file($bib_url);
            if ($bib_content) {
                my $file_name = "$file_counter.bib";
                $file_counter++;  # Increment the counter for each file
                modify_bib_entry(
                    $bib_content,
                    $pdf_link ? $pdf_link->attr('href') : undef,
                    $review_link ? $review_link->attr('href') : undef,
                    $code_link ? $code_link->attr('href') : undef,
                    $badge ne 'None' ? $badge : undef,  # Set badge to undef if not present
                    $file_name,
                    $bib_url  # Pass the URL for error logging
                );
                $logger->info("Successfully processed $bib_url" . ($badge ne 'None' ? " with badge: $badge" : ""));
            } else {
                $logger->error("Failed to download $bib_url");
            }
        }
    }
    $tree->delete;  # Clean up
}

# Function to concatenate all bib files for a specific year into one
sub concatenate_bibs {
    my $combined_bib_file = "$BIB_DIR/tmlr.bib";

    # Open the combined file for writing
    open my $combined_fh, '>:encoding(UTF8)', $combined_bib_file
      or $logger->error("Cannot open file: $combined_bib_file");

    # Iterate over all bib files in the year directory
    opendir my $dir, $BIB_DIR or croak "Cannot open directory: $BIB_DIR";
    while ( my $file = readdir $dir ) {
        next if $file =~ /^\./;    # Skip special directories . and ..
        next if $file eq 'tmlr.bib';    # Skip the combined .bib file

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
        next if $file eq 'tmlr.bib';    # Skip the combined .bib file

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