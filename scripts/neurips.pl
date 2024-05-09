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

# Constants
Readonly my $SCRIPT_DIR      => dirname(abs_path($0));
Readonly my $BASE_URL        => 'https://proceedings.neurips.cc';
Readonly my $PAGE_URL        => 'https://proceedings.neurips.cc/paper_files/paper';
Readonly my $START_YEAR      => 1987;
Readonly my $END_YEAR        => 2023;
Readonly my $BASE_DIR        => File::Spec->catdir($SCRIPT_DIR, '..', 'data', 'neurips');
Readonly my $BIB_DIR         => File::Spec->catdir($BASE_DIR, 'bibliography');
Readonly my $LOG_DIR         => File::Spec->catdir($SCRIPT_DIR, '..', 'logs');
Readonly my $LOG_FILE        => File::Spec->catfile($LOG_DIR, 'neurips.log');
Readonly my $RETRY_COUNT     => 3;
Readonly my $TIMEOUT_SECONDS => 10;
Readonly my $MAX_PARALLEL    => 38;

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

Readonly my %BIBTEX_KEY_MAP => (
    'BibTeX'         => 'bib',
    'Paper'          => 'pdf',
    'Metadata'       => 'metadata',
    'Supplemental'   => 'supplemental',
    'Code'           => 'code',
    'GitHub'         => 'code',
    'Review'         => 'review',
    'MetaReview'     => 'metareview',
    'AuthorFeedback' => 'feedback',
);

Readonly my %ERROR_CODES => (
    SUCCESS             => 0,
    FILE_OPEN_ERROR     => 2,
    FILE_WRITE_ERROR    => 3,
    REQUEST_FAILED      => 4,
    RETRIES_EXCEEDED    => 5,
    UNAUTHORIZED        => 6,
    FORBIDDEN           => 7,
    NOT_FOUND           => 8,
    INTERNAL_SERVER_ERR => 9,
    UNKNOWN_ERROR       => 10,
);

Readonly my %HTTP_ERROR_MAP => (
    401 => $ERROR_CODES{UNAUTHORIZED},
    403 => $ERROR_CODES{FORBIDDEN},
    404 => $ERROR_CODES{NOT_FOUND},
    500 => $ERROR_CODES{INTERNAL_SERVER_ERR},
);

Readonly my %HTTP_ERROR_RESOLUTIONS => (
    400 => "Check the request parameters.",
    401 => "Ensure valid authentication credentials.",
    403 => "Confirm appropriate permissions for the requested resource.",
    404 => "Verify the URL is correct and the resource exists.",
    500 => "Check the server status or contact the server administrator.",
    503 => "The server is unavailable. Try again later.",
);

my $ua = LWP::UserAgent->new;
$ua->agent('Mozilla/5.0');

# Create directories if they do not exist
for my $dir ( $BASE_DIR, $BIB_DIR ) {
    eval {
        make_path($dir) unless -d $dir;
        1;
    } or $logger->fatal("Failed to create directory: $dir");
}

# Function to prepend the base URL if a relative URL is provided
sub prepend_base_url {
    my ($relative_url) = @_;
    return unless defined $relative_url;
    return $relative_url if $relative_url =~ /^https?:\/\//;
    return $BASE_URL . $relative_url;
}

sub download_file {
    my ( $url, $file_path ) = @_;
    my $full_url = prepend_base_url($url);

    my $attempt         = 0;
    my $current_timeout = $TIMEOUT_SECONDS;

    while ( $attempt < $RETRY_COUNT ) {
        $attempt++;
        my $response = $ua->get($full_url);

        if ( $response->is_success ) {
            if ( open my $fh, '>:encoding(UTF8)', $file_path ) {
                unless ( print {$fh} $response->decoded_content ) {
                    $logger->error(
                        sprintf(
                            "Failed to write to file %s (attempt %d)",
                            $attempt, $file_path
                        )
                    );
                    close $fh;
                    return $ERROR_CODES{FILE_WRITE_ERROR};
                }
                close $fh;
                $logger->info(
                    sprintf(
                        "Downloaded %s to %s (attempt %d)",
                        $full_url, $file_path, $attempt
                    )
                );
                return $ERROR_CODES{SUCCESS};
            }
            else {
                $logger->error(
                    sprintf(
                        "Failed to open file %s for writing (attempt %d)",
                        $attempt, $file_path
                    )
                );
                next;
            }
        }

        my $status = $response->code;
        if ( $status == 429 && $attempt < $RETRY_COUNT ) {
            $logger->warn(
                sprintf(
"(429 Too Many Requests) encountered (attempt %d). Waiting for %d seconds before retry...",
                    $attempt, $current_timeout
                )
            );
            sleep $current_timeout;
            $current_timeout *= 2;
        }
        elsif ( exists $HTTP_ERROR_MAP{$status} ) {
            my $error_code = $HTTP_ERROR_MAP{$status};
            my $resolution = $HTTP_ERROR_RESOLUTIONS{$status}
              // "Refer to the server administrator for further assistance";
            $logger->error(
                sprintf(
                    "Attempt %d: HTTP %d Error (%s) for URL %s. Resolution: %s",
                    $attempt,  $status, $response->status_line,
                    $full_url, $resolution
                )
            );
            return $error_code;
        }
        else {
            $logger->error(
                sprintf(
"Attempt %d: Unhandled HTTP Error (%s) for URL %s. Retrying...",
                    $attempt, $response->status_line, $full_url
                )
            );
            sleep 2;
        }
    }
    $logger->error(
        sprintf(
            "All retries (%d) exceeded for URL %s",
            $RETRY_COUNT, $full_url
        )
    );
    return $ERROR_CODES{RETRIES_EXCEEDED};
}

# Sets BibTeX fields from the extracted links
sub set_bibtex_entry {
    my ( $entry, %link_map ) = @_;
    for my $key ( keys %link_map ) {
        my $field = $BIBTEX_KEY_MAP{$key} // $key;
        if ( defined $link_map{$key} ) {
            $entry->set( $field, prepend_base_url( $link_map{$key} ) );
        }
    }
}

# Safely extracts a URL from the HTML tree given a pattern
sub safe_attr {
    my ( $tree, $pattern ) = @_;
    my $element =
      $tree->look_down( _tag => 'a', sub { $_[0]->as_text =~ /$pattern/i } );
    return defined $element ? $element->attr('href') : undef;
}

# Modifies the BibTeX entry with the relevant links
sub modify_bib_entry {
    my ( $file_path, %links ) = @_;
    eval {
        my $bib_file = Text::BibTeX::File->new($file_path);
        my $entry    = Text::BibTeX::Entry->new($bib_file);
        set_bibtex_entry( $entry, %links );
        open my $fh, '>', $file_path or croak "Cannot open file: $!";
        print {$fh} $entry->print_s or croak "Cannot write to file: $!";
        close $fh;
        1;
    } or do {
        $logger->error("Failed to modify BibTeX file: $file_path");
        return $ERROR_CODES{FILE_WRITE_ERROR};
    };
    return $ERROR_CODES{SUCCESS};
}

# Extracts the links from the HTML tree using a generic pattern
sub extract_links {
    my ($tree) = @_;
    my %links;
    while ( my ( $key, $field ) = each %BIBTEX_KEY_MAP ) {
        $links{$key} = safe_attr( $tree, $key );
    }
    return %links;
}

# Processes an individual paper by extracting its links and modifying the BibTeX file
sub process_paper {
    my ( $paper_url, $year, $file_index ) = @_;
    my $response = $ua->get( $BASE_URL . $paper_url );
    if ( $response->is_success ) {
        my $tree =
          HTML::TreeBuilder->new_from_content( $response->decoded_content );
        my %links = extract_links($tree);
        if ( $links{'BibTeX'} ) {
            my $year_dir = "$BIB_DIR/$year";
            if ( !-d $year_dir ) {
                make_path($year_dir)
                  or croak "Cannot create year directory: $!";
            }
            my $file_path = "$year_dir/$file_index.bib";
            my $bib_result = download_file( $links{'BibTeX'}, $file_path );
            if ( $bib_result == $ERROR_CODES{SUCCESS} ) {
                modify_bib_entry( $file_path, %links, url => $paper_url );
            }
            else {
                $logger->error(
                    "Failed to download .bib from: $links{'BibTeX'}");
            }
        }
        $tree->delete;
    }
    else {
        $logger->error(
            "Failed to access individual paper page: $BASE_URL$paper_url");
    }
}

# Handles all the papers for a specific year
sub process_year {
    my ($year)   = @_;
    my $year_url = "$PAGE_URL/$year";
    my $response = $ua->get($year_url);
    if ( $response->is_success ) {
        my $tree =
          HTML::TreeBuilder->new_from_content( $response->decoded_content );
        my $file_index = 1;
        foreach my $link (
            $tree->look_down(
                _tag => 'a',
                sub {
                    $_[0]->attr('href')
                      && $_[0]->attr('href') =~
                      /-Abstract(?:-Conference)?\.html$/;
                }
            )
          )
        {
            my $paper_url = $link->attr('href');
            process_paper( $paper_url, $year, $file_index );
            $file_index++;
        }
        $tree->delete;
    }
    else {
        $logger->error("Failed to access proceedings for $year: $year_url");
    }
}

# Function to concatenate all bib files for a specific year into one
sub concatenate_bibs {
    my ($year)            = @_;
    my $year_dir          = "$BIB_DIR/$year";
    my $combined_bib_file = "$BIB_DIR/$year.bib";

    # Open the combined file for writing
    open my $combined_fh, '>:encoding(UTF8)', $combined_bib_file
      or croak "Cannot open file: $combined_bib_file";

    # Iterate over all bib files in the year directory
    opendir my $dir, $year_dir or croak "Cannot open directory: $year_dir";
    while ( my $file = readdir $dir ) {
        next if $file =~ /^\./;    # Skip special directories . and ..

        my $file_path = "$year_dir/$file";
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
"Successfully concatenated BibTeX files for year: $year into $combined_bib_file"
    );
}

# Function to clean up the individual bib files and year directory
sub cleanup {
    my ($year) = @_;
    my $year_dir = "$BIB_DIR/$year";

    # Remove individual bib files and the year directory
    unlink glob "$year_dir/*";
    rmdir $year_dir or croak "Failed to remove directory: $year_dir";

    $logger->info(
        "Successfully cleaned up BibTeX files and directory for year: $year");
}

sub main {
    my $pm = Parallel::ForkManager->new($MAX_PARALLEL);
    for my $year ( $START_YEAR .. $END_YEAR ) {
        $pm->start and next;    # Fork a child process
        process_year($year);
        concatenate_bibs($year);    # Concatenate all bib files for the year
        cleanup($year);             # Clean up individual files and directory
        $pm->finish;                # End the child process
    }
    $pm->wait_all_children;
}

main();
