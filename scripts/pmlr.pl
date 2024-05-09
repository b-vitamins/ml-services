#!/usr/bin/perl

use strict;
use warnings;
use Readonly;
use LWP::UserAgent;
use Cwd 'abs_path';
use File::Path qw(make_path);
use File::Spec;
use File::Basename;
use File::Spec;
use Carp qw(croak carp);
use Log::Log4perl;

# Constants
Readonly my $SCRIPT_DIR   => dirname( abs_path($0) );
Readonly my $BASE_URL     => 'http://proceedings.mlr.press';
Readonly my $START_VOLUME => 1;
Readonly my $END_VOLUME   => 238;
Readonly my $BASE_DIR =>
  File::Spec->catdir( $SCRIPT_DIR, '..', 'data', 'pmlr' );
Readonly my $BIB_DIR      => File::Spec->catdir( $BASE_DIR,   'bibliography' );
Readonly my $CITEPROC_DIR => File::Spec->catdir( $BASE_DIR,   'citeproc' );
Readonly my $LOG_DIR      => File::Spec->catdir( $SCRIPT_DIR, '..', 'logs' );
Readonly my $LOG_FILE        => File::Spec->catfile( $LOG_DIR, 'pmlr.log' );
Readonly my $RETRY_COUNT     => 3;
Readonly my $TIMEOUT_SECONDS => 10;

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
for my $dir ( $BASE_DIR, $BIB_DIR, $CITEPROC_DIR ) {
    eval {
        make_path($dir) unless -d $dir;
        1;
    } or $logger->fatal("Failed to create directory: $dir");
}

sub download_file {
    my ( $url, $file_path ) = @_;

    my $attempt         = 0;
    my $current_timeout = $TIMEOUT_SECONDS;

    while ( $attempt < $RETRY_COUNT ) {
        $attempt++;
        my $response = $ua->get( $url, 'Accept' => 'text/plain' );

        if ( $response->is_success ) {
            if ( open my $fh, '>:encoding(UTF8)', $file_path ) {
                unless ( print {$fh} $response->decoded_content ) {
                    $logger->error(
                        "Failed to write to file $file_path (attempt $attempt)"
                    );
                    close $fh;
                    return $ERROR_CODES{FILE_WRITE_ERROR};
                }
                close $fh;
                $logger->info(
                    "Downloaded $url to $file_path (attempt $attempt)");
                return $ERROR_CODES{SUCCESS};
            }
            else {
                $logger->error(
"Failed to open file $file_path for writing (attempt $attempt)"
                );
            }
        }
        else {
            handle_http_error( $response, $url, $attempt, $current_timeout );
        }
    }
    $logger->error("All retries exceeded for URL $url");
    return $ERROR_CODES{RETRIES_EXCEEDED};
}

sub handle_http_error {
    my ( $response, $url, $attempt, $current_timeout ) = @_;
    my $status = $response->code;

    if ( $status == 429 || $status == 503 ) {
        $current_timeout *= 2;
        $logger->warn(
"HTTP $status encountered. Retrying after $current_timeout seconds..."
        );
        sleep $current_timeout;
    }
    elsif ( exists $HTTP_ERROR_MAP{$status} ) {
        my $error_code = $HTTP_ERROR_MAP{$status};
        my $resolution = $HTTP_ERROR_RESOLUTIONS{$status}
          // "Contact server administrator.";
        $logger->error(
"HTTP $status error (attempt $attempt) for URL $url. Resolution: $resolution"
        );
        return $error_code;
    }
    else {
        $logger->error(
"Unhandled HTTP error $status (attempt $attempt) for URL $url. Retrying..."
        );
        sleep 2;
    }
}

# Main process to download all bibliography and citeproc files
sub main {
    $logger->info(
"Starting download of bibliography and citeproc files from Volume $START_VOLUME to $END_VOLUME."
    );

    for my $i ( $START_VOLUME .. $END_VOLUME ) {

        # Download and log `.bib` file
        my $bib_path   = File::Spec->catfile( $BIB_DIR, "v$i.bib" );
        my $bib_url    = "$BASE_URL/v$i/assets/bib/bibliography.bib";
        my $bib_result = download_file( $bib_url, $bib_path );

        # Download and log `.yaml` file
        my $citeproc_path   = File::Spec->catfile( $CITEPROC_DIR, "v$i.yaml" );
        my $citeproc_url    = "$BASE_URL/v$i/assets/bib/citeproc.yaml";
        my $citeproc_result = download_file( $citeproc_url, $citeproc_path );

    }

    $logger->info(
        "Completed downloading files from Volume $START_VOLUME to $END_VOLUME."
    );
    return;
}

main();
