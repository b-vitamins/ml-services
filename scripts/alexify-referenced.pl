#!/usr/bin/perl

use strict;
use warnings;
use Readonly;
use File::Basename;
use Cwd 'abs_path';
use File::Spec;
use File::Path qw(make_path);
use JSON;
use LWP::UserAgent;
use HTTP::Status qw(:constants);
use Log::Log4perl;
use threads;
use threads::shared;
use Thread::Queue;
use Time::HiRes qw(sleep);
use File::Find;
use Encode qw(encode);
use Getopt::Long;
use List::Util qw(min);
use POSIX qw(strftime);

# Constants and configuration
Readonly my $SCRIPT_DIR     => dirname( abs_path($0) );
Readonly my $DATA_DIR       => File::Spec->catfile( $SCRIPT_DIR, "..", "data" );
Readonly my $REFERENCED_DIR => File::Spec->catfile( $DATA_DIR, "referenced" );
Readonly my $LOG_DIR        => File::Spec->catfile( $SCRIPT_DIR, "..", "logs" );
Readonly my $STATE_FILE     => File::Spec->catfile( $SCRIPT_DIR, "state.json" );
Readonly my $NUM_THREADS    => 12;
Readonly my $MAX_CALLS_PER_DAY => 100000;
Readonly my $SECONDS_PER_DAY   => 86400;
Readonly my $GLOBAL_RATE_LIMIT => $MAX_CALLS_PER_DAY / $SECONDS_PER_DAY;
Readonly my $RATE_LIMIT        => $GLOBAL_RATE_LIMIT / $NUM_THREADS;
Readonly my $MAX_RETRIES       => 10;
Readonly my $INITIAL_BACKOFF   => 1;
Readonly my $BACKOFF_LIMIT     => 32;

# Command-line options
my $email;
GetOptions( 'email=s' => \$email );

# Generate a timestamp for log rotation
my $timestamp = strftime( "%Y%m%d.%H%M%S", localtime );
Readonly my $LOG_FILE =>
  File::Spec->catfile( $LOG_DIR, "alexify.referenced.$timestamp.log" );

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

# Create directories if they do not exist
for my $dir ( $REFERENCED_DIR, $LOG_DIR ) {
    eval {
        make_path($dir) unless -d $dir;
        1;
    } or $logger->fatal("Failed to create directory: $dir");
}

# Persistent state handling
my %processed_urls;

sub load_state {
    if ( -e $STATE_FILE ) {
        open my $fh, '<:encoding(UTF-8)', $STATE_FILE
          or die "Cannot open state file: $!";
        local $/ = undef;
        my $json_text = <$fh>;
        close $fh;
        %processed_urls = %{ decode_json($json_text) };
    }
}

sub save_state {
    open my $fh, '>:encoding(UTF-8)', $STATE_FILE
      or die "Cannot open state file: $!";
    print $fh encode_json( \%processed_urls );
    close $fh;
}

# Ensure state is saved periodically
my $state_save_interval  = 600;    # Save state every 10 minutes
my $last_state_save_time = time;

sub save_json {
    my ( $data, $folder, $original_filename ) = @_;
    my $filename = "$original_filename.json";
    my $json     = JSON->new->allow_nonref->allow_blessed->convert_blessed;
    open my $json_file, '>:encoding(UTF-8)',
      File::Spec->catfile( $folder, $filename )
      or die "Cannot open file: $!";
    print $json_file $json->encode($data);
    close $json_file;
}

sub unique_referenced {
    my ($directory) = @_;
    my %urls;

    find(
        sub {
            return if -d;
            return unless /\.json$/;

            my $file_path = $File::Find::name;

            # Skip files in the REFERENCED_DIR
            return if $file_path =~ /\Q$REFERENCED_DIR\E/;

            open my $f, '<:encoding(UTF-8)', $file_path or do {
                $logger->error("Error processing file $file_path: $!");
                return;
            };
            local $/ = undef;
            my $json_text = <$f>;
            close $f;
            my $data = decode_json($json_text);

            if ( ref $data->{referenced_works} eq 'ARRAY' ) {
                foreach my $url ( @{ $data->{referenced_works} } ) {
                    $urls{$url} = 1 unless exists $processed_urls{$url};
                }
            }
        },
        $directory
    );

    return keys %urls;
}

sub filter_existing_works {
    my @urls = @_;
    return grep {
        my $work_id = ( split "/", $_ )[-1];
        !-e File::Spec->catfile( $REFERENCED_DIR, "$work_id.json" );
    } @urls;
}

# Shared variable to signal threads to stop
my $stop : shared = 0;

sub process_referenced {
    my ($link) = @_;
    my $work_id = ( split "/", $link )[-1];
    my $file_path = File::Spec->catfile( $REFERENCED_DIR, "$work_id.json" );

    my $url = "https://api.openalex.org/works/$work_id";
    my $ua  = LWP::UserAgent->new;

    # Add the email header if provided
    if ($email) {
        $ua->default_header( 'From' => $email );
    }

    my $retry_count = 0;
    my $backoff     = $INITIAL_BACKOFF;

    while ( $retry_count < $MAX_RETRIES && !$stop ) {
        my $response = $ua->get($url);

        if ( $response->is_success ) {
            my $work = decode_json( $response->decoded_content );
            save_json( $work, $REFERENCED_DIR, $work_id );
            $logger->info(
                "Successfully processed and saved work $work_id.json");
            $processed_urls{$link} = 1;
            save_state_periodically();
            return;
        }
        elsif ( $response->code == HTTP_TOO_MANY_REQUESTS ) {
            $logger->warn(
"Received 429 Too Many Requests. Backing off for $backoff seconds."
            );
        }
        elsif ($response->code == HTTP_SERVICE_UNAVAILABLE
            || $response->code == HTTP_GATEWAY_TIMEOUT )
        {
            $logger->warn(
"Received service unavailable or gateway timeout. Backing off for $backoff seconds."
            );
        }
        else {
            $logger->error( "Error fetching work for ID $work_id: "
                  . $response->status_line );
        }

        sleep( $backoff + rand($backoff) );    # Adding jitter
        $backoff =
          min( $backoff * 2, $BACKOFF_LIMIT );  # Exponential backoff with limit
        $retry_count++;
        if ( $retry_count == $MAX_RETRIES ) {
            $logger->error(
                "Max retries reached for work ID $work_id. Skipping.");
        }
    }
}

sub save_state_periodically {
    if ( time - $last_state_save_time >= $state_save_interval ) {
        save_state();
        $last_state_save_time = time;
    }
}

sub main {
    load_state();
    my @referenced_list  = unique_referenced($DATA_DIR);
    my $total_referenced = scalar @referenced_list;
    @referenced_list = filter_existing_works(@referenced_list);
    my $to_be_downloaded = scalar @referenced_list;
    $logger->info(
"Found $total_referenced unique referenced works (of which $to_be_downloaded will be downloaded)."
    );

    if ( $to_be_downloaded == 0 ) {
        $logger->info("No new works to process. Exiting.");
        return;
    }

    my $queue = Thread::Queue->new(@referenced_list);
    my @threads;

    for ( 1 .. $NUM_THREADS ) {
        push @threads, threads->create(
            sub {
                while ( my $referenced = $queue->dequeue_nb ) {
                    last if $stop;
                    process_referenced($referenced);
                    last if $stop;
                    sleep($RATE_LIMIT);    # Rate limit
                    last if $stop;
                }
            }
        );
    }

    while ( !$stop ) {
        sleep(1);    # Main thread keeps running until stopped
    }

    $_->join for @threads;
    save_state();    # Ensure state is saved at the end
}

# Handle graceful shutdown
$SIG{INT} = sub {
    $stop = 1;
    $logger->info("Caught SIGINT. Shutting down gracefully...");
};

main();
