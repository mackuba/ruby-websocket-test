#!/usr/bin/env ruby

require 'bundler/setup'
require 'skyfall/firehose'
require 'skyfall/version'
require 'async'
require 'async/http/endpoint'
require 'async/websocket/client'

class FirehoseStream
  DEFAULT_RELAY = 'bsky.network'

  # start with a very old cursor to just stream from the beginning of the available buffer
  START_CURSOR = '1000000'

  def initialize(interval)
    @interval = interval.to_i
  end

  def yjit_status
    RubyVM::YJIT.enabled? ? 'on' : 'off'
  end

  def log(text)
    puts "[#{Time.now}] #{text}"
  end

  def start_eventmachine
    log "Starting firehose process using Faye/Eventmachine (YJIT = #{yjit_status})"

    @event_count = 0

    @sky = Skyfall::Firehose.new(DEFAULT_RELAY, :subscribe_repos, START_CURSOR)
    @sky.on_connect { log "Connected" }

    @sky.on_raw_message do |m|
      process_message(m)
    end

    @sky.connect
  end

  def start_async
    log "Starting firehose process using Async (YJIT = #{yjit_status})"

    @event_count = 0

    Async do |task|
      url = "wss://#{DEFAULT_RELAY}/xrpc/com.atproto.sync.subscribeRepos?cursor=#{START_CURSOR}"
      endpoint = Async::HTTP::Endpoint.parse(url)

      Async::WebSocket::Client.connect(endpoint) do |connection|
        @connection = connection
        log "Connected"

        while message = connection.read
          process_message(message.buffer)
        end
      end
    end
  end

  def process_message(msg)
    @benchmark_start ||= Time.now
    @event_count += 1

    if @event_count >= @interval
      log "Processing #{@event_count / (Time.now - @benchmark_start)} evt/s"
      @benchmark_start = Time.now
      @event_count = 0
    end
  end

  def stop
    @connection&.close
    @sky&.disconnect
  end
end

if $PROGRAM_NAME == __FILE__
  RubyVM::YJIT.enable

  if ARGV.length < 2
    puts "Usage: #{$PROGRAM_NAME} <async|eventmachine> <benchmark_interval>"
    puts "  e.g. #{$PROGRAM_NAME} async 30000"
    exit 1
  end

  stream = FirehoseStream.new(ARGV[1])

  trap("SIGINT") {
    puts "Stopping..."
    stream.stop
  }

  case ARGV[0]
  when 'async'
    stream.start_async
  when 'eventmachine'
    stream.start_eventmachine
  else
    puts "Unknown engine type: #{ARGV[0].inspect}"
    exit 1
  end
end
