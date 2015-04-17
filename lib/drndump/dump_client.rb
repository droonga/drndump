# Copyright (C) 2014-2015 Droonga Project
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1 as published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

require "socket"

require "droonga/client"

module Drndump
  class DumpClient
    DEFAULT_MESSAGES_PER_SECOND = 10000 # same to droogna-engine's one
    MIN_REPORTED_THROUGHPUT = 0.01

    attr_reader :n_forecasted_messages, :n_received_messages
    attr_reader :error_message
    attr_writer :on_finish, :on_progress, :on_error

    class NilMessage < StandardError
    end

    class InvalidMessage < StandardError
    end

    class ClientError < StandardError
      def initialize(error)
        super(error.inspect)
      end
    end

    def initialize(params)
      @host     = params[:host]
      @port     = params[:port]
      @tag      = params[:tag]
      @dataset  = params[:dataset]

      @receiver_host = params[:receiver_host]
      @receiver_port = params[:receiver_port]

      @n_forecasted_messages = 0
      @n_received_messages = 0
      @n_messages_per_second = DEFAULT_MESSAGES_PER_SECOND

      @error_message = nil

      @on_finish = nil
      @on_progress = nil
      @on_error = nil
    end

    def run(options={}, &block)
      extra_client_options = {
        :backend => options[:backend],
        :loop    => options[:loop],
      }
      client = Droonga::Client.new(client_options.merge(extra_client_options))
      client.on_error = lambda do |error|
        on_error(ClientError.new(error))
      end

      n_dumpers = 0

      @n_messages_per_second = options[:messages_per_second] || DEFAULT_MESSAGES_PER_SECOND
      @n_messages_per_second = [@n_messages_per_second, 1].max

      @measure_start_time = Time.now
      @previous_measure_time = @measure_start_time
      @previous_n_received_messages = 0.0

      dump_message = {
        "type"    => "dump",
        "dataset" => @dataset,
        "body"    => {
          "messagesPerSecond" => @n_messages_per_second,
        },
      }
      client.subscribe(dump_message) do |message|
        on_progress(message)
        case message
        when Droonga::Client::Error
          client.close
          on_error(message)
          @error_message = message.to_s
        when Hash
          case message["type"]
          when "dump.result", "dump.error"
            if message["statusCode"] != 200
              client.close
              error = message["body"]
              on_error(error)
              @error_message = "#{error['name']}: #{error['message']}"
            end
          when "dump.table"
            @n_received_messages += 1
            table_create_message = convert_to_table_create_message(message)
            yield(table_create_message)
          when "dump.column"
            @n_received_messages += 1
            column_create_message = convert_to_column_create_message(message)
            yield(column_create_message)
          when "dump.record"
            @n_received_messages += 1
            add_message = message.dup
            add_message.delete("inReplyTo")
            add_message["type"] = "add"
            yield(add_message)
          when "dump.start"
            n_dumpers += 1
          when "dump.end"
            n_dumpers -= 1
            if n_dumpers <= 0
              client.close
              on_finish
            end
          when "dump.forecast"
            @n_forecasted_messages += message["body"]["nMessages"]
          end
        when NilClass
          client.close
          error = NilMessage.new("nil message in dump")
          on_error(error)
          @error_message = error.to_s
        else
          client.close
          error = InvalidMessage.new("invalid message in dump",
                                     :message => message.inspect)
          on_error(error)
          @error_message = error.to_s
        end
      end

      @error_message
    end

    def recent_throughput
      now = Time.now
      n_messages = @n_received_messages - @previous_n_received_messages
      if now - @previous_measure_time < 1
        now = @previous_measure_time
        n_messages = @previous_n_received_messages
      else
        @previous_measure_time = now
        @previous_n_received_messages = n_messages.to_f
      end
      elapsed_seconds = now - @measure_start_time

      [n_messages / elapsed_seconds, MIN_REPORTED_THROUGHPUT].max
    end

    def n_remaining_messages
      [@n_forecasted_messages - @n_received_messages, 0].max
    end

    def remaining_seconds
      throughput = [recent_throughput, @n_messages_per_second].min
      n_remaining_messages.to_f / throughput
    end

    ONE_MINUTE_IN_SECONDS = 60
    ONE_HOUR_IN_SECONDS = ONE_MINUTE_IN_SECONDS * 60

    def formatted_remaining_time
      seconds  = remaining_seconds
      hours    = (seconds / ONE_HOUR_IN_SECONDS).floor
      seconds -= hours * ONE_HOUR_IN_SECONDS
      minutes  = (seconds / ONE_MINUTE_IN_SECONDS).floor
      seconds -= minutes * ONE_MINUTE_IN_SECONDS
      sprintf("%02i:%02i:%02i", hours, minutes, seconds)
    end

    def progress_percentage
      return 0 if @n_forecasted_messages.zero?
      progress = @n_received_messages.to_f / @n_forecasted_messages
      [(progress * 100).to_i, 100].min
    end

    private
    def client_options
      {
        :host          => @host,
        :port          => @port,
        :tag           => @tag,
        :protocol      => :droonga,
        :receiver_host => @receiver_host,
        :receiver_port => @receiver_port,
      }
    end

    def convert_to_table_create_message(message)
      body = message["body"]
      flags = []
      case body["type"]
      when "Array"
        flags << "TABLE_NO_KEY"
      when "Hash"
        flags << "TABLE_HASH_KEY"
      when "PatriciaTrie"
        flags << "TABLE_PAT_KEY"
      when "DoubleArrayTrie"
        flags << "TABLE_DAT_KEY"
      end
      table_create_message = {
        "type"    => "table_create",
        "dataset" => message["dataset"],
        "body" => {
          "name"              => body["name"],
          "flags"             => flags.join("|"),
          "key_type"          => body["keyType"],
        }
      }

      if body["tokenizer"]
        table_create_message["body"]["default_tokenizer"] = body["tokenizer"]
      end
      if body["normalizer"]
        table_create_message["body"]["normalizer"] = body["normalizer"]
      end

      table_create_message
    end

    def convert_to_column_create_message(message)
      body = message["body"]
      column_create_message = {
        "type"    => "column_create",
        "dataset" => message["dataset"],
        "body" => {
          "table"  => body["table"],
          "name"   => body["name"],
          "type"   => body["valueType"],
        }
      }

      flags = []
      case body["type"]
      when "Scalar"
        flags << "COLUMN_SCALAR"
      when "Vector"
        flags << "COLUMN_VECTOR"
        vector_options = body["vectorOptions"] || {}
        flags << "WITH_WEIGHT" if vector_options["weight"]
      when "Index"
        flags << "COLUMN_INDEX"
        index_options = body["indexOptions"] || {}
        flags << "WITH_SECTION"  if index_options["section"]
        flags << "WITH_WEIGHT"   if index_options["weight"]
        flags << "WITH_POSITION" if index_options["position"]
      end

      column_create_message["body"]["flags"] = flags.join("|")

      if body["type"] == "Index"
        index_options = body["indexOptions"] || {}
        sources = index_options["sources"] || []
        unless sources.empty?
          column_create_message["body"]["source"] = sources.join(",")
        end
      end

      column_create_message
    end

    def on_finish
      @on_finish.call if @on_finish
    end

    def on_progress(message)
      @on_progress.call(message) if @on_progress
    end

    def on_error(error)
      @on_error.call(error) if @on_error
    end
  end
end
