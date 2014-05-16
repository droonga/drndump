# Copyright (C) 2014 Droonga Project
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
require "optparse"
require "json"

require "cool.io"

require "droonga/client"

require "drndump/version"

module Drndump
  class Command
    class << self
      def run(argv)
        new.run(argv)
      end
    end

    def initialize
      @loop = Coolio::Loop.default
      @host = "localhost"
      @port = 10031
      @tag  = "droonga"
      @dataset  = "Droonga"
      @receiver_host = Socket.gethostname
      @receiver_port = 0
    end

    def run(argv)
      parse_command_line_arguments!(argv)

      error_message = dump

      if error_message
        $stderr.puts(error_message)
        false
      else
        true
      end
    end

    private
    def parse_command_line_arguments!(argv)
      parser = create_option_parser
      parser.parse!(argv)
    end

    def create_option_parser
      parser = OptionParser.new
      parser.version = VERSION

      parser.separator("")
      parser.separator("Connect:")
      parser.on("--host=HOST",
                "Host name to be connected.",
                "(#{@host})") do |host|
        @host = host
      end
      parser.on("--port=PORT", Integer,
                "Port number to be connected.",
                "(#{@port})") do |port|
        @port = port
      end
      parser.on("--tag=TAG",
                "Tag name to be used to communicate with Droonga system.",
                "(#{@tag})") do |tag|
        @tag = tag
      end

      parser.separator("")
      parser.separator("Data:")
      parser.on("--dataset=DATASET",
                "Dataset to be dumped.",
                "(#{@dataset})") do |dataset|
        @dataset = dataset
      end

      parser.separator("")
      parser.separator("Droonga protocol:")
      parser.on("--receiver-host=HOST",
                "Host name to be received a response from Droonga engine.",
                "(#{@receiver_host})") do |host|
        @receiver_host = host
      end
      parser.on("--receiver-port=PORT", Integer,
                "Port number to be received a response from Droonga engine.",
                "(#{@receiver_port})") do |port|
        @receiver_port = port
      end

      parser
    end

    def client_options
      {
        :host          => @host,
        :port          => @port,
        :tag           => @tag,
        :protocol      => :droonga,
        :receiver_host => @receiver_host,
        :receiver_port => @receiver_port,
        :backend       => :coolio,
        :loop          => @loop,
      }
    end

    def dump
      client = Droonga::Client.new(client_options)

      error_message = nil
      n_dumpers = 0

      dump_message = {
        "type"    => "dump",
        "dataset" => @dataset,
      }
      client.subscribe(dump_message) do |message|
        case message
        when Droonga::Client::Error
          client.close
          error_message = message.to_s
        else
          case message["type"]
          when "dump.result", "dump.error"
            if message["statusCode"] != 200
              client.close
              error = message["body"]
              error_message = "#{error['name']}: #{error['message']}"
            end
          when "dump.table"
            table_create_message = convert_to_table_create_message(message)
            puts(JSON.pretty_generate(table_create_message))
          when "dump.column"
            column_create_message = convert_to_column_create_message(message)
            puts(JSON.pretty_generate(column_create_message))
          when "dump.record"
            add_message = message.dup
            add_message.delete("inReplyTo")
            add_message["type"] = "add"
            puts(JSON.pretty_generate(add_message))
          when "dump.start"
            n_dumpers += 1
          when "dump.end"
            n_dumpers -= 1
            client.close if n_dumpers <= 0
          end
        end
      end
      @loop.run

      error_message
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
  end
end
