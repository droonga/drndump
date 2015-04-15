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

require "drndump/version"
require "drndump/dump_client"

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
      @dataset  = "Default"
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

    def dumper_params
      {
        :host          => @host,
        :port          => @port,
        :tag           => @tag,
        :dataset       => @dataset,
        :receiver_host => @receiver_host,
        :receiver_port => @receiver_port,
      }
    end

    def dump
      @dumper = DumpClient.new(dumper_params)
      dump_options => {
        :backend => :coolio,
        :loop    => @loop,
      }
      @dumper.run(dump_options) do |message|
        puts(JSON.pretty_generate(message))
      end
      @loop.run
      @dumper.error_message
    end
  end
end
