#!/usr/bin/env ruby
require "date"

#
# a ruby program that converts shoutcast log files to the new soundexchange required reporting format.
# reads and writes to stdin/stdout by default.  use -i or -o flags to set an input and/or output file
# use -s to specify a stream_id. 
#
# tim@lifelike.com
#
# Copyright 2013 Tim M Edwards. All rights reserved.
# 
# Redistribution and use, with or without modification, are permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
# AUTHOR OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
# POSSIBILITY OF SUCH DAMAGE.

class LogEntry
  attr_accessor :uid, :ip_address, :date, :duration, :status, :agent, :stream_id
  
  # SOUNDEXCHANGE STANDARD FILE FORMAT DETAILS
  #
  # File type: tab delimited .txt
  #
  # Do not combine logs; send separate files for each stream
  #
  # We will no longer accept data for two streams in 1 log file.
  #
  # Columns (in order):
  #
  # • IP address (#.#.#.#; Do NOT include port numbers (127.0.0.1:3600))
  #
  # • Date listener tuned in (YYYY-MM-DD)
  #
  # • Time listener tuned in (HH:MM:SS; 24-hour military time; UTC time zone)
  #
  # • Stream ID (No spaces)
  #
  # • Duration of listening (Seconds)
  #
  # • HTTP Status Code
  #
  # • Referrer/Client Player  
  #
  #  Example:
  #  IP Address      Date       Time     StreamID  Duration Status  Referrer
  #  208.100.99.87   2012-09-01 22:02:26 kwmr128   62       200     AppleCoreMedia

  def to_s
    begin
      return "" if duration == "0"
      date_str = self.date.strftime("%Y-%m-%d\t%H:%M:%S")
      ip_address + "\t" + date_str + "\t" + stream_id + "\t" + duration + "\t" + status.to_s + "\t" + agent + "\n"
    rescue
      return ""
    end
  end
end

########################

class LogParser
  
  def initialize(in_file,out_file,stream)
    @input_file = in_file
    @output_file = out_file
    @stream_id = stream
    @logEntries = {}
  end
  
  def parse
    @input_file.each { |line|   
      process_line(line)
    }
    
    @logEntries.sort_by { |k,v| v.date }
    
    @logEntries.each_pair { |k,v|
      @output_file << v.to_s
    }
  end
  
  # Input log example
  #
  # <01/03/13@16:23:58> [SHOUTcast] DNAS/Linux v1.9.7 (Jun 23 2006) starting up...
  # <01/03/13@16:23:58> [main] pid: 26440
  # <01/03/13@16:23:58> [main] loaded config from /root/shoutcast/kwmr128.conf
  # <07/01/13@12:35:59> [dest: 108.236.114.218] starting stream (UID: 206245)[L: 9]{A: iTunes/11.0.2 (Macintosh; OS X 10.6.8) AppleWebKit/534.58.2}(P: 8)
  # <07/01/13@12:36:27> [dest: 108.236.114.218] connection closed (28 seconds) (UID: 206245)[L: 8]{Bytes: 664784}(P: 8)
  
  def process_line(line)
    match_pattern = /<(?<date>\d{2}(.\d{2}){2}@\d{2}(:\d{2}){2})>\s\[(?<event_type>.+)\] (?<event>.+)\[.+\]\{(?<agent>.+)\}/

    if match_pattern =~ line

      return if $~[:event_type][0..4] != "dest:"

      case $~[:event][0..2]
        when "sta"
          entry = LogEntry.new
          date_str = $~[:date] + " " + DateTime.now.offset.numerator.to_s  #add local tz offset
          local_date = DateTime.strptime(date_str, "%D@%T %z")
          entry.date = local_date.new_offset(0) ## convert to utc
          entry.ip_address = $~[:event_type][6..-1]
          entry.agent = $~[:agent][2..-1]

          if /.+\(UID: (?<uid>\d+)\)/ =~ $~[:event]
           entry.uid = uid
          end
          
          @logEntries[uid] = entry
          return
          
        when "con"
          if /.+\((?<duration>\d+) seconds\) \(UID: (?<uid>\d+)\)/ =~ $~[:event]
            entry = @logEntries[uid]
            return if entry == nil
            entry.duration = duration
            entry.status = 200
            entry.stream_id = @stream_id
            return
          end  
      end
    end
  end
  
end

#####################################

STDIN.binmode
STDOUT.binmode

input_file = STDIN
output_file = STDOUT
stream_id = "stream1"
nargs = ARGV.length

if nargs > 0 
  ARGV.each_with_index do |arg,i|
    if arg == "-i"
      input_file = File.open(ARGV[i+1],"rb")
    end
    
    if arg == "-o"
      output_file = File.open(ARGV[i+1],"wb")
    end
    
    if arg == "-s"
      stream_id = ARGV[i+1]
    end
    
  end
end

lp = LogParser.new(input_file,output_file,stream_id)
lp.parse

