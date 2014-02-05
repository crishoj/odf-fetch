require 'net/sftp'
require 'trollop'
require 'date'
require 'set'


opts = Trollop::options do
  opt :test, "Fetch from test server"
end

hostname = 'ftpbif.sochi2014.com'
hostname = 'e2e-bif.sochi2014.com' if opts[:test]
username = 'ODF.VW'
password = '#2B!wjsv'

wanted_types = %w{
DT_MEDALLISTS_DAY
DT_MEDALS
DT_PARTIC
DT_PARTIC_UPDATE
}

wanted_files = {}
disciplines = Set.new
found_files = Hash.new { |h, k| h[k]= [] }

target_dir = ARGV[1] || 'C:/XML'

def sorted_filenames(sftp_entries)
  sftp_entries.map(&:name).reject { |e| e[0] == '.' }.sort.reverse
end

puts "Will save the latest #{wanted_types * ', '} for all disciplines to #{target_dir}"
puts "Connecting to sftp://#{username}@#{hostname}/ ..."

Net::SFTP.start(hostname, username, password: password) do |sftp|
  pattern = '*/*DT_*'
  sorted_filenames(sftp.dir.entries '/').each do |date|
    puts "  Checking #{date} ..."
    sorted_filenames(sftp.dir.glob date, pattern).each do |path|
      discipline = /^\d{8}([A-Z]{2})/.match(File.basename(path))[1]
      if not disciplines.include? discipline
        #puts "    Discovered discipline: #{discipline}"
        disciplines << discipline
        wanted_files[discipline] = wanted_types.clone
      end
      wanted_files[discipline].each do |type|
        next unless path.match("_#{type}__")
        found_files[discipline].push type
        target = File.join(target_dir, File.basename(path))
        progress = "#{found_files[discipline].count}/#{wanted_types.count}"
        timestamp = DateTime.parse /___(\d{14})\d{6}\.xml$/.match(path)[1]
        puts "    [#{discipline} #{progress}] Found #{type} \t(#{timestamp.strftime('%c')})"
        #puts "      => #{target}"
        sftp.download! "#{date}/#{path}", target
        wanted_files[discipline].delete type
        wanted_files.delete(discipline) if wanted_files[discipline].empty?
      end
    end
  end
end

puts "Done"

puts "\nSummary:"
puts "  Found:"
found_files.keys.each do |discipline|
  found = found_files[discipline]
  fragment = (found.count == 1) ? 'this' : ("these #{found.count}")
  puts "    [#{discipline}] #{fragment}: [#{found * ', '}]"
end

if wanted_files.empty?
  puts "Found all files"
else
  puts "  Did not find:"
  wanted_files.keys.each do |discipline|
    missing = wanted_files[discipline]
    fragment = (missing.count == 1) ? 'this one' : ("these #{missing.count}")
    puts "    [#{discipline}] #{fragment}: [#{missing * ', '}]"
  end
end


