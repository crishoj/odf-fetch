require 'trollop'
require 'net/sftp'
require 'nokogiri'
require 'date'
require 'set'
require 'pry'

opts = Trollop::options do
  opt :test,       "Fetch from test server"
  opt :target,     "Directory to store XML files in",  :type => :string, :default => 'C:/XML'
  opt :username,   "Username for the ODF SFTP server", :type => :string, :default => 'ODF.VW'
  opt :password,   "Password for the ODF SFTP server", :type => :string, :default => '#2B!wjsv'
  opt :hostname,   "Address of the ODF SFTP server",   :type => :string, :default => 'ftpbif.sochi2014.com'
  opt :path,       "Path on the server",               :type => :string, :default => ''
  opt :discipline, "Limit to a certain discipline",    :type => :string
  opt :skip_update, "Skip updating DT_PARTIC files"
end

opts[:hostname] = 'e2e-ftpbif.sochi2014.com' if opts[:test]

wanted_types = %w{
DT_PARTIC
DT_MEDALS
DT_MEDALLISTS
DT_MEDALLISTS_DAY
DT_MEDALLISTS_DISCIPLINE
}


wanted_files = {}
disciplines = Set.new
found_files = Hash.new { |h, k| h[k]= [] }
updatable = {}
consolidatable = {}

def sorted_filenames(sftp_entries)
  sftp_entries.map(&:name).reject { |e| e[0] == '.' }.sort
end

puts "Will save the latest #{wanted_types * ', '} for all disciplines to #{opts.target}"
puts "Connecting to sftp://#{opts[:username]}@#{opts[:hostname]}/ ..."

def timestamp_from_path(path)
  /___(\d{14})\d{6}\.xml$/.match(path)[1]
end

Net::SFTP.start(opts[:hostname], opts[:username], password: opts[:password]) do |sftp|
  pattern = '*/*DT_*'
  date_folders = sorted_filenames(sftp.dir.entries opts[:path]).select { |entry|
    entry =~ /^\d{4}-\d{2}-\d{2}$/ }.collect { |date| "#{opts[:path]}/#{date}" }
  date_folders.reverse.each do |date_dir|
    puts "  Checking #{date_dir} for #{pattern}..."
    begin
      sorted_filenames(sftp.dir.glob date_dir, pattern).reverse.each do |path|
        discipline = /^\d{8}([A-Z]{2})/.match(File.basename(path))[1]
        next if opts[:discipline] and discipline != opts[:discipline]
        if not disciplines.include? discipline
          disciplines << discipline
          wanted_files[discipline] = wanted_types.clone
        end
        if wanted_files[discipline]
          wanted_files[discipline].each do |type|
            next unless path.match("_#{type}__")
            timestamp = DateTime.parse timestamp_from_path(path)
            found_files[discipline].push type
            progress = "#{found_files[discipline].count}/#{wanted_types.count}"
            puts "    [#{discipline} #{progress}] Found #{type} \t(#{timestamp.strftime('%c')})"
            target = File.join(opts.target, File.basename(path))
            if type == 'DT_MEDALS'
              target = File.join(opts.target, 'DT_MEDALS.xml')
              puts "    Saving DT_MEDALS (#{timestamp}) as #{target}"
            end
            sftp.download! "#{date_dir}/#{path}", target
            wanted_files[discipline].delete type
            wanted_files.delete(discipline) if wanted_files[discipline].empty?
            updatable[discipline] = target if type == 'DT_PARTIC'
            consolidatable[discipline] = target if type == 'DT_MEDALLISTS_DISCIPLINE'
          end
        end
      end
    rescue Net::SFTP::Exception => e
      puts "[SERVER ERROR] While checking #{date_dir}: #{e}"
    end
  end
  unless opts[:skip_update]
    if updatable.empty?
      puts "No DT_PARTIC files to update"
    else
      puts "Updating the latest DT_PARTIC files with DT_PARTIC_UPDATEs for [#{updatable.keys * ', '}]"
      updatable.keys.each do |discipline|
        basefile = updatable[discipline]
        base_timestamp = timestamp_from_path(basefile)
        puts "  Consolidating updates since #{base_timestamp} for [#{discipline}]"
        puts "    [#{discipline}] Parsing base file"
        base_xml = Nokogiri::XML.parse(File.read basefile)
        pattern = "*/*#{discipline}*__DT_PARTIC_UPDATE__*"
        date_folders.each do |date_dir|
          next if Date.parse(File.basename(date_dir)) < Date.parse(base_timestamp)
          sorted_filenames(sftp.dir.glob date_dir, pattern).each do |path|
            update_timestamp = File.basename(path)[-24...-10]
            next if update_timestamp < base_timestamp
            target = File.join(opts.target, File.basename(path))
            sftp.download! "#{date_dir}/#{path}", target
            update_xml = Nokogiri::XML.parse(File.read target)
            stats = update_xml.xpath('//Participant').reduce(Hash.new(0)) do |stats, updated_participant|
              participant_code = updated_participant.attributes['Code'].value
              existing = base_xml.xpath("//Participant[@Code=#{participant_code}]")
              if existing.any?
                existing.each { |e| e.replace(updated_participant) }
                stats[:updated] += 1
              else
                base_xml.root.child << updated_participant
                stats[:added] += 1
              end
              stats
            end
            puts "    [#{discipline}] Update #{update_timestamp}: #{stats[:added]} new participants, #{stats[:updated]} updated"
            File.unlink(target)
          end
        end
        puts "    [#{discipline}] Writing consolidated file to #{basefile}"
        File.write(basefile, base_xml.to_s)
      end
    end
  end

  if consolidatable.empty?
    puts "No DT_MEDALLISTS_DISCIPLINE files to consolidate"
  else
    puts "Consolidating the latest DT_MEDALLISTS_DISCIPLINE files for [#{consolidatable.keys * ', '}]"
    basefile = consolidatable.values.max
    base_discipline = consolidatable.key(basefile)
    consolidatable.delete(base_discipline)
    base_xml = Nokogiri::XML.parse(File.read basefile)
    target = File.join File.dirname(basefile), File.basename(basefile).sub(base_discipline, 'GL')
    consolidatable.keys.each do |discipline|
      timestamp = timestamp_from_path(consolidatable[discipline])
      puts "  Consolidating [#{discipline}] (#{timestamp})"
      puts "    [#{discipline}] Parsing standings"
      xml = Nokogiri::XML.parse(File.read consolidatable[discipline])
      xml.xpath('//Discipline').each do |standings|
        base_xml.root.child << standings
      end
    end
    puts "  Writing consolidated DT_MEDALLISTS_DISCIPLINE file\n      => #{target}"
    File.write(target, base_xml.to_s)
  end

end

puts "All done"

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


