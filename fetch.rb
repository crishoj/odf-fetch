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
DT_MEDALLISTS_DISCIPLINE
}

wanted_files = {}
disciplines = Set.new
found_medal_standings = false
found_files = Hash.new { |h, k| h[k]= [] }
updatable = {}
daily_medallist_files = {}
consolidatable = {}
event_timestamps = {}

puts "Will save the latest #{wanted_types * ', '} for all disciplines to #{opts.target}"
puts "Connecting to sftp://#{opts[:username]}@#{opts[:hostname]}/ ..."

def timestamp_from_path(path)
  matches = /(\d{14})\d{6}\.xml$/.match(path)
  binding.pry unless matches
  matches[1]
end

def code_from_path(path)
  File.basename(path)[8...14]
end

def code_for_event(event)
  event_gender = event.parent
  event_discipline = event_gender.parent
  "#{event_discipline['Code']}#{event_gender['Code']}#{event['Code']}"
end

def sorted_entries(sftp_entries)
  sftp_entries.select { |e|
    matched = File.basename(e.name).match /^\d{8}[A-Z]+\d+__+DT_.+\d{20}.xml$/
    puts "    Skipping unmatched filename: #{File.basename(e.name)}" unless matched
    matched
  }.sort_by { |e|
    timestamp_from_path e.name
  }
end

def fetch(sftp, dir, entry, target, message = nil)
  if File.exist? target
    if File.size?(target) == entry.attributes.size
      if File.ctime(target) > Time.at(entry.attributes.mtime)
        puts "#{message} [cached]" if message
        return
      end
    end
  end
  puts "#{message} [#{entry.attributes.size/1024}K download]" if message
  sftp.download! "#{dir}/#{entry.name}", target
end

Net::SFTP.start(opts[:hostname], opts[:username], password: opts[:password]) do |sftp|
  pattern = '*/*_DT_*.xml'
  date_folders = sftp.dir.entries(opts[:path]).map(&:name).
      select { |entry| entry =~ /^\d{4}-\d{2}-\d{2}$/ }.
      collect { |date| "#{opts[:path]}/#{date}" }
  date_folders.sort.reverse.each do |date_dir|
    puts "  Checking #{date_dir} for #{pattern}..."
    begin
      sorted_entries(sftp.dir.glob date_dir, pattern).reverse.each do |entry|
        matches = /^\d{8}([A-Z]{2})/.match(File.basename(entry.name))
        discipline = matches[1]
        next if opts[:discipline] and discipline != opts[:discipline]
        if not disciplines.include? discipline
          disciplines << discipline
          wanted_files[discipline] = wanted_types.clone
        end
        target = File.join(opts.target, File.basename(entry.name))
        timestamp = DateTime.parse timestamp_from_path(entry.name)
        if entry.name.match('__DT_MEDALLISTS__')
          event_code = code_from_path(entry.name)
          unless event_timestamps.include? event_code
            puts "    [#{discipline}] Noting time for event #{event_code} (#{timestamp})"
            event_timestamps[event_code] = timestamp
          end
        elsif entry.name.match('__DT_MEDALS__')
          unless found_medal_standings
            target = File.join(opts.target, 'DT_MEDALS.xml')
            fetch sftp, date_dir, entry, target, "    Found DT_MEDALS (#{timestamp})"
            found_medal_standings = true
          end
        elsif wanted_files[discipline]
          wanted_files[discipline].each do |type|
            next unless entry.name.match("__#{type}__")
            found_files[discipline].push type
            progress = "#{found_files[discipline].count}/#{wanted_types.count}"
            fetch sftp, date_dir, entry, target, "    [#{discipline} #{progress}] Found #{type} \t(#{timestamp.strftime('%c')})"
            wanted_files[discipline].delete type
            wanted_files.delete(discipline) if wanted_files[discipline].empty?
            updatable[discipline] = target if type == 'DT_PARTIC'
            consolidatable[discipline] = target if type == 'DT_MEDALLISTS_DISCIPLINE'
            daily_medallist_files[timestamp] = target if type == 'DT_MEDALLISTS_DAY'
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
          sorted_entries(sftp.dir.glob date_dir, pattern).each do |entry|
            update_timestamp = File.basename(entry.name)[-24...-10]
            next if update_timestamp < base_timestamp
            target = File.join(opts.target, File.basename(entry.name))
            fetch sftp, date_dir, entry, target
            update_xml = Nokogiri::XML.parse(File.read target)
            stats = update_xml.xpath('//Participant').reduce(Hash.new(0)) do |stats, updated_participant|
              participant_code = updated_participant['Code']
              if participant_code.nil? or participant_code.empty?
                stats[:blank] += 1
              else
                existing = base_xml.xpath("//Participant[@Code=#{participant_code}]")
                if existing.any?
                  existing.each { |e| e.replace(updated_participant) }
                  stats[:updated] += 1
                else
                  base_xml.root.child << updated_participant
                  stats[:added] += 1
                end
              end
              stats
            end
            puts "    [#{discipline}] Update #{update_timestamp}: #{stats[:added]} new participants, #{stats[:updated]} updated, #{stats[:blank]} skipped"
          end
        end
        target = basefile.clone
        target[-41, 5] = 'SYNTH'
        target[-24, 14] = Time.now.strftime('%Y%m%d%H%M%S')
        puts "    [#{discipline}] Writing consolidated file\n      => #{target}"
        File.write(target, base_xml.to_s)
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
    target[-41, 5] = 'SYNTH'
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

    puts "Creating list of 3 latest medallists"
    tpl = '<?xml version="1.0" encoding="utf-8"?><OdfBody><Competition Code="OWG2014"></Competition></OdfBody>'
    latest_xml = Nokogiri::XML.parse(tpl)
    base_xml.xpath("//Event[Medal[@Code='ME_GOLD']]").
        sort_by { |e| event_timestamps[code_for_event(e)] or DateTime.parse('20140201') }.
        reverse[0...3].reverse.each do |e|
      code = code_for_event e
      gender = e.parent.clone
      discipline = e.parent.parent.clone
      discipline.children.map(&:remove)
      gender.children.map(&:remove)
      puts "  Adding #{code} (#{event_timestamps[code]})"
      gender << e
      discipline << gender
      latest_xml.root.child << discipline
    end
    target = File.join(opts[:target], 'LATEST3.xml')
    puts "  Writing latest gold medallists\n    => #{target}"
    File.write(target, latest_xml.to_s)
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



