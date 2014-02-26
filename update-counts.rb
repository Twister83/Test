#!/usr/bin/env ruby
%w|ostruct json yaml pstore tmpdir fileutils|.each { |t| require t }

def usage!
  puts "Usage: #{$0} <config.yml>"
  exit(-1)
end

def log(*args)
  $stderr.puts args.map { |arg| arg.is_a?(String) ? arg : arg.inspect }.join(' ')
end

def insert_intel_link(portal)
  lat = (portal[2]['latE6'] * 1e-6).round(6)
  lng = (portal[2]['lngE6'] * 1e-6).round(6)
  link = "https://www.ingress.com/intel?ll=#{lat},#{lng}&z=17&pll=#{lat},#{lng}"
  portal[2]['IntelURL'] = link
  portal
end

def insert_address(portal)
  #http://nominatim.openstreetmap.org/reverse?format=json&lat=55.882363&lon=37.565537&zoom=18&accept-language=ru&email=me@inye.cc
  portal
end

class Configuration < OpenStruct
  def initialize(fname)
    super(YAML.load(File.read(fname)))
  end

  def regions
    @regions ||= super.inject({}) { |memo, (k,v)| memo.merge(k => OpenStruct.new(v).freeze) }
  end
end

ARGV.size == 1 && File.file?(ARGV[0]) && File.readable?(ARGV[0]) or usage!
CONFIG = Configuration.new(ARGV[0])
STATE = PStore.new(CONFIG.state_file)

index_json = []

STATE.transaction do
  CONFIG.regions.each_pair do |name, config|
    output = { 'Timestamp' => Time.now.to_i, 'Region' => config.name, 'Bounds' => config.rectangle, 'MinLevel' => config.level }

    # Выпекаем IntelURL
    llz = `#{CONFIG.dashboard_tools} calculate-center-llz #{config.rectangle}`
    if $? != 0
      log "Failed to calculate-center-llz for #{name}. dashboard-tools message: #{llz}"
      next
    end
    llz = JSON.parse(llz)

    output['IntelURL'] = "https://www.ingress.com/intel?ll=#{llz['lat']},#{llz['lng']}&z=#{llz['zoom']}"

    index_entry = output.dup
    index_entry['Name'] = name
    index_json << index_entry

    start = Time.now.to_f

    # Читаем состояние.
    credentials = CONFIG.credentials_json.is_a?(Array) ? CONFIG.credentials_json : [ CONFIG.credentials_json ]
    credentials = credentials.map { |t| "--credentials #{t}" }.join(' ')
    current_state = `#{CONFIG.dashboard_tools} fetch-entities #{config.rectangle} #{config.level} #{credentials} --no-links --no-fields --hard-uniq --exact-match`
    if $? != 0
      log "Failed to fetch-entities for #{name}. dashboard-tools message: #{current_state}"
      next
    end
    current_state = current_state.force_encoding('UTF-8')
    # log "#{(Time.now.to_f - start).round(3)} DT COMPLETE"
    current_state = JSON.parse(current_state).inject({}) { |memo, portal| memo[portal[0]] = portal ; memo }
    # log "#{(Time.now.to_f - start).round(3)} CS BUILT"

    # Считаем порталы.
    %w|ENL RES|.each do |team|
      output[team] = {}
      (config.level..8).each do |lvl|
        output[team][lvl.to_s] = 0
      end
    end
    current_state.each_value do |portal|
      next if portal[2]['level'] < config.level # Бывает, попадаются.
      hash = portal[2]['team'] == 'RESISTANCE' ? output['RES'] : output['ENL']
      hash[portal[2]['level'].to_s] += 1
    end
    # log "#{(Time.now.to_f - start).round(3)} PORTALS COUNTED"

    # Считаем текущие GUID'ы порталов. Не забываем потом убрать :-)
    current_portals = {}
    config.full_diff.each do |line|
      team, lvl = line.split(',',2)
      team = team == 'RES' ? 'RESISTANCE' : 'ENLIGHTENED'

      current_portals[team] ||= {}
      current_portals[team][lvl] = current_state.values.select { |portal| portal[2]['team'] == team && portal[2]['level'] == lvl.to_i }.map(&:first)
    end
    # log "#{(Time.now.to_f - start).round(3)} CURRENT DIFF GUIDS COUNTED"

    # Теперь берём последнее состояние... Которого может и не быть.
    STATE[name] = {} unless STATE.root?(name)
    STATE[name]['output'] ||= []
    STATE[name]['last_portals'] ||= {}

    last_output = STATE[name]['output'].last || {}
    last_portals = STATE[name]['last_portals']

    # Считаем дифф.
    %w|ENL RES|.each do |team|
      hash = (output["#{team}_diff"] ||= {})
      output[team].each_key do |lvl|
        hash[lvl] = output[team][lvl] - last_output.fetch(team, {}).fetch(lvl, 0)
      end
    end
    # log "#{(Time.now.to_f - start).round(3)} DIFF GUIDS COUNTED"

    # Считаем новые порталы.
    output['NewPortals'] = {}
    current_portals.each_key do |team|
      hash = (output['NewPortals'][team] = {})
      current_portals[team].each_key do |lvl|
        new_portals = current_portals[team][lvl] - last_portals.fetch(team, {}).fetch(lvl, [])
        hash[lvl] = new_portals.map { |guid| insert_address(insert_intel_link(current_state[guid])) }
      end
    end
    # log "#{(Time.now.to_f - start).round(3)} NEW PORTALS BUILT"

    # Теперь составляем список для полного вывода...
    kill_list = {}
    config.kill_list.each do |line|
      team, lvl = line.split(',',2)
      team = team == 'RES' ? 'RESISTANCE' : 'ENLIGHTENED'

      kill_list[team] ||= {}
      kill_list[team][lvl] = current_state.values.select { |portal| portal[2]['team'] == team && portal[2]['level'] == lvl.to_i }

      # Прозреваю, это будут запрашивать не переставая.
      kill_list[team][lvl].each do |portal|
        insert_address(insert_intel_link(portal))
      end
    end
    # log "#{(Time.now.to_f - start).round(3)} KILL LIST COMPLETE"

    # Записываем PreviousTimestamp
    output['PreviousTimestamp'] = last_output['Timestamp']

    STATE[name]['last_portals'] = current_portals

    STATE[name]['output'] << output
    STATE[name]['output'].shift while STATE[name]['output'].size > config.history_size

    # Теперь записываем вывод...
    # Сначала legacy.
    Dir::Tmpname.create("#{name}.json", CONFIG.public_dir) do |tmpname|
      File.open(tmpname, 'w+') { |f| f.puts(JSON.dump(output)) } 
      File.rename(tmpname, File.join(CONFIG.public_dir, "#{name}.json"))
    end
    # Теперь лог.
    File.open(File.join(CONFIG.log_dir, "#{name}.log"), 'a+') { |f| f.puts(`date -u`.strip + ' ' + JSON.dump(output)) }

    # Теперь новый клёвый формат.
    city_dir = File.join(CONFIG.public_dir, name)
    FileUtils.mkdir_p(city_dir)

    Dir::Tmpname.create("count.json", city_dir) do |tmpname|
      File.open(tmpname, 'w+') { |f| f.puts(JSON.dump(output)) }
      File.rename(tmpname, File.join(city_dir, 'count.json'))
    end

    Dir::Tmpname.create("count_history.json", city_dir) do |tmpname|
      File.open(tmpname, 'w+') { |f| f.puts(JSON.dump(STATE[name]['output'])) }
      File.rename(tmpname, File.join(city_dir, 'count_history.json'))
    end

    Dir::Tmpname.create("kill_list.json", city_dir) do |tmpname|
      File.open(tmpname, 'w+') { |f| f.puts(JSON.dump(kill_list)) }
      File.rename(tmpname, File.join(city_dir, 'kill_list.json'))
    end
    log "#{(Time.now.to_f - start).round(3)} WRITE COMPLETE"
  end
  Dir::Tmpname.create("index.json", CONFIG.public_dir) do |tmpname|
    File.open(tmpname, 'w+') { |f| f.puts(JSON.dump(index_json)) }
    File.rename(tmpname, File.join(CONFIG.public_dir, 'index.json'))
  end
end
