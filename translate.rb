require 'json'
require 'uri'
require 'httparty'
require 'toml-rb'
require 'fileutils'
require 'zip'

config = TomlRB.load_file('config.toml')
$server = config['server']
$url = $server['api_url']
puts $url
dictionary = File.read('dictionary.json')
$dictionary_str = JSON.parse(dictionary)
$file_count = 0
$skip_count = 0
$newly_translated_files = []

def iterate_json(file_path)
    puts "Reading #{file_path} JSON File..."
    file = File.read(file_path)
    file_json = JSON.parse(file)
    text = file_json['text_block_list']
    sliced_path = file_path.byteslice(4, 256)
    output_path = File.join("translated", sliced_path)
    output_dir = File.dirname(output_path)
    FileUtils.mkdir_p(output_dir)

    if File.exist?(output_path)
        puts "#{sliced_path} already exists, skipping"
        $skip_count += 1
        return
    end    

    title = file_json["title"]
    if title && !title.empty?
            enTitle = translate_api(title)
            file_json["title"] = enTitle
            puts "Story Title: #{enTitle}"
        else
            puts "No title"
        end

    text.each_with_index do |text, text_index|
        #raw line info
        puts "Raw Line ##{text_index}"
        puts "Name: #{text["name"]}"
        puts "Text: #{text["text"]}"

        #name translation logic
        if (text["name"] == 'モノローグ' or text["name"] == '') #checks if the name is a monologue blank
            text["text"] = ''
            enName = ''
        else
            enName = translate_api(text['name'])
        end
        #text translation logic
        enText = translate_api(text['text'])  
        puts "Translated Line ##{text_index}" 
        puts "Name: #{enName}\nText: #{enText}"
        #write to save
        text["name"] = enName
        text["text"] = enText

        (text['choice_data_list'] || []).each_with_index do |choices, choice_index|
            #raw choices info
            puts "Raw Choice ##{choice_index}:"
            puts "Text: #{choices}"
            #choice translation logic
            enChoice = translate_api(choices)
            puts "Translated Choice #{choice_index}: #{enChoice}"
            #write to save
            text['choice_data_list'][choice_index] = enChoice 
        end
    end

    File.write(output_path, JSON.pretty_generate(file_json))
    puts "Saved to: #{output_path}"
    $file_count += 1
    $newly_translated_files << output_path
end

def char_system_text()
    batch_start_time = Time.now
    raw = $server['char_system_text_raw']
    ref = $server['char_system_text_reference']
    puts "Reading #{raw} JSON file..."

    if !File.exist?(raw)
        puts "No character_system_text.json file to translate."
        return
    end
    file_raw_json = JSON.parse(File.read(raw))
    file_ref_json = {}
    reference_toggle = false

    if File.exist?(ref)
        content = File.read(ref)
        if content.strip.empty?
             puts "Reference file is empty. Disabling referencing."
        else
             file_ref_json = JSON.parse(content)
             reference_toggle = true
        end
    else
        puts "No reference file found. Will create new one."
    end

    file_raw_json.each do |char_id, messages_hash|
        puts "Character ID: #{char_id}"
        messages_hash.each do |msg_id, text|
            puts "[#{msg_id}]: #{text}"
            reference_text = file_ref_json.dig(char_id, msg_id) 
            if reference_text and reference_toggle == true
                puts "Translation for [#{char_id}][#{msg_id}] already exists in #{ref}. Skipping."
                $skip_count += 1
            else
                enText = translate_api(text)
                file_raw_json[char_id][msg_id] = enText    
            end
            if reference_toggle == false
                enText = translate_api(text)
                file_raw_json[char_id][msg_id] = enText   
                puts "[#{msg_id}]: #{enText}"
                $file_count += 1
            end
        end
    end

    FileUtils.mkdir_p("translated")
    output_path = "translated/character_system_text.json"
    File.write(output_path, JSON.pretty_generate(file_raw_json))
    puts "Completed translating character system text"
    $newly_translated_files << output_path if $file_count > 0
    batch_end_time = Time.now
    batch_duration = batch_end_time - batch_start_time
    puts "Lines processed: #{$file_count}"
    puts "Lines Skipped: #{$skip_count}"
    puts "Total batch time: #{'%.2f' % batch_duration} seconds."
end

def translate_api(rawText)
    payload = {
        model: $server['model'],
        temperature: $server['temperature'],
        messages: [
            {
                role: 'system',
                content: "#{$server['system_prompt']} Refer to below for a dictionary in json format with the order japanese_text : english_text. (example\"ミホノブルボン\": \"Mihono Bourbon\", which means translate ミホノブルボン to Mihono Bourbon. \n #{$dictionary_str} \n translate the below text",
            },
            {
                role: 'user', 
                content: rawText
            }
        ],
        top_p: $server['top_p'],
        top_k: $server['top_k'],
        repetition__penalty: $server['repetition_penalty']
    }
    
    attempts = 0
    while attempts <= $server['retry_attempts']
        response = HTTParty.post($url,
            body: payload.to_json,
            headers:{'Content-Type' => 'application/json'},
        )
        returned_response = response["choices"][0]['message']['content']
        if returned_response.include?("###")
            attempts += 1
            puts "Found junk output, retrying... (Attempt #{attempts})"
        else
            return returned_response
        end
    end
end

def create_update_zip()
    return if $newly_translated_files.empty?
    FileUtils.mkdir_p("updates")

    # Find next available number
    zip_number = 1
    while File.exist?("updates/update_#{zip_number}.zip")
        zip_number += 1
    end

    zip_filename = "updates/update_#{zip_number}.zip"
    puts "\nCreating #{zip_filename} with #{$newly_translated_files.length} file(s)..."

    Zip::File.open(zip_filename, create: true) do |zipfile|
        $newly_translated_files.each do |file_path|
            zip_path = file_path.gsub('\\', '/').sub(/^translated\//, '')
            zipfile.add(zip_path, file_path)
            puts "  Added: #{zip_path}"
        end
    end

    puts "Successfully created #{zip_filename}"
end

def trans_loop(target_folder)
    unless Dir.exist?(target_folder)
    puts "Folder does not exist"
    return
    end

    puts "Running through all files in \"#{target_folder}\""
    batch_start_time = Time.now

    Dir.glob(File.join(target_folder, "**/*.json")).each do |file_path|
    iterate_json(file_path)
    end

    batch_end_time = Time.now
    batch_duration = batch_end_time - batch_start_time
    puts "Files processed: #{$file_count}"
    puts "Files Skipped: #{$skip_count}"
    puts "Total batch time: #{'%.2f' % batch_duration} seconds."
    $file_count = 0
    $skip_count = 0
end

def main()
    puts "TransHonse LLM Slop \n Translate Folder (f) or Character System Text (c) or leave blank for both."
    input = gets.chomp.downcase

    puts "Create update zip after translation? (y/n)"
    create_zip = gets.chomp.downcase == 'y'

    if input == "folder" || input == "f"
        trans_loop($server['raw_folder'])
    elsif input == "character system text" || input == "c"
        char_system_text()
    elsif input == ""
        trans_loop($server['raw_folder'])
        char_system_text()
    end

    puts "All tasks complete"
    create_update_zip() if create_zip
end

main()

