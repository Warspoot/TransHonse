require 'json'
require 'uri'
require 'httparty'
require 'toml-rb'
require 'fileutils'

config = TomlRB.load_file('config.toml')
$server = config['server']
$url = $server['api_url']
puts $url
dictionary = File.read('dictionary.json')
$dictionary_str = JSON.parse(dictionary)
rawText = "monologue"

def iterate_json(file_path)
    puts "Reading #{file_path} JSON File..."
    file = File.read(file_path)
    file_json = JSON.parse(file)
    text = file_json['text_block_list']

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
    sliced_path = file_path.byteslice(4, 256)
    output_path = File.join("translated", sliced_path)
    output_dir = File.dirname(output_path)
    FileUtils.mkdir_p(output_dir)
    File.write(output_path, JSON.dump(JSON.pretty_generate(file_json)))
    puts "Saved to: #{output_path}"
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

    response = HTTParty.post($url,
        body: payload.to_json,
        headers:{'Content-Type' => 'application/json'},
    )
    returned_response = response["choices"][0]['message']['content']
    return returned_response
end

def trans_loop(target_folder)
  unless Dir.exist?(target_folder)
    puts "Folder does not exist"
    return
  end

  puts "Running through all files in #{target_folder}"
  batch_start_time = Time.now
  file_count = 0

  Dir.glob(File.join(target_folder, "**/*.json")).each do |file_path|
    iterate_json(file_path)
    file_count += 1
  end

  batch_end_time = Time.now
  batch_duration = batch_end_time - batch_start_tim
  puts "Files processed: #{file_count}"
  puts "Total batch time: #{'%.2f' % batch_duration} seconds."
end

trans_loop('raw')