require 'json'
require 'uri'
require 'httparty'
require 'toml-rb'

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
    text = file_json['text']

    text.each_with_index do |text, text_index|
        #raw line info
        puts "Raw Line ##{text_index}"
        puts "Name: #{text["jpName"]}"
        puts "Text: #{text["jpText"]}"

        #name translation logic
        if (text["jpName"] == 'モノローグ' or text["jpName"] == '') #checks if the name is a monologue blank
            text["jpName"] = ''
        else
            enName = translate_api(text['jpName'])
        end
        #text translation logic
        enText = translate_api(text['jpText'])  
        puts "Translated Line ##{text_index}" 
        puts "Name: #{enName}\nText:#{enText}"

        (text['choices'] || []).each_with_index do |choices, choice_index|
            #raw choices info
            puts "Raw Choice ##{choice_index}:"
            puts "Text: #{choices['jpText']}"
            #choice translation logic
            enChoice = translate_api(choices['jpText'])
            puts "Translated Choice #{choice_index}: #{enChoice}"
        end
    end 
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

iterate_json('example.json')
