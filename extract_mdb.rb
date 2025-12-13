#!/usr/bin/env ruby
# frozen_string_literal: true

require 'sqlite3'
require 'json'
require 'fileutils'

##
# ExtractCharacterSystemText - Extracts text data from Umamusume master.mdb
#
# Requirements:
# - SQLite3
# - Ruby gems: sqlite3
#
# Usage:
#   ruby extract.rb /path/to/master.mdb [output_dir]
#
class ExtractCharacterSystemText
  def initialize(mdb_path, output_dir = 'extracted_text')
    @mdb_path = mdb_path
    @output_dir = output_dir
    @db = nil
    validate_mdb_path!
  end

  def extract
    puts "Opening database: #{@mdb_path}"
    open_database
    
    puts "Creating output directory: #{@output_dir}"
    FileUtils.mkdir_p(@output_dir)

    # Extract the main character dialogue
    if table_exists?('character_system_text')
      extract_grouped_character_text('character_system_text')
    end

    # Extract the general text data dictionary
    extract_text_data_dict

    # Extract card names (optional helper)
    extract_card_names
    
    puts "\nExtraction complete! Files saved to: #{@output_dir}"
  ensure
    @db&.close
  end

  private

  def validate_mdb_path!
    unless File.exist?(@mdb_path)
      raise "MDB file not found: #{@mdb_path}"
    end
  end

  def open_database
    @db = SQLite3::Database.new(@mdb_path)
    @db.results_as_hash = true
  rescue SQLite3::Exception => e
    raise "Failed to open database: #{e.message}"
  end

  # Extracts character_system_text grouped by char_id -> voice_id
  def extract_grouped_character_text(table_name)
    puts "\n=== Extracting Character System Text ==="
    rows = @db.execute("SELECT * FROM #{table_name}")
    
    if rows.empty?
      puts "  No data found in #{table_name}"
      return
    end

    puts "  Grouping #{rows.length} rows by character_id..."

    grouped_data = {}

    rows.each do |row|
      char_id = row['character_id'].to_s
      voice_id = row['voice_id'].to_s
      text = row['text']

      grouped_data[char_id] ||= {}
      grouped_data[char_id][voice_id] = text
    end

    # Sort keys for consistent output
    sorted_data = sort_hash_keys(grouped_data)

    output_file = "#{table_name}.json"
    save_json(output_file, sorted_data, indent: '    ')
    
    puts "  Extracted formatted data to #{output_file}"
  end

  # Extracts text_data grouped by category -> index
  def extract_text_data_dict
    return unless table_exists?('text_data')
    
    puts "\n=== Extracting Text Data Dictionary ==="
    
    # We select all text data to create a complete dictionary
    # Quoting "index" to avoid SQL keyword conflicts
    query = "SELECT category, `index`, text FROM text_data"
    rows = @db.execute(query)
    
    grouped_data = {}
    
    rows.each do |row|
      category = row['category'].to_s
      index = row['index'].to_s
      text = row['text']
      
      grouped_data[category] ||= {}
      grouped_data[category][index] = text
    end
    
    # Sort keys for consistent output
    sorted_data = sort_hash_keys(grouped_data)
    
    # Output to text_data_dict.json as requested
    save_json('text_data_dict.json', sorted_data, indent: '    ')
    puts "  Extracted #{rows.length} entries to text_data_dict.json"
  rescue SQLite3::SQLException => e
    puts "  [ERROR] Could not extract text_data: #{e.message}"
  end

  def extract_card_names
    return unless table_exists?('card_data')
    
    puts "\n=== Extracting Card/Character Names ==="
    
    query = <<-SQL
      SELECT id, chara_id, card_name, charaName as chara_name
      FROM card_data WHERE card_name IS NOT NULL ORDER BY id
    SQL
    
    begin
      rows = @db.execute(query)
      output = rows.map do |row|
        {
          id: row['id'],
          chara_id: row['chara_id'],
          card_name: row['card_name'],
          chara_name: row['chara_name']
        }
      end
      save_json('card_names.json', output)
    rescue SQLite3::SQLException
      puts "  [NOTE] Skipping card_names (schema differs from expected)"
    end
  end

  # Helper to recursively sort hash keys as integers where possible
  def sort_hash_keys(hash)
    hash.sort_by { |k, _| k.to_i }.to_h.tap do |sorted|
      sorted.each do |k, v|
        sorted[k] = sort_hash_keys(v) if v.is_a?(Hash)
      end
    end
  end

  def table_exists?(table_name)
    result = @db.execute(
      "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
      [table_name]
    )
    result.any?
  end

  def save_json(filename, data, opts = nil)
    output_path = File.join(@output_dir, filename)
    json_content = opts ? JSON.pretty_generate(data, opts) : JSON.pretty_generate(data)
    File.write(output_path, json_content)
  end
end

# CLI Interface
if __FILE__ == $PROGRAM_NAME
  if ARGV.empty?
    puts "Usage: ruby extract.rb MDB_FILE [OUTPUT_DIR]"
    exit 0
  end

  mdb_path = ARGV[0]
  output_dir = ARGV[1] || 'extracted_text'

  begin
    extractor = ExtractCharacterSystemText.new(mdb_path, output_dir)
    extractor.extract
  rescue => e
    puts "Error: #{e.message}"
    exit 1
  end
end