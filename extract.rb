#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'optparse'
require 'pathname'
require 'open3'
require 'fileutils'

# Ruby wrapper for extract.py - provides Ruby interface to Python UnityPy extraction
class UnityAssetExtractor
  SUPPORTED_TYPES = %w[story home race lyrics preview ruby mdb].freeze
  TARGET_TYPES = %w[story home lyrics preview].freeze

  attr_reader :python_script, :options

  def initialize(python_script_path = nil)
    @python_script = python_script_path || find_python_script
    raise "Python script not found: #{@python_script}" unless File.exist?(@python_script)
    
    @options = {
      type: 'story',
      set: nil,
      group: nil,
      id: nil,
      idx: nil,
      story_id: nil,
      dst: Pathname.new('raw'),
      overwrite: false,
      workers: 4,
      meta: nil,
      verbose: false,
      debug: false
    }
  end

  # Find the extract.py script in common locations
  def find_python_script
    candidates = [
      'extract.py',
      File.join(__dir__, 'extract.py'),
      File.join(Dir.pwd, 'extract.py')
    ]
    
    candidates.find { |path| File.exist?(path) } || 'extract.py'
  end

  # Set extraction type
  def type=(value)
    raise ArgumentError, "Invalid type. Must be one of: #{TARGET_TYPES.join(', ')}" unless TARGET_TYPES.include?(value)
    @options[:type] = value
  end

  # Set story set
  def set=(value)
    @options[:set] = value
  end

  # Set story group
  def group=(value)
    @options[:group] = value
  end

  # Set story ID
  def id=(value)
    @options[:id] = value
  end

  # Set story index
  def idx=(value)
    @options[:idx] = value
  end

  # Set full story ID string
  def story_id=(value)
    @options[:story_id] = value
  end

  # Set output directory
  def dst=(value)
    @options[:dst] = Pathname.new(value)
  end

  # Set overwrite flag
  def overwrite=(value)
    @options[:overwrite] = value
  end

  # Set number of parallel workers
  def workers=(value)
    @options[:workers] = value.to_i
  end

  # Set meta file path
  def meta=(value)
    @options[:meta] = value ? Pathname.new(value) : nil
  end

  # Set verbose flag
  def verbose=(value)
    @options[:verbose] = value
  end

  # Set debug flag
  def debug=(value)
    @options[:debug] = value
  end

  # Build command-line arguments for Python script
  def build_python_args
    args = []
    
    args << '-t' << @options[:type]
    args << '-s' << @options[:set] if @options[:set]
    args << '-g' << @options[:group] if @options[:group]
    args << '-id' << @options[:id] if @options[:id]
    args << '-idx' << @options[:idx] if @options[:idx]
    args << '-sid' << @options[:story_id] if @options[:story_id]
    args << '-dst' << @options[:dst].to_s
    args << '-O' if @options[:overwrite]
    args << '-w' << @options[:workers].to_s
    args << '-meta' << @options[:meta].to_s if @options[:meta]
    args << '-vb' if @options[:verbose]
    args << '-dbg' if @options[:debug]
    
    args
  end

  # Execute the Python extraction script
  def extract
    python_cmd = find_python_executable
    cmd = [python_cmd, @python_script] + build_python_args
    
    puts "Executing: #{cmd.join(' ')}" if @options[:verbose] || @options[:debug]
    
    begin
      stdout, stderr, status = Open3.capture3(*cmd)
      
      # Always output stdout if there's content
      puts stdout unless stdout.empty?
      
      # Output stderr if verbose/debug or if there was an error
      warn stderr unless stderr.empty? || (status.success? && !@options[:verbose] && !@options[:debug])
      
      if status.success?
        { success: true, output: stdout, error: stderr }
      else
        warn "Extraction failed with status: #{status.exitstatus}" unless stderr.include?("failed")
        { success: false, output: stdout, error: stderr, exit_code: status.exitstatus }
      end
    rescue Errno::ENOENT => e
      warn "Error: Python script not found at #{@python_script}"
      warn "Exception: #{e.message}"
      { success: false, error: e.message, exit_code: 127 }
    rescue => e
      warn "Unexpected error during extraction: #{e.message}"
      warn e.backtrace.first(5).join("\n") if @options[:debug]
      { success: false, error: e.message, exit_code: 1 }
    end
  end

  # Find Python executable
  def find_python_executable
    # On Windows, try py launcher first, then python, then python3
    # On Unix, try python3 first, then python
    candidates = if RUBY_PLATFORM =~ /mingw|mswin|cygwin/
                   %w[py python python3]
                 else
                   %w[python3 python]
                 end
    
    candidates.each do |cmd|
      # On Windows, 'which' doesn't work well, so try running the command directly
      begin
        result = `#{cmd} --version 2>&1`
        return cmd if $?.success?
      rescue
        next
      end
    end
    
    raise 'Python executable not found. Please install Python 3 and ensure it is in your PATH.'
  end

  # Extract specific story by ID
  def extract_story(story_id)
    self.story_id = story_id
    extract
  end

  # Extract specific story with separate parameters
  def extract_story_parts(type:, set: nil, group: nil, id: nil, idx: nil)
    self.type = type
    self.set = set
    self.group = group
    self.id = id
    self.idx = idx
    extract
  end

  # Read extracted JSON file
  def read_extracted_json(filename)
    filepath = @options[:dst].join(@options[:type], filename)
    return nil unless filepath.exist?
    
    JSON.parse(File.read(filepath))
  rescue JSON::ParserError => e
    warn "Failed to parse JSON file #{filepath}: #{e.message}"
    nil
  end

  # List all extracted files for the current type
  def list_extracted_files
    type_dir = @options[:dst].join(@options[:type])
    return [] unless type_dir.exist?
    
    Dir.glob(type_dir.join('**', '*.json'))
  end

  # Check if Python dependencies are installed
  def check_dependencies
    python_cmd = find_python_executable
    
    required_modules = %w[UnityPy apsw]
    missing = []
    
    required_modules.each do |mod|
      stdout, _stderr, status = Open3.capture3(python_cmd, '-c', "import #{mod}")
      missing << mod unless status.success?
    end
    
    if missing.empty?
      puts '✓ All Python dependencies are installed'
      true
    else
      warn '✗ Missing Python dependencies:'
      missing.each { |mod| warn "  - #{mod}" }
      warn "\nInstall with: pip install #{missing.join(' ')}"
      false
    end
  end
end

# CLI Interface
class ExtractorCLI
  def self.run(argv = ARGV)
    options = {}
    
    parser = OptionParser.new do |opts|
      opts.banner = "Usage: #{File.basename($0)} [options]"
      opts.separator ''
      opts.separator 'Ruby wrapper for Unity asset extraction using UnityPy'
      opts.separator ''
      opts.separator 'Options:'
      
      opts.on('-t', '--type TYPE', UnityAssetExtractor::TARGET_TYPES, 
              "Asset type (#{UnityAssetExtractor::TARGET_TYPES.join(', ')})") do |t|
        options[:type] = t
      end
      
      opts.on('-s', '--set SET', 'The set to process') do |s|
        options[:set] = s
      end
      
      opts.on('-g', '--group GROUP', 'The group to process') do |g|
        options[:group] = g
      end
      
      opts.on('-i', '--id ID', 'The id (subgroup) to process') do |i|
        options[:id] = i
      end
      
      opts.on('-x', '--idx INDEX', 'The specific asset index to process') do |idx|
        options[:idx] = idx
      end
      
      opts.on('-S', '--story-id STORY_ID', 'The storyid to process') do |sid|
        options[:story_id] = sid
      end
      
      opts.on('-d', '--dst DIRECTORY', 'Output directory (default: raw)') do |d|
        options[:dst] = d
      end
      
      opts.on('-O', '--overwrite', 'Overwrite existing files') do
        options[:overwrite] = true
      end
      
      opts.on('-w', '--workers NUM', Integer, 'Number of parallel workers (default: 4)') do |w|
        options[:workers] = w
      end
      
      opts.on('-m', '--meta PATH', 'Explicit path to meta file') do |m|
        options[:meta] = m
      end
      
      opts.on('-p', '--python-script PATH', 'Path to extract.py script') do |p|
        options[:python_script] = p
      end
      
      opts.on('-v', '--verbose', 'Verbose output') do
        options[:verbose] = true
      end
      
      opts.on('--debug', 'Debug output') do
        options[:debug] = true
      end
      
      opts.on('-c', '--check', 'Check Python dependencies') do
        options[:check] = true
      end
      
      opts.on('--test', 'Test configuration and show what would be executed') do
        options[:test] = true
      end
      
      opts.on('-h', '--help', 'Show this help message') do
        puts opts
        exit
      end
    end
    
    # Show help if no arguments provided
    if argv.empty?
      puts parser
      puts "\nExamples:"
      puts "  #{File.basename($0)} -t story -S 010001"
      puts "  #{File.basename($0)} -t home -s 10001 -g 01 -i 0001"
      puts "  #{File.basename($0)} --check"
      exit 0
    end
    
    begin
      parser.parse!(argv)
    rescue OptionParser::InvalidOption, OptionParser::MissingArgument => e
      warn "Error: #{e.message}"
      puts "\n#{parser}"
      exit 1
    end
    
    # Validate required Python script
    begin
      extractor = UnityAssetExtractor.new(options[:python_script])
    rescue => e
      warn "Error initializing extractor: #{e.message}"
      warn "Make sure extract.py is in the same directory or specify with -p option"
      exit 1
    end
    
    # Set all options BEFORE running check or test
    extractor.type = options[:type] if options[:type]
    extractor.set = options[:set] if options[:set]
    extractor.group = options[:group] if options[:group]
    extractor.id = options[:id] if options[:id]
    extractor.idx = options[:idx] if options[:idx]
    extractor.story_id = options[:story_id] if options[:story_id]
    extractor.dst = options[:dst] if options[:dst]
    extractor.overwrite = options[:overwrite] if options[:overwrite]
    extractor.workers = options[:workers] if options[:workers]
    extractor.meta = options[:meta] if options[:meta]
    extractor.verbose = options[:verbose] if options[:verbose]
    extractor.debug = options[:debug] if options[:debug]
    
    if options[:check]
      exit(extractor.check_dependencies ? 0 : 1)
    end
    
    if options[:test]
      puts "Configuration Test"
      puts "=" * 60
      puts "Python script: #{extractor.python_script}"
      puts "Python script exists: #{File.exist?(extractor.python_script)}"
      
      begin
        python_cmd = extractor.find_python_executable
        puts "Python executable: #{python_cmd}"
        
        # Get Python version
        version = `#{python_cmd} --version 2>&1`.strip
        puts "Python version: #{version}"
      rescue => e
        puts "Python executable: ERROR - #{e.message}"
      end
      
      puts "\nConfiguration:"
      extractor.options.each do |key, value|
        puts "  #{key}: #{value.inspect}"
      end
      
      puts "\nCommand that would be executed:"
      puts "  #{python_cmd} #{extractor.python_script} #{extractor.build_python_args.join(' ')}"
      
      puts "\nRun without --test to execute"
      exit 0
    end
    
    # Execute extraction
    result = extractor.extract
    exit(result[:success] ? 0 : 1)
    
  rescue => e
    warn "Error: #{e.message}"
    warn e.backtrace.join("\n") if options[:debug]
    exit 1
  end
end

# Run CLI if executed directly
if __FILE__ == $PROGRAM_NAME
  ExtractorCLI.run
end
