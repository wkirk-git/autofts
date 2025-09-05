#!/usr/bin/env ruby
require 'sqlite3'

if ARGV.size != 1
  puts "Usage: ruby query_db.rb <database_file>"
  exit 1
end

db_path = ARGV[0]

begin
  db = SQLite3::Database.new(db_path)
rescue SQLite3::Exception => e
  puts "Failed to open database: #{e}"
  exit 1
end

puts "Connected to #{db_path}"

loop do
  print "\nEnter FTS table name (or 'exit'): "
  fts_table = STDIN.gets
  break if fts_table.nil?
  fts_table = fts_table.encode('UTF-8', invalid: :replace, undef: :replace, replace: '').strip
  break if fts_table.downcase == 'exit'

  print "Enter search query: "
  query = STDIN.gets
  next if query.nil?
  query = query.encode('UTF-8', invalid: :replace, undef: :replace, replace: '').strip
  next if query.empty?

  begin
    sql = "SELECT rowid, * FROM #{fts_table} WHERE #{fts_table} MATCH ?"
    results = []
    db.execute(sql, query) do |row|
      results << row
    end

    if results.empty?
      puts "No results found."
      next
    end

    # Display numbered list with first 200 chars
    results.each_with_index do |row, idx|
      snippet = row.map(&:to_s).join(" | ")[0..199]
      puts "#{idx + 1}. #{snippet}"
    end

    loop do
      print "\nEnter result number to view full result (or 'back'): "
      choice = STDIN.gets
      break if choice.nil?
      choice = choice.encode('UTF-8', invalid: :replace, undef: :replace, replace: '').strip
      break if choice.downcase == 'back'

      num = choice.to_i
      if num < 1 || num > results.size
        puts "Invalid number."
      else
        full_row = results[num - 1].map(&:to_s).join(" | ")
        puts "\nFULL RESULT:\n#{full_row}"
      end
    end
  rescue SQLite3::Exception => e
    puts "Query failed: #{e}"
  end
end

puts "Goodbye!"
