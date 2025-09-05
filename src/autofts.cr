require "sqlite3"

# --- Command-line argument ---
if ARGV.size != 1
  puts "Usage: autofts <database_file>"
  exit 1
end

db_path = ARGV[0]

# --- Open the database ---
DB.open "sqlite3://#{db_path}" do |db|
  puts "Opened database: #{db_path}"

  # --- Get all user tables ---
  tables = [] of String
  db.query "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'" do |rs|
    rs.each do
      tables << rs.read(String)
    end
  end

  tables.each do |table|
    # --- Get TEXT columns ---
    columns = [] of String
    db.query "PRAGMA table_info(#{table})" do |rs|
      rs.each do
        rs.read # skip cid
        col_name = rs.read(String)
        col_type = rs.read(String).upcase
        columns << col_name if col_type == "TEXT"
        3.times { rs.read } # skip remaining columns: notnull, dflt_value, pk
      end
    end
    next if columns.empty?

    fts_table = "#{table}_fts"
    col_list = columns.join(", ")

    # --- Create FTS table ---
    db.exec "CREATE VIRTUAL TABLE IF NOT EXISTS #{fts_table} USING fts5(#{col_list})"

    # --- Populate FTS table with existing data ---
    db.exec "INSERT INTO #{fts_table} (rowid, #{col_list}) SELECT rowid, #{col_list} FROM #{table}"

    puts "Created and populated FTS table #{fts_table}"

    # --- Prepare trigger statements ---
    insert_cols = columns.map { |c| "new." + c }.join(", ")
    update_set  = columns.map { |c| "#{c} = new.#{c}" }.join(", ")

    # --- INSERT trigger ---
    db.exec <<-SQL
      CREATE TRIGGER IF NOT EXISTS #{table}_ai AFTER INSERT ON #{table}
      BEGIN
        INSERT INTO #{fts_table} (rowid, #{col_list}) VALUES (#{insert_cols});
      END;
    SQL

    # --- UPDATE trigger ---
    db.exec <<-SQL
      CREATE TRIGGER IF NOT EXISTS #{table}_au AFTER UPDATE ON #{table}
      BEGIN
        UPDATE #{fts_table} SET #{update_set} WHERE rowid = old.rowid;
      END;
    SQL

    # --- DELETE trigger ---
    db.exec <<-SQL
      CREATE TRIGGER IF NOT EXISTS #{table}_ad AFTER DELETE ON #{table}
      BEGIN
        DELETE FROM #{fts_table} WHERE rowid = old.rowid;
      END;
    SQL

    puts "Triggers created for #{table} -> #{fts_table}"
  end

  puts "autofts completed successfully!"
end
