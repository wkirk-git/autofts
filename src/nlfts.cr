require "sqlite3"
require "json"

# --- Type inference ---
enum ColumnType
  Int32Type
  Float64Type
  TimeType
  StringType
end

struct ColumnSchema
  property name : String
  property col_type : ColumnType

  def initialize(@name : String, @col_type : ColumnType)
  end
end

struct TableSchema
  property name : String
  property columns : Array(ColumnSchema)

  def initialize(@name : String, @columns : Array(ColumnSchema))
  end
end

# --- Schema inspection and type inference ---
class SchemaInspector
  def self.infer_column_type(db : DB::Database, table : String, col : String) : ColumnType
    sample_values = [] of String
    db.query "SELECT #{col} FROM #{table} WHERE #{col} IS NOT NULL LIMIT 10" do |rs|
      rs.each do
        val = rs.read(String)
        sample_values << val
      end
    end

    return ColumnType::StringType if sample_values.empty?

    if sample_values.all? { |v| v =~ /^\d+$/ }
      ColumnType::Int32Type
    elsif sample_values.all? { |v| v =~ /^\d+(\.\d+)?$/ }
      ColumnType::Float64Type
    elsif sample_values.all? { |v| v =~ /^\d{4}-\d{2}-\d{2}/ }
      ColumnType::TimeType
    else
      ColumnType::StringType
    end
  end

  def self.load_schema(db : DB::Database) : Array(TableSchema>
    schemas = [] of TableSchema

    db.query "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_fts'" do |rs|
      rs.each do
        fts_name = rs.read(String)
        base_table = fts_name.sub(/_fts$/, "")
        cols = [] of ColumnSchema

        db.query "PRAGMA table_info(#{base_table})" do |rs2|
          rs2.each do
            rs2.read # cid
            col_name = rs2.read(String)
            rs2.read; rs2.read; rs2.read; rs2.read # skip rest
            inferred = infer_column_type(db, base_table, col_name)
            cols << ColumnSchema.new(col_name, inferred)
          end
        end

        schemas << TableSchema.new(base_table, cols)
      end
    end

    schemas
  end
end

# --- NL to SQL builder ---
class FTSQueryBuilder
  getter schemas : Array(TableSchema)

  def initialize(@schemas : Array(TableSchema))
  end

  def to_sql(nl_query : String) : String?
    q = nl_query.downcase.strip
    table = guess_table(q)
    return nil unless table

    search_term = extract_search_term(q)
    filters = extract_filters(q, table)
    order = extract_order(q, table)
    limit = extract_limit(q)

    sql = "SELECT rowid, * FROM #{table.name}_fts WHERE #{table.name}_fts MATCH '#{search_term}'"
    filters.each { |f| sql += " AND #{f}" }
    sql += " #{order}" if order
    sql += " LIMIT #{limit}" if limit
    sql
  end

  private def guess_table(query : String) : TableSchema?
    @schemas.find do |t|
      query.includes?(t.name.downcase) || query.includes?(t.name.downcase.chomp("s"))
    end
  end

  private def extract_search_term(query : String) : String
    markers = ["containing", "named", "called", "with", "matching", "about"]
    segment = nil

    markers.each do |m|
      if query.includes?(m)
        segment = query.split(m, 2)[1]?.try(&.strip)
        break
      end
    end

    segment ||= query

    # Preserve quoted phrases
    phrases = [] of String
    segment = segment.gsub(/"([^"]+)"/) do |m|
      phrases << m
      "__PHRASE#{phrases.size - 1}__"
    end

    # Normalize boolean operators
    segment = segment.gsub(/\band\b/, "AND")
                     .gsub(/\bor\b/, "OR")
                     .gsub(/\bnot\b/, "NOT")

    # Detect "near" expressions: "word1 near 5 word2" -> "word1 NEAR/5 word2"
    segment = segment.gsub(/(\w+)\s+near\s+(\d+)\s+(\w+)/i) do
      "#{$1} NEAR/#{$2} #{$3}"
    end

    # Restore quoted phrases
    phrases.each_with_index do |p, i|
      segment = segment.gsub("__PHRASE#{i}__", p)
    end

    segment.strip
  end

  private def extract_filters(query : String, table : TableSchema) : Array(String)
    filters = [] of String

    # Time filters
    if query =~ /after (\d{4})/
      year = $1
      col = table.columns.find { |c| c.col_type == ColumnType::TimeType }
      filters << "#{col.name} >= '#{year}-01-01'" if col
    end
    if query =~ /before (\d{4})/
      year = $1
      col = table.columns.find { |c| c.col_type == ColumnType::TimeType }
      filters << "#{col.name} < '#{year}-01-01'" if col
    end

    # Numeric comparisons
    if query =~ /(less than|under) (\d+(\.\d+)?)/
      val = $2
      col = table.columns.find { |c| c.col_type == ColumnType::Float64Type || c.col_type == ColumnType::Int32Type }
      filters << "#{col.name} < #{val}" if col
    end
    if query =~ /(greater than|over|more than) (\d+(\.\d+)?)/
      val = $2
      col = table.columns.find { |c| c.col_type == ColumnType::Float64Type || c.col_type == ColumnType::Int32Type }
      filters << "#{col.name} > #{val}" if col
    end

    # ID filter
    if query =~ /id (\d+)/
      col = table.columns.find { |c| c.name == "id" && c.col_type == ColumnType::Int32Type }
      if col
        filters << "id = #{$1}"
      else
        filters << "rowid = #{$1}"
      end
    end

    filters
  end

  private def extract_order(query : String, table : TableSchema) : String?
    col = table.columns.find { |c| c.col_type == ColumnType::TimeType }
    return nil unless col
    if query.includes?("latest")
      "ORDER BY #{col.name} DESC"
    elsif query.includes?("oldest")
      "ORDER BY #{col.name} ASC"
    else
      nil
    end
  end

  private def extract_limit(query : String) : Int32?
    if query =~ /(top|first) (\d+)/
      $2.to_i
    else
      nil
    end
  end
end

# --- Runner ---
if ARGV.size < 2
  puts "Usage: crystal run autofts_nlsql.cr <database_file> '<query>'"
  exit 1
end

db_path = ARGV[0]
nl_query = ARGV[1]

DB.open "sqlite3://#{db_path}" do |db|
  schemas = SchemaInspector.load_schema(db)
  builder = FTSQueryBuilder.new(schemas)

  if sql = builder.to_sql(nl_query)
    results = [] of Hash(String, JSON::Any)
    db.query sql do |rs|
      cols = (0...rs.column_count).map { |i| rs.column_name(i) }
      rs.each do
        row = Hash(String, JSON::Any).new
        cols.each { |col| row[col] = JSON::Any.new(rs.read.to_s) }
        results << row
      end
    end

    puts({
      "query" => nl_query,
      "sql"   => sql,
      "count" => results.size,
      "rows"  => results
    }.to_json)
  else
    puts({ "error" => "Could not interpret query" }.to_json)
  end
end
