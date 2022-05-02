require 'date'
require_relative 'my_sqlite_request'

# It splits by spaces, so don't do email='janedoe'.
class MySqliteCLI
  KEYWORDS = %w[select insert update delete from join where order values set]

  def initialize(database)
    @database = database
    # Dir.mkdir(database) unless Dir.exist?(database)
    start_message
    ARGV.clear
    run
  end

  def run
    while true
      print 'my_sqlite_cli>'
      input = gets.chomp
      break if input == 'quit'
      input_arr = parse_input(input)
      action(input_arr)
    end
  end

  private

  def action(input)
    req = MySqliteRequest.new
    until input.empty?
      case input[0].upcase
      when 'SELECT'
        handle_select(req, input)
      when 'INSERT'
        handle_insert(req, input)
      when 'UPDATE'
        handle_update(req, input)
      when 'DELETE'
        handle_delete(req, input)
      when 'FROM'
        handle_from(req, input)
      when 'JOIN'
        handle_join(req, input)
      when 'WHERE'
      when 'ORDER'
      when 'VALUES'
      when 'SET'
      else
        raise 'Invalid syntax'
      end
    end
    run_query(req)
  end

  def handle_from(req, input)
    req.from(input[1])
    input.shift(2)
  end

  def handle_select(req, input)
    column_names = []
    while true
      input.shift
      column_names << input.first
      break unless input.first.end_with?(',')
    end
    input.shift
    req.select(*column_names)
  end

  def handle_insert(req, input)
    raise "Must be INSERT INTO" unless input[1] == 'INTO'
    req.insert(input[2])
    req.shift(3)
  end

  def handle_update(req, input)
    req.update(input[1])
    req.shift(2)
  end

  def handle_delete(req, input)
    req.delete
    input.shift
  end

  def handle_join(req, input)
    raise 'Invalid join syntax' if input[5].nil? || 
                                   input[2].upcase != 'ON' || 
                                   input[4] != '='
            
    table_b = input[1]
    col_a = input[3]
    col_b = input[5]
    req.join(col_a, table_b, col_b)
    input.shift(6)
  end
  
  def run_query(req)
    req.query_type == :select ? output_query_results(req.run) : req.run
  end

  def output_query_results(results)
    results.each do |result| 
      result.values.each_with_index do |val, i|
        print val
        if i == (result.length - 1)
          print "\n"
        else
          print '|'
        end
      end
    end
  end

  def parse_input(input)
    final = input[-1]
    raise 'Query must end with semiclolon!' unless final == ';'

    CSV.parse_line(input[0...-1], col_sep: ' ', quote_char: "'")
  end

  def start_message
    puts "MySQLite version 0.1 #{Date.today}"
  end
end

if __FILE__ == $PROGRAM_NAME
  MySqliteCLI.new(ARGV[0])
end