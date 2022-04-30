# frozen_string_literal: true

# SQL Order of Execution:
# FROM -> JOIN -> WHERE -> GROUP BY -> HAVING -> SELECT -> DISTINCT ->
# ORDER BY -> LIMIT -> OFFSET
# Query can either be: select, insert, update, delete
# Insert/Update/Delete can only operate on a single table
# Insert/Update/Delete only takes WHERE; joins/order are invalid
# FROM/JOIN can be done any time, but for sake of the assignment
# specifications, it will be done when run is executed.

class MySqliteRequest
  ORDER = %w[DESC ASC].freeze

  def initialize(table_name = nil)
    @table = table_name
    @query_type = nil
    @joins = []
    @wheres = []
    @select = []
    @order = []
    @values = nil
    @set = nil
  end

  def run
    run_errors
  end

  # Takes a csv file name
  def from(table_name)
    raise "Can't have two FROMs" if @table

    @table = table_name
    self
  end

  def select(*column_names)
    @query_type ||= :select
    one_query_type(:select)

    @select.concat column_names
    self
  end

  def where(column_name, criteria)
    @wheres << [column_name, criteria]
  end

  def join(col_a, table_name_b, col_b)
    @joins << [col_a, table_name_b, col_b]
  end

  def order(order, column_name)
    raise 'Order must be ASC or DESC' unless ORDER.include?(order.upcase)

    @order << [order, column_name]
  end

  def insert(table_name)
    raise "Can't have two FROMs" if @table

    @table = table_name
    @query_type ||= :insert
    one_query_type(:insert)
  end

  # Since insert only takes in table_name, # columns should match
  def values(data)
    raise "Can't have multiple values" if @values
    raise "Can't have value and set" if @set

    @values = data
  end

  def update(table_name)
    raise "Can't have two FROMs" if @table

    @table = table_name
    @query_type ||= :update
    one_query_type(:update)
  end

  def set(data)
    raise "Can't have multiple sets" if @set
    raise "Can't have value and set" if @values

    @set = data
  end

  def delete
    @query_type ||= :delete
    one_query_type(:delete)
  end

  private

  def one_query_type(query_type)
    raise "Can't have different query types" unless @query_type == query_type
  end

  def run_errors
    raise 'Must have a table!' unless @table
    raise 'Must have a query type!' unless @query_type
    raise 'Order and join can only be used with select!' if @query_type != :select &&
                                                            (@order.any? || @joins.any?)
    raise 'Insert must have values!' if @query_type == :insert && !@values
    raise 'Update must have set!' if @query_type == :update && !@set
    raise 'Delete must have where!' if @query_type == :delete && @wheres.empty?
    raise "Insert can't have where!" if @query_type == :insert && @wheres.any?
    raise "Select can't have values or set" if @query_type == :select && (@values || @set)
  end
end

request = MySqliteRequest.new
request = request.from('nba_player_data.csv')
request = request.select('name')
request = request.where('birth_state', 'Indiana')
request.run
# [{"name" => "Andre Brown"]

MySqliteRequest.new('nba_player_data').select('name').where('birth_state', 'Indiana').run
# [{"name" => "Andre Brown"]
