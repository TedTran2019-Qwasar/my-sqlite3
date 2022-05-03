# my-sqlite3
Making Sqlite using Ruby.

Both SqLiteRequest and MySqliteCLI have lots of pre-written examples to use at the bottom of the file.
- The CLI uses space as its row_sep and single quotations as its quote character. 
- So please format queries in the CLI with this in mind.
- Remember that INSERT/UPDATE/DELETE changes the CSV file, aka representation of the database.
- Followed SQL behavior as closely as possible. E.g, DELETE/UPDATE with no where operates on all rows, etc

I should probably refactor, but eh, whatever. This was fun to do. 