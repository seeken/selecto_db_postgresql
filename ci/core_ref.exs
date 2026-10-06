Code.require_file("source_refs.exs", __DIR__)
IO.puts(SelectoDBPostgreSQL.CI.SourceRefs.core!(Path.expand("..", __DIR__)))
