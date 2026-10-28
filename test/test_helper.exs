# R47: record every statement the adapter generates during the suite and
# fail the run if any of them resolves an operator or a function by name
# (SelectoDBPostgreSQL.ExplicitSQL.violations/1).
SelectoDBPostgreSQL.SQLRecorder.start()

ExUnit.after_suite(fn _result ->
  statements = SelectoDBPostgreSQL.SQLRecorder.statements()

  # Business statements: comparisons and functions (R47); Selecto's own
  # guard and catalog SQL: every operator and function (R48).
  violations =
    for {kind, sql} <- statements,
        found =
          if(kind == :guard,
            do: SelectoDBPostgreSQL.ExplicitSQL.guard_violations(sql),
            else: SelectoDBPostgreSQL.ExplicitSQL.violations(sql)
          ),
        found != [],
        do: {found, sql}

  if violations != [] do
    IO.puts(
      "\nR47 lint: #{length(violations)} of #{length(statements)} generated statements resolve by name:"
    )

    for {found, sql} <- Enum.take(violations, 20) do
      IO.puts(
        "  #{inspect(found)}\n    #{String.slice(String.replace(sql, ~r/\s+/, " "), 0, 600)}"
      )
    end

    System.at_exit(fn _ -> exit({:shutdown, 1}) end)
  else
    IO.puts("\nR47 lint: #{length(statements)} generated statements, none resolves by name")
  end
end)

ExUnit.start()
ExUnit.configure(exclude: [postgres: true])
