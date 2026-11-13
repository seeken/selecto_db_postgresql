defmodule SelectoDBPostgreSQL.Adapter do
  @moduledoc """
  PostgreSQL adapter for Selecto.

  Writes reach this adapter through the governed entry point, `SelectoUpdato`.
  `execute_write/3` and `execute_prepared_write/3` refuse a write without the
  `Selecto.Write.Authorization` issued for exactly that payload
  (`:ungoverned_write`). `execute_write_unsafe/3` and
  `execute_prepared_write_unsafe/3` skip that check and exist for trusted
  tooling and adapter tests only.

  ## Reads

  With no transaction open a Selecto read is a transaction of its own:
  `BEGIN`, one statement that makes it `READ ONLY` (unless it locks rows),
  sets the read's `statement_timeout` and checks `client_encoding` is UTF8,
  the query, `ROLLBACK`. Inside the host's transaction (`DBConnection.status/1`,
  so a raw `BEGIN` is the host's) it is the host's own statement, with its
  `statement_timeout` set before it and put back after it; a read that fails
  leaves that transaction for the host to end. Hand-built text must be a read
  by its first word, and in the host's transaction refuses by its words when
  it could write (`hand_built_in_host_transaction`). Relations a domain
  declares a schema for are written with it (`SelectoDBPostgreSQL.DeclaredSchema`).

  ## What the host must provide

  Selecto's statements run in the host's session and resolve names as the
  host's own queries do: `client_encoding` UTF8, a `search_path` naming the
  application's schemas (or `source_schema` on its domains), no untrusted role
  with `CREATE` on a schema on the path, and transactions through a handle.

  ## Prepared statements

  Every statement runs unnamed, always: `pg_prepared_statements` stays empty.
  Code a statement reaches can DEALLOCATE and PREPARE a named statement, in a
  READ ONLY transaction and past a ROLLBACK, and the driver would bind the
  replacement without parsing it. This works behind a transaction-mode pooler
  too. Ecto repository connections keep the repository's own `:prepare`
  setting for the repository's queries; the adapter's run unnamed there too.
  """

  require Logger

  defmodule Control do
    @moduledoc false
    # See control/1.
    @enforce_keys [:sql]
    defstruct [:sql]
  end

  defmodule ReadOpening do
    @moduledoc false
    # Only adapter-owned SQL and an integer deadline, never business text.
    @enforce_keys [:read_only?, :timeout]
    defstruct [:read_only?, :timeout]
  end

  @behaviour Selecto.DB.Adapter
  @behaviour Selecto.DB.WriteAdapter

  alias SelectoDBPostgreSQL.PostImage
  alias SelectoDBPostgreSQL.{DeclaredSchema, HandBuilt}

  @read_committed "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
  alias SelectoDBPostgreSQL.FunctionVerification
  alias SelectoDBPostgreSQL.Identifier
  alias SelectoDBPostgreSQL.GraphCompiler
  alias SelectoDBPostgreSQL.WriteCompiler

  alias Selecto.Write.{
    Authorization,
    Batch,
    CandidateRequest,
    CandidateState,
    Capabilities,
    Command,
    CommittedEffectSink,
    Error,
    Graph,
    Result
  }

  alias Selecto.Write.{RecordRequest, RecordState}

  alias Selecto.Write.Graph.Materializer

  @native_type_mappings %{
    "int2" => :integer,
    "int4" => :integer,
    "int8" => :integer,
    "float4" => :float,
    "float8" => :float,
    "numeric" => :decimal,
    "decimal" => :decimal,
    "money" => :decimal,
    "bpchar" => :string,
    "name" => :string,
    "citext" => :string,
    "bool" => :boolean,
    "timestamptz" => :utc_datetime,
    "jsonb" => :map,
    "_int4" => {:array, :integer},
    "_text" => {:array, :string},
    "_varchar" => {:array, :string},
    "_numeric" => {:array, :decimal},
    "inet" => :string,
    "cidr" => :string,
    "macaddr" => :string,
    "point" => :string,
    "line" => :string,
    "lseg" => :string,
    "box" => :string,
    "path" => :string,
    "polygon" => :string,
    "circle" => :string,
    "public.geometry" => :geometry,
    "public.geography" => :geography,
    "bytea" => :binary,
    "tsvector" => :text_search_document
  }

  @impl true
  def name, do: :postgresql

  @impl true
  def dialect, do: SelectoDBPostgreSQL.Dialect

  @impl true
  def identifier_policy, do: %{max_bytes: 63}

  @impl true
  def capability(:text_search) do
    %{
      feature: :text_search,
      supported?: true,
      modes: [:websearch, :plain, :phrase, :boolean, :natural, :prefix],
      governed_lookup?: true,
      default_mode: :websearch,
      document_type: :text_search_document,
      help: "Full-text search with web-style, plain, phrase, or boolean query modes."
    }
  end

  def capability(feature) do
    %{feature: feature, supported?: supports?(feature)}
  end

  @impl true
  def analyze_query(selecto, options),
    do: SelectoDBPostgreSQL.QueryAnalyzer.analyze_query(selecto, options)

  @impl true
  def analyze_index_usage(selecto, options),
    do: SelectoDBPostgreSQL.QueryAnalyzer.analyze_index_usage(selecto, options)

  @impl true
  def table_statistics(selecto, _options),
    do: SelectoDBPostgreSQL.QueryAnalyzer.get_table_statistics(selecto)

  @impl true
  def normalize_type(type) when is_binary(type) do
    normalized = type |> String.trim() |> String.downcase()
    Map.get(@native_type_mappings, normalized, type)
  end

  def normalize_type(:jsonb), do: :map
  def normalize_type(:tsvector), do: :text_search_document
  def normalize_type(:citext), do: :string
  def normalize_type(type), do: Selecto.TypeSystem.normalize_type(type)

  @impl true
  def type_family(type), do: type |> normalize_type() |> Selecto.TypeFamily.of()

  @impl true
  def normalize_execution_result(result), do: {:ok, normalize_result(result)}

  @impl true
  def normalize_error(%Selecto.Error{} = error), do: error

  # Returned errors carry a stable category and PostgreSQL error code, never the
  # server message, detail, hint, SQL text or bound parameters: those can echo
  # other tenants' values (for example a unique-violation detail) or the query.
  def normalize_error(%Postgrex.Error{} = error) do
    native = Map.get(error, :postgres) || %{}
    code = Map.get(native, :code)
    category = normalize_error_category(code || Map.get(native, :pg_code))

    Selecto.Error.query_error(safe_query_error_message(category), nil, [], %{
      adapter: :postgresql,
      category: category,
      code: if(is_atom(code), do: code),
      sqlstate: safe_sqlstate(Map.get(native, :pg_code)),
      constraint: Map.get(native, :constraint),
      column: Map.get(native, :column),
      recoverable?: category in [:unique_violation, :foreign_key_violation, :not_null_violation]
    })
  end

  def normalize_error(%DBConnection.ConnectionError{}), do: safe_connection_error()

  def normalize_error({:query_exception, module, _message}) when is_atom(module),
    do: safe_exception_error(module)

  def normalize_error({:connection_exit, _reason}) do
    Selecto.Error.connection_error("PostgreSQL connection exited", %{
      adapter: :postgresql,
      category: :connection_exit
    })
  end

  def normalize_error(%{__exception__: true, __struct__: module}),
    do: safe_exception_error(module)

  def normalize_error(reason), do: Selecto.Error.from_reason(reason)

  defp safe_exception(%Postgrex.Error{} = error), do: normalize_error(error)
  defp safe_exception(%DBConnection.ConnectionError{}), do: safe_connection_error()
  defp safe_exception(%{__struct__: module}), do: safe_exception_error(module)

  defp safe_connection_error do
    Selecto.Error.connection_error("PostgreSQL connection failed", %{
      adapter: :postgresql,
      category: :connection_error
    })
  end

  defp safe_exception_error(module) do
    Selecto.Error.query_error("PostgreSQL query failed", nil, [], %{
      adapter: :postgresql,
      category: :query_exception,
      exception: inspect(module)
    })
  end

  defp safe_query_error_message(:unique_violation),
    do: "PostgreSQL rejected the query: unique constraint violated"

  defp safe_query_error_message(:foreign_key_violation),
    do: "PostgreSQL rejected the query: foreign key constraint violated"

  defp safe_query_error_message(:not_null_violation),
    do: "PostgreSQL rejected the query: not-null constraint violated"

  defp safe_query_error_message(:check_violation),
    do: "PostgreSQL rejected the query: check constraint violated"

  defp safe_query_error_message(_category), do: "PostgreSQL query failed"

  defp safe_sqlstate(<<_::binary-size(5)>> = sqlstate), do: sqlstate
  defp safe_sqlstate(_sqlstate), do: nil

  @impl true
  def connect({:pool, _} = pool_ref), do: {:ok, pool_ref}

  def connect(connection) when is_pid(connection) do
    if Process.alive?(connection),
      do: {:ok, connection},
      else: {:error, {:invalid_connection, connection}}
  end

  def connect(connection) when is_atom(connection) and not is_nil(connection) do
    if valid_named_connection?(connection),
      do: {:ok, connection},
      else: {:error, {:invalid_connection, connection}}
  end

  def connect(%DBConnection{} = connection), do: {:ok, connection}

  def connect(opts) when is_map(opts), do: connect(Map.to_list(opts))

  def connect(opts) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_connection_options, :expected_keyword_list}}

      Keyword.has_key?(opts, :url) ->
        {:error, {:invalid_connection_options, :url_option_not_supported}}

      not read_only_setting_valid?() ->
        {:error, {:invalid_configuration, :read_only_reads}}

      true ->
        warn_statement_cache()
        connect_postgrex(opts)
    end
  end

  def connect(_other),
    do: {:error, {:invalid_connection_options, :expected_options_or_connection}}

  # R63 removed the setting; a host that still sets it should know.
  defp warn_statement_cache do
    if match?({:ok, _}, Application.fetch_env(:selecto_db_postgresql, :statement_cache)) do
      Logger.warning(
        "selecto_db_postgresql: :statement_cache is ignored; every statement runs unnamed"
      )
    end
  end

  @impl true
  def disconnect(connection) when is_pid(connection) do
    if Process.alive?(connection), do: GenServer.stop(connection)
    :ok
  end

  def disconnect(_connection), do: :ok

  # DBConnection's per-call timeout when none is given. Before this adapter
  # enforced Selecto's `:timeout` itself it applied to every `Selecto.execute/2`
  # query, firing before Selecto's 30 second task timeout.
  @postgrex_default_timeout 15_000

  @doc """
  Executes a statement.

  With an integer `timeout: milliseconds` option (the time `Selecto.execute/2`
  has left, see `supports?(:execute_timeout)`) the statement is abandoned and
  `{:error, reason}` returned once the shorter of that time and the driver
  timeout that applies without it has elapsed: Postgrex's 15 second default,
  or the repository's configured `:timeout` for an Ecto repository.

  A Selecto read's timeout is the server's `statement_timeout`, local to the
  read, on any connection: nothing kills it, so a transaction the host holds
  goes on. A host's own statement on a connection the calling process already
  holds checked out (a `%DBConnection{}`, or an Ecto repository inside a
  transaction or checkout) runs under its checkout's deadline, which ignores a
  per-call timeout, so such a statement runs in a separate process that is shut
  down when `timeout` elapses.

  Without an integer `:timeout` the statement runs exactly as before.
  """
  @impl true
  def execute(_connection, query, _params, _opts) when not (is_binary(query) or is_list(query)) do
    {:error,
     %Selecto.Error{
       type: :query_error,
       message: "PostgreSQL statement must be SQL text",
       details: %{reason: :invalid_statement, adapter: :postgresql}
     }}
  end

  def execute(connection, query, params, opts) do
    if Selecto.Executor.selecto_read?(opts) do
      with {:ok, query, opts} <- classify_read(query, opts) do
        guarded_read(connection, query, params, opts)
      end
    else
      execute_plain(connection, query, params, opts)
    end
  end

  # R71: what a read is, from what Selecto's executor said about its compile.
  #
  # `:generated`: every part of it is Selecto's. `:mixed`: Selecto built it and
  # it carries a host fragment, kept verbatim. Both name the relations their
  # domains declare a schema for with it (prepare_read/2). `:hand_built`: the
  # text is wholly the host's (R68), runs as written, and must be a read by its
  # first word.
  defp classify_read(query, opts) do
    kind = read_kind(opts)
    opts = Keyword.put(opts, :selecto_read_kind, kind)

    case {kind, Keyword.get(opts, :selecto_qualified_relations)} do
      {:hand_built, _relations} ->
        with :ok <- hand_built_read(query), do: {:ok, query, opts}

      {_kind, %{} = relations} when map_size(relations) > 0 ->
        case DeclaredSchema.qualify(query, relations) do
          {:ok, query} -> {:ok, query, opts}
          {:error, %Error{} = error} -> {:error, read_refusal(error)}
        end

      _undeclared ->
        {:ok, query, opts}
    end
  end

  @doc false
  # Before a read compiles (Selecto.Executor.prepare_read/1): the schemas its
  # domains declare for its relations (SelectoDBPostgreSQL.DeclaredSchema).
  # Nothing is read from the database.
  def prepare_read(%{domain: domain} = selecto, _connection) when is_map(domain) do
    case DeclaredSchema.relations(selecto) do
      {:ok, relations} when map_size(relations) == 0 ->
        {:ok, selecto}

      {:ok, relations} ->
        set = (Map.get(selecto, :set) || %{}) |> Map.put(:qualified_relations, relations)
        {:ok, %{selecto | set: set}}

      {:error, %Error{} = error} ->
        {:error, read_refusal(error)}
    end
  end

  def prepare_read(selecto, _connection), do: {:ok, selecto}

  defp read_kind(opts) do
    cond do
      Keyword.get(opts, :selecto_raw_sql) == true -> :mixed
      Keyword.get(opts, :selecto_generated) == true -> :generated
      true -> :hand_built
    end
  end

  defp hand_built_read(query) do
    with {:error, %Error{} = error} <- HandBuilt.read(query) do
      {:error, read_refusal(error)}
    end
  end

  defp execute_plain(connection, query, params, opts) do
    case Keyword.get(opts, :timeout) do
      timeout when is_integer(timeout) and timeout >= 0 ->
        execute_within(connection, query, params, opts, timeout)

      _other ->
        execute_statement(connection, query, params, opts)
    end
  end

  # A Selecto read (marked by Selecto's executor, Selecto.Executor.selecto_read?/1)
  # runs on one checked-out connection. The server's transaction status decides
  # its shape (DBConnection.status/1, so a raw BEGIN is the host's too):
  #
  # - idle: BEGIN, one statement that makes the transaction READ ONLY, sets the
  #   read's statement_timeout and checks client_encoding, the query, ROLLBACK;
  # - in the host's transaction: the query as the host's own statement, with its
  #   statement_timeout set before it and the prior value put back after it.
  #
  # A connection the host holds is never cancelled by killing a process: that
  # abandons an unread reply. The read's deadline is the server's
  # statement_timeout (read_deadline/1), so the reply is consumed and the
  # host's transaction survives.
  defp guarded_read(%DBConnection{} = connection, query, params, opts) do
    run_guarded_read(
      &query_postgrex(connection, &1, &2, opts),
      &guard_statement(connection, &1, &2),
      DBConnection.status(connection) == :transaction,
      query,
      params,
      Map.put(read_plan(opts), :close, fn -> close_connection(connection) end)
    )
  end

  defp guarded_read({:pool, pool_ref} = connection, query, params, opts) do
    if own_pool_reference?(pool_ref) do
      case resolve_live_pool_pid(pool_ref) do
        {:ok, pool_pid} -> guarded_checkout(pool_pid, query, params, opts)
        {:error, reason} -> {:error, reason}
      end
    else
      execute_plain(connection, query, params, opts)
    end
  end

  defp guarded_read(connection, query, params, opts) when is_pid(connection) do
    if Process.alive?(connection),
      do: guarded_checkout(connection, query, params, opts),
      else: {:error, {:invalid_connection, connection}}
  end

  defp guarded_read(connection, query, params, opts)
       when is_atom(connection) and not is_nil(connection) do
    cond do
      not valid_named_connection?(connection) ->
        {:error, {:invalid_connection, connection}}

      ecto_repo?(connection) ->
        guarded_ecto_read(connection, query, params, opts)

      named_postgrex_connection?(connection) ->
        guarded_checkout(connection, query, params, opts)

      true ->
        # A module with query/2 alone is no Postgrex pool:
        # execute_module_connection/4 hands it to the driver, which exits, so
        # the read refuses. It never reaches the module's query/2.
        run_guarded_read(
          &execute_module_connection(connection, &1, &2, opts),
          &execute_module_connection(connection, &1, &2, opts),
          transaction_open?(&execute_module_connection(connection, &1, &2, opts)),
          query,
          params,
          read_plan(opts)
        )
    end
  end

  defp guarded_read(connection, query, params, opts),
    do: execute_plain(connection, query, params, opts)

  defp guarded_checkout(connection, query, params, opts) do
    opts =
      case Keyword.get(opts, :timeout) do
        timeout when is_integer(timeout) and timeout >= 0 ->
          bounded_opts(opts, timeout, @postgrex_default_timeout)

        _other ->
          opts
      end

    DBConnection.run(
      connection,
      fn checked_out ->
        in_transaction? = DBConnection.status(checked_out) == :transaction

        run_guarded_read(
          &query_postgrex(checked_out, &1, &2, opts),
          &guard_statement(checked_out, &1, &2),
          in_transaction?,
          query,
          params,
          Map.put(read_plan(opts), :close, fn -> close_connection(checked_out) end)
        )
      end,
      client_cut(opts)
    )
  rescue
    exception -> {:error, safe_exception(exception)}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  # The checkout's own limit comes after the server's, so a read that times out
  # ends by statement_timeout, on a connection that stays; only a server that
  # does not answer at all reaches this, and then the connection goes.
  @client_grace 5_000

  defp client_cut(opts) do
    opts
    |> Keyword.take([:timeout, :deadline, :queue_target, :queue_interval])
    |> Enum.map(fn
      {key, value} when key in [:timeout, :deadline] and is_integer(value) ->
        {key, value + @client_grace}

      other ->
        other
    end)
  end

  # R72: a last resort, when a read's own transaction could not be ended:
  # visible to the holder, whose next statement fails.
  defp close_connection(%DBConnection{pool_ref: pool_ref}) do
    error = DBConnection.ConnectionError.exception("Selecto could not undo a read")
    _ = DBConnection.Holder.disconnect(pool_ref, error)
    :ok
  end

  defp guarded_ecto_read(repo, query, params, opts) do
    {_checked_out?, repo_timeout} = ecto_execution_context(repo)

    opts =
      case Keyword.get(opts, :timeout) do
        timeout when is_integer(timeout) and timeout >= 0 ->
          bounded_opts(opts, timeout, repo_timeout)

        _other ->
          opts
      end

    apply(repo, :checkout, [
      fn ->
        in_transaction? = ecto_in_transaction?(repo)

        run_guarded_read(
          &execute_ecto_query(repo, &1, &2, Keyword.take(opts, [:timeout])),
          &execute_ecto_query(repo, &1, &2, Keyword.take(opts, [:timeout])),
          in_transaction?,
          query,
          params,
          read_plan(opts)
        )
      end,
      # A checked-out connection runs under its checkout's deadline.
      Keyword.take(opts, [:timeout])
    ])
  rescue
    exception -> {:error, safe_exception(exception)}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp run_guarded_read(execute, guard, true, query, params, plan) do
    # In the host's transaction nothing would stop a write, so hand-built text
    # whose words could write refuses before anything is sent.
    with :ok <- host_hand_built(query, plan),
         {:ok, ms} <- remaining(plan),
         {:ok, prior} <- open_host_read(guard, ms) do
      with {:ok, result} <- read_result(execute.(plan.prefix <> text(query), params)),
           :ok <- restore_deadline(guard, prior) do
        {:ok, result}
      end
    end
  end

  # A read of its own: nothing it reaches can write (READ ONLY, unless it locks
  # rows), and nothing it changes outlasts it (ROLLBACK). A read that cannot be
  # ended closes the connection, as the last resort.
  defp run_guarded_read(execute, guard, false, query, params, plan) do
    read_only? = plan.kind != :generated or read_only?(query)

    with {:ok, ms} <- remaining(plan),
         {:ok, _} <- guard.(control("BEGIN"), []) do
      result =
        with :ok <- open_read(guard, read_only?, ms) do
          read_result(execute.(plan.prefix <> text(query), params))
        end

      case guard.(control("ROLLBACK"), []) do
        {:ok, _} ->
          result

        {:error, _} = failure ->
          Map.get(plan, :close, fn -> :ok end).()
          if match?({:ok, _}, result), do: failure, else: result
      end
    end
  end

  defp text(query), do: IO.iodata_to_binary(query)

  @encoding_column "CAST(pg_catalog.pg_client_encoding() AS pg_catalog.text) AS selecto_guard_encoding"

  # The one statement after BEGIN: READ ONLY, the read's statement_timeout, and
  # client_encoding by its canonical name (the setting keeps a spelling like
  # UNICODE, which is UTF8). Postgrex sends parameters as UTF-8 bytes, which the
  # server reads in the session's client_encoding: under LATIN1 the tenant café
  # arrives as another tenant's name (R65).
  defp open_read(guard, read_only?, ms) do
    opening = %ReadOpening{read_only?: read_only?, timeout: ms}

    case guard.(opening, []) do
      {:ok, %{rows: [[encoding | _]]}} -> utf8(encoding)
      {:error, _} = error -> error
      _unreadable -> {:error, read_refusal(session_unreadable())}
    end
  end

  # This fixed guard has no host SQL or parameter values. Sending its bounded
  # integer deadline as a literal lets Postgrex use the simple protocol, rather
  # than waiting for a Describe reply just to discover that $1 is text.
  defp read_opening_sql(%ReadOpening{read_only?: read_only?, timeout: ms})
       when is_boolean(read_only?) and (is_nil(ms) or (is_integer(ms) and ms > 0)) do
    columns =
      [@encoding_column] ++
        if(read_only?,
          do: [
            "pg_catalog.set_config('transaction_read_only', 'on', true) AS selecto_guard_read_only"
          ],
          else: []
        ) ++
        if(ms,
          do: [deadline_sql("'" <> Integer.to_string(ms) <> "'") <> " AS selecto_guard_deadline"],
          else: []
        )

    "SELECT " <> Enum.join(columns, ", ")
  end

  # In the host's transaction: the encoding check and, with a deadline, the
  # read's statement_timeout, set local to the transaction; the prior value
  # comes back with restore_deadline/2.
  defp open_host_read(guard, ms) do
    {columns, params} =
      if ms,
        do:
          {", pg_catalog.current_setting('statement_timeout') AS selecto_guard_deadline_prior, " <>
             deadline_sql("$1") <> " AS selecto_guard_deadline", [Integer.to_string(ms)]},
        else: {"", []}

    case guard.("SELECT " <> @encoding_column <> columns, params) do
      {:ok, %{rows: [[encoding | rest]]}} ->
        with :ok <- utf8(encoding) do
          case rest do
            [prior, _armed] when is_binary(prior) -> {:ok, prior}
            [] -> {:ok, nil}
            _unreadable -> {:error, read_refusal(session_unreadable())}
          end
        end

      {:error, _} = error ->
        error

      _unreadable ->
        {:error, read_refusal(session_unreadable())}
    end
  end

  defp restore_deadline(_guard, nil), do: :ok

  defp restore_deadline(guard, prior) do
    case guard.(
           "SELECT pg_catalog.set_config('statement_timeout', $1::pg_catalog.text, true) " <>
             "AS selecto_guard_deadline",
           [prior]
         ) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  # SQL arming a read's deadline, `param` in milliseconds, local to the current
  # transaction. A shorter statement_timeout of the host's stands.
  @current_timeout "CAST(pg_catalog.current_setting('statement_timeout') AS pg_catalog.interval)"

  defp deadline_sql(param) do
    wanted = "CAST(pg_catalog.concat(#{param}::pg_catalog.text, ' ms') AS pg_catalog.interval)"

    "pg_catalog.set_config('statement_timeout', CASE WHEN #{@current_timeout} " <>
      "OPERATOR(pg_catalog.=) CAST('0' AS pg_catalog.interval) OR #{@current_timeout} " <>
      "OPERATOR(pg_catalog.>) #{wanted} THEN #{param}::pg_catalog.text " <>
      "ELSE pg_catalog.current_setting('statement_timeout') END, true)"
  end

  defp utf8("UTF8"), do: :ok

  defp utf8(_encoding),
    do:
      {:error,
       read_refusal(
         Error.new(
           :query_enforcement,
           "PostgreSQL the session's client_encoding is not UTF8: parameters would be misread",
           details: %{
             code: :query_rule_not_evaluable,
             reason: :client_encoding,
             adapter: :postgresql
           }
         )
       )}

  defp session_unreadable,
    do:
      Error.new(:query_enforcement, "PostgreSQL cannot read the session state",
        details: %{
          code: :query_rule_not_evaluable,
          reason: :session_state_unavailable,
          adapter: :postgresql
        }
      )

  defp host_hand_built(query, %{kind: :hand_built}) do
    with {:error, %Error{} = error} <- HandBuilt.host_transaction(query) do
      {:error, read_refusal(error)}
    end
  end

  defp host_hand_built(_query, _plan), do: :ok

  # How long this read may still run, in milliseconds, or nil with no deadline.
  defp remaining(%{deadline: nil}), do: {:ok, nil}

  defp remaining(%{deadline: deadline, timeout: timeout}) do
    case deadline - System.monotonic_time(:millisecond) do
      ms when ms > 0 ->
        {:ok, ms}

      _spent ->
        {:error,
         DBConnection.ConnectionError.exception(
           "PostgreSQL statement exceeded its timeout of #{timeout}ms"
         )}
    end
  end

  # R55 awaits a decision: `config :selecto_db_postgresql, read_only_reads:
  # false` turns READ ONLY off for generated reads in this one place. A read that
  # locks rows cannot be READ ONLY.
  defp read_only?(query), do: read_only_reads() and not HandBuilt.row_lock?(query)

  # Anything but false keeps reads read-only; connect/1 refuses a bad value.
  defp read_only_reads,
    do: Application.get_env(:selecto_db_postgresql, :read_only_reads, true) != false

  defp read_only_setting_valid?,
    do: Application.get_env(:selecto_db_postgresql, :read_only_reads, true) in [true, false]

  # A read that wrote refuses, typed. Whatever else a read fails with is the
  # driver's answer, as written.
  defp read_result({:error, %Postgrex.Error{postgres: %{code: :read_only_sql_transaction}}}),
    do: {:error, read_refusal(read_wrote())}

  defp read_result(result), do: result

  defp read_wrote do
    Error.new(:query_enforcement, "PostgreSQL refuses a read that tried to write",
      details: %{code: :query_rule_not_evaluable, reason: :read_only_read, adapter: :postgresql}
    )
  end

  defp read_plan(opts) do
    {deadline, timeout} = read_deadline(opts)

    %{
      prefix: Keyword.get(opts, :selecto_explain, ""),
      kind: Keyword.get(opts, :selecto_read_kind, :generated),
      deadline: deadline,
      timeout: timeout
    }
  end

  defp read_deadline(opts) do
    case Keyword.get(opts, :timeout) do
      timeout when is_integer(timeout) and timeout >= 0 ->
        deadline = System.monotonic_time(:millisecond) + timeout

        case Keyword.get(opts, :deadline) do
          existing when is_integer(existing) -> {min(existing, deadline), timeout}
          _other -> {deadline, timeout}
        end

      _none ->
        {nil, nil}
    end
  end

  @doc """
  EXPLAIN of a Selecto read (`explain` the prefix, "EXPLAIN (...) "): the
  statement runs exactly as the read itself would.
  """
  def explain_read(connection, query, params, explain, opts) do
    execute(connection, query, params, Keyword.put(opts, :selecto_explain, explain))
  end

  defp with_checkout(%DBConnection{} = connection, fun),
    do:
      fun.(&guard_statement(connection, &1, &2), DBConnection.status(connection) == :transaction)

  defp with_checkout({:pool, pool_ref}, fun) do
    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} -> with_checkout(pool_pid, fun)
      {:error, reason} -> {:error, reason}
    end
  end

  defp with_checkout(connection, fun) when is_pid(connection), do: checkout_run(connection, fun)

  defp with_checkout(connection, fun) when is_atom(connection) and not is_nil(connection) do
    cond do
      ecto_repo?(connection) ->
        apply(connection, :checkout, [
          fn ->
            fun.(&execute_ecto_query(connection, &1, &2), ecto_in_transaction?(connection))
          end
        ])

      named_postgrex_connection?(connection) ->
        checkout_run(connection, fun)

      true ->
        guard = &execute_module_connection(connection, &1, &2, [])
        fun.(guard, transaction_open?(guard))
    end
  rescue
    exception -> {:error, safe_exception(exception)}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp with_checkout(connection, fun), do: fun.(&guard_statement(connection, &1, &2), false)

  defp checkout_run(connection, fun) do
    DBConnection.run(connection, fn checked_out ->
      fun.(
        &query_postgrex(checked_out, &1, &2, []),
        DBConnection.status(checked_out) == :transaction
      )
    end)
  rescue
    exception -> {:error, safe_exception(exception)}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  # The statements Selecto runs around a read or write (a read's opening, its
  # deadline put back, a write's facts) go through the same traced driver call
  # as the statement itself; their result columns carry the `selecto_guard_`
  # tag (R22), so a certification harness can recognise exactly these receipts.
  defp guard_statement(connection, sql, params),
    do: guard_statement_run(connection, sql, params)

  defp guard_statement_run({:pool, pool_ref}, sql, params) do
    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} -> guard_statement_run(pool_pid, sql, params)
      {:error, reason} -> {:error, reason}
    end
  end

  defp guard_statement_run(connection, sql, params)
       when is_atom(connection) and not is_nil(connection) do
    if ecto_repo?(connection),
      do: execute_ecto_query(connection, sql, params),
      else: query_postgrex(connection, sql, params, [])
  end

  defp guard_statement_run(connection, sql, params),
    do: query_postgrex(connection, sql, params, [])

  defp read_refusal(%Error{} = error),
    do: %Selecto.Error{type: error.type, message: error.message, details: error.details}

  defp execute_within({:pool, pool_ref} = connection, query, params, opts, timeout) do
    if own_pool_reference?(pool_ref) do
      execute_statement(
        connection,
        query,
        params,
        bounded_opts(opts, timeout, @postgrex_default_timeout)
      )
    else
      # Another adapter's pool: its execute decides how the timeout applies.
      execute_in_process(fn -> execute_statement(connection, query, params, opts) end, timeout)
    end
  end

  defp execute_within(connection, query, params, opts, timeout)
       when is_atom(connection) and not is_nil(connection) do
    cond do
      not valid_named_connection?(connection) ->
        {:error, {:invalid_connection, connection}}

      ecto_repo?(connection) ->
        execute_ecto_within(connection, query, params, opts, timeout)

      true ->
        query_postgrex(
          connection,
          query,
          params,
          bounded_opts(opts, timeout, @postgrex_default_timeout)
        )
    end
  end

  defp execute_within(connection, query, params, opts, timeout) when is_pid(connection) do
    execute_statement(
      connection,
      query,
      params,
      bounded_opts(opts, timeout, @postgrex_default_timeout)
    )
  end

  # A checked-out connection's deadline is set when it was checked out, and
  # DBConnection ignores the per-call timeout.
  defp execute_within(%DBConnection{} = connection, query, params, opts, timeout) do
    execute_in_process(fn -> query_postgrex(connection, query, params, opts) end, timeout)
  end

  defp execute_within(connection, query, params, opts, _timeout),
    do: execute_statement(connection, query, params, opts)

  defp execute_ecto_within(repo, query, params, opts, timeout) do
    {checked_out?, repo_timeout} = ecto_execution_context(repo)
    opts = bounded_opts(opts, timeout, repo_timeout)

    if checked_out? do
      # Inside a transaction or checkout of the calling process Ecto would run
      # the statement on that connection, whose deadline ignores the per-call
      # timeout. A separate process takes its own connection, as Selecto's
      # task did.
      execute_in_process(fn -> execute_ecto_query(repo, query, params, opts) end, timeout)
    else
      execute_ecto_query(repo, query, params, opts)
    end
  end

  # Whether the calling process holds the repository's connection, and the
  # repository's configured query timeout (Ecto's default is DBConnection's).
  defp ecto_execution_context(repo) do
    meta = apply(:"Elixir.Ecto.Adapter", :lookup_meta, [repo])
    adapter = apply(repo, :__adapter__, [])

    checked_out? =
      module_exports?(adapter, :checked_out?, 1) and apply(adapter, :checked_out?, [meta])

    repo_timeout =
      case meta do
        %{opts: repo_opts} when is_list(repo_opts) ->
          Keyword.get(repo_opts, :timeout, @postgrex_default_timeout)

        _meta ->
          @postgrex_default_timeout
      end

    {checked_out? == true, repo_timeout}
  rescue
    # A repository that is not running fails the same way when queried.
    _exception -> {false, @postgrex_default_timeout}
  end

  # The driver timeout becomes the shorter of `timeout` and `limit` (the one
  # that applies without `timeout`), and a deadline bounds the whole call,
  # including DBConnection's checkout retries.
  defp bounded_opts(opts, timeout, limit) do
    timeout = if is_integer(limit), do: min(timeout, limit), else: timeout
    deadline = System.monotonic_time(:millisecond) + timeout

    deadline =
      case Keyword.get(opts, :deadline) do
        existing when is_integer(existing) -> min(existing, deadline)
        _other -> deadline
      end

    opts
    |> Keyword.put(:timeout, timeout)
    |> Keyword.put(:deadline, deadline)
  end

  defp own_pool_reference?(pool_ref) do
    pool_adapter =
      case pool_ref do
        %{adapter: adapter} when is_atom(adapter) -> adapter
        _other -> Selecto.AdapterSupport.default_adapter()
      end

    pool_adapter == __MODULE__
  end

  # Runs `fun` in a separate process and abandons it when `timeout` elapses.
  defp execute_in_process(fun, timeout) do
    caller = self()
    reply = make_ref()
    callers = [caller | Process.get(:"$callers", [])]

    {pid, monitor} =
      spawn_monitor(fn ->
        Process.put(:"$callers", callers)
        send(caller, {reply, fun.()})
      end)

    receive do
      {^reply, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:error, {:connection_exit, reason}}
    after
      timeout ->
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])

        receive do
          {^reply, _late_result} -> :ok
        after
          0 -> :ok
        end

        {:error,
         DBConnection.ConnectionError.exception(
           "PostgreSQL statement exceeded its timeout of #{timeout}ms"
         )}
    end
  end

  defp execute_statement({:pool, pool_ref}, query, params, opts) do
    case Selecto.ConnectionPool.execute(pool_ref, normalize_query(query), params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_statement(connection, query, params, opts) when is_atom(connection) do
    if valid_named_connection?(connection) do
      execute_module_connection(connection, query, params, opts)
    else
      {:error, {:invalid_connection, connection}}
    end
  end

  defp execute_statement(connection, query, params, opts) when is_pid(connection) do
    if Process.alive?(connection),
      do: query_postgrex(connection, query, params, opts),
      else: {:error, {:invalid_connection, connection}}
  end

  defp execute_statement(%DBConnection{} = connection, query, params, opts) do
    query_postgrex(connection, query, params, opts)
  end

  defp execute_statement(connection, _query, _params, _opts),
    do: {:error, {:invalid_connection, connection}}

  # Each column's type, with a domain (or a domain over a domain) resolved to
  # its base type and the base typmod the domain declares, so a domain over
  # varchar(2) reports `character varying(2)`. The walk stops at 16 levels;
  # a type still a domain there is reported unresolved (typtype 'd').
  @column_catalog_sql """
  WITH RECURSIVE walk (attnum, attname, typid, typmod, colloid, depth) AS (
    SELECT a.attnum, a.attname::pg_catalog.text, a.atttypid, a.atttypmod, a.attcollation, 0
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass($1::pg_catalog.text)
      AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped
    UNION ALL
    SELECT w.attnum, w.attname, t.typbasetype, t.typtypmod, w.colloid, w.depth OPERATOR(pg_catalog.+) 1
    FROM walk w JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) w.typid
    WHERE t.typtype OPERATOR(pg_catalog.=) 'd' AND w.depth OPERATOR(pg_catalog.<) 16
  )
  SELECT DISTINCT ON (w.attnum)
         w.attname AS selecto_catalog_column,
         pg_catalog.format_type(w.typid, w.typmod) AS selecto_catalog_type,
         t.typtype::pg_catalog.text AS selecto_catalog_typtype,
         COALESCE(c.collisdeterministic, true) AS selecto_catalog_deterministic
  FROM walk w
  JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) w.typid
  LEFT JOIN pg_catalog.pg_collation c ON c.oid OPERATOR(pg_catalog.=) w.colloid
  ORDER BY w.attnum, w.depth DESC
  """

  @impl Selecto.DB.WriteAdapter
  # format_type of each column's resolved base type and typmod, with the
  # column collation's collisdeterministic (query_enforced_scalar v2
  # verified metadata). The relation resolves as the write will name it: by
  # its declared schema, else by the session's search_path. No relation by
  # that name verifies nothing. An enum reports base `enum`. A domain the walk
  # could not resolve is reported `base_verified`, so only identity-safe
  # decisions are made on it. The selecto_catalog_* column names tag the read
  # in driver traces.
  def column_catalog(connection, table) when is_binary(table),
    do: read_column_catalog(connection, quote_relation(table))

  defp read_column_catalog(connection, relation) do
    with {:ok, %{rows: rows}} <-
           execute_statement(connection, @column_catalog_sql, [relation], []) do
      {:ok,
       Map.new(rows, fn [name, type, typtype, deterministic] ->
         info =
           case typtype do
             "e" -> %{type: "enum", deterministic: deterministic == true}
             "d" -> %{type: type, deterministic: deterministic == true, trust: :base_verified}
             _ -> %{type: type, deterministic: deterministic == true}
           end

         {name, info}
       end)}
    end
  end

  @impl Selecto.DB.WriteAdapter
  def write_capabilities(connection) do
    server_major =
      case server_version_major(connection) do
        {:ok, major} -> major
        _ -> nil
      end

    %{
      protocol_version: Selecto.Write.Capabilities.protocol_version(),
      insert: true,
      update: true,
      upsert: true,
      delete: true,
      write_graph: true,
      prepared_candidate_state: true,
      native_constraint_mapping: true,
      returning: true,
      generated_keys: :returning,
      atomic_batch: true,
      transactions: true,
      committed_effect_sink: true,
      dialect: :postgresql,
      server_major: server_major,
      merge: is_integer(server_major) and server_major >= 15,
      merge_returning: is_integer(server_major) and server_major >= 17,
      merge_delete_missing: is_integer(server_major) and server_major >= 17
    }
  end

  @impl Selecto.DB.WriteAdapter
  def preview_write(connection, command, opts \\ [])

  def preview_write(connection, %Graph{} = graph, opts) do
    with :ok <- validate_write_graph(graph) do
      GraphCompiler.preview(graph, graph_server_major(connection, opts), opts)
    end
  end

  def preview_write(_connection, %Batch{} = batch, opts) do
    with :ok <- validate_write_batch(batch) do
      WriteCompiler.preview(batch, opts)
    end
  end

  def preview_write(_connection, %Command{} = command, opts) do
    with :ok <- validate_write_command(command) do
      WriteCompiler.preview(command, opts)
    end
  end

  def preview_write(_connection, command, _opts), do: invalid_write_input(command)

  @doc """
  Executes a governed write.

  `opts[:authorization]` must be the `Selecto.Write.Authorization` that the
  governed entry point (`SelectoUpdato`) issued for exactly this command,
  batch, or graph. Without it the write fails with `:ungoverned_write` before
  any statement runs.
  """
  @impl Selecto.DB.WriteAdapter
  def execute_write(connection, write, opts \\ []) do
    with :ok <- Authorization.require_for(write, opts) do
      execute_write_unsafe(connection, write, opts)
    end
  end

  @doc """
  Executes a write without domain governance.

  For trusted tooling and adapter tests only; application code writes through
  `SelectoUpdato`.
  """
  @impl Selecto.DB.WriteAdapter
  def execute_write_unsafe(connection, command, opts \\ [])

  def execute_write_unsafe(connection, %Command{} = command, opts) do
    with :ok <- validate_write_command(command) do
      with_write_transaction(connection, opts, fn transactional_connection, opts ->
        with {:ok, result} <- execute_write_command(transactional_connection, command, opts),
             :ok <- invoke_committed_effect_sink(transactional_connection, result, command, opts) do
          {:ok, result}
        end
      end)
    end
  end

  def execute_write_unsafe(connection, %Batch{} = batch, opts) do
    with :ok <- validate_write_batch(batch) do
      with_write_transaction(connection, opts, fn transactional_connection, opts ->
        batch.commands
        |> Enum.reduce_while({:ok, []}, fn command, {:ok, results} ->
          case execute_write_command(transactional_connection, command, opts) do
            {:ok, result} -> {:cont, {:ok, [result | results]}}
            {:error, %Error{} = error} -> {:halt, {:error, error}}
          end
        end)
        |> case do
          {:ok, results} ->
            results = Enum.reverse(results)

            with :ok <-
                   invoke_committed_effect_sink(
                     transactional_connection,
                     results,
                     batch,
                     opts
                   ) do
              {:ok, results}
            end

          {:error, error} ->
            {:error, error}
        end
      end)
    end
  end

  def execute_write_unsafe(connection, %Graph{} = graph, opts) do
    with :ok <- validate_write_graph(graph) do
      server_major = graph_server_major(connection, opts)

      with_write_transaction(connection, opts, fn transactional_connection, opts ->
        with {:ok, result} <- execute_graph(transactional_connection, graph, server_major, opts),
             :ok <- invoke_committed_effect_sink(transactional_connection, result, graph, opts) do
          {:ok, result}
        end
      end)
    end
  end

  def execute_write_unsafe(_connection, command, _opts), do: invalid_write_input(command)

  @doc """
  Executes a governed prepared write.

  The preparation must return `{:ok, write, context, authorization}` with the
  governed entry point's authorization for exactly that write; otherwise the
  write fails with `:ungoverned_write` and the transaction rolls back.
  """
  @impl Selecto.DB.WriteAdapter
  def execute_prepared_write(connection, prepare_fun, opts \\ [])

  def execute_prepared_write(connection, prepare_fun, opts) when is_function(prepare_fun, 1) do
    execute_prepared_write_unsafe(
      connection,
      Authorization.governed_preparation(prepare_fun),
      opts
    )
  end

  def execute_prepared_write(_connection, prepare_fun, _opts),
    do: invalid_preparation(prepare_fun)

  @doc """
  Executes a prepared write without domain governance. The preparation returns
  `{:ok, write, context}`.

  For trusted tooling and adapter tests only; application code writes through
  `SelectoUpdato`.
  """
  @impl Selecto.DB.WriteAdapter
  def execute_prepared_write_unsafe(connection, prepare_fun, opts \\ [])

  def execute_prepared_write_unsafe(connection, prepare_fun, opts)
      when is_function(prepare_fun, 1) do
    with_write_transaction(connection, opts, fn transactional_connection, opts ->
      protected_state_loader =
        &load_prepared_state(transactional_connection, &1, opts)

      with {:ok, write, context} <- invoke_prepare(prepare_fun, protected_state_loader),
           :ok <- validate_prepared_write(write),
           :ok <- Capabilities.require(write_capabilities(transactional_connection), write),
           write_opts = Keyword.put(opts, :context, context),
           :ok <- lock_prepared_membership_parent(transactional_connection, write, write_opts),
           {:ok, result} <- execute_prepared(transactional_connection, write, write_opts),
           :ok <-
             invoke_committed_effect_sink(transactional_connection, result, write, write_opts) do
        {:ok, result}
      end
    end)
  end

  def execute_prepared_write_unsafe(_connection, prepare_fun, _opts),
    do: invalid_preparation(prepare_fun)

  defp invalid_preparation(prepare_fun) do
    {:error,
     Error.new(:invalid_preparation, "prepared write requires a one-argument function",
       details: %{actual: prepare_fun}
     )}
  end

  @doc false
  @spec load_candidate_state(term(), CandidateRequest.t(), keyword()) ::
          {:ok, CandidateState.t()} | {:error, Error.t()}
  def load_candidate_state(connection, request, opts \\ [])

  def load_candidate_state(connection, %CandidateRequest{} = request, opts) do
    with :ok <- validate_candidate_request(request),
         {:ok, predicate} <-
           WriteCompiler.compile_predicate(request.parent_command.predicate,
             context: request.context
           ),
         parent_query =
           "SELECT #{quote_identifier(request.parent_key)} FROM " <>
             "#{quote_relation(request.parent_command.relation)} WHERE #{predicate.text} FOR UPDATE",
         {:ok, parent_result} <-
           execute_candidate_query(connection, parent_query, predicate.params, opts),
         {:ok, parent_id} <- exactly_one_parent(parent_result, request),
         fields = candidate_fields(request),
         child_query = candidate_child_query(request, fields),
         {:ok, child_result} <-
           execute_candidate_query(connection, child_query, [parent_id], opts),
         :ok <- ensure_candidate_bound(child_result, request) do
      {:ok,
       %CandidateState{
         rows: result_rows(child_result),
         complete?: true,
         protection: :locked,
         revision: %{parent_key: request.parent_key, parent_id: parent_id}
       }}
    end
  end

  def load_candidate_state(_connection, request, _opts) do
    {:error,
     Error.new(:invalid_candidate_request, "expected a portable candidate-state request",
       details: %{actual: request}
     )}
  end

  @doc false
  @spec load_record_state(term(), RecordRequest.t(), keyword()) ::
          {:ok, RecordState.t()} | {:error, Error.t()}
  def load_record_state(connection, request, opts \\ [])

  def load_record_state(connection, %RecordRequest{} = request, opts) do
    with :ok <- validate_record_request(request),
         :ok <- ensure_upsert_serializable(connection, request, opts),
         {:ok, predicate} <-
           WriteCompiler.compile_predicate(request.predicate,
             context: request.context
           ),
         fields = request.fields |> Enum.map(&to_string/1) |> Enum.uniq() |> Enum.sort(),
         selected = Enum.map_join(fields, ", ", &quote_identifier/1),
         query =
           "SELECT #{selected} FROM #{quote_relation(request.relation)} " <>
             "WHERE #{predicate.text} FOR UPDATE",
         {:ok, result} <- execute_candidate_query(connection, query, predicate.params, opts),
         {:ok, values, exists?} <- record_state_values(result, request) do
      protection = if request.operation in [:upsert, "upsert"], do: :serializable, else: :locked

      {:ok,
       %RecordState{values: values, complete?: true, protection: protection, exists?: exists?}}
    end
  end

  def load_record_state(_connection, request, _opts) do
    {:error,
     Error.new(:invalid_record_request, "expected a portable record-state request",
       details: %{actual: request}
     )}
  end

  defp load_prepared_state(connection, %CandidateRequest{} = request, opts),
    do: load_candidate_state(connection, request, opts)

  defp load_prepared_state(connection, %RecordRequest{} = request, opts),
    do: load_record_state(connection, request, opts)

  defp load_prepared_state(_connection, request, _opts) do
    {:error,
     Error.new(:invalid_preparation, "prepared state loader received an unsupported request",
       details: %{actual: request}
     )}
  end

  defp validate_write_batch(%Batch{} = batch), do: Batch.validate(batch)
  defp validate_write_graph(%Graph{} = graph), do: Graph.validate(graph)
  defp validate_write_command(%Command{} = command), do: Command.validate(command)

  defp validate_prepared_write(%Command{} = command), do: validate_write_command(command)
  defp validate_prepared_write(%Batch{} = batch), do: validate_write_batch(batch)
  defp validate_prepared_write(%Graph{} = graph), do: validate_write_graph(graph)

  defp validate_prepared_write(other),
    do: invalid_write_input(other) |> elem(1) |> then(&{:error, &1})

  defp execute_prepared(connection, %Command{} = command, opts),
    do: execute_write_command(connection, command, opts)

  defp execute_prepared(connection, %Batch{} = batch, opts) do
    batch.commands
    |> Enum.reduce_while({:ok, []}, fn command, {:ok, results} ->
      case execute_write_command(connection, command, opts) do
        {:ok, result} -> {:cont, {:ok, [result | results]}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      error -> error
    end
  end

  defp execute_prepared(connection, %Graph{} = graph, opts),
    do: execute_graph(connection, graph, graph_server_major(connection, opts), opts)

  # Candidate rules acquire this lock while loading the protected collection.
  # Nested membership writers without a candidate rule still need the same
  # boundary, so Updato marks the root lock obligation on its governed graph.
  # Direct raw graphs are deliberately outside this claim and have no marker.
  defp lock_prepared_membership_parent(connection, %Graph{} = graph, opts) do
    case Map.get(graph.metadata, :membership_parent_lock) do
      nil -> :ok
      %{parent_key: parent_key} -> lock_graph_root_parent(connection, graph, parent_key, opts)
      _other -> invalid_membership_parent_lock(graph)
    end
  end

  defp lock_prepared_membership_parent(_connection, _write, _opts), do: :ok

  defp lock_graph_root_parent(
         connection,
         %Graph{root: {node_id, row_id}} = graph,
         parent_key,
         opts
       ) do
    with true <- candidate_identifier?(parent_key),
         {:ok, root_command} <- graph_root_command(graph, node_id, row_id),
         true <- root_command.operation in [:update, :delete],
         false <- is_nil(root_command.predicate),
         {:ok, predicate} <-
           WriteCompiler.compile_predicate(root_command.predicate,
             context: Keyword.get(opts, :context, %{})
           ),
         query =
           "SELECT #{quote_identifier(parent_key)} FROM #{quote_relation(root_command.relation)} " <>
             "WHERE #{predicate.text} FOR UPDATE",
         {:ok, result} <- execute_candidate_query(connection, query, predicate.params, opts),
         :ok <- exactly_one_locked_parent(result, root_command, parent_key) do
      :ok
    else
      false -> invalid_membership_parent_lock(graph)
      {:error, _reason} = error -> error
    end
  end

  defp lock_graph_root_parent(_connection, graph, _parent_key, _opts),
    do: invalid_membership_parent_lock(graph)

  defp graph_root_command(%Graph{nodes: nodes}, node_id, row_id) do
    with %Graph.Node{rows: rows} <- Enum.find(nodes, &(&1.id == node_id)),
         %Graph.Row{command: %Command{} = command} <- Enum.find(rows, &(&1.id == row_id)) do
      {:ok, command}
    else
      _ -> {:error, Error.new(:invalid_membership_parent_lock, "graph root is unavailable")}
    end
  end

  defp exactly_one_locked_parent(%{rows: [[_parent_id]]}, _command, _parent_key), do: :ok

  defp exactly_one_locked_parent(result, command, parent_key) do
    {:error,
     Error.new(:cardinality_mismatch, "membership parent lock requires exactly one root row",
       details: %{
         relation: command.relation,
         parent_key: parent_key,
         actual: length(Map.get(result, :rows, []))
       }
     )}
  end

  defp invalid_membership_parent_lock(graph) do
    {:error,
     Error.new(
       :invalid_membership_parent_lock,
       "governed nested graph has an invalid parent lock obligation",
       details: %{root: graph.root}
     )}
  end

  defp invoke_prepare(prepare_fun, candidate_loader) do
    case prepare_fun.(candidate_loader) do
      {:ok, write, context} when is_map(context) ->
        {:ok, write, context}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, %{type: type, message: message, details: details}}
      when is_atom(type) and is_binary(message) and is_map(details) ->
        {:error, Error.new(type, message, details: details)}

      {:error, _reason} = error ->
        error

      other ->
        {:error,
         Error.new(:invalid_preparation, "prepared write returned an invalid result",
           details: %{actual: other}
         )}
    end
  rescue
    exception ->
      {:error,
       Error.adapter_failure(
         :candidate_preparation_failed,
         :postgresql,
         exception.__struct__,
         "PostgreSQL candidate preparation failed"
       )}
  catch
    kind, _reason ->
      {:error,
       Error.adapter_failure(
         :candidate_preparation_failed,
         :postgresql,
         kind,
         "PostgreSQL candidate preparation failed"
       )}
  end

  defp validate_candidate_request(%CandidateRequest{} = request) do
    identifiers =
      [
        request.parent_command.relation,
        request.parent_key,
        request.child_relation,
        request.child_key
      ] ++ request.identity_fields ++ request.fields

    cond do
      request.parent_command.operation not in [:update, :delete] ->
        {:error,
         Error.new(:invalid_candidate_request, "candidate parent must be an existing row",
           details: %{operation: request.parent_command.operation}
         )}

      is_nil(request.parent_command.predicate) ->
        {:error,
         Error.new(:invalid_candidate_request, "candidate parent requires a scoped predicate")}

      not is_integer(request.max_rows) or request.max_rows < 1 or request.max_rows > 1_000 ->
        {:error,
         Error.new(:invalid_candidate_request, "candidate row bound is invalid",
           details: %{max_rows: request.max_rows, maximum: 1_000}
         )}

      request.identity_fields == [] ->
        {:error, Error.new(:invalid_candidate_request, "candidate identity fields are required")}

      not Enum.all?(identifiers, &candidate_identifier?/1) ->
        {:error, Error.new(:invalid_candidate_request, "candidate identifiers are invalid")}

      true ->
        :ok
    end
  end

  defp validate_record_request(%RecordRequest{} = request) do
    identifiers = [request.relation | request.fields]

    cond do
      request.operation not in [:update, "update", :upsert, "upsert"] ->
        {:error,
         Error.new(
           :invalid_record_request,
           "record state is only available for updates or upserts"
         )}

      is_nil(request.predicate) ->
        {:error, Error.new(:invalid_record_request, "record state requires a scoped predicate")}

      request.fields == [] or not Enum.all?(identifiers, &candidate_identifier?/1) ->
        {:error, Error.new(:invalid_record_request, "record-state identifiers are invalid")}

      true ->
        :ok
    end
  end

  defp candidate_identifier?(identifier) when is_atom(identifier), do: not is_nil(identifier)

  defp candidate_identifier?(identifier) when is_binary(identifier),
    do: String.trim(identifier) != ""

  defp candidate_identifier?(_identifier), do: false

  defp execute_candidate_query(connection, query, params, opts) do
    connection
    |> execute_statement(query, params, Keyword.take(opts, [:timeout, :log]))
    |> case do
      {:ok, result} -> {:ok, result}
      # A typed refusal stays what it is.
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:candidate_state_load_failed, reason)}
    end
  end

  defp exactly_one_parent(%{rows: [[parent_id]]}, _request), do: {:ok, parent_id}

  defp exactly_one_parent(%{rows: rows}, request) do
    {:error,
     Error.new(:cardinality_mismatch, "candidate parent matched an unexpected number of rows",
       details: %{relationship: request.relationship, expected: 1, actual: length(rows)}
     )}
  end

  defp exactly_one_record(%{rows: [_row], columns: _columns} = result, _request),
    do: {:ok, result_rows(result) |> hd()}

  defp exactly_one_record(%{rows: rows}, _request) do
    {:error,
     Error.new(:cardinality_mismatch, "record candidate matched an unexpected number of rows",
       details: %{expected: 1, actual: length(rows)}
     )}
  end

  defp record_state_values(result, %RecordRequest{operation: operation})
       when operation in [:upsert, "upsert"] do
    case Map.get(result, :rows, []) do
      [] -> {:ok, %{}, false}
      [_row] -> with {:ok, values} <- exactly_one_record(result, nil), do: {:ok, values, true}
      _rows -> exactly_one_record(result, nil) |> then(fn {:error, error} -> {:error, error} end)
    end
  end

  defp record_state_values(result, request) do
    with {:ok, values} <- exactly_one_record(result, request), do: {:ok, values, true}
  end

  # An absent conflict target has no row lock. Serializable isolation gives the
  # branch decision its required predicate protection: a competing insert or
  # update causes PostgreSQL to abort one transaction instead of committing an
  # unchecked upsert branch.
  defp ensure_upsert_serializable(connection, %RecordRequest{operation: operation}, opts)
       when operation in [:upsert, "upsert"] do
    case execute_candidate_query(
           connection,
           "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
           [],
           opts
         ) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp ensure_upsert_serializable(_connection, _request, _opts), do: :ok

  defp candidate_fields(request) do
    (request.fields ++ request.identity_fields)
    |> Enum.map(&to_string/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp candidate_child_query(request, fields) do
    selected = Enum.map_join(fields, ", ", &quote_identifier/1)
    order = Enum.map_join(request.identity_fields, ", ", &quote_identifier/1)

    "SELECT #{selected} FROM #{quote_relation(request.child_relation)} " <>
      "WHERE #{quote_identifier(request.child_key)} OPERATOR(pg_catalog.=) $1 ORDER BY #{order} " <>
      "LIMIT #{request.max_rows + 1}"
  end

  defp ensure_candidate_bound(%{rows: rows}, request) when length(rows) <= request.max_rows,
    do: :ok

  defp ensure_candidate_bound(%{rows: rows}, request) do
    {:error,
     Error.new(:candidate_state_limit_exceeded, "candidate state exceeds its row bound",
       details: %{
         relationship: request.relationship,
         max_rows: request.max_rows,
         observed_at_least: length(rows)
       }
     )}
  end

  defp quote_relation(relation) do
    relation
    |> to_string()
    |> String.split(".")
    |> Enum.map_join(".", &quote_identifier/1)
  end

  defp invalid_write_input(command) do
    {:error,
     Error.new(:invalid_command, "expected a portable write command, batch, or graph",
       details: %{actual: command}
     )}
  end

  defp invoke_committed_effect_sink(connection, result, write, opts) do
    CommittedEffectSink.invoke(
      Keyword.get(opts, :committed_effect_sink),
      connection,
      result,
      %{adapter: :postgresql, write: write}
    )
  end

  defp execute_graph(connection, %Graph{} = graph, server_major, opts) do
    graph.nodes
    |> Enum.reduce_while({:ok, %{}, 0, []}, fn node, {:ok, results, affected_rows, strategies} ->
      with {:ok, materialized} <- Materializer.materialize_node(node, results),
           {:ok, node_results, node_affected, strategy} <-
             execute_graph_node(connection, materialized, server_major, opts) do
        results =
          Map.merge(
            results,
            Map.new(node_results, fn {row_id, result} -> {{node.id, row_id}, result} end)
          )

        {:cont,
         {:ok, results, affected_rows + node_affected, strategies ++ [{node.id, strategy}]}}
      else
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, results, affected_rows, strategies} ->
        {:ok,
         %Result{
           operation: :graph,
           affected_rows: affected_rows,
           rows: Materializer.root_rows(graph, results),
           metadata:
             %{
               dialect: :postgresql,
               atomic?: true,
               server_major: server_major,
               node_strategies: Map.new(strategies)
             }
             |> Map.merge(Materializer.outcome_metadata(graph, results))
         }}

      error ->
        error
    end
  end

  defp execute_graph_node(connection, node, server_major, opts) do
    if GraphCompiler.merge_eligible?(node, server_major) do
      with {:ok, statement} <- GraphCompiler.compile_merge(node, opts),
           {:ok, query_result} <-
             execute_statement(connection, statement.text, statement.params, opts),
           {:ok, {results, affected_rows}} <- GraphCompiler.merge_results(node, query_result) do
        {:ok, results, affected_rows, :merge}
      else
        {:error, %Error{} = error} -> {:error, error}
        {:error, reason} -> {:error, write_error(:execution_failed, reason)}
      end
    else
      execute_graph_node_fallback(connection, node, opts)
    end
  end

  defp execute_graph_node_fallback(connection, node, opts) do
    with {:ok, row_results, affected_rows} <- execute_graph_rows(connection, node.rows, opts),
         {:ok, cleanup} <- Materializer.delete_missing_command(node, row_results),
         {:ok, cleanup_affected} <- execute_graph_cleanup(connection, cleanup, opts) do
      {:ok, row_results, affected_rows + cleanup_affected, :ordered_fallback}
    end
  end

  defp execute_graph_rows(connection, rows, opts) do
    Enum.reduce_while(rows, {:ok, %{}, 0}, fn row, {:ok, results, affected_rows} ->
      case execute_write_command(connection, row.command, opts) do
        {:ok, result} ->
          {:cont, {:ok, Map.put(results, row.id, result), affected_rows + result.affected_rows}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp execute_graph_cleanup(_connection, nil, _opts), do: {:ok, 0}

  defp execute_graph_cleanup(connection, command, opts) do
    case execute_write_command(connection, command, opts) do
      {:ok, result} -> {:ok, result.affected_rows}
      {:error, _} = error -> error
    end
  end

  defp graph_server_major(connection, opts) do
    case Keyword.fetch(opts, :server_version_major) do
      {:ok, major} when is_integer(major) and major > 0 ->
        major

      _ ->
        case server_version_major(connection) do
          {:ok, major} -> major
          _ -> 0
        end
    end
  end

  defp execute_write_command(connection, %Command{} = command, opts) do
    declared = command.relation

    with {:ok, facts} <- write_facts(connection, command, opts),
         opts = Keyword.merge(opts, uuid_columns: facts.uuid_columns, isolation: facts.isolation),
         :ok <- verify_read_committed(command, opts),
         :ok <- verify_native_constraints(connection, command, declared, opts),
         {:ok, statement} <- WriteCompiler.compile(command, opts),
         {:ok, query_result} <- execute_guarded_statement(connection, command, statement, opts),
         {:ok, affected_rows} <- enforce_cardinality(command, query_result) do
      {:ok,
       %Result{
         operation: command.operation,
         affected_rows: affected_rows,
         rows: result_rows(query_result),
         metadata: %{dialect: :postgresql}
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:execution_failed, reason, command)}
    end
  end

  # One statement before a command: client_encoding (UTF8, as for a read), the
  # transaction's isolation level (a guarded write needs READ COMMITTED) and
  # the written relation's uuid columns
  # (a domain over uuid included), whose text values bind as the driver
  # encodes a uuid. The relation resolves as the write names it.
  @write_facts_sql """
  WITH RECURSIVE walk (attname, typid, depth) AS (
    SELECT a.attname::pg_catalog.text, a.atttypid, 0
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass($1::pg_catalog.text)
      AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped
    UNION ALL
    SELECT w.attname, t.typbasetype, w.depth OPERATOR(pg_catalog.+) 1
    FROM walk w JOIN pg_catalog.pg_type t ON t.oid OPERATOR(pg_catalog.=) w.typid
    WHERE t.typtype OPERATOR(pg_catalog.=) 'd' AND w.depth OPERATOR(pg_catalog.<) 16
  )
  SELECT CAST(pg_catalog.pg_client_encoding() AS pg_catalog.text) AS selecto_guard_encoding,
         pg_catalog.current_setting('transaction_isolation') AS selecto_guard_isolation,
         ARRAY(SELECT w.attname FROM walk w
               WHERE w.typid OPERATOR(pg_catalog.=) CAST('pg_catalog.uuid' AS pg_catalog.regtype))
           AS selecto_guard_uuid_columns
  """

  defp write_facts(connection, %Command{relation: relation}, opts) do
    case execute_statement(
           connection,
           @write_facts_sql,
           [quote_relation(relation)],
           Keyword.take(opts, [:timeout, :log])
         ) do
      {:ok, %{rows: [["UTF8", isolation, uuid_columns]]}} when is_binary(isolation) ->
        {:ok, %{isolation: isolation, uuid_columns: List.wrap(uuid_columns)}}

      {:ok, %{rows: [[encoding, _isolation, _uuid_columns]]}} when is_binary(encoding) ->
        {:error,
         Error.new(
           :query_enforcement,
           "PostgreSQL the session's client_encoding is not UTF8: parameters would be misread",
           details: %{
             code: :query_rule_not_evaluable,
             reason: :client_encoding,
             adapter: :postgresql
           }
         )}

      {:ok, _unreadable} ->
        {:error,
         Error.new(:query_enforcement, "PostgreSQL cannot read the written relation's facts",
           details: %{code: :query_rule_not_evaluable, adapter: :postgresql}
         )}

      {:error, reason} ->
        {:error, write_error(:execution_failed, reason)}
    end
  end

  # A guarded INSERT/UPDATE (TW-09 post-image) locks and resolves its
  # relation first, returns its rows' (tableoid, ctid) and is re-read by them
  # after the statement (SelectoDBPostgreSQL.PostImage). A bound-scope write
  # without a post-image checks its relation's statement triggers and rules
  # before and after the statement.
  defp execute_guarded_statement(connection, command, statement, opts) do
    guard = &guard_statement(connection, &1, &2)
    check = &execute_statement(connection, &1, &2, Keyword.take(opts, [:timeout, :log]))

    case PostImage.prepare(command) do
      {:plain, _command} ->
        if PostImage.bound_delete?(command) do
          with {:ok, target} <- PostImage.lock_delete(command, check, guard),
               {:ok, query_result} <-
                 execute_statement(connection, statement.text, statement.params, opts),
               :ok <- PostImage.check_delete(command, target, check) do
            {:ok, query_result}
          end
        else
          execute_statement(connection, statement.text, statement.params, opts)
        end

      {:post_image, command} ->
        text = PostImage.returning(statement.text, command)

        with {:ok, target} <- PostImage.resolve(command, guard, guard),
             {:ok, query_result} <- execute_statement(connection, text, statement.params, opts),
             {query_result, keys} = PostImage.split_result(query_result, command),
             :ok <-
               PostImage.verify(
                 command,
                 target,
                 keys,
                 &WriteCompiler.compile_predicate(&1, opts, &2),
                 check
               ) do
          {:ok, query_result}
        end
    end
  end

  defp with_write_transaction(connection, opts, fun),
    do: with_postgres_transaction(connection, opts, &fun.(&1, opts))

  # Under REPEATABLE READ or SERIALIZABLE, catalog reads use the
  # transaction snapshot: a trigger committed after it passes every check yet
  # still fires (w3exrev16 item 16). A guarded write (bound tenant, trusted or
  # host scope, admission guard or post-image) runs only under READ
  # COMMITTED, checked inside its own transaction.
  @guard_metadata [
    :post_image,
    :insert_admission,
    :query_enforcement,
    :tenant_fields,
    :bound_scope
  ]

  defp verify_read_committed(%Command{metadata: metadata}, opts) when is_map(metadata) do
    if Enum.any?(@guard_metadata, &(Map.get(metadata, &1) not in [nil, []])) do
      case Keyword.fetch!(opts, :isolation) do
        "read committed" ->
          :ok

        level ->
          {:error,
           Error.new(
             :query_enforcement,
             "PostgreSQL guarded writes require READ COMMITTED",
             details: %{
               code: :query_rule_not_evaluable,
               reason: :transaction_isolation,
               isolation: level,
               adapter: :postgresql
             }
           )}
      end
    else
      :ok
    end
  end

  defp verify_read_committed(_command, _opts), do: :ok

  defp verify_native_constraints(_connection, %Command{native_constraints: []}, _declared, _opts),
    do: :ok

  # `declared` is the relation as the command named it (errors report it).
  defp verify_native_constraints(
         connection,
         %Command{native_constraints: constraints, relation: relation},
         declared,
         opts
       )
       when is_list(constraints) do
    Enum.reduce_while(constraints, :ok, fn native, :ok ->
      case verify_native_constraint(connection, relation, declared, native, opts) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp verify_native_constraint(connection, relation, declared, native, opts) do
    with %{adapter: "postgresql", constraint: constraint, category: category} <- native,
         expected_type when is_binary(expected_type) <- native_constraint_type(category),
         {:ok, %{rows: [[^expected_type]]}} <-
           execute_statement(
             connection,
             "SELECT c.contype FROM pg_catalog.pg_constraint c WHERE c.conname OPERATOR(pg_catalog.=) $1::pg_catalog.name AND c.conrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass($2::pg_catalog.text)",
             [constraint, to_string(relation)],
             Keyword.take(opts, [:timeout, :log])
           ) do
      :ok
    else
      _ ->
        {:error,
         Error.new(
           :native_constraint_unavailable,
           "PostgreSQL cannot attest the declared native constraint for this relation",
           details: %{
             relation: to_string(declared),
             native_constraint: safe_native_constraint(native)
           }
         )}
    end
  end

  defp native_constraint_type(:unique_violation), do: "u"
  defp native_constraint_type(:foreign_key_violation), do: "f"
  defp native_constraint_type(:not_null_violation), do: "n"
  defp native_constraint_type(:check_violation), do: "c"
  defp native_constraint_type(_category), do: nil

  defp safe_native_constraint(%{} = native),
    do: Map.take(native, [:binding_id, :adapter, :constraint, :category])

  defp safe_native_constraint(_native), do: %{}

  @impl true
  def execute_pool(pool_ref, query, params, opts) do
    query = normalize_query(query)
    use_prepared = Keyword.get(opts, :prepared, true)
    cache_key = if use_prepared, do: Selecto.ConnectionPool.generate_cache_key(query), else: nil

    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        execute_with_pool_pid(pool_pid, query, params, cache_key, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def execute_raw(connection, query, params) do
    cond do
      match?({:pool, _}, connection) ->
        case execute_statement(connection, query, params, prepared: false) do
          {:ok, result} -> {:ok, result}
          {:error, reason} -> {:error, Selecto.Error.from_reason(reason)}
        end

      is_atom(connection) and ecto_repo?(connection) ->
        execute_ecto_query(connection, query, params)

      valid_postgrex_connection?(connection) ->
        case execute_statement(connection, query, params, []) do
          {:ok, result} -> {:ok, result}
          {:error, reason} -> {:error, Selecto.Error.from_reason(reason)}
        end

      true ->
        {:error,
         Selecto.Error.connection_error("Invalid connection type", %{
           connection: inspect(connection)
         })}
    end
  rescue
    e ->
      {:error, Selecto.Error.from_reason(e)}
  end

  @impl true
  def placeholder(index), do: ["$", Integer.to_string(index)]

  @impl true
  def quote_identifier(identifier) when is_binary(identifier) do
    escaped = String.replace(identifier, "\"", "\"\"")
    "\"#{escaped}\""
  end

  def quote_identifier(identifier), do: identifier |> to_string() |> quote_identifier()

  @impl true
  def format_datetime(expression, format) when is_binary(format) do
    escaped = String.replace(format, "'", "''")
    ["to_char(", expression, ", '", escaped, "')"]
  end

  @impl true
  def rollup_sql(grouped_clauses), do: ["rollup( ", grouped_clauses, " )"]

  @impl true
  def supports?(feature) do
    feature in [
      :cte,
      :jsonb,
      :array_ops,
      :array_any_comparison,
      :native_null_ordering,
      :rollup,
      :returning,
      :text_search,
      :window_functions,
      :lateral_join,
      :prefix,
      :stream,
      :function_verification,
      :schema_introspection,
      :materialized_view_refresh,
      :materialized_view_refresh_concurrently,
      :projection_sum,
      :execute_timeout,
      :exact_instant_comparisons
    ]
  end

  @impl true
  def verify_function(connection, %Selecto.FunctionVerification.Request{} = request, opts) do
    FunctionVerification.verify(
      request,
      opts,
      fn query, params -> introspection_query(connection, query, params) end,
      fn statement, probe_opts -> prepare_function_probe(connection, statement, probe_opts) end
    )
  end

  @impl true
  def refresh_materialized_view(connection, database_name, opts \\ []) do
    with {:ok, quoted_name} <-
           Selecto.SQL.QualifiedIdentifier.quote(database_name, __MODULE__) do
      concurrently = if Keyword.get(opts, :concurrently, false), do: " CONCURRENTLY", else: ""
      query = "REFRESH MATERIALIZED VIEW#{concurrently} #{quoted_name};"

      case introspection_query(connection, query, []) do
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_tables(connection, opts \\ []) do
    schema = Keyword.get(opts, :schema, "public")

    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = $1
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """

    case introspection_query(connection, query, [schema]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [table_name] -> table_name end)}
      {:error, reason} -> {:error, {:query_failed, reason}}
    end
  end

  @impl true
  def list_relations(connection, opts \\ []) do
    schema = Keyword.get(opts, :schema, "public")
    include_views = Keyword.get(opts, :include_views, false)

    query =
      if include_views do
        """
        SELECT table_name,
               CASE table_type
                 WHEN 'BASE TABLE' THEN 'table'
                 WHEN 'VIEW' THEN 'view'
               END AS source_kind
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_type IN ('BASE TABLE', 'VIEW')
        UNION ALL
        SELECT matviewname AS table_name,
               'materialized_view' AS source_kind
        FROM pg_catalog.pg_matviews
        WHERE schemaname = $1
        ORDER BY table_name
        """
      else
        """
        SELECT table_name, 'table' AS source_kind
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
      end

    case introspection_query(connection, query, [schema]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [table_name, source_kind] ->
           %{name: table_name, source_kind: normalize_relation_source_kind(source_kind)}
         end)}

      {:error, reason} ->
        {:error, {:query_failed, reason}}
    end
  end

  @impl true
  def introspect_table(connection, table_name, opts \\ []) do
    schema = Keyword.get(opts, :schema, "public")
    include_associations = Keyword.get(opts, :include_associations, true)
    expand = Keyword.get(opts, :expand, false)

    with {:ok, columns} <- get_columns(connection, table_name, schema),
         {:ok, primary_key} <- get_primary_key(connection, table_name, schema),
         {:ok, foreign_keys} <- get_foreign_keys(connection, table_name, schema) do
      fields = Enum.map(columns, & &1.column_name)

      field_types =
        Enum.into(columns, %{}, fn column ->
          {column.column_name, map_pg_type(connection, column.data_type, column.udt_name)}
        end)

      with {:ok, associations} <-
             table_associations(
               connection,
               table_name,
               schema,
               primary_key,
               foreign_keys,
               include_associations,
               expand
             ) do
        column_metadata =
          Enum.into(columns, %{}, fn column ->
            {column.column_name,
             %{
               type: Map.get(field_types, column.column_name),
               nullable: column.is_nullable == "YES",
               default: column.column_default,
               max_length: column.character_maximum_length,
               precision: column.numeric_precision,
               scale: column.numeric_scale
             }}
          end)

        {:ok,
         %{
           table_name: table_name,
           schema: schema,
           fields: fields,
           field_types: field_types,
           primary_key: primary_key,
           associations: associations,
           columns: column_metadata,
           source: :postgresql
         }}
      end
    end
  end

  defp table_associations(
         _connection,
         _table_name,
         _schema,
         _primary_key,
         _foreign_keys,
         false,
         _expand
       ),
       do: {:ok, %{}}

  defp table_associations(
         connection,
         table_name,
         schema,
         primary_key,
         _foreign_keys,
         true,
         true
       ) do
    build_expanded_associations(connection, table_name, schema, primary_key)
  end

  defp table_associations(
         _connection,
         _table_name,
         _schema,
         _primary_key,
         foreign_keys,
         true,
         false
       ),
       do: {:ok, build_associations(foreign_keys)}

  @impl true
  def rollup_literal_order(index), do: [Integer.to_string(index), " asc nulls first"]

  @impl true
  def rollup_sort_fix(connection) do
    case server_version_major(connection) do
      {:ok, major} when is_integer(major) and major >= 18 -> false
      _ -> true
    end
  end

  @impl true
  def stream(connection, query, params, opts) do
    # A Selecto read's stream (Selecto.Executor.selecto_read?/1, captured here:
    # the stream's producer runs in its own process) is a read of its own.
    if Selecto.Executor.selecto_read?(opts) do
      with {:ok, query, opts} <- classify_read(query, opts),
           opts = Keyword.put(opts, :selecto_read_stream, true),
           :ok <- stream_refusal(connection) do
        open_stream(connection, query, params, opts)
      end
    else
      open_stream(connection, query, params, opts)
    end
  end

  defp open_stream({:pool, pool_ref}, query, params, opts) do
    case resolve_stream_pool_connection(pool_ref) do
      {:ok, pool_conn} -> {:ok, build_postgrex_cursor_stream(pool_conn, query, params, opts)}
      {:error, details} -> {:error, {:invalid_stream_pool, details}}
    end
  end

  defp open_stream(conn, query, params, opts) when is_pid(conn) do
    if Process.alive?(conn),
      do: {:ok, build_postgrex_cursor_stream(conn, query, params, opts)},
      else: {:error, {:invalid_connection, conn}}
  end

  defp open_stream(conn, query, params, opts) when is_atom(conn) and not is_nil(conn) do
    if valid_named_connection?(conn),
      do: {:ok, build_postgrex_cursor_stream(conn, query, params, opts)},
      else: {:error, {:invalid_connection, conn}}
  end

  defp open_stream(connection, _query, _params, _opts) do
    {:error, {:invalid_connection, connection}}
  end

  @server_version_num_query "show server_version_num"

  # Postgrex keeps the `server_version` the server reports when a connection
  # starts (and again after a reconnect), so a live Postgrex connection's major
  # version is read without a round trip. Each write checks capabilities
  # (twice for a governed one) and configure resolves rollup_sort_fix from it,
  # so querying `server_version_num` every time cost a round trip per check.
  # Connections Postgrex does not manage (Ecto repositories, modules with
  # `query/2`, generic pools) still run the query.
  @impl true
  def server_version_major(connection) do
    fetch_server_version_major(connection)
  end

  @impl true
  def validate_connection(connection) do
    cond do
      is_atom(connection) and not is_nil(connection) ->
        if valid_named_connection?(connection),
          do: :ok,
          else: {:error, "Named Postgrex connection is not registered"}

      match?({:pool, _}, connection) ->
        validate_pool_connection(connection)

      is_pid(connection) ->
        if Process.alive?(connection),
          do: :ok,
          else: {:error, "Postgrex connection process is not alive"}

      true ->
        {:error, "Invalid connection configuration"}
    end
  end

  @impl true
  def connection_info(connection) do
    cond do
      is_atom(connection) and not is_nil(connection) ->
        %{
          type: :postgrex,
          pid: connection,
          status: if(valid_named_connection?(connection), do: :connected, else: :disconnected)
        }

      match?({:pool, _}, connection) ->
        %{
          type: :connection_pool,
          pool_ref: elem(connection, 1),
          status: :connected,
          pool_stats: pool_stats(connection)
        }

      is_pid(connection) ->
        %{
          type: :postgrex,
          pid: connection,
          status: if(Process.alive?(connection), do: :connected, else: :disconnected)
        }

      true ->
        %{type: :unknown, value: connection, status: :invalid}
    end
  end

  @impl true
  def with_connection(pool_ref, fun) when is_function(fun, 1) do
    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        try do
          result = fun.(pool_pid)
          {:ok, result}
        rescue
          e ->
            {:error, safe_exception(e)}
        catch
          :exit, reason ->
            {:error,
             Selecto.Error.connection_error("PostgreSQL connection pool exited", %{
               exit_reason: reason
             })}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def transaction(connection, fun, opts \\ [])

  def transaction(%DBConnection{} = connection, fun, opts) when is_function(fun, 1) do
    run_postgrex_transaction(connection, fun, opts)
  end

  def transaction(connection, fun, opts)
      when is_function(fun, 1) and (is_pid(connection) or is_atom(connection)) do
    if not ecto_repo?(connection) and valid_postgrex_connection?(connection) do
      run_postgrex_transaction(connection, fun, opts)
    else
      transaction_from_managed_pool(connection, fun, opts)
    end
  end

  def transaction(pool_ref, fun, opts) when is_function(fun, 1) do
    transaction_from_managed_pool(pool_ref, fun, opts)
  end

  defp transaction_from_managed_pool(pool_ref, fun, opts) do
    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} ->
        run_postgrex_transaction(pool_pid, fun, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_postgrex_transaction(connection, fun, opts) do
    Selecto.Telemetry.span_for(
      [:selecto_db_postgresql, :telemetry, :transaction],
      %{adapter: :postgresql},
      fn -> do_run_postgrex_transaction(connection, fun, opts) end
    )
  end

  defp do_run_postgrex_transaction(connection, fun, opts) do
    Postgrex.transaction(connection, fun, opts)
  rescue
    e ->
      {:error, safe_exception(e)}
  catch
    :exit, reason ->
      {:error,
       Selecto.Error.connection_error("PostgreSQL connection pool exited", %{
         exit_reason: reason
       })}
  end

  defp normalize_relation_source_kind("table"), do: :table
  defp normalize_relation_source_kind("view"), do: :view
  defp normalize_relation_source_kind("materialized_view"), do: :materialized_view
  defp normalize_relation_source_kind(other), do: other

  @impl true
  def start_pool(connection_config, pool_config, pool_name) do
    case Selecto.ConnectionPool.get_manager_pid_by_name(pool_name) do
      {:ok, manager_pid} ->
        Selecto.ConnectionPool.build_pool_ref_from_manager(manager_pid)

      :error ->
        dbconnection_opts = [
          name: pool_name,
          pool: DBConnection.ConnectionPool,
          pool_size: pool_config[:pool_size],
          pool_overflow: pool_config[:max_overflow],
          timeout: pool_config[:connection_timeout],
          queue_target: pool_config[:checkout_timeout],
          queue_interval: 1000
        ]

        postgrex_opts = Keyword.merge(connection_config, dbconnection_opts)

        case start_postgrex_connection(postgrex_opts) do
          {:ok, pool_pid, started_new_pool?} ->
            manager_opts = [
              adapter: __MODULE__,
              pool_pid: pool_pid,
              pool_name: pool_name,
              pool_config: pool_config,
              connection_config: connection_config
            ]

            case Selecto.ConnectionPool.start_manager(manager_opts) do
              {:ok, manager_pid, :started} ->
                Selecto.ConnectionPool.build_pool_ref_from_manager(manager_pid)

              {:ok, manager_pid, :existing} ->
                if started_new_pool?, do: GenServer.stop(pool_pid)
                Selecto.ConnectionPool.build_pool_ref_from_manager(manager_pid)

              {:error, reason} ->
                if started_new_pool?, do: GenServer.stop(pool_pid)
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp normalize_query(%Control{} = control),
    do: control_sql(control) || raise(ArgumentError, "not a transaction statement")

  defp normalize_query(query) when is_binary(query), do: query
  defp normalize_query(query), do: IO.iodata_to_binary(query)

  defp resolve_live_pool_pid(pool_ref) do
    case Selecto.ConnectionPool.get_pool_pid(pool_ref) do
      {:ok, pool_pid} when is_pid(pool_pid) ->
        if Process.alive?(pool_pid) do
          {:ok, pool_pid}
        else
          {:error,
           Selecto.Error.connection_error("PostgreSQL connection pool is not available", %{
             reason: :pool_process_not_alive
           })}
        end

      {:ok, _pool_pid} ->
        {:error,
         Selecto.Error.connection_error("PostgreSQL connection pool is not available", %{
           reason: :invalid_pool_process
         })}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # An Ecto repository is a supervision tree, not a DBConnection process. Route it
  # through Ecto when the host application has Ecto available; keep it optional so
  # native Postgrex users do not need Ecto in their dependency graph.
  defp execute_module_connection(connection, query, params, opts) do
    if ecto_repo?(connection) do
      execute_ecto_query(connection, query, params)
    else
      query_postgrex(connection, query, params, opts)
    end
  end

  # Whether the checked-out connection is in a transaction, as the server
  # reported it: Repo.in_transaction?/0 only knows of Repo.transaction/1, and
  # is false under the SQL Sandbox and after a raw BEGIN. Selecto must never
  # COMMIT a transaction it did not open.
  defp ecto_in_transaction?(repo) do
    case ecto_connection(repo) do
      %DBConnection{} = connection -> DBConnection.status(connection) != :idle
      nil -> transaction_open?(&execute_ecto_query(repo, &1, &2))
    end
  end

  # {Ecto.Adapters.SQL, pool} is Ecto's own process key for a checked-out
  # connection: private, read here because nothing public gives its status.
  # Checked on ecto_sql 3.13.4 and 3.14.0. If it moves this returns nil and
  # transaction_open?/1 asks the server.
  defp ecto_connection(repo) do
    dynamic =
      if module_exports?(repo, :get_dynamic_repo, 0), do: repo.get_dynamic_repo(), else: repo

    case apply(Ecto.Adapter, :lookup_meta, [dynamic]) do
      %{pid: pool} -> Process.get({Ecto.Adapters.SQL, pool})
      _other -> nil
    end
  rescue
    _no_repo -> nil
  end

  # A connection with no status to read: the server's answer. SAVEPOINT works
  # in a transaction and fails outside one; anything else counts as a
  # transaction, whose savepoint then refuses rather than COMMIT.
  defp transaction_open?(execute) do
    name = "selecto_probe_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)

    case execute.(control("SAVEPOINT " <> name), []) do
      {:ok, _} ->
        _ = execute.(control("RELEASE SAVEPOINT " <> name), [])
        true

      {:error, %{postgres: %{code: :no_active_sql_transaction}}} ->
        false

      _other ->
        true
    end
  end

  # The Sandbox wraps each query in a savepoint of its own and releases it,
  # which would take the adapter's savepoints with it.
  defp execute_ecto_query(repo, query, params, opts \\ [])

  defp execute_ecto_query(repo, %ReadOpening{} = opening, [], opts) do
    execute_ecto_query(repo, read_opening_sql(opening), [], Keyword.put(opts, :query_type, :text))
  end

  defp execute_ecto_query(repo, %Control{} = control, params, opts) do
    case control_sql(control) do
      nil ->
        {:error, {:invalid_statement, :control}}

      sql ->
        execute_ecto_query(repo, sql, params, Keyword.put(opts, :sandbox_subtransaction, false))
    end
  end

  defp execute_ecto_query(repo, query, params, opts) do
    statement = query |> normalize_query()

    case apply(ecto_sql_module(), :query, [repo, statement, params, opts]) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    exception -> {:error, {:query_exception, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp ecto_repo?(module) when is_atom(module) do
    module_exports?(module, :__adapter__, 0) and module_exports?(ecto_sql_module(), :query, 3)
  end

  defp ecto_repo?(_), do: false

  defp ecto_sql_module, do: :"Elixir.Ecto.Adapters.SQL"

  defp module_exports?(module, function, arity) do
    case :code.ensure_loaded(module) do
      {:module, ^module} -> function_exported?(module, function, arity)
      {:error, _reason} -> false
    end
  end

  defp valid_named_connection?(connection) when is_atom(connection) and not is_nil(connection) do
    ecto_repo?(connection) or module_exports?(connection, :query, 2) or
      named_postgrex_connection?(connection)
  end

  defp valid_named_connection?(_connection), do: false

  defp named_postgrex_connection?(connection) do
    with pid when is_pid(pid) <- Process.whereis(connection),
         {:dictionary, dictionary} <- Process.info(pid, :dictionary) do
      Keyword.get(dictionary, :connection_module) == Postgrex.Protocol
    else
      _other -> false
    end
  end

  defp valid_postgrex_connection?(connection) when is_pid(connection),
    do: Process.alive?(connection)

  defp valid_postgrex_connection?(connection), do: valid_named_connection?(connection)

  defp connect_postgrex(opts) do
    with {:ok, _started_apps} <- Application.ensure_all_started(:postgrex),
         {:ok, connection} <- Postgrex.start_link(opts) do
      {:ok, connection}
    end
  rescue
    exception ->
      {:error, {:invalid_connection_options, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp query_postgrex(connection, %ReadOpening{} = opening, [], opts) do
    run_postgrex(
      connection,
      read_opening_sql(opening),
      [],
      opts |> unnamed() |> Keyword.put(:query_type, :text)
    )
  end

  defp query_postgrex(connection, %Control{} = control, [], opts) do
    case control_sql(control) do
      nil ->
        {:error, {:invalid_statement, :control}}

      sql ->
        run_postgrex(
          connection,
          sql,
          [],
          opts |> unnamed() |> Keyword.put(:query_type, :text)
        )
    end
  end

  defp query_postgrex(connection, query, params, opts) do
    statement = query |> normalize_query()
    run_postgrex(connection, statement, params, unnamed(opts))
  end

  defp run_postgrex(connection, statement, params, opts) do
    case Postgrex.query(connection, statement, params, opts) do
      {:ok, result} -> {:ok, normalize_result(result)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    exception -> {:error, {:query_exception, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  @doc false
  # R63: no statement of this adapter is ever named. PREPARE and DEALLOCATE
  # work from any function a statement reaches, in a READ ONLY transaction,
  # and survive ROLLBACK, and Postgrex binds a name without parsing it again,
  # so a named guard, or a named business statement, can be swapped for
  # another under the same name. An unnamed statement is parsed from the text
  # sent with it. Only adapter-owned transaction controls and ReadOpening's
  # fixed session guard take the simple protocol.
  def unnamed(opts), do: Keyword.delete(opts, :cache_statement)

  # A transaction statement of the adapter's own, marked: only a statement
  # built this way takes the simple protocol, which would run anything after a
  # semicolon. Text is never classified by what it starts with, and the mark
  # carries one of these statements or none: control_sql/1 checks it again.
  defp control(sql) when is_binary(sql) do
    true = control_sql?(sql)
    %Control{sql: sql}
  end

  defp control_sql(%Control{sql: sql}) when is_binary(sql),
    do: if(control_sql?(sql), do: sql)

  defp control_sql?(sql),
    do:
      Regex.match?(
        ~r/\A(?:BEGIN|ROLLBACK|(?:SAVEPOINT|RELEASE SAVEPOINT) selecto_[a-z]+_[0-9a-f]{16})\z/,
        sql
      )

  defp introspection_query(connection, query, params) do
    case connection do
      %{query_fun: query_fun} when is_function(query_fun, 3) ->
        query_fun.(query, params, prepared: false)

      _ ->
        execute_statement(connection, query, params, prepared: false)
    end
  end

  defp prepare_function_probe(%{prepare_fun: prepare_fun}, statement, opts)
       when is_function(prepare_fun, 2) do
    prepare_fun.(statement, opts)
  end

  defp prepare_function_probe(connection, statement, opts) do
    with {:ok, target} <- function_probe_target(connection) do
      DBConnection.run(target, fn checked_out_connection ->
        case Postgrex.prepare(checked_out_connection, "", statement, opts) do
          {:ok, query} ->
            probe = %{
              columns: query.columns || [],
              result_oids: query.result_oids || []
            }

            case Postgrex.close(checked_out_connection, query, opts) do
              :ok -> {:ok, probe}
              {:error, reason} -> {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end
      end)
    end
  rescue
    exception -> {:error, {:probe_exception, exception.__struct__}}
  catch
    :exit, _reason -> {:error, :probe_connection_exit}
  end

  defp function_probe_target({:pool, pool_ref}), do: resolve_live_pool_pid(pool_ref)
  defp function_probe_target(%DBConnection{} = connection), do: {:ok, connection}

  defp function_probe_target(connection) when is_pid(connection) do
    if Process.alive?(connection),
      do: {:ok, connection},
      else: {:error, :invalid_probe_connection}
  end

  defp function_probe_target(connection) when is_atom(connection) and not is_nil(connection) do
    cond do
      ecto_repo?(connection) -> ecto_pool_target(connection)
      is_pid(Process.whereis(connection)) -> {:ok, connection}
      true -> {:error, :invalid_probe_connection}
    end
  end

  defp function_probe_target(_connection), do: {:error, :unsupported_probe_connection}

  defp ecto_pool_target(repo) do
    ecto_adapter = :"Elixir.Ecto.Adapter"

    if module_exports?(ecto_adapter, :lookup_meta, 1) do
      case apply(ecto_adapter, :lookup_meta, [repo]) do
        %{pid: pool_pid} when is_pid(pool_pid) -> {:ok, pool_pid}
        _metadata -> {:error, :unsupported_ecto_pool}
      end
    else
      {:error, :unsupported_ecto_pool}
    end
  end

  defp get_columns(connection, table_name, schema) do
    query = """
    SELECT
      column_name,
      data_type,
      udt_name,
      is_nullable,
      column_default,
      character_maximum_length,
      numeric_precision,
      numeric_scale,
      ordinal_position
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             column_name,
                             data_type,
                             udt_name,
                             is_nullable,
                             column_default,
                             max_length,
                             precision,
                             scale,
                             _ordinal_position
                           ] ->
           %{
             column_name: Identifier.to_atom!(column_name),
             data_type: data_type,
             udt_name: udt_name,
             is_nullable: is_nullable,
             column_default: column_default,
             character_maximum_length: max_length,
             numeric_precision: precision,
             numeric_scale: scale
           }
         end)}

      {:error, reason} ->
        {:error, {:columns_query_failed, reason}}
    end
  end

  defp get_primary_key(connection, table_name, schema) do
    query = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid
      AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = ($1 || '.' || $2)::regclass
      AND i.indisprimary
    ORDER BY a.attnum
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[single_key]]}} -> {:ok, Identifier.to_atom!(single_key)}
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [key] -> Identifier.to_atom!(key) end)}
      {:error, reason} -> {:error, {:primary_key_query_failed, reason}}
    end
  end

  defp get_foreign_keys(connection, table_name, schema) do
    query = """
    SELECT
      tc.constraint_name,
      kcu.column_name,
      ccu.table_schema AS foreign_table_schema,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = $1
      AND tc.table_name = $2
    ORDER BY tc.constraint_name, kcu.ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             constraint_name,
                             column_name,
                             foreign_schema,
                             foreign_table,
                             foreign_col
                           ] ->
           %{
             constraint_name: constraint_name,
             column_name: Identifier.to_atom!(column_name),
             foreign_table_schema: foreign_schema,
             foreign_table_name: foreign_table,
             foreign_column_name: Identifier.to_atom!(foreign_col)
           }
         end)}

      {:error, reason} ->
        {:error, {:foreign_keys_query_failed, reason}}
    end
  end

  defp get_reverse_foreign_keys(connection, table_name, schema) do
    query = """
    SELECT
      tc.table_name AS referencing_table,
      kcu.column_name AS referencing_column,
      ccu.column_name AS referenced_column,
      tc.constraint_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND ccu.table_schema = $1
      AND ccu.table_name = $2
    ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
    """

    case introspection_query(connection, query, [schema, table_name]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [
                             referencing_table,
                             referencing_column,
                             referenced_column,
                             constraint_name
                           ] ->
           %{
             referencing_table: referencing_table,
             referencing_column: Identifier.to_atom!(referencing_column),
             referenced_column: Identifier.to_atom!(referenced_column),
             constraint_name: constraint_name
           }
         end)}

      {:error, reason} ->
        {:error, {:reverse_foreign_keys_query_failed, reason}}
    end
  end

  defp build_associations(foreign_keys) do
    Enum.into(foreign_keys, %{}, fn foreign_key ->
      association_name =
        foreign_key.column_name
        |> Atom.to_string()
        |> String.replace_suffix("_id", "")
        |> Identifier.to_atom!()

      related_module_name = table_name_to_module(foreign_key.foreign_table_name)

      {association_name,
       %{
         type: :belongs_to,
         association_type: :belongs_to,
         related_schema: related_module_name,
         related_module_name: related_module_name,
         related_table: foreign_key.foreign_table_name,
         queryable: Identifier.to_atom!(foreign_key.foreign_table_name),
         field: association_name,
         owner_key: foreign_key.column_name,
         related_key: foreign_key.foreign_column_name,
         join_type: :inner,
         is_through: false,
         constraint_name: foreign_key.constraint_name
       }}
    end)
  end

  defp build_expanded_associations(connection, table_name, schema, primary_key) do
    with {:ok, foreign_keys} <- get_foreign_keys(connection, table_name, schema),
         {:ok, reverse_foreign_keys} <- get_reverse_foreign_keys(connection, table_name, schema),
         {:ok, junction_tables} <- detect_junction_tables(connection, schema) do
      belongs_to = build_associations(foreign_keys)

      primary_key_field = normalize_primary_key(primary_key)

      has_many =
        Enum.into(reverse_foreign_keys, %{}, fn reverse_foreign_key ->
          association_name = Identifier.to_atom!(reverse_foreign_key.referencing_table)
          related_module_name = table_name_to_module(reverse_foreign_key.referencing_table)

          {association_name,
           %{
             type: :has_many,
             association_type: :has_many,
             related_schema: related_module_name,
             related_module_name: related_module_name,
             related_table: reverse_foreign_key.referencing_table,
             queryable: Identifier.to_atom!(reverse_foreign_key.referencing_table),
             field: association_name,
             owner_key: primary_key_field,
             related_key: reverse_foreign_key.referencing_column,
             join_type: :left,
             is_through: false,
             constraint_name: reverse_foreign_key.constraint_name
           }}
        end)

      many_to_many =
        junction_tables
        |> Enum.filter(fn junction -> table_name in junction.tables end)
        |> Enum.flat_map(fn junction ->
          {this_foreign_keys, other_foreign_keys} =
            Enum.split_with(junction.foreign_keys, fn foreign_key ->
              foreign_key.foreign_table_name == table_name
            end)

          Enum.map(other_foreign_keys, fn other_foreign_key ->
            association_name = Identifier.to_atom!(other_foreign_key.foreign_table_name)
            related_module_name = table_name_to_module(other_foreign_key.foreign_table_name)

            owner_foreign_key =
              case this_foreign_keys do
                [foreign_key | _] -> foreign_key.column_name
                _ -> primary_key_field
              end

            {association_name,
             %{
               type: :many_to_many,
               association_type: :many_to_many,
               related_schema: related_module_name,
               related_module_name: related_module_name,
               related_table: other_foreign_key.foreign_table_name,
               queryable: Identifier.to_atom!(other_foreign_key.foreign_table_name),
               field: association_name,
               owner_key: primary_key_field,
               related_key: other_foreign_key.foreign_column_name,
               join_type: :left,
               is_through: false,
               join_through: junction.table,
               join_keys: [
                 {owner_foreign_key, primary_key_field},
                 {other_foreign_key.column_name, other_foreign_key.foreign_column_name}
               ]
             }}
          end)
        end)
        |> Enum.into(%{})

      {:ok, belongs_to |> Map.merge(has_many) |> Map.merge(many_to_many)}
    end
  end

  defp detect_junction_tables(connection, schema) do
    with {:ok, tables} <- list_tables(connection, schema: schema) do
      Enum.reduce_while(tables, {:ok, []}, fn table, {:ok, junction_tables} ->
        case analyze_junction_table(connection, table, schema) do
          {:ok, junction_table} -> {:cont, {:ok, [junction_table | junction_tables]}}
          {:error, :not_junction_table} -> {:cont, {:ok, junction_tables}}
          {:error, reason} -> {:halt, {:error, {:junction_introspection_failed, table, reason}}}
        end
      end)
      |> case do
        {:ok, junction_tables} -> {:ok, Enum.reverse(junction_tables)}
        {:error, _reason} = error -> error
      end
    end
  end

  defp analyze_junction_table(connection, table, schema) do
    with {:ok, columns} <- get_columns(connection, table, schema),
         {:ok, foreign_keys} <- get_foreign_keys(connection, table, schema),
         {:ok, primary_key} <- get_primary_key(connection, table, schema),
         true <- junction_table?(columns, foreign_keys) do
      primary_key_fields = normalize_primary_keys(primary_key)
      foreign_key_fields = Enum.map(foreign_keys, & &1.column_name)
      all_fields = Enum.map(columns, & &1.column_name)

      {:ok,
       %{
         table: table,
         foreign_keys: foreign_keys,
         primary_key: primary_key,
         extra_columns: all_fields -- Enum.uniq(primary_key_fields ++ foreign_key_fields),
         tables: Enum.map(foreign_keys, & &1.foreign_table_name)
       }}
    else
      false -> {:error, :not_junction_table}
      {:error, reason} -> {:error, reason}
    end
  end

  defp junction_table?(columns, foreign_keys) do
    foreign_key_fields = MapSet.new(Enum.map(foreign_keys, & &1.column_name))

    data_fields =
      columns
      |> Enum.map(& &1.column_name)
      |> Enum.reject(fn field ->
        field_name = Atom.to_string(field)

        field_name in ["id", "inserted_at", "updated_at", "created_at"] or
          String.ends_with?(field_name, "_at")
      end)

    length(foreign_keys) == 2 and Enum.all?(data_fields, &MapSet.member?(foreign_key_fields, &1))
  end

  defp normalize_primary_key([primary_key | _]), do: primary_key
  defp normalize_primary_key(primary_key) when is_atom(primary_key), do: primary_key
  defp normalize_primary_key(_), do: :id

  defp normalize_primary_keys(primary_key) when is_list(primary_key), do: primary_key
  defp normalize_primary_keys(primary_key) when is_atom(primary_key), do: [primary_key]
  defp normalize_primary_keys(_), do: []

  defp map_pg_type(connection, data_type, udt_name) do
    case {data_type, udt_name} do
      {"smallint", _} ->
        :integer

      {"integer", _} ->
        :integer

      {"bigint", _} ->
        :integer

      {"smallserial", _} ->
        :integer

      {"serial", _} ->
        :integer

      {"bigserial", _} ->
        :integer

      {"numeric", _} ->
        :decimal

      {"decimal", _} ->
        :decimal

      {"real", _} ->
        :float

      {"double precision", _} ->
        :float

      {"character varying", _} ->
        :string

      {"character", _} ->
        :string

      {"text", _} ->
        :string

      {"citext", _} ->
        :string

      {"boolean", _} ->
        :boolean

      {"date", _} ->
        :date

      {"time without time zone", _} ->
        :time

      {"time with time zone", _} ->
        :time

      {"timestamp without time zone", _} ->
        :naive_datetime

      {"timestamp with time zone", _} ->
        :utc_datetime

      {"json", _} ->
        :jsonb

      {"jsonb", _} ->
        :jsonb

      {"uuid", _} ->
        :binary_id

      {"ARRAY", udt} ->
        {:array, map_pg_type(connection, base_data_type_for_array(udt), normalize_array_udt(udt))}

      {"USER-DEFINED", udt} ->
        map_user_defined_type(connection, udt)

      _ ->
        map_udt_fallback(connection, data_type, udt_name)
    end
  end

  defp map_udt_fallback(connection, _data_type, udt_name) when is_binary(udt_name) do
    case map_user_defined_type(connection, udt_name) do
      :string -> :string
    end
  end

  defp map_udt_fallback(_connection, _data_type, _udt_name), do: :string

  defp map_user_defined_type(connection, udt_name) do
    case get_enum_values(connection, udt_name) do
      {:ok, [_ | _]} -> :string
      _ -> :string
    end
  end

  defp get_enum_values(connection, enum_type_name) do
    query = """
    SELECT e.enumlabel
    FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    WHERE t.typname = $1
    ORDER BY e.enumsortorder
    """

    case introspection_query(connection, query, [enum_type_name]) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [label] -> label end)}
      {:error, reason} -> {:error, {:enum_values_query_failed, reason}}
    end
  end

  defp base_data_type_for_array(<<base::binary>>) do
    case normalize_array_udt(base) do
      "int2" -> "smallint"
      "int4" -> "integer"
      "int8" -> "bigint"
      "varchar" -> "character varying"
      "text" -> "text"
      "bool" -> "boolean"
      "uuid" -> "uuid"
      "jsonb" -> "jsonb"
      "json" -> "json"
      "numeric" -> "numeric"
      "date" -> "date"
      "timestamp" -> "timestamp without time zone"
      "timestamptz" -> "timestamp with time zone"
      _ -> "USER-DEFINED"
    end
  end

  defp normalize_array_udt("_" <> base), do: base
  defp normalize_array_udt(base), do: base

  defp table_name_to_module(table_name) when is_binary(table_name) do
    table_name
    |> singularize()
    |> Macro.camelize()
  end

  defp singularize(word) do
    cond do
      String.ends_with?(word, "ies") ->
        String.replace_suffix(word, "ies", "y")

      String.ends_with?(word, "sses") ->
        String.replace_suffix(word, "sses", "ss")

      String.ends_with?(word, "ses") ->
        String.replace_suffix(word, "ses", "s")

      String.ends_with?(word, "s") and not String.ends_with?(word, "ss") ->
        String.replace_suffix(word, "s", "")

      true ->
        word
    end
  end

  defp validate_pool_connection({:pool, pool_ref}) do
    try do
      case Selecto.ConnectionPool.pool_stats(pool_ref) do
        %{error: _} -> {:error, "Connection pool is not available"}
        stats when is_map(stats) -> :ok
      end
    catch
      :exit, _ -> {:error, "Connection pool is not available"}
    end
  end

  defp pool_stats({:pool, pool_ref}) do
    try do
      Selecto.ConnectionPool.pool_stats(pool_ref)
    catch
      :exit, _ -> %{error: "Pool manager not available"}
    end
  end

  defp execute_with_pool_pid(pool_pid, query, params, cache_key, opts) do
    query_opts = [timeout: Keyword.get(opts, :timeout, 15_000)] ++ Keyword.take(opts, [:deadline])

    try do
      if cache_key do
        execute_with_prepared_cache(pool_pid, query, params, cache_key, query_opts)
      else
        Postgrex.query(pool_pid, query, params, unnamed(query_opts))
      end
    rescue
      e ->
        {:error, safe_exception(e)}
    catch
      :exit, reason ->
        {:error,
         Selecto.Error.connection_error("PostgreSQL connection pool exited", %{
           exit_reason: reason
         })}
    end
  end

  defp execute_with_prepared_cache(pool_pid, query, params, cache_key, query_opts) do
    case Selecto.ConnectionPool.prepared_statement_cached?(pool_pid, cache_key) do
      false ->
        result = Postgrex.query(pool_pid, query, params, unnamed(query_opts))

        if match?({:ok, _}, result) do
          Selecto.ConnectionPool.mark_prepared_statement(pool_pid, cache_key)
        end

        result

      true ->
        Postgrex.query(pool_pid, query, params, unnamed(query_opts))
    end
  end

  defp start_postgrex_connection(postgrex_opts) do
    case Postgrex.start_link(postgrex_opts) do
      {:ok, pool_pid} -> {:ok, pool_pid, true}
      {:error, {:already_started, pool_pid}} -> {:ok, pool_pid, false}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_result(%{rows: rows, columns: columns} = result) do
    rows =
      case rows do
        missing when missing in [nil, false] -> []
        rows when is_list(rows) -> rows
      end

    %{
      rows: rows,
      columns: normalize_columns(columns),
      num_rows: normalize_row_count(Map.get(result, :num_rows, length(rows)))
    }
  end

  @spec normalize_columns(list() | nil | false) :: [String.t()]
  defp normalize_columns(columns), do: Enum.map(columns || [], &to_string/1)

  defp normalize_row_count(count) when is_integer(count) and count >= 0, do: count

  defp normalize_row_count(_count),
    do: raise(ArgumentError, "PostgreSQL result row count must be a nonnegative integer")

  defp normalize_error_category(code) when code in [:unique_violation, "23505"],
    do: :unique_violation

  defp normalize_error_category(code) when code in [:foreign_key_violation, "23503"],
    do: :foreign_key_violation

  defp normalize_error_category(code) when code in [:not_null_violation, "23502"],
    do: :not_null_violation

  defp normalize_error_category(code) when code in [:check_violation, "23514"],
    do: :check_violation

  defp normalize_error_category(_code), do: :database_error

  defp enforce_cardinality(%Command{expected_cardinality: expected}, result) do
    affected_rows = Map.get(result, :num_rows, length(Map.get(result, :rows, [])))

    if cardinality_matches?(affected_rows, expected) do
      {:ok, affected_rows}
    else
      {:error,
       Error.new(:cardinality_mismatch, "write affected an unexpected number of rows",
         details: %{expected: expected, actual: affected_rows}
       )}
    end
  end

  defp cardinality_matches?(count, {:exactly, expected}), do: count == expected
  defp cardinality_matches?(count, {:at_most, expected}), do: count <= expected
  defp cardinality_matches?(count, {:at_least, expected}), do: count >= expected

  defp cardinality_matches?(count, {:between, minimum, maximum}),
    do: count >= minimum and count <= maximum

  defp cardinality_matches?(_count, :many), do: true

  defp result_rows(%{rows: rows, columns: columns}) do
    Enum.map(rows, fn row -> Enum.zip(columns, row) |> Map.new() end)
  end

  defp with_postgres_transaction({:pool, pool_ref}, opts, fun) do
    case Selecto.ConnectionPool.get_pool_pid(pool_ref) do
      {:ok, pool_pid} -> with_postgres_transaction(pool_pid, opts, fun)
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  end

  defp with_postgres_transaction(connection, opts, fun)
       when is_atom(connection) and not is_nil(connection) do
    if ecto_repo?(connection) do
      with_ecto_transaction(connection, opts, fun)
    else
      with_native_postgres_transaction(connection, opts, fun)
    end
  end

  defp with_postgres_transaction(connection, opts, fun)
       when is_pid(connection) do
    with_native_postgres_transaction(connection, opts, fun)
  end

  # A checked-out connection inside the caller's transaction: the write runs
  # under its own savepoint, so a refusal after its DML (a post-image
  # violation) undoes that DML even when the caller ignores the error and
  # commits (w3exrev16 item 18). An idle checked-out connection gets a
  # managed transaction.
  defp with_postgres_transaction(%DBConnection{} = connection, opts, fun) do
    if DBConnection.status(connection) != :idle do
      with_savepoint(&Postgrex.query(connection, &1, []), fn -> fun.(connection) end)
    else
      with_native_postgres_transaction(connection, opts, fun)
    end
  end

  defp with_postgres_transaction(connection, _opts, _fun) do
    {:error, write_error(:transaction_failed, {:invalid_connection, connection})}
  end

  # R23: a refused statement that cannot be rolled back. PostgreSQL has
  # aborted the transaction holding it, or it is out of reach; the session
  # cannot be trusted either way, and the refusal says so.
  defp broken_connection do
    Error.new(
      :query_enforcement,
      "PostgreSQL could not undo a refused statement: the connection is broken",
      details: %{
        code: :query_rule_not_evaluable,
        reason: :connection_broken,
        adapter: :postgresql
      }
    )
  end

  # The first error stands (R66): a deadlock keeps its SQLSTATE. It says the
  # connection is broken too, and a refusal that had no error of its own is
  # that alone.
  defp broken({:error, %Error{details: details} = error}) do
    {:error, %{error | details: Map.put(details || %{}, :connection_broken, true)}}
  end

  defp broken(_error), do: {:error, broken_connection()}

  defp unowned_transaction do
    Error.new(
      :query_enforcement,
      "PostgreSQL write refused: this connection is in a transaction Selecto did not open and " <>
        "the caller does not hold; use DBConnection.run/3 and pass its connection",
      details: %{
        code: :query_rule_not_evaluable,
        reason: :unowned_transaction,
        adapter: :postgresql
      }
    )
  end

  @doc false
  # R70: a write reported failed must not stay in the host's transaction. A
  # failed RELEASE goes back to the savepoint like any other failed write; if
  # that fails too, the transaction is marked broken.
  def with_savepoint(query, fun) do
    name = "selecto_write_#{Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)}"

    case query.("SAVEPOINT #{name}") do
      {:ok, _} ->
        result =
          try do
            fun.()
          rescue
            exception ->
              {:error, write_error(:transaction_failed, exception)}
          end

        case result do
          {:ok, _} = ok ->
            case query.("RELEASE SAVEPOINT #{name}") do
              {:ok, _} ->
                ok

              {:error, reason} ->
                undo_savepoint(query, name, {:error, write_error(:transaction_failed, reason)})
            end

          error ->
            undo_savepoint(query, name, error)
        end

      {:error, reason} ->
        {:error, write_error(:transaction_failed, reason)}
    end
  end

  defp undo_savepoint(query, name, error, attempts \\ 20) do
    with {:ok, _} <- query.("ROLLBACK TO SAVEPOINT #{name}"),
         {:ok, _} <- query.("RELEASE SAVEPOINT #{name}") do
      error
    else
      {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} when attempts > 1 ->
        undo_savepoint(query, name, error, attempts - 1)

      {:error, %Postgrex.Error{postgres: %{pg_code: "3B001"}}} ->
        error

      _failed ->
        broken(error)
    end
  end

  defp with_native_postgres_transaction(%DBConnection{} = connection, opts, fun) do
    case postgres_transaction(connection, opts, fun) do
      {:ok, results} -> {:ok, results}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  end

  # Checked out first, so the status decides. A pool hands out any of its
  # connections, so a transaction found here is not provably the caller's:
  # joining could lose an acknowledged write, and a managed transaction would
  # disconnect it. It refuses; a host in its own transaction passes the
  # connection from DBConnection.run/3, which joins under a savepoint.
  defp with_native_postgres_transaction(connection, opts, fun) do
    if valid_postgrex_connection?(connection) do
      DBConnection.run(
        connection,
        fn checked_out ->
          if DBConnection.status(checked_out) == :idle,
            do: with_postgres_transaction(checked_out, opts, fun),
            else: {:error, unowned_transaction()}
        end,
        Keyword.take(opts, [:timeout, :deadline, :queue_target, :queue_interval])
      )
    else
      {:error, write_error(:transaction_failed, {:invalid_connection, connection})}
    end
  catch
    :exit, reason -> {:error, write_error(:transaction_failed, {:connection_exit, reason})}
  end

  defp with_ecto_transaction(repo, opts, fun) do
    Selecto.Telemetry.span_for(
      [:selecto_db_postgresql, :telemetry, :transaction],
      %{adapter: :postgresql},
      fn -> do_with_ecto_transaction(repo, opts, fun) end
    )
  end

  defp do_with_ecto_transaction(repo, opts, fun) do
    transaction_opts = Keyword.take(opts, [:timeout, :log])
    # Only a transaction this call opens can be set READ COMMITTED; inside
    # the host's own transaction the guarded-write check decides.
    # One checkout around the decision and the work: under a shared Sandbox
    # another process's statements would otherwise land inside the savepoint.
    transaction_result =
      if module_exports?(repo, :checkout, 1) do
        apply(repo, :checkout, [
          fn ->
            if ecto_in_transaction?(repo),
              do: with_savepoint(&safe_ecto_query(repo, &1), fn -> fun.(repo) end),
              else: managed_ecto_transaction(repo, transaction_opts, fun)
          end,
          Keyword.take(opts, [:timeout])
        ])
      else
        if module_exports?(repo, :in_transaction?, 0) and apply(repo, :in_transaction?, []),
          do: with_savepoint(&safe_ecto_query(repo, &1), fn -> fun.(repo) end),
          else: managed_ecto_transaction(repo, transaction_opts, fun)
      end

    case transaction_result do
      {:ok, results} -> {:ok, results}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, write_error(:transaction_failed, reason)}
    end
  rescue
    exception ->
      {:error, write_error(:transaction_failed, exception)}
  end

  defp managed_ecto_transaction(repo, transaction_opts, fun) do
    transaction_result =
      apply(repo, :transaction, [
        fn ->
          with {:error, reason} <- safe_ecto_query(repo, @read_committed) do
            apply(repo, :rollback, [write_error(:transaction_failed, reason)])
          end

          ecto_transaction_result(repo, fun)
        end,
        transaction_opts
      ])

    transaction_result
  rescue
    exception ->
      {:error,
       write_error(
         :transaction_failed,
         {:transaction_exception, exception.__struct__, Exception.message(exception)}
       )}
  catch
    :exit, reason -> {:error, write_error(:transaction_failed, {:connection_exit, reason})}
  end

  defp safe_ecto_query(repo, sql) do
    apply(ecto_sql_module(), :query, [repo, sql, [], [sandbox_subtransaction: false]])
  rescue
    exception -> {:error, exception}
  end

  defp ecto_transaction_result(repo, fun) do
    case fun.(repo) do
      {:ok, results} -> results
      {:error, error} -> apply(repo, :rollback, [error])
    end
  end

  defp write_error(type, %Postgrex.Error{} = reason) do
    write_error(type, reason, nil)
  end

  defp write_error(type, reason) do
    Error.adapter_failure(type, :postgresql, reason, "PostgreSQL write failed")
  end

  defp write_error(type, reason, _command) when not is_struct(reason, Postgrex.Error),
    do: write_error(type, reason)

  defp write_error(type, %Postgrex.Error{} = reason, command) do
    native = Map.get(reason, :postgres) || %{}
    category = normalize_error_category(Map.get(native, :code) || Map.get(native, :pg_code))

    if category in [
         :unique_violation,
         :foreign_key_violation,
         :not_null_violation,
         :check_violation
       ] do
      details = %{
        adapter: :postgresql,
        write_stage: type,
        category: category,
        constraint: Map.get(native, :constraint),
        column: Map.get(native, :column),
        recoverable?: true
      }

      details =
        case matching_native_constraint(command, category, details.constraint) do
          nil -> details
          native -> Map.put(details, :binding_id, native.binding_id)
        end

      Error.new(:native_constraint_violation, "PostgreSQL constraint rejected write",
        details: details
      )
    else
      failure = Error.adapter_failure(type, :postgresql, reason, "PostgreSQL write failed")

      case Map.get(native, :pg_code) do
        code when is_binary(code) ->
          %{failure | details: Map.put(failure.details, :sqlstate, code)}

        _none ->
          failure
      end
    end
  end

  defp matching_native_constraint(%Command{native_constraints: constraints}, category, constraint)
       when is_list(constraints) and is_binary(constraint) do
    Enum.find(constraints, fn native ->
      native.adapter == "postgresql" and native.constraint == constraint and
        native.category == category
    end)
  end

  defp matching_native_constraint(_command, _category, _constraint), do: nil

  defp postgres_transaction(connection, opts, fun) do
    Selecto.Telemetry.span_for(
      [:selecto_db_postgresql, :telemetry, :transaction],
      %{adapter: :postgresql},
      fn -> do_postgres_transaction(connection, opts, fun) end
    )
  end

  defp do_postgres_transaction(connection, opts, fun) do
    Postgrex.transaction(
      connection,
      fn transaction_connection ->
        # A managed write transaction runs READ COMMITTED whatever the
        # connection's default_transaction_isolation: the guarded-write
        # catalog checks need a fresh snapshot per statement.
        Postgrex.query!(transaction_connection, @read_committed, [])

        case fun.(transaction_connection) do
          {:ok, results} -> results
          {:error, error} -> Postgrex.rollback(transaction_connection, error)
        end
      end,
      opts
    )
  rescue
    exception ->
      {:error, {:transaction_exception, exception.__struct__, Exception.message(exception)}}
  catch
    :exit, reason -> {:error, {:connection_exit, reason}}
  end

  defp resolve_stream_pool_connection(pool_ref) when is_pid(pool_ref) do
    if Process.alive?(pool_ref),
      do: {:ok, pool_ref},
      else: {:error, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}
  end

  defp resolve_stream_pool_connection(pool_ref) when is_atom(pool_ref) and not is_nil(pool_ref) do
    if valid_named_connection?(pool_ref),
      do: {:ok, pool_ref},
      else: {:error, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}
  end

  defp resolve_stream_pool_connection(%{pool: pool_conn})
       when is_pid(pool_conn) or is_atom(pool_conn) do
    resolve_stream_pool_connection(pool_conn)
  end

  defp resolve_stream_pool_connection(pool_ref) do
    {:error, %{stream_context: :pool, pool_ref: inspect(pool_ref)}}
  end

  # A stream runs in a transaction of its own, on its producer's process. On
  # a connection already in the host's transaction that would disconnect it
  # and the host's next statement would autocommit: it refuses.
  defp own_stream_transaction(conn, timeout, fun) do
    DBConnection.run(
      conn,
      fn checked_out ->
        if DBConnection.status(checked_out) == :idle,
          do: Postgrex.transaction(checked_out, fun, timeout: timeout),
          else: {:error, stream_in_transaction()}
      end,
      timeout: timeout
    )
  end

  defp stream_in_transaction do
    %Selecto.Error{
      type: :query_enforcement,
      message: "PostgreSQL cannot stream inside the host's transaction on this connection",
      details: %{
        code: :query_rule_not_evaluable,
        reason: :stream_in_transaction,
        adapter: :postgresql
      }
    }
  end

  # A stream opens lazily, on the process that enumerates it. A refusal does
  # not wait for that: a connection in the host's transaction, or a session
  # that is not UTF8, is refused at the call.
  defp stream_refusal(connection) do
    with_checkout(connection, fn guard, in_transaction? ->
      if in_transaction? do
        {:error, stream_in_transaction()}
      else
        case guard.("SELECT " <> @encoding_column, []) do
          {:ok, %{rows: [[encoding]]}} -> utf8(encoding)
          {:error, _} = error -> error
          _unreadable -> {:error, read_refusal(session_unreadable())}
        end
      end
    end)
  end

  defp build_postgrex_cursor_stream(conn, query, params, opts) do
    query = normalize_query(query)
    max_rows = Keyword.get(opts, :max_rows, 500)
    stream_timeout = Keyword.get(opts, :stream_timeout, 30_000)
    receive_timeout = Keyword.get(opts, :receive_timeout, 60_000)
    queue_timeout = Keyword.get(opts, :queue_timeout, 100)

    producer =
      Keyword.get(opts, :stream_producer, fn send_chunk ->
        # A Selecto read streams in a transaction of its own, opened as a read
        # is (open_read/3): READ ONLY unless it locks rows, client_encoding
        # checked. The transaction ends with the stream.
        read? = Keyword.get(opts, :selecto_read_stream) == true

        read_only? =
          Keyword.get(opts, :selecto_read_kind) != :generated or read_only?(query)

        own_stream_transaction(conn, stream_timeout, fn tx_conn ->
          if read? do
            case open_read(&guard_statement(tx_conn, &1, &2), read_only?, nil) do
              :ok -> :ok
              {:error, reason} -> Postgrex.rollback(tx_conn, reason)
            end
          end

          try do
            tx_conn
            |> Postgrex.stream(query, params, max_rows: max_rows)
            |> Enum.each(fn %Postgrex.Result{rows: rows, columns: columns} ->
              send_chunk.(rows, columns)
            end)
          rescue
            error in Postgrex.Error ->
              if match?(%{postgres: %{code: :read_only_sql_transaction}}, error),
                do: Postgrex.rollback(tx_conn, read_refusal(read_wrote())),
                else: reraise(error, __STACKTRACE__)
          end
        end)
      end)

    Stream.resource(
      fn ->
        parent = self()
        ref = make_ref()

        task =
          Selecto.TaskSupervisor.async(fn ->
            run_stream_producer(parent, ref, producer)
          end)

        %{task: task, ref: ref, monitor_ref: task.ref, awaiting_ack?: false}
      end,
      fn state ->
        state = acknowledge_stream_chunk(state)
        ref = state.ref
        monitor_ref = state.monitor_ref

        receive do
          {^ref, {:chunk, rows, columns}} ->
            stream_rows = Enum.map(rows, &{&1, columns || []})
            {stream_rows, %{state | awaiting_ack?: true}}

          {^ref, {:done, {:ok, _}}} ->
            {:halt, state}

          {^ref, {:done, {:error, %Selecto.Error{} = refusal}}} ->
            raise Selecto.Error.Refused, refusal

          # A failure once the stream is returned raises typed: the error
          # the non-stream path returns for it (Selecto.Error.stream_failure/2).
          {^ref, {:done, {:error, reason}}} ->
            Selecto.Error.raise_stream_failure!(__MODULE__, reason)

          {^ref, {:producer_failed, reason}} ->
            Selecto.Error.raise_stream_failure!(__MODULE__, reason)

          {^monitor_ref, _reply} ->
            {:halt, state}

          {:DOWN, ^monitor_ref, :process, _pid, :normal} ->
            {:halt, state}

          {:DOWN, ^monitor_ref, :process, _pid, reason} ->
            Selecto.Error.raise_stream_failure!(__MODULE__, {:connection_exit, reason})
        after
          receive_timeout ->
            raise Selecto.Error.Refused,
                  Selecto.Error.timeout_error(
                    "Timed out waiting for streamed rows after #{receive_timeout}ms",
                    %{adapter: :postgresql}
                  )
        end
      end,
      fn state ->
        stop_stream_producer(state, queue_timeout)
      end
    )
  end

  defp run_stream_producer(parent, ref, producer) do
    parent_monitor_ref = Process.monitor(parent)

    tx_result =
      try do
        producer.(fn rows, columns ->
          send(parent, {ref, {:chunk, rows, columns}})
          await_stream_ack(parent, parent_monitor_ref, ref)
        end)
      rescue
        # The raw failure, as the non-stream path returns it: the server's
        # error itself, any other exception by its module (normalize_error/1).
        error in Postgrex.Error ->
          {:producer_failed, error}

        exception ->
          {:producer_failed,
           {:query_exception, exception.__struct__, Exception.message(exception)}}
      catch
        :throw, {:selecto_stream_cancelled, ^ref} ->
          {:consumer_cancelled, ref}

        :exit, reason ->
          {:producer_failed, {:connection_exit, reason}}

        kind, reason ->
          {:producer_failed, {kind, reason}}
      after
        Process.demonitor(parent_monitor_ref, [:flush])
      end

    case tx_result do
      {:consumer_cancelled, ^ref} -> :ok
      {:producer_failed, reason} -> send(parent, {ref, {:producer_failed, reason}})
      result -> send(parent, {ref, {:done, result}})
    end
  end

  defp await_stream_ack(parent, parent_monitor_ref, ref) do
    receive do
      {^ref, :ack} ->
        :ok

      {^ref, :cancel} ->
        throw({:selecto_stream_cancelled, ref})

      {:DOWN, ^parent_monitor_ref, :process, ^parent, _reason} ->
        throw({:selecto_stream_cancelled, ref})
    end
  end

  defp acknowledge_stream_chunk(%{awaiting_ack?: false} = state), do: state

  defp acknowledge_stream_chunk(%{task: task, ref: ref, awaiting_ack?: true} = state) do
    send(task.pid, {ref, :ack})
    %{state | awaiting_ack?: false}
  end

  defp stop_stream_producer(state, queue_timeout) do
    send(state.task.pid, {state.ref, :cancel})
    # Give the cancellation handshake time to unwind the cursor transaction.
    # Task.shutdown/2 sends an exit immediately and can discard the DB session
    # before Postgrex has rolled back and returned it to the pool.
    _result = Task.yield(state.task, queue_timeout) || Task.shutdown(state.task, :brutal_kill)
    Process.demonitor(state.monitor_ref, [:flush])
    drain_stream_messages(state.ref)
  end

  defp drain_stream_messages(ref) do
    receive do
      {^ref, _message} -> drain_stream_messages(ref)
    after
      0 -> :ok
    end
  end

  defp fetch_server_version_major({:pool, pool_ref}) do
    case resolve_live_pool_pid(pool_ref) do
      {:ok, pool_pid} -> postgrex_server_version_major(pool_pid)
      {:error, _reason} -> query_pool_server_version_major(pool_ref)
    end
  end

  defp fetch_server_version_major(connection) when is_atom(connection) do
    cond do
      function_exported?(connection, :query, 2) ->
        case apply(connection, :query, [@server_version_num_query, []]) do
          {:ok, result} -> extract_server_version_major(result)
          {:error, _reason} = error -> error
          _other -> {:error, :invalid_query_result}
        end

      is_pid(Process.whereis(connection)) ->
        postgrex_server_version_major(connection)

      true ->
        {:error, :unsupported_connection}
    end
  end

  defp fetch_server_version_major(connection) when is_pid(connection) do
    postgrex_server_version_major(connection)
  end

  defp fetch_server_version_major(connection) when is_list(connection) do
    case Postgrex.start_link(Keyword.put_new(connection, :supervisor, false)) do
      {:ok, pid} ->
        result = postgrex_server_version_major(pid)
        GenServer.stop(pid)
        result

      {:error, _reason} = error ->
        error
    end
  end

  defp fetch_server_version_major(%DBConnection{} = connection) do
    postgrex_server_version_major(connection)
  end

  defp fetch_server_version_major(connection) when is_map(connection) do
    connection
    |> Map.to_list()
    |> fetch_server_version_major()
  end

  defp fetch_server_version_major(_connection), do: {:error, :unsupported_connection}

  defp query_pool_server_version_major(pool_ref) do
    try do
      case Selecto.ConnectionPool.execute(pool_ref, @server_version_num_query, [],
             prepared: false
           ) do
        {:ok, result} -> extract_server_version_major(result)
        {:error, _reason} = error -> error
      end
    catch
      :exit, _reason -> {:error, :pool_unavailable}
    end
  end

  # A connection that cannot be checked out fails here as the query would, so
  # it is not retried; only a missing or unrecognised `server_version` falls
  # back to asking the server.
  defp postgrex_server_version_major(connection) do
    case cached_server_version_major(connection) do
      {:ok, major} -> {:ok, major}
      :unknown -> query_postgrex_server_version_major(connection)
      {:error, _reason} = error -> error
    end
  end

  defp cached_server_version_major(connection) do
    connection
    |> Postgrex.parameters()
    |> Map.get("server_version")
    |> parse_server_version_major()
  rescue
    exception in Postgrex.Error ->
      if exception.message == "parameters not available", do: :unknown, else: {:error, exception}

    exception ->
      {:error, exception}
  catch
    :exit, _reason -> {:error, :query_failed}
  end

  # `server_version` is `<major>.<minor>` from PostgreSQL 10 (`<major>devel`,
  # `<major>beta1` and the like before release, often followed by a
  # distribution suffix) and `9.<minor>.<patch>` before it, so its leading
  # integer is `server_version_num` divided by 10,000.
  @doc false
  def parse_server_version_major(version) when is_binary(version) do
    case Integer.parse(version) do
      {major, _rest} when major > 0 -> {:ok, major}
      _ -> :unknown
    end
  end

  def parse_server_version_major(_version), do: :unknown

  defp query_postgrex_server_version_major(connection) do
    case Postgrex.query(connection, @server_version_num_query, []) do
      {:ok, result} -> extract_server_version_major(result)
      {:error, _reason} = error -> error
    end
  rescue
    _ -> {:error, :query_failed}
  end

  defp extract_server_version_major(result) do
    case extract_server_version_num(result) do
      {:ok, version_num} -> {:ok, div(version_num, 10_000)}
      {:error, _reason} = error -> error
    end
  end

  defp extract_server_version_num(%{rows: [[value | _] | _]}) do
    parse_server_version_num(value)
  end

  defp extract_server_version_num(_result), do: {:error, :missing_server_version_num}

  defp parse_server_version_num(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp parse_server_version_num(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> {:ok, parsed}
      _ -> {:error, :invalid_server_version_num}
    end
  end

  defp parse_server_version_num(_value), do: {:error, :invalid_server_version_num}
end
