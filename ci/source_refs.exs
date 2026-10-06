defmodule SelectoDBPostgreSQL.CI.SourceRefs do
  @moduledoc false
  @core_urls ["https://github.com/seeken/selecto.git", "git@github.com:seeken/selecto.git"]

  def core!(root) do
    {:%{}, _, entries} =
      root |> Path.join("mix.lock") |> File.read!() |> Code.string_to_quoted!()

    entry = List.keyfind(entries, :selecto, 0) || List.keyfind(entries, "selecto", 0)
    {:{}, _, [:git, url, locked, opts]} = elem(entry, 1)
    true = url in @core_urls and valid_ref?(locked) and opts[:ref] == locked

    {_, refs} =
      root
      |> Path.join("mix.exs")
      |> File.read!()
      |> Code.string_to_quoted!()
      |> Macro.prewalk([], fn
        {:@, _, [{:selecto_ref, _, [ref]}]} = node, refs -> {node, [ref | refs]}
        node, refs -> {node, refs}
      end)

    true = refs == [locked]
    locked
  rescue
    _ -> raise "CI requires matching immutable Core declaration and lock"
  end

  def valid_ref?(ref), do: is_binary(ref) and Regex.match?(~r/\A[0-9a-f]{40}\z/, ref)

  def git_metadata!(directory, repository, expected \\ nil) do
    {head, 0} = System.cmd("git", ["-C", directory, "rev-parse", "HEAD"], stderr_to_stdout: true)

    {status, 0} =
      System.cmd("git", ["-C", directory, "status", "--porcelain"], stderr_to_stdout: true)

    head = String.trim(head)

    true =
      valid_ref?(head) and (expected == nil or head == expected) and String.trim(status) == ""

    %{repository: repository, commit: head, dirty: false}
  rescue
    _ -> raise "CI source provenance failed"
  end

  def source!(module, repository, expected, directory) do
    source = module.module_info(:compile) |> Keyword.fetch!(:source) |> to_string()

    {top, 0} =
      System.cmd("git", ["-C", Path.dirname(source), "rev-parse", "--show-toplevel"],
        stderr_to_stdout: true
      )

    true = String.trim(top) == Path.expand(directory)

    true =
      String.starts_with?(Path.expand(source), Path.join(Path.expand(directory), "lib") <> "/")

    git_metadata!(directory, repository, expected)
  rescue
    _ -> raise "CI loaded source provenance failed"
  end
end
