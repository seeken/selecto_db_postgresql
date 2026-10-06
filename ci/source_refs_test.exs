ExUnit.start()
Code.require_file("source_refs.exs", __DIR__)

defmodule SelectoDBPostgreSQL.CI.SourceRefsTest do
  use ExUnit.Case, async: false
  alias SelectoDBPostgreSQL.CI.SourceRefs
  @ref String.duplicate("a", 40)

  setup do
    root = Path.join(System.tmp_dir!(), "selecto-ci-ref-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, root: root}
  end

  defp project(
         root,
         declared \\ @ref,
         locked \\ @ref,
         url \\ "https://github.com/seeken/selecto.git"
       ) do
    File.write!(
      Path.join(root, "mix.exs"),
      "defmodule Project do\n@selecto_ref #{inspect(declared)}\nend"
    )

    File.write!(
      Path.join(root, "mix.lock"),
      inspect(%{selecto: {:git, url, locked, [ref: locked]}})
    )
  end

  test "only matching immutable declaration and lock are accepted", %{root: root} do
    project(root)
    assert SourceRefs.core!(root) == @ref
    project(root, String.duplicate("b", 40))
    assert_raise RuntimeError, ~r/matching immutable/, fn -> SourceRefs.core!(root) end
  end

  test "mutable refs, a different repository and duplicate declarations fail", %{root: root} do
    for invalid <- ["main", "v0.5.0", String.duplicate("a", 39)] do
      project(root, invalid, invalid)
      assert_raise RuntimeError, fn -> SourceRefs.core!(root) end
    end

    project(root, @ref, @ref, "https://github.com/other/selecto.git")
    assert_raise RuntimeError, fn -> SourceRefs.core!(root) end
    project(root)
    File.write!(Path.join(root, "mix.exs"), "@selecto_ref #{inspect(@ref)}\n", [:append])
    assert_raise RuntimeError, fn -> SourceRefs.core!(root) end
  end

  test "dirty and unknown Git status cannot be attested clean", %{root: root} do
    {_, 0} = System.cmd("git", ["init", "--quiet", root])
    File.write!(Path.join(root, "source.txt"), "fixture")
    {_, 0} = System.cmd("git", ["-C", root, "add", "source.txt"])

    {_, 0} =
      System.cmd("git", [
        "-C",
        root,
        "-c",
        "user.name=CI fixture",
        "-c",
        "user.email=ci@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "fixture"
      ])

    assert %{dirty: false} = SourceRefs.git_metadata!(root, "fixture")
    File.write!(Path.join(root, "source.txt"), "changed")
    assert_raise RuntimeError, fn -> SourceRefs.git_metadata!(root, "fixture") end
    File.write!(Path.join(root, "source.txt"), "fixture")
    File.write!(Path.join(root, ".git/index"), "invalid index")
    assert_raise RuntimeError, fn -> SourceRefs.git_metadata!(root, "fixture") end
  end
end
