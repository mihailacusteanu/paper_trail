defmodule PaperTrail do
  import Ecto.Changeset

  alias Ecto.Multi
  alias PaperTrail.Version
  alias PaperTrail.RepoClient

  defdelegate get_version(record), to: PaperTrail.VersionQueries
  defdelegate get_version(model_or_record, id_or_options), to: PaperTrail.VersionQueries
  defdelegate get_version(model, id, options), to: PaperTrail.VersionQueries
  defdelegate get_versions(record), to: PaperTrail.VersionQueries
  defdelegate get_versions(model_or_record, id_or_options), to: PaperTrail.VersionQueries
  defdelegate get_versions(model, id, options), to: PaperTrail.VersionQueries
  defdelegate get_current_model(version), to: PaperTrail.VersionQueries

  @doc """
  Inserts a record to the database with a related version insertion in one transaction
  """
  def insert(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, repo: nil]) do
    repo = options[:repo] || RepoClient.repo()

    transaction_order =
      case RepoClient.strict_mode() do
        true ->
          Multi.new()
          |> Multi.run(:initial_version, fn repo, %{} ->
            version_id = get_sequence_id("versions") + 1

            changeset_data =
              Map.get(changeset, :data, changeset)
              |> Map.merge(%{
                id: get_sequence_from_model(changeset) + 1,
                first_version_id: version_id,
                current_version_id: version_id
              })
            initial_version = make_version_struct(%{event: "insert"}, changeset_data, options)
            RepoClient.repo().insert(initial_version)
          end)
          |> Multi.run(:model, fn repo, %{initial_version: initial_version} ->
            updated_changeset =
              changeset
              |> change(%{
                first_version_id: initial_version.id,
                current_version_id: initial_version.id
              })

            repo.insert(updated_changeset)
          end)
          |> Multi.run(:version, fn repo, %{initial_version: initial_version, model: model} ->
            target_version =
              make_version_struct(%{event: "insert"}, model, options) |> serialize()

            Version.changeset(initial_version, target_version) |> RepoClient.repo().update
          end)

        _ ->
          Multi.new()
          |> Multi.insert(:model, changeset)
          |> Multi.run(:version, fn repo, %{model: model} ->
            version = make_version_struct(%{event: "insert"}, model, options)
            RepoClient.repo().insert(version)
          end)
      end

    transaction = repo.transaction(transaction_order)

    case RepoClient.strict_mode() do
      true ->
        case transaction do
          {:error, :model, changeset, %{}} ->
            filtered_changes =
              Map.drop(changeset.changes, [:current_version_id, :first_version_id])

            {:error, Map.merge(changeset, %{repo: repo, changes: filtered_changes})}

          {:ok, map} ->
            {:ok, Map.drop(map, [:initial_version])}
        end

      _ ->
        case transaction do
          {:error, :model, changeset, %{}} -> {:error, Map.merge(changeset, %{repo: repo})}
          _ -> transaction
        end
    end
  end

  @doc """
  Same as insert/2 but returns only the model struct or raises if the changeset is invalid.
  """
  def insert!(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, repo: nil]) do
    repo = options[:repo] || RepoClient.repo()

    repo.transaction(fn ->
      case RepoClient.strict_mode() do
        true ->
          version_id = get_sequence_id("versions") + 1

          changeset_data =
            Map.get(changeset, :data, changeset)
            |> Map.merge(%{
              id: get_sequence_from_model(changeset) + 1,
              first_version_id: version_id,
              current_version_id: version_id
            })

          initial_version =
            make_version_struct(%{event: "insert"}, changeset_data, options)
            |> RepoClient.repo().insert!

          updated_changeset =
            changeset
            |> change(%{
              first_version_id: initial_version.id,
              current_version_id: initial_version.id
            })

          model = repo.insert!(updated_changeset)
          target_version = make_version_struct(%{event: "insert"}, model, options) |> serialize()
          Version.changeset(initial_version, target_version) |> PaperTrail.RepoClient.repo().update!
          model

        _ ->
          model = repo.insert!(changeset)
          make_version_struct(%{event: "insert"}, model, options) |> PaperTrail.RepoClient.repo().insert!
          model
      end
    end)
    |> elem(1)
  end

  @doc """
  Updates a record from the database with a related version insertion in one transaction
  """
  def update(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, repo: nil, just_log_changes: false]) do
    repo = options[:repo] || PaperTrail.RepoClient.repo()
    just_log_changes = options[:just_log_changes]
    client = PaperTrail.RepoClient

    transaction_order =
      case client.strict_mode() do
        true ->
          Multi.new()
          |> Multi.run(:initial_version, fn repo, %{} ->
            version_data =
              changeset.data
              |> Map.merge(%{
                current_version_id: get_sequence_id("versions")
              })

            target_changeset = changeset |> Map.merge(%{data: version_data})
            target_version = make_version_struct(%{event: "update"}, target_changeset, options)
            RepoClient.repo().insert(target_version)
          end)
          |> Multi.run(:model, fn repo, %{initial_version: initial_version} ->
            updated_changeset = changeset |> change(%{current_version_id: initial_version.id})
            repo.update(updated_changeset)
          end)
          |> Multi.run(:version, fn repo, %{initial_version: initial_version} ->
            new_item_changes =
              initial_version.item_changes
              |> Map.merge(%{
                current_version_id: initial_version.id
              })

            initial_version |> change(%{item_changes: new_item_changes}) |> RepoClient.repo().update
          end)

        _ ->
          multi = Multi.new()
          multi =
            case just_log_changes do
              true -> multi
              _ -> Multi.update(multi, :model, changeset)
            end
          multi
          |> Multi.run(:version, fn repo, _changes  ->
            version = make_version_struct(%{event: "update"}, changeset, options)
            RepoClient.repo().insert(version)
          end)
      end

    transaction = repo.transaction(transaction_order)

    case client.strict_mode() do
      true ->
        case transaction do
          {:error, :model, changeset, %{}} ->
            filtered_changes = Map.drop(changeset.changes, [:current_version_id])
            {:error, Map.merge(changeset, %{repo: repo, changes: filtered_changes})}

          {:ok, map} ->
            {:ok, Map.delete(map, :initial_version)}
        end

      _ ->
        case transaction do
          {:error, :model, changeset, %{}} -> {:error, Map.merge(changeset, %{repo: repo})}
          _ ->
            case just_log_changes do
              true ->
                {:ok, changes} = transaction
                changes = Map.merge(changes, %{model: changeset.data})
                {:ok, changes}
              _ -> transaction
            end
        end
    end
  end

  @doc """
  Same as update/2 but returns only the model struct or raises if the changeset is invalid.
  """
  def update!(changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, repo: nil]) do
    repo = options[:repo] || PaperTrail.RepoClient.repo()
    client = PaperTrail.RepoClient

    repo.transaction(fn ->
      case client.strict_mode() do
        true ->
          version_data =
            changeset.data
            |> Map.merge(%{
              current_version_id: get_sequence_id("versions")
            })

          target_changeset = changeset |> Map.merge(%{data: version_data})
          target_version = make_version_struct(%{event: "update"}, target_changeset, options)
          initial_version = PaperTrail.RepoClient.repo().insert!(target_version)
          updated_changeset = changeset |> change(%{current_version_id: initial_version.id})
          model = repo.update!(updated_changeset)

          new_item_changes =
            initial_version.item_changes
            |> Map.merge(%{
              current_version_id: initial_version.id
            })

          initial_version |> change(%{item_changes: new_item_changes}) |> RepoClient.repo().update!
          model

        _ ->
          model = repo.update!(changeset)
          version_struct = make_version_struct(%{event: "update"}, changeset, options)
          PaperTrail.RepoClient.repo().insert!(version_struct)
          model
      end
    end)
    |> elem(1)
  end

  @doc """
  Deletes a record from the database with a related version insertion in one transaction
  """
  def delete(struct, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, repo: nil]) do
    repo = options[:repo] || PaperTrail.RepoClient.repo()

    transaction =
      Multi.new()
      |> Multi.delete(:model, struct, options)
      |> Multi.run(:version, fn repo, %{} ->
        version = make_version_struct(%{event: "delete"}, struct, options)
        PaperTrail.RepoClient.repo().insert(version, options)
      end)
      |> repo.transaction(options)

    case transaction do
      {:error, :model, changeset, %{}} -> {:error, Map.merge(changeset, %{repo: repo})}
      _ -> transaction
    end
  end

  @doc """
  Same as delete/2 but returns only the model struct or raises if the changeset is invalid.
  """
  def delete!(struct, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, repo: nil]) do
    repo = options[:repo] || PaperTrail.RepoClient.repo()

    repo.transaction(fn ->
      model = repo.delete!(struct, options)
      version_struct = make_version_struct(%{event: "delete"}, struct, options)
      PaperTrail.RepoClient.repo().insert!(version_struct, options)
      model
    end)
    |> elem(1)
  end

  def insert_version(%{action: action} = changeset, options \\ [origin: nil, meta: nil, originator: nil, prefix: nil, repo: nil]) do
    struct = if action == :update, do: changeset, else: changeset.data
    version = make_version_struct(%{event: to_string(action)}, struct, options)
    PaperTrail.RepoClient.repo().insert!(version, options)
  end

  defp make_version_struct(%{event: "insert"}, model, options) do
    originator = PaperTrail.RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]

    %Version{
      event: "insert",
      item_type: model.__struct__ |> Module.split() |> List.last(),
      item_id: get_model_id(model),
      item_changes: serialize(model),
      originator_id:
        case originator_ref do
          nil -> nil
          _ -> originator_ref |> Map.get(:id)
        end,
      origin: options[:origin],
      meta: options[:meta]
    }
    |> add_prefix(options[:prefix])
  end

  defp make_version_struct(%{event: "update"}, changeset, options) do
    originator = PaperTrail.RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]

    %Version{
      event: "update",
      item_type: changeset.data.__struct__ |> Module.split() |> List.last(),
      item_id: get_model_id(changeset.data),
      item_changes: serialize_changes(changeset) || %{},
      originator_id:
        case originator_ref do
          nil -> nil
          _ -> originator_ref |> Map.get(:id)
        end,
      origin: options[:origin],
      meta: options[:meta]
    }
    |> add_prefix(options[:prefix])
  end

  defp make_version_struct(%{event: "delete"}, model, options) do
    originator = PaperTrail.RepoClient.originator()
    originator_ref = options[originator[:name]] || options[:originator]

    %Version{
      event: "delete",
      item_type: model.__struct__ |> Module.split() |> List.last(),
      item_id: get_model_id(model),
      item_changes: serialize(model),
      originator_id:
        case originator_ref do
          nil -> nil
          _ -> originator_ref |> Map.get(:id)
        end,
      origin: options[:origin],
      meta: options[:meta]
    }
    |> add_prefix(options[:prefix])
  end

  defp get_sequence_from_model(changeset) do
    table_name =
      case Map.get(changeset, :data) do
        nil -> changeset.__struct__.__schema__(:source)
        _ -> changeset.data.__struct__.__schema__(:source)
      end

    get_sequence_id(table_name)
  end

  defp get_sequence_id(table_name) do
    Ecto.Adapters.SQL.query!(RepoClient.repo(), "select last_value FROM #{table_name}_id_seq").rows
    |> List.first()
    |> List.first()
  end

  defp serialize(model) do
    relationships = model.__struct__.__schema__(:associations)
    Map.drop(model, [:__struct__, :__meta__] ++ relationships)
  end

  defp serialize_changes(data), do: serialize_changes(data, true)
  defp serialize_changes(data, is_root) do
    case is_map(data) do
      false -> data
      true -> deep_serialize(data, is_root)
    end
  end

  defp deep_serialize(data, is_root) do
    if Map.has_key?(data, :__struct__) do
      case data.__struct__ do
        Ecto.Changeset -> extract_values(data, is_root)
        _ -> data
      end
    else
      data
    end
  end

  defp extract_values(changeset, is_root) do
    if changeset.changes != %{} do
      init = initial_value(changeset, is_root)
      changeset.changes
      |> Enum.reduce(init, fn({key, value}, accum) ->
        value = if is_list(value) do
          Enum.map(value, &serialize_changes(&1, false))
          |> Enum.filter(fn(v) -> ! is_nil(v) end)
        else
          serialize_changes(value, false)
        end
        Map.put(accum, key, value)
      end)
    end
  end

  defp initial_value(_, true), do: %{}
  defp initial_value(changeset, false), do: %{id: changeset.data.id}

  defp add_prefix(changeset, nil), do: changeset
  defp add_prefix(changeset, prefix), do: Ecto.put_meta(changeset, prefix: prefix)

  def get_model_id(model) do
    {_, model_id} = List.first(Ecto.primary_key(model))

    case PaperTrail.Version.__schema__(:type, :item_id) do
      :integer ->
        model_id
      _ ->
        "#{model_id}"
    end
  end
end
