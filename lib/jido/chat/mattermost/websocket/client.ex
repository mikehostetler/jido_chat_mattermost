defmodule Jido.Chat.Mattermost.WebSocket.Client do
  @moduledoc """
  Fresh-based WebSocket client for Mattermost event ingestion.

  On each `posted` event the raw Mattermost payload map is forwarded to the
  configured sink via MFA dispatch. The sink receives the raw payload and is
  responsible for dispatching it through `Jido.Chat.Adapter.transform_incoming/2`
  (adapter_module, payload) to normalize into `%Jido.Chat.Incoming{}`.

      apply(module, function, extra_args ++ [payload, sink_opts])

  ## Flow

      Mattermost WS frame
        → handle_in/2 (decode + filter, ~1ms)
          → raw payload map (post + channel metadata)
          → apply(sink_module, sink_fun, sink_args ++ [payload, sink_opts])
  """

  use Fresh

  require Logger

  @doc "Returns the current Mattermost WebSocket authentication status."
  @spec auth_status(pid(), timeout()) :: {:ok, :ok | :pending | :unknown} | {:error, term()}
  def auth_status(pid, timeout \\ 5_000) when is_pid(pid) do
    ref = make_ref()
    monitor_ref = Process.monitor(pid)

    send(pid, {:auth_status, self(), ref})

    receive do
      {^ref, status} ->
        Process.demonitor(monitor_ref, [:flush])
        {:ok, status}

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        {:error, reason}
    after
      timeout ->
        Process.demonitor(monitor_ref, [:flush])
        {:error, :timeout}
    end
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    state = Map.put(state, :auth_status, :pending)

    auth =
      Jason.encode!(%{
        seq: 1,
        action: "authentication_challenge",
        data: %{token: state.token}
      })

    Logger.info("[Mattermost WS] Connected bridge_id=#{state.bridge_id}, sending auth")
    {:reply, [{:text, auth}], state}
  end

  @impl Fresh
  def handle_in({:text, data}, state) do
    case Jason.decode(data) do
      {:ok, event} when is_map(event) ->
        handle_event(event, state)

      {:error, reason} ->
        Logger.warning("[Mattermost WS] JSON decode failed reason=#{inspect(reason)}")
        {:ok, state}
    end
  rescue
    e ->
      Logger.warning("[Mattermost WS] Exception in handle_in: #{Exception.message(e)}")
      {:ok, state}
  end

  def handle_in(_frame, state), do: {:ok, state}

  @impl Fresh
  def handle_error(error, _state) do
    {:close, {:transport_failed, error}}
  end

  @impl Fresh
  def handle_info({:auth_status, from}, state) when is_pid(from) do
    send(from, {:auth_status, Map.get(state, :auth_status, :unknown)})
    {:ok, state}
  end

  def handle_info({:auth_status, from, ref}, state) when is_pid(from) do
    send(from, {ref, Map.get(state, :auth_status, :unknown)})
    {:ok, state}
  end

  def handle_info(_message, state), do: {:ok, state}

  @impl Fresh
  def handle_disconnect(code, reason, state) do
    Logger.warning(
      "[Mattermost WS] Disconnected bridge_id=#{state.bridge_id} " <>
        "code=#{inspect(code)} reason=#{inspect(reason)}, reconnecting"
    )

    {:reconnect, state}
  end

  defp handle_event(event, state) do
    case classify_auth_status(event) do
      {:ok, :authenticated} ->
        {:ok, Map.put(state, :auth_status, :ok)}

      {:error, reason} ->
        exit({:auth_failed, reason})

      :not_auth ->
        handle_non_auth_event(event, state)
    end
  end

  defp handle_non_auth_event(%{"event" => "posted"} = event, state) do
    state =
      case handle_posted(event, state) do
        :dispatched -> auth_success_fallback(state)
        _ignored -> state
      end

    {:ok, state}
  end

  defp handle_non_auth_event(_event, state), do: {:ok, state}

  defp classify_auth_status(%{"seq_reply" => 1} = event) do
    cond do
      auth_success?(event) -> {:ok, :authenticated}
      auth_failure?(event) -> {:error, auth_failure_reason(event)}
      true -> :not_auth
    end
  end

  defp classify_auth_status(%{"seq_reply" => "1"} = event),
    do: classify_auth_status(Map.put(event, "seq_reply", 1))

  defp classify_auth_status(_event), do: :not_auth

  defp auth_success?(event) do
    event
    |> Map.get("status")
    |> normalize_status()
    |> Kernel.==(:ok)
  end

  defp auth_failure?(event) do
    Map.has_key?(event, "error") or normalize_status(Map.get(event, "status")) == :error
  end

  defp normalize_status(status) when is_binary(status) do
    case String.downcase(status) do
      "ok" -> :ok
      "success" -> :ok
      "fail" -> :error
      "failed" -> :error
      "error" -> :error
      _ -> :unknown
    end
  end

  defp normalize_status(:ok), do: :ok
  defp normalize_status(:success), do: :ok
  defp normalize_status(:error), do: :error
  defp normalize_status(:fail), do: :error
  defp normalize_status(:failed), do: :error
  defp normalize_status(_status), do: :unknown

  defp auth_failure_reason(%{"error" => error}), do: error
  defp auth_failure_reason(%{"status" => status}), do: status
  defp auth_failure_reason(_event), do: :authentication_failed

  defp auth_success_fallback(%{auth_status: :ok} = state), do: state
  defp auth_success_fallback(state), do: Map.put(state, :auth_status, :ok)

  defp handle_posted(%{"data" => data}, state) do
    channel_type = Map.get(data, "channel_type")

    with {:ok, post} <- decode_post(data),
         true <- not_bot?(post, state),
         true <- in_tracked_channel?(post, channel_type, state) do
      emit_event(data, post, state)
    else
      _ -> :ignored
    end
  end

  defp decode_post(%{"post" => post_json}) when is_binary(post_json) do
    Jason.decode(post_json)
  end

  defp decode_post(_), do: {:error, :missing_post}

  defp not_bot?(post, %{bot_user_id: bot_user_id}) when is_binary(bot_user_id) do
    post["user_id"] != bot_user_id
  end

  defp not_bot?(_post, _state), do: true

  # DM ("D") and group DM ("G") channels are always tracked — their IDs are
  # dynamic and never registered in the retrieval_channels allowlist.
  defp in_tracked_channel?(_post, channel_type, _state) when channel_type in ["D", "G"], do: true
  defp in_tracked_channel?(_post, _channel_type, %{channel_ids: :all}), do: true

  defp in_tracked_channel?(post, _channel_type, %{channel_ids: channel_ids})
       when is_list(channel_ids) do
    post["channel_id"] in channel_ids
  end

  defp in_tracked_channel?(_post, _channel_type, _state), do: true

  defp emit_event(data, post, state) do
    payload = %{
      "post" => post,
      "channel_type" => Map.get(data, "channel_type"),
      "channel_display_name" => Map.get(data, "channel_display_name")
    }

    sink_opts = Keyword.put(state.sink_opts, :transport, "websocket")
    invoke_sink(state.sink_mfa, payload, sink_opts)

    Logger.info(
      "[Mattermost WS] Posted event dispatched post_id=#{post["id"]} " <>
        "root_id=#{inspect(post["root_id"])}"
    )

    :dispatched
  end

  defp invoke_sink({module, function, extra_args}, incoming, sink_opts) do
    apply(module, function, extra_args ++ [incoming, sink_opts])
  end
end
