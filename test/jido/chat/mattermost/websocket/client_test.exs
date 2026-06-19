defmodule Jido.Chat.Mattermost.WebSocket.ClientTest do
  use ExUnit.Case, async: true

  alias Jido.Chat.Mattermost.WebSocket.Client

  @base_state %{
    token: "test-token",
    bot_user_id: "bot-uid",
    bot_name: "zaq",
    channel_ids: :all,
    bridge_id: "mattermost_1",
    sink_mfa: {__MODULE__, :sink_noop, []},
    sink_opts: []
  }

  def sink_noop(_payload, _opts), do: :ok

  defp raising_sink_state do
    Map.put(@base_state, :sink_mfa, {__MODULE__, :sink_raise, []})
  end

  def sink_raise(_payload, _opts), do: raise("sink called")

  defp posted_frame(post_attrs) do
    post =
      Map.merge(
        %{
          "id" => "post-1",
          "user_id" => "user-123",
          "channel_id" => "chan-abc",
          "message" => "Hello",
          "root_id" => ""
        },
        post_attrs
      )

    event = %{
      "event" => "posted",
      "data" => %{"post" => Jason.encode!(post)}
    }

    {:text, Jason.encode!(event)}
  end

  # ── handle_connect/3 ──────────────────────────────────────────────────

  describe "handle_connect/3" do
    test "sends authentication_challenge with bot token" do
      {:reply, [{:text, json}], _state} = Client.handle_connect(101, [], @base_state)

      assert {:ok, %{"action" => "authentication_challenge", "data" => %{"token" => "test-token"}}} =
               Jason.decode(json)
    end

    test "marks auth as pending" do
      {:reply, _frames, state} = Client.handle_connect(101, [], @base_state)

      assert state.auth_status == :pending
    end
  end

  # ── handle_in/2 — auth responses ─────────────────────────────────────

  describe "handle_in/2 auth responses" do
    test "marks authentication success" do
      frame = {:text, Jason.encode!(%{"seq_reply" => 1, "status" => "OK"})}

      assert {:ok, state} = Client.handle_in(frame, @base_state)
      assert state.auth_status == :ok
    end

    test "crashes on authentication failure" do
      reason = %{"message" => "invalid token"}
      frame = {:text, Jason.encode!(%{"seq_reply" => 1, "status" => "FAIL", "error" => reason})}

      assert catch_exit(Client.handle_in(frame, @base_state)) == {:auth_failed, reason}
    end

    test "posted event success marks auth as fallback" do
      frame = posted_frame(%{"user_id" => "other-user"})

      assert {:ok, state} = Client.handle_in(frame, @base_state)
      assert state.auth_status == :ok
    end
  end

  # ── handle_error/2 ───────────────────────────────────────────────────

  describe "handle_error/2" do
    test "closes with a standardized transport error reason" do
      error = {:streaming_failed, :closed}

      assert {:close, {:transport_failed, ^error}} = Client.handle_error(error, @base_state)
    end
  end

  # ── handle_info/2 — auth status requests ─────────────────────────────

  describe "handle_info/2 auth status requests" do
    test "replies with the current auth status" do
      ref = make_ref()
      state = Map.put(@base_state, :auth_status, :pending)

      assert {:ok, ^state} = Client.handle_info({:auth_status, self(), ref}, state)
      assert_receive {^ref, :pending}
    end

    test "replies with :unknown when auth status is absent" do
      assert {:ok, @base_state} = Client.handle_info({:auth_status, self()}, @base_state)
      assert_receive {:auth_status, :unknown}
    end

    test "ignores unrelated process messages" do
      assert {:ok, @base_state} = Client.handle_info(:unrelated, @base_state)
    end
  end

  # ── auth_status/2 ────────────────────────────────────────────────────

  describe "auth_status/2" do
    test "requests auth status from a websocket process" do
      pid =
        spawn(fn ->
          receive do
            {:auth_status, from, ref} -> send(from, {ref, :ok})
          end
        end)

      assert {:ok, :ok} = Client.auth_status(pid)
    end

    test "returns standardized process exit reasons" do
      pid =
        spawn(fn ->
          receive do
            {:auth_status, _from, _ref} -> exit({:auth_failed, :invalid_token})
          end
        end)

      assert {:error, {:auth_failed, :invalid_token}} = Client.auth_status(pid)
    end

    test "returns timeout when the process does not reply" do
      pid =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      assert {:error, :timeout} = Client.auth_status(pid, 1)

      send(pid, :stop)
    end
  end

  # ── handle_disconnect/3 ───────────────────────────────────────────────

  describe "handle_disconnect/3" do
    test "returns :reconnect with state" do
      assert {:reconnect, @base_state} = Client.handle_disconnect(1001, "going away", @base_state)
    end
  end

  # ── handle_in/2 — non-posted events ──────────────────────────────────

  describe "handle_in/2 non-posted events" do
    test "ignores typing event" do
      frame = {:text, Jason.encode!(%{"event" => "typing", "data" => %{}})}
      assert {:ok, @base_state} = Client.handle_in(frame, @base_state)
    end

    test "ignores status_change event" do
      frame = {:text, Jason.encode!(%{"event" => "status_change", "data" => %{}})}
      assert {:ok, @base_state} = Client.handle_in(frame, @base_state)
    end

    test "ignores hello event (auth confirmation)" do
      frame = {:text, Jason.encode!(%{"event" => "hello", "data" => %{}})}
      assert {:ok, @base_state} = Client.handle_in(frame, @base_state)
    end

    test "ignores binary frames" do
      assert {:ok, @base_state} = Client.handle_in({:binary, <<0, 1, 2>>}, @base_state)
    end
  end

  # ── handle_in/2 — malformed JSON ─────────────────────────────────────

  describe "handle_in/2 malformed JSON" do
    test "does not crash on invalid JSON, returns {:ok, state}" do
      frame = {:text, "not json at all {{{}"}
      assert {:ok, @base_state} = Client.handle_in(frame, @base_state)
    end

    test "does not crash when post field is not double-encoded JSON" do
      event = %{"event" => "posted", "data" => %{"post" => %{"not" => "a string"}}}
      frame = {:text, Jason.encode!(event)}
      assert {:ok, @base_state} = Client.handle_in(frame, @base_state)
    end

    test "does not crash when data.post key is missing" do
      event = %{"event" => "posted", "data" => %{}}
      frame = {:text, Jason.encode!(event)}
      assert {:ok, @base_state} = Client.handle_in(frame, @base_state)
    end
  end

  # ── handle_in/2 — bot filtering ──────────────────────────────────────

  describe "handle_in/2 bot message filtering" do
    test "filters out messages from the bot user, oban_worker never invoked" do
      # oban_worker is nil — would crash if called
      frame = posted_frame(%{"user_id" => "bot-uid"})
      assert {:ok, _state} = Client.handle_in(frame, @base_state)
    end

    test "passes through messages from a different user" do
      frame = posted_frame(%{"user_id" => "other-user"})
      assert {:ok, _state} = Client.handle_in(frame, raising_sink_state())
    end
  end

  # ── handle_in/2 — channel filtering ──────────────────────────────────

  describe "handle_in/2 channel filtering" do
    test "passes through when channel_ids is :all" do
      frame = posted_frame(%{"channel_id" => "any-channel"})
      assert {:ok, _state} = Client.handle_in(frame, raising_sink_state())
    end

    test "filters out messages from untracked channels, sink never invoked" do
      state = Map.put(@base_state, :channel_ids, ["tracked-chan"])
      frame = posted_frame(%{"channel_id" => "other-chan"})
      assert {:ok, _state} = Client.handle_in(frame, state)
    end

    test "passes through messages from tracked channels" do
      state = Map.put(raising_sink_state(), :channel_ids, ["tracked-chan"])
      frame = posted_frame(%{"channel_id" => "tracked-chan"})
      assert {:ok, _state} = Client.handle_in(frame, state)
    end
  end
end
