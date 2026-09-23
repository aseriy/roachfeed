defmodule RoachFeed.Tests.Messages do
	use RoachFeed.Tests.Base
	alias RoachFeed.Tests.FakeConsumer

	@messages Path.join(__DIR__, "messages.jsonl") |> File.stream!() |> Enum.map(&Jason.decode!/1)
	@feed_interval {1_000, 5_000}
	@receive_timeout elem(@feed_interval, 1) + 2_000
	@phase_time div(length(@messages) * elem(@feed_interval, 0), 4)

	setup_all do
		query!("DROP TABLE IF EXISTS table_msg")
		query!("""
		CREATE TABLE table_msg (
			id UUID NOT NULL DEFAULT gen_random_uuid(),
			body STRING NOT NULL,
			body_vector VECTOR(384) NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ,
			user_id UUID NULL,
			author STRING NULL,
			CONSTRAINT messages_pkey PRIMARY KEY (id ASC)
		)
		""")
		on_exit(fn ->
			query!("DROP TABLE IF EXISTS table_msg")
		end)
		:ok
	end

	@tag timeout: @phase_time * 6
	test "column projection across consumer recycles" do
		rows = @messages |> Enum.with_index() |> Map.new(fn {row, position} -> {row["id"], {position, row}} end)
		feeder = start_feeder(@messages, @feed_interval)

		{changes1, cursor} = run_phase([table: "table_msg", resolved: "1s", columns: ["id"]], rows, [:id])
		{changes2, cursor} = run_phase([table: "table_msg", resolved: "1s", columns: ["id", "body"], after: cursor], rows, [:body, :id])
		{changes3, cursor} = run_phase([table: "table_msg", resolved: "1s", columns: ["id", "body", "created_at"], after: cursor], rows, [:body, :created_at, :id])
		{changes4, _cursor} = run_phase([table: "table_msg", resolved: "1s", columns: ["id", "body"], after: cursor], rows, [:body, :id])

		Process.unlink(feeder)
		Process.exit(feeder, :kill)

		positions =
			(changes1 ++ changes2 ++ changes3 ++ changes4)
			|> Enum.map(fn change -> elem(Map.fetch!(rows, hd(change.key)), 0) end)
			|> Enum.uniq()
			|> Enum.sort()
		assert positions == Enum.to_list(0..List.last(positions))
	end

	defp run_phase(change_feed, rows, expected_keys) do
		IO.puts("\n[phase] columns=#{inspect(change_feed[:columns])} after=#{inspect(change_feed[:after])}")
		pid = start_consumer(change_feed: change_feed)
		deadline = System.monotonic_time(:millisecond) + @phase_time
		changes = collect_changes(deadline, [])
		GenServer.stop(pid)
		IO.puts("[phase] collected=#{length(changes)} cursor=#{inspect(last_cursor(changes))}")
		assert changes != []
		for change <- changes do
			assert change.table == "table_msg"
			cond do
				change.data.after == nil ->
					assert [_id] = change.key
					assert change.data.before.id == hd(change.key)

				change.data.before == nil ->
					fields = change.data.after
					assert Enum.sort(Map.keys(fields)) == expected_keys
					assert change.key == [fields.id]
					{_position, row} = Map.fetch!(rows, fields.id)
					if Map.has_key?(fields, :body) do
						assert fields.body == row["body"]
					end
					if Map.has_key?(fields, :created_at) do
						{:ok, emitted, _} = DateTime.from_iso8601(fields.created_at)
						{:ok, expected, _} = DateTime.from_iso8601(row["created_at"])
						assert DateTime.compare(emitted, expected) == :eq
					end

				true ->
					fields = change.data.after
					assert Enum.sort(Map.keys(fields)) == expected_keys
					assert change.key == [fields.id]
					assert Map.has_key?(change.data.before, :author)
					{_position, row} = Map.fetch!(rows, fields.id)
					if Map.has_key?(fields, :body) do
						assert fields.body == "updated: " <> row["body"]
					end
					if Map.has_key?(fields, :created_at) do
						{:ok, emitted, _} = DateTime.from_iso8601(fields.created_at)
						{:ok, expected, _} = DateTime.from_iso8601(row["created_at"])
						assert DateTime.compare(emitted, expected) == :eq
					end
			end
		end
		{changes, last_cursor(changes)}
	end

	defp collect_changes(deadline, acc) do
		remaining = deadline - System.monotonic_time(:millisecond)
		if remaining <= 0 do
			Enum.reverse(acc)
		else
			case forwarded(:change, min(remaining, @receive_timeout)) do
				nil ->
					collect_changes(deadline, acc)
				change ->
					IO.puts("[feed] #{change.table} key=#{inspect(change.key)}\n" <> Jason.encode!(change.data, pretty: true))
					collect_changes(deadline, [change | acc])
			end
		end
	end

	defp last_cursor([]), do: nil
	defp last_cursor(changes), do: changes |> List.last() |> Map.fetch!(:data) |> Map.fetch!(:mvcc_timestamp)

	defp start_feeder(messages, {min, max}) do
		spawn_link(fn -> feed(Enum.with_index(messages), messages, min, max) end)
	end

	defp feed([], _all, _min, _max), do: :ok
	defp feed([{row, i} | rest], all, min, max) do
		:timer.sleep(min - 1 + :rand.uniform(max - min + 1))
		{:ok, created_at, _} = DateTime.from_iso8601(row["created_at"])
		vector = case row["body_vector"] do
			nil -> "NULL"
			v -> "'#{v}'"
		end
		query!("INSERT INTO table_msg (id, body, body_vector, created_at, user_id, author) VALUES ($1, $2, #{vector}, $3, $4, $5)",
			[uuid(row["id"]), row["body"], created_at, uuid(row["user_id"]), row["author"]])
		if rem(i, 10) == 3 and i >= 3 do
			target = Enum.at(all, i - 3)
			query!("UPDATE table_msg SET body = 'updated: ' || body WHERE id = $1", [uuid(target["id"])])
		end
		if rem(i, 10) == 8 and i >= 8 do
			target = Enum.at(all, i - 3)
			query!("DELETE FROM table_msg WHERE id = $1", [uuid(target["id"])])
		end
		feed(rest, all, min, max)
	end

	defp uuid(nil), do: nil
	defp uuid(id), do: id |> String.replace("-", "") |> Base.decode16!(case: :lower)

	defp query!(sql, args \\ []) do
		Postgrex.query!(:testdb, sql, args)
	end

	defp start_consumer(opts) do
		default = [
			test: self()  # used by our fake consumer in setup to forward messages to this pid (our test)
		] ++ db_config()
		{:ok, pid} = FakeConsumer.start_link(Keyword.merge(default, opts))
		pid
	end

	def forwarded(type, timeout \\ @receive_timeout) do
		receive do
			{^type, msg} -> msg
		after
			timeout -> nil
		end
	end
end
