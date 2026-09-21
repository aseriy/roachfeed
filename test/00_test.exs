defmodule RoachFeed.Tests do
	use RoachFeed.Tests.Base
	alias RoachFeed.Tests.FakeConsumer

	setup_all do
		query!("DROP TABLE IF EXISTS table_a")
		query!("DROP TABLE IF EXISTS table_b")
		query!("CREATE TABLE table_a (id INT PRIMARY KEY, value TEXT)")
		query!("CREATE TABLE table_b (id TEXT PRIMARY KEY, value INT)")
		on_exit(fn ->
			query!("DROP TABLE IF EXISTS table_a")
			query!("DROP TABLE IF EXISTS table_b")
		end)
		:ok
	end

	test "this is hard to test, let's just do what we can" do
		query!("INSERT INTO table_a (id, value) VALUES ($1, $2), ($3, $4)", [1, "over", 2, "9000!"])
		pid = start_consumer(change_feed: [for: "table_a", with: [resolved: "1s", cursor: nil]])
		change = forwarded(:change)
		assert change.key == [1]
		assert change.table == "table_a"
		assert change.data ==  %{after: %{id: 1, value: "over"}}

		change = forwarded(:change)
		assert change.key == [2]
		assert change.table == "table_a"
		assert change.data ==  %{after: %{id: 2, value: "9000!"}}

		%{resolved: r} = forwarded(:resolved)

		query!("INSERT INTO table_a (id, value) VALUES ($1, $2)", [3, "spice"])
		change = forwarded(:change)
		assert change.key == [3]
		assert change.table == "table_a"
		assert change.data ==  %{after: %{id: 3, value: "spice"}}

		GenServer.stop(pid)

		start_consumer(change_feed: [for: "table_a", with: [resolved: "1s", cursor: r]])
		change = forwarded(:change)
		assert change.key == [3]
		assert change.table == "table_a"
		assert change.data ==  %{after: %{id: 3, value: "spice"}}
	end


	defp query!(sql, args \\ []) do
		Postgrex.query!(:testdb, sql, args)
	end

	defp start_consumer(opts \\ []) do
		default = [
			test: self()  # used by our fake consumer in setup to forward messages to this pid (our test)
		] ++ db_config()
		{:ok, pid} = FakeConsumer.start_link(Keyword.merge(default, opts))
		pid
	end

	def forwarded(type) do
		receive do
			{^type, msg} -> msg
		after
			2000 -> nil
		end
	end

end
