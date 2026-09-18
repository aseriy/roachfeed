defmodule RoachFeed.Tests do
	use RoachFeed.Tests.Base
	alias RoachFeed.Tests.FakeConsumer

	@db_defaults [hostname: "localhost", port: 26257, username: "root", database: "roachfeed_test"]

	setup_all do
		{:ok, _} = Postgrex.start_link([name: :testdb] ++ @db_defaults)
		query!("drop table if exists table_a")
		query!("drop table if exists table_b")
		query!("create table table_a (id int primary key, value text)")
		query!("create table table_b (id text primary key, value int)")
		query!("set cluster setting kv.rangefeed.enabled = true")
		:ok
	end

	test "this is hard to test, let's just do what we can" do
		query!("insert into table_a (id, value) values ($1, $2), ($3, $4)", [1, "over", 2, "9000!"])
		pid = start_consumer()
		change = forwarded(:change)
		assert change.key == [1]
		assert change.table == "table_a"
		assert change.data ==  %{after: %{id: 1, value: "over"}}

		change = forwarded(:change)
		assert change.key == [2]
		assert change.table == "table_a"
		assert change.data ==  %{after: %{id: 2, value: "9000!"}}

		%{resolved: r} = forwarded(:resolved)

		query!("insert into table_a (id, value) values ($1, $2)", [3, "spice"])
		change = forwarded(:change)
		assert change.key == [3]
		assert change.table == "table_a"
		assert change.data ==  %{after: %{id: 3, value: "spice"}}

		GenServer.stop(pid)

		start_consumer(resolved: r)
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
		] ++ @db_defaults
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
