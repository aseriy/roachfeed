defmodule RoachFeed.Tests.QuotedNames do
	use RoachFeed.Tests.Base
	alias RoachFeed.Tests.FakeConsumer

	@receive_timeout 10_000

	setup_all do
		query!("DROP TABLE IF EXISTS table_msg")
		query!(~s|DROP TABLE IF EXISTS "table-msg-2"|)
		for name <- ["table_msg", ~s|"table-msg-2"|] do
			query!("""
			CREATE TABLE #{name} (
				id UUID NOT NULL DEFAULT gen_random_uuid(),
				body STRING NOT NULL,
				body_vector VECTOR(384) NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ,
				user_id UUID NULL,
				author STRING NULL,
				CONSTRAINT messages_pkey PRIMARY KEY (id ASC)
			)
			""")
		end
		on_exit(fn ->
			query!("DROP TABLE IF EXISTS table_msg")
			query!(~s|DROP TABLE IF EXISTS "table-msg-2"|)
		end)
		:ok
	end

	test "plain name (control)" do
		exercise("table_msg", "table_msg")
	end

	test "quoted plain name" do
		exercise(~s|"table_msg"|, ~s|"table_msg"|)
	end

	test "quoted hyphenated name" do
		exercise(~s|"table-msg-2"|, ~s|"table-msg-2"|)
	end

	# Starts a consumer with `passed_name` as the table option, runs insert, update and delete against
	# `sql_ident`, and checks the three changes carry the projected record and the key in old_record.
	defp exercise(passed_name, sql_ident) do
		pid = start_consumer(change_feed: [table: passed_name, resolved: "1s", columns: ["id", "body"]])
		assert forwarded(:resolved) != nil, "no resolved message: the feed did not start for #{passed_name}"

		%{rows: [[id]]} = query!("INSERT INTO #{sql_ident} (body) VALUES ('hello') RETURNING id")
		query!("UPDATE #{sql_ident} SET body = 'updated' WHERE id = $1", [id])
		query!("DELETE FROM #{sql_ident} WHERE id = $1", [id])

		insert = forwarded(:change)
		update = forwarded(:change)
		delete = forwarded(:change)

		assert insert != nil, "no INSERT change for #{passed_name}"
		assert insert.data.type == "INSERT"
		assert Enum.sort(Map.keys(insert.data.record)) == [:body, :id]
		assert insert.data.columns != []

		assert update != nil, "no UPDATE change for #{passed_name}"
		assert update.data.type == "UPDATE"
		assert update.data.old_record.id == hd(update.key)

		assert delete != nil, "no DELETE change for #{passed_name}"
		assert delete.data.type == "DELETE"
		assert delete.data.old_record.id == hd(delete.key)

		GenServer.stop(pid)
	end

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
			{^type, msg} ->
				case msg do
					%{data: data} -> IO.puts("[msg] #{msg.table} key=#{inspect(msg.key)}\n" <> Jason.encode!(data, pretty: true))
					_ -> IO.puts("[msg] " <> Jason.encode!(msg, pretty: true))
				end
				msg
		after
			timeout -> nil
		end
	end
end
