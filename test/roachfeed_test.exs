defmodule RoachFeed.Tests do
	use RoachFeed.Tests.Base
	alias RoachFeed.Tests.FakeConsumer

	@db_defaults [hostname: "localhost", port: 26257, username: "root", database: "roachfeed_test"]

	setup_all do
		{:ok, _} = Postgrex.start_link([name: :testdb] ++ postgrex_config())
		query!("drop table if exists table_a")
		query!("drop table if exists table_b")
		query!("create table table_a (id int primary key, value text)")
		query!("create table table_b (id text primary key, value int)")
		# query!("set cluster setting kv.rangefeed.enabled = true")
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

	defp db_config do
		case System.get_env("CRDB_DSN") do
			nil -> @db_defaults
			dsn -> parse_dsn(dsn)
		end
	end

	defp parse_dsn(dsn) do
		uri = URI.parse(dsn)
		[username, password] = String.split(uri.userinfo, ":", parts: 2)
		query = URI.decode_query(uri.query || "")
		config = [
			hostname: uri.host,
			port: uri.port || 26257,
			username: URI.decode(username),
			password: URI.decode(password),
			database: String.trim_leading(uri.path, "/")
		]
		case query["sslmode"] do
			nil -> config
			sslmode -> config ++ [sslmode: sslmode, cacertfile: query["sslrootcert"] || Path.expand("~/.postgresql/root.crt")]
		end
	end

	defp postgrex_config do
		config = db_config()
		case config[:sslmode] do
			"verify-full" ->
				ssl_opts = [
					verify: :verify_peer,
					cacertfile: config[:cacertfile],
					server_name_indication: String.to_charlist(config[:hostname]),
					customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
				]
				Keyword.drop(config, [:sslmode, :cacertfile]) ++ [ssl: ssl_opts]
			_ ->
				config
		end
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
