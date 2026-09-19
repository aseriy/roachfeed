defmodule RoachFeed.Tests.Base do
	use ExUnit.CaseTemplate

	using do
		quote do
			import RoachFeed.Tests.Base
		end
	end

	@db_defaults [hostname: "localhost", port: 26257, username: "root", database: "roachfeed_test"]

	def db_config do
		case System.get_env("CRDB_DSN") do
			nil -> @db_defaults
			dsn -> parse_dsn(dsn)
		end
	end

	def parse_dsn(dsn) do
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

	def postgrex_config do
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
end
