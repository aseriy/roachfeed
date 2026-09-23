defmodule RoachFeed do
	defmacro __using__(_opts) do
		quote location: :keep do
			use GenServer
			require Logger

			@timeout :timer.seconds(10_000)

			def start_link(opts) do
				{name, opts} = Keyword.pop(opts, :name, __MODULE__)
				GenServer.start_link(__MODULE__, opts, name: name)
			end

			def init(opts) do
				{:ok, opts, {:continue, :init}}
			end

			def handle_continue(:init, opts) do
				{state, config} = setup(opts)
				socket = connect(config, 0)
				Process.put(:socket, socket)
				{:noreply, state}
			rescue
				err ->
					:timer.sleep(:timer.seconds(1))
					reraise err, __STACKTRACE__
			end

			defp connect(opts, tries) do
				with {:ok, socket} <- connect(opts) do
					socket
				else
					{:error, err} ->
						Logger.error("failed to connect: #{inspect(err)}")
						case tries do
							0 -> :ok
							1 -> :ok
							2 -> :timer.sleep(100)
							3 -> :timer.sleep(300)
							4 -> :timer.sleep(600)
							5 -> :timer.sleep(1000)
							6 -> :timer.sleep(2000)
							7 -> :timer.sleep(3000)
							_ -> :timer.sleep(4000)
						end
						connect(opts, tries + 1)
				end
			end

			defp setup(opts), do: {nil, opts}
			defoverridable [setup: 1]

			defp connect(opts) do
				port = Keyword.get(opts, :port, 26257)
				host = String.to_charlist(Keyword.get(opts, :hostname, "127.0.0.1"))

				with {:ok, socket} <- :gen_tcp.connect(host, port, [packet: :raw, mode: :binary, active: false], @timeout),
				     :ok <- :inet.setopts(socket, send_timeout: @timeout),
				     {:ok, socket} <- RoachFeed.maybe_ssl(socket, host, opts, @timeout),
				     :ok <- RoachFeed.authenticate(socket, opts),
				     :ok <- RoachFeed.sock_setopts(socket, active: :once)
				do
					{:ok, socket}
				end
			end

			def handle_info({:tcp, _socket, data}, state), do: handle_socket_data(data, state)
			def handle_info({:ssl, _socket, data}, state), do: handle_socket_data(data, state)

			def handle_info({:tcp_closed, _socket}, state) do
				{:stop, :closed, state}
			end

			def handle_info({:ssl_closed, _socket}, state) do
				{:stop, :closed, state}
			end

			defp handle_socket_data(data, state) do
				socket = Process.get(:socket)
				case process_data(socket, data, state) do
					{:ok, state} ->
						RoachFeed.sock_setopts(socket, active: :once)
						{:noreply, state}
					{:error, err} ->
						# not sure this is right
						RoachFeed.sock_close(socket)
						{:stop, err, state}
				end
			end

			defp process_data(socket, <<>>, state), do: {:ok, state}

			defp process_data(socket, data, state) when byte_size(data) < 5 do
				with {:ok, more} <- RoachFeed.sock_recv(socket, 5 - byte_size(data), @timeout),
				     <<type, length::big-32>> = data <> more,  # both are very short
				     {:ok, payload} <- RoachFeed.sock_recv(socket, length-4, @timeout)
				do
					process_message(type, payload, state)
				end
			end

			defp process_data(socket, <<type, length::big-32, rest::binary>>, state) when byte_size(rest) < (length-4) do
				missing = length - 4 - byte_size(rest)
				with {:ok, payload} <- RoachFeed.sock_recv(socket, missing, @timeout) do
					payload = :erlang.iolist_to_binary([rest, payload])
					process_message(type, payload, state)
				end
			end

			# we have at least 1 message
			defp process_data(socket, <<type, length::big-32, rest::binary>>, state) do
				length = length - 4
				<<payload::bytes-size(length), rest::binary>> = rest
				with {:ok, state} <- process_message(type, payload, state) do
					process_data(socket, rest, state)
				end
			end

			# server properties, ignore
			defp process_message(?S, _msg, state), do: {:ok, state}

			# backend key data, ignore
			defp process_message(?K, _msg, state), do: {:ok, state}

			# reply to the bind from the experimental changefeed query
			# can't process this synchronously, because cockroachdb doesn't send the
			# reply until there's data in the changefeed
			defp process_message(?2, _msg, state), do: {:ok, state}

			defp process_message(?E, error, state) do
				{:error, RoachFeed.Error.cockroach(error)}
			end

			# ready for query
			defp process_message(?Z, _msg, state) do
				socket = Process.get(:socket)
				{state, config} = query(state)

				cond do
					config[:for] == nil and config[:table] == nil ->
						{:error, RoachFeed.Error.driver("changefeed config requires one of :for or :table", nil)}

					config[:for] != nil and config[:table] != nil ->
						{:error, RoachFeed.Error.driver("changefeed config cannot have both :for and :table", nil)}

					true ->
						schema = config[:schema] || "public"

						with_opts = case config[:table] do
							nil -> config[:with]
							_ -> [envelope: "bare", resolved: config[:resolved] || "10s", cursor: config[:after], mvcc_timestamp: true]
						end

						{w, values, _} = Enum.reduce(with_opts || [], {[], [], 1}, fn
							{:cursor, nil}, acc -> acc  # crdb doesn't support a nil cursor, just don't add the option
							{key, true}, {w, values, index} -> {[", #{key}", w], values, index}
							{key, value}, {w, values, index} -> {[", #{key} = $#{index}", w], [value | values], index + 1}
						end)
						values = Enum.reverse(values)
						w = :erlang.iolist_to_binary(w)

						sql = case config[:table] do
							nil ->
								sql = ["CREATE CHANGEFEED FOR TABLE ", config |> Keyword.fetch!(:for) |> List.wrap() |> Enum.join(", ")]
								case w do
									"" -> sql
									<<", ", w::binary>> -> [sql, " WITH ", w]
								end
							table ->
								<<", ", w::binary>> = w
								columns = case config[:columns] do
									c when c in [nil, []] -> "*"
									c -> Enum.join(c, ", ")
								end
								where = case config[:where] do
									p when p in [nil, "", []] -> ""
									p -> " WHERE #{p}"
								end
								["CREATE CHANGEFEED WITH ", w, " AS SELECT ", columns, " FROM ", schema, ".", table, where]
						end

						sql = :erlang.iolist_to_binary(sql)
						parse_describe_sync = [
							RoachFeed.build_message(?P, <<0, sql::binary, 0, 0, 0>>),
							<<?D, 0, 0, 0, 6, ?S, 0>>,
							<<?S, 0, 0, 0, 4>>
						]

						{args_count, args_length, args} = Enum.reduce(values, {0, 0, []}, fn
							nil, {count, length, acc} -> {count + 1, length + 4, [<<255, 255, 255, 255>> | acc]}
							value, {count, length, acc} ->
								value = to_string(value)
								acc = [acc, <<byte_size(value)::big-32, value::binary>>]
								{count + 1, length + byte_size(value) + 4, acc}
						end)

						bind_execute_close_sync = [
							[?B, 0, 0, 0, 14 + args_length, 0, 0, 0, 0, <<args_count::big-16>>, args, 0, 1, 0, 1],
							<<?E, 0, 0, 0, 9, 0, 0, 0, 0, 0>>,
							<<?C, 0, 0, 0, 5, ?S>>,
							<<?S, 0, 0, 0, 4>>
						]

						column_types = case config[:table] do
							nil -> {:ok, nil}
							table -> RoachFeed.fetch_column_types(socket, schema, table, config[:columns])
						end

						with {:ok, column_types} <- column_types,
						     {?1, nil} <- RoachFeed.send_recv_message(socket, parse_describe_sync),
						     {?t, _} <- RoachFeed.recv_message(socket), # parameter info
						     {?T, _} <- RoachFeed.recv_message(socket), # column info
						     {?Z, _} <- RoachFeed.recv_message(socket), # wait until server is ready
						     :ok <- RoachFeed.sock_send(socket, bind_execute_close_sync)
						do
							if column_types != nil do
								IO.puts("column types: " <> Enum.map_join(column_types, ", ", fn {name, type} -> "#{name}=#{type}" end))
								Process.put(:column_types, column_types)
							end
							{:ok, state}
						else
							{:error, _} = err -> err
							{?E, err} -> {:error, RoachFeed.Error.cockroach(err)}
							invalid -> {:error, RoachFeed.Error.driver("unexpected reply to parse+describe+sync", invalid)}
						end
				end
			end

			# row descriptor (can ignore since we know what the parameter types are)
			defp process_message(?T, msg, state), do: {:ok, state}

			# resolved value
			defp process_message(?D, <<3::big-16, 255, 255, 255, 255, 255, 255, 255, 255, l::big-32, msg::binary>>, state) do
				state = handle_resolved(msg, state)
				{:ok, state}
			end

			defp process_message(?D, <<3::big-16, l1::big-32, rest::binary>>, state) do
				<<table::bytes-size(l1), l2::big-32, rest::binary>> = rest
				<<key::bytes-size(l2), _l3::big-32, rest::binary>> = rest

				value = case rest == "" do
					true -> nil # when envelope = 'key_only' is specified
					false -> rest
				end

				state = handle_change(table, key, value, state)
				{:ok, state}
			end
		end
	end

	@doc false
	def maybe_ssl(socket, host, opts, timeout) do
		case Keyword.get(opts, :sslmode) do
			nil -> {:ok, {:gen_tcp, socket}}
			"disable" -> {:ok, {:gen_tcp, socket}}
			"verify-full" -> ssl_connect(socket, host, opts, timeout)
			other -> {:error, RoachFeed.Error.driver("unsupported sslmode", other)}
		end
	end

	defp ssl_connect(socket, host, opts, timeout) do
		tls_opts = [
			verify: :verify_peer,
			cacertfile: Keyword.fetch!(opts, :cacertfile),
			server_name_indication: host,
			customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
		]
		with :ok <- :gen_tcp.send(socket, <<8::big-32, 1234::big-16, 5679::big-16>>),
		     {:ok, "S"} <- :gen_tcp.recv(socket, 1, timeout),
		     {:ok, ssl_socket} <- :ssl.connect(socket, tls_opts, timeout)
		do
			{:ok, {:ssl, ssl_socket}}
		else
			{:ok, "N"} -> {:error, RoachFeed.Error.driver("server refused TLS", nil)}
			err -> err
		end
	end

	@doc false
	def sock_send({:gen_tcp, socket}, data), do: :gen_tcp.send(socket, data)
	def sock_send({:ssl, socket}, data), do: :ssl.send(socket, data)

	@doc false
	def sock_recv({:gen_tcp, socket}, n, timeout), do: :gen_tcp.recv(socket, n, timeout)
	def sock_recv({:ssl, socket}, n, timeout), do: :ssl.recv(socket, n, timeout)

	@doc false
	def sock_setopts({:gen_tcp, socket}, opts), do: :inet.setopts(socket, opts)
	def sock_setopts({:ssl, socket}, opts), do: :ssl.setopts(socket, opts)

	@doc false
	def sock_close({:gen_tcp, socket}), do: :gen_tcp.close(socket)
	def sock_close({:ssl, socket}), do: :ssl.close(socket)

	@doc false
	def authenticate(socket, opts) do
		username = Keyword.get(opts, :username, System.get_env("USER"))
		database = Keyword.get(opts, :database, username)
		payload = <<0, 3, 0, 0, "user", 0, username::binary, 0, "database", 0, database::binary, 0, 0>>
		with :ok <- sock_send(socket, <<(byte_size(payload)+4)::big-32, payload::binary>>)
		do
			finalize_authentication(socket, recv_message(socket), opts)
		end
	end

	# authenticated, nothing else to do
	defp finalize_authentication(_socket, {?R, <<0, 0, 0, 0>>}, _opts), do: :ok

	# asking for plaintext password
	defp finalize_authentication(socket, {?R, <<0, 0, 0, 3>>}, opts) do
		send_password(socket, Keyword.get(opts, :password, ""))
	end

	# asking for hashed password
	defp finalize_authentication(socket, {?R, <<0, 0, 0, 5, salt::binary>>}, opts) do
		hash = :crypto.hash(:md5, Keyword.get(opts, :password, "") <> Keyword.get(opts, :username))
		hash = :crypto.hash(:md5, Base.encode64(hash, case: :lower) <> salt)
		send_password(socket, Base.encode16(hash, case: :lower))
	end

	# asking for SASL authentication (SCRAM-SHA-256)
	defp finalize_authentication(socket, {?R, <<0, 0, 0, 10, mechanisms::binary>>}, opts) do
		case Enum.member?(:binary.split(mechanisms, <<0>>, [:global]), "SCRAM-SHA-256") do
			true -> scram_sha_256(socket, opts)
			false -> {:error, RoachFeed.Error.driver("unsupported authentication type", mechanisms)}
		end
	end

	defp finalize_authentication(_socket, {?R, message}, _opts) do
		{:error, RoachFeed.Error.driver("unsupported authentication type", message)}
	end

	defp finalize_authentication(_socket, {?E, err}, _opts) do
		{:error, RoachFeed.Error.cockroach(err)}
	end

	defp finalize_authentication(_socket, unexpected, _opts) do
		{:error, RoachFeed.Error.driver("unexpected authentication response", unexpected)}
	end

	defp send_password(socket, password) do
		message = build_message(?p, password)
		case send_recv_message(socket, message) do
			{?R, <<0, 0, 0, 0>>} -> :ok
			err -> err
		end
	end

	defp scram_sha_256(socket, opts) do
		password = Keyword.get(opts, :password, "")
		client_nonce = Base.encode64(:crypto.strong_rand_bytes(18))
		client_first_bare = "n=,r=" <> client_nonce
		client_first = "n,," <> client_first_bare
		initial = "SCRAM-SHA-256" <> <<0, byte_size(client_first)::big-32, client_first::binary>>

		with {?R, <<0, 0, 0, 11, server_first::binary>>} <- send_recv_message(socket, <<?p, (byte_size(initial)+4)::big-32, initial::binary>>),
		     attrs = (for <<k, ?=, v::binary>> <- :binary.split(server_first, ",", [:global]), into: %{}, do: {k, v}),
		     server_nonce = attrs[?r],
		     true <- String.starts_with?(server_nonce, client_nonce) || {:error, RoachFeed.Error.driver("SCRAM nonce mismatch", server_nonce)},
		     salted = :crypto.pbkdf2_hmac(:sha256, password, Base.decode64!(attrs[?s]), String.to_integer(attrs[?i]), 32),
		     client_key = :crypto.mac(:hmac, :sha256, salted, "Client Key"),
		     client_final_bare = "c=biws,r=" <> server_nonce,
		     auth_message = client_first_bare <> "," <> server_first <> "," <> client_final_bare,
		     client_signature = :crypto.mac(:hmac, :sha256, :crypto.hash(:sha256, client_key), auth_message),
		     proof = Base.encode64(:crypto.exor(client_key, client_signature)),
		     client_final = client_final_bare <> ",p=" <> proof,
		     {?R, <<0, 0, 0, 12, "v=", server_sig::binary>>} <- send_recv_message(socket, <<?p, (byte_size(client_final)+4)::big-32, client_final::binary>>),
		     server_key = :crypto.mac(:hmac, :sha256, salted, "Server Key"),
		     true <- server_sig == Base.encode64(:crypto.mac(:hmac, :sha256, server_key, auth_message)) || {:error, RoachFeed.Error.driver("SCRAM server signature mismatch", server_sig)},
		     {?R, <<0, 0, 0, 0>>} <- recv_message(socket)
		do
			:ok
		else
			{?E, err} -> {:error, RoachFeed.Error.cockroach(err)}
			{:error, _} = err -> err
			invalid -> {:error, RoachFeed.Error.driver("unexpected SCRAM response", invalid)}
		end
	end

	@doc false
	def send_recv_message(socket, message) do
		case sock_send(socket, message) do
			:ok -> recv_message(socket)
			err -> err
		end
	end

	@doc false
	def recv_message(socket) do
		case recv_n(socket, 5, 5000) do
			{:ok, <<type, length::big-32>>} -> read_message_body(socket, type, length - 4)
			err -> err
		end
	end

	defp read_message_body(_socket, type, 0), do: {type, nil}
	defp read_message_body(socket, type, length) do
		case recv_n(socket, length, 5000) do
			{:ok, message} -> {type, message}
			err -> err
		end
	end

	defp recv_n(socket, n, timeout) do
		case sock_recv(socket, n, timeout) do
			{:ok, data} -> {:ok, data}
			err -> err
		end
	end

	@doc false
	def build_message(type, <<payload::binary>>) do
		# +5 for the length itself + null terminator
		[type, <<(byte_size(payload)+5)::big-32>>, payload, 0]
	end

	@doc false
	def fetch_column_types(socket, schema, table, columns) do
		filter = case columns do
			c when c in [nil, []] -> ""
			c -> [" AND column_name IN (", c |> Enum.map(fn column -> "'#{column}'" end) |> Enum.join(", "), ")"]
		end
		sql = :erlang.iolist_to_binary([
			"SELECT column_name, udt_name FROM information_schema.columns WHERE table_schema = '", schema,
			"' AND table_name = '", table, "'", filter, " ORDER BY ordinal_position"
		])

		parse_describe_sync = [
			build_message(?P, <<0, sql::binary, 0, 0, 0>>),
			<<?D, 0, 0, 0, 6, ?S, 0>>,
			<<?S, 0, 0, 0, 4>>
		]

		bind_execute_close_sync = [
			[?B, 0, 0, 0, 14, 0, 0, 0, 0, <<0::big-16>>, [], 0, 1, 0, 1],
			<<?E, 0, 0, 0, 9, 0, 0, 0, 0, 0>>,
			<<?C, 0, 0, 0, 6, ?S, 0>>,
			<<?S, 0, 0, 0, 4>>
		]

		with {?1, nil} <- send_recv_message(socket, parse_describe_sync),
		     {?t, _} <- recv_message(socket), # parameter info
		     {?T, _} <- recv_message(socket), # column info
		     {?Z, _} <- recv_message(socket), # wait until server is ready
		     :ok <- sock_send(socket, bind_execute_close_sync),
		     {?2, nil} <- recv_message(socket)
		do
			recv_column_types(socket, [])
		else
			{:error, _} = err -> err
			{?E, err} -> {:error, RoachFeed.Error.cockroach(err)}
			invalid -> {:error, RoachFeed.Error.driver("unexpected reply to column type query", invalid)}
		end
	end

	defp recv_column_types(socket, acc) do
		case recv_message(socket) do
			{?D, <<2::big-16, l1::big-32, rest::binary>>} ->
				<<name::bytes-size(l1), l2::big-32, rest::binary>> = rest
				<<udt_name::bytes-size(l2)>> = rest
				recv_column_types(socket, [{name, udt_name} | acc])
			{?C, _} ->
				with {?3, nil} <- recv_message(socket),
				     {?Z, _} <- recv_message(socket)
				do
					{:ok, Enum.reverse(acc)}
				else
					{:error, _} = err -> err
					{?E, err} -> {:error, RoachFeed.Error.cockroach(err)}
					invalid -> {:error, RoachFeed.Error.driver("unexpected reply to column type query", invalid)}
				end
			{?E, err} -> {:error, RoachFeed.Error.cockroach(err)}
			{:error, _} = err -> err
			invalid -> {:error, RoachFeed.Error.driver("unexpected reply to column type query", invalid)}
		end
	end

end
