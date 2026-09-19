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

				sql = ["experimental changefeed for ", config |> Keyword.fetch!(:for) |> List.wrap() |> Enum.join(", ")]
				{sql, values} = case config[:with] do
					nil -> {sql, []}
					w ->
						{w, values, _} = Enum.reduce(w, {[], [], 1}, fn
							{:cursor, nil}, acc -> acc  # crdb doesn't support a nil cursor, just don't add the option
							{key, value}, {w, values, index} -> {[", #{key} = $#{index}", w], [value | values], index + 1}
						end)

						sql = case :erlang.iolist_to_binary(w) do
							"" -> sql
							<<", ", w::binary>> -> [sql, " with ", w]
						end
						{sql, Enum.reverse(values)}
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

				with {?1, nil} <- RoachFeed.send_recv_message(socket, parse_describe_sync),
				     {?t, _} <- RoachFeed.recv_message(socket), # parameter info
				     {?T, _} <- RoachFeed.recv_message(socket), # column info
				     {?Z, _} <- RoachFeed.recv_message(socket), # wait until server is ready
				     :ok <- RoachFeed.sock_send(socket, bind_execute_close_sync)
				do
					{:ok, state}
				else
					{:error, _} = err -> err
					{?E, err} -> {:error, RoachFeed.Error.cockroach(err)}
					invalid -> {:error, RoachFeed.Error.driver("unexpected reply to parse+describe+sync", invalid)}
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

end
