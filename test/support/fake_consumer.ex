defmodule RoachFeed.Tests.FakeConsumer do
	use RoachFeed

	defp setup(opts) do
		state = {1, Keyword.fetch!(opts, :test), Keyword.fetch!(opts, :change_feed)}
		{state, opts}
	end

	defp query({count, pid, change_feed} = _state) do
		state = {count, pid} # we don't need the change_feed anymore
		{state, change_feed}
	end

	defp handle_resolved(msg, {count, pid}) do
		send(pid, {:resolved, Jason.decode!(msg, keys: :atoms)})
		{count + 1, pid}
	end

	defp handle_change(table, key, data, {count, pid}) do
		send(pid, {:change, %{table: table, key: Jason.decode!(key), data: Jason.decode!(data, keys: :atoms)}})
		{count + 1, pid}
	end

	def handle_cast({:test, pid}, {count, _}) do
		{:noreply, {count, pid}}
	end
end
