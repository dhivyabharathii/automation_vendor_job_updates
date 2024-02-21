  defmodule Jobs do
    @moduledoc """
    Module for processing job updates.
    """

    @doc """
    Reads content in the file

    Returns `:ok`

    ## Examples
    iex(1)> file_path="final.txt"
    "final.txt"
    iex(2)> Jobs.read(file_path)
    {:ok, "contents in the file"}
    """
    def read(file_path) do
      File.read(file_path)
    end

    @doc """
    Filters updates based on farm ID.
    """
    def filter_updates(updates) do
      Enum.filter(updates, fn update ->
        update["farm_id"] == 3
      end)
    end

    @doc """
    Calculates the time difference between update and job timestamps.
    """
    def calculate_time_difference(updates) do
      Enum.map(updates, fn update ->
        timestamp = parse_iso8601(update["timestamp"])
        jobs = Enum.map(update["jobs"], fn job ->
          job_timestamp = parse_iso8601(job["UpdatedAt"])
          time_difference_in_seconds = NaiveDateTime.diff(job_timestamp,timestamp)
          {abs(time_difference_in_seconds)}
        end)
      end)
    end

    defp parse_iso8601(timestamp) do
      case NaiveDateTime.from_iso8601(timestamp) do
        {:ok, dt} -> dt
        :error -> nil
      end
    end

    
    def format_file(input_file, output_file) do
      input_stream = File.stream!(input_file)
      output_stream = File.open!(output_file, [:write])

      Enum.each(input_stream, fn line ->
        parts = String.split(line, "\t")
        output = Enum.join(parts, "\n")
        output =
          if String.ends_with?(line, "\n") do
            output
          else
            output <> "\n"
          end

        IO.write(output_stream, output)
      end)

      File.close(output_stream)
    end
  end
  # file_path = "final.txt"
  # {:ok, file_content} = Jobs.read(file_path)
  # updates = Jason.decode!(file_content)
  # filtered_updates = Jobs.filter_updates(updates)
  # IO.inspect(filtered_updates)
  # time_difference=Jobs.calculate_time_difference(filtered_updates)
  # IO.inspect(time_difference)
  Jobs.format_file("v1_jobs_1h.txt","output.txt")
