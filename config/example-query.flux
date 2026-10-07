// InfluxDB 2 UI > Data Explorer > Script Editor
from(bucket: "sensors")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "raw_sensor_data")
  |> filter(fn: (r) => r._field == "temperature")

// Processed numeric fields have matching *_valid flags.
// For exports/analysis use dashboard CSV, which preserves null values.
