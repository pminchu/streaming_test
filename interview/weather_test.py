import unittest

from .weather import WeatherDataProcessor, WeatherStation


class TestWeatherDataProcessor(unittest.TestCase):

    def setUp(self):
        self.processor = WeatherDataProcessor()
        self.sample_message = {
            "type": "sample",
            "stationName": "Foster Weather Station",
            "timestamp": 1672531200000,
            "temperature": 37.1,
        }
        self.snapshot_message = {"type": "control", "command": "snapshot"}
        self.reset_message = {"type": "control", "command": "reset"}

    def _process_sample(self, message=None):
        if message is None:
            message = self.sample_message
        self.processor.handle_message(message)

    def test_process_sample(self):
        self._process_sample()
        station_data = self.processor.stations["Foster Weather Station"]
        self.assertEqual(station_data.high, 37.1)
        self.assertEqual(station_data.low, 37.1)
        self.assertEqual(self.processor.latest_timestamp, 1672531200000)

    def test_process_multiple_samples_same_station(self):
        self._process_sample()
        self._process_sample(
            {
                "type": "sample",
                "stationName": "Foster Weather Station",
                "timestamp": 1672531300000,
                "temperature": 35.0,
            }
        )
        self._process_sample(
            {
                "type": "sample",
                "stationName": "Foster Weather Station",
                "timestamp": 1672531400000,
                "temperature": 40.0,
            }
        )
        station_data = self.processor.stations["Foster Weather Station"]
        self.assertEqual(station_data.high, 40.0)
        self.assertEqual(station_data.low, 35.0)
        self.assertEqual(self.processor.latest_timestamp, 1672531400000)

    def test_process_multiple_stations(self):
        self._process_sample()
        self._process_sample(
            {
                "type": "sample",
                "stationName": "Oak Street Weather Station",
                "timestamp": 1672531300000,
                "temperature": 32.0,
            }
        )
        self._process_sample(
            {
                "type": "sample",
                "stationName": "North Avenue Weather Station",
                "timestamp": 1672531400000,
                "temperature": 45.0,
            }
        )
        foster_data = self.processor.stations["Foster Weather Station"]
        oak_data = self.processor.stations["Oak Street Weather Station"]
        north_data = self.processor.stations["North Avenue Weather Station"]
        self.assertEqual(foster_data.high, 37.1)
        self.assertEqual(foster_data.low, 37.1)
        self.assertEqual(oak_data.high, 32.0)
        self.assertEqual(oak_data.low, 32.0)
        self.assertEqual(north_data.high, 45.0)
        self.assertEqual(north_data.low, 45.0)
        self.assertEqual(self.processor.latest_timestamp, 1672531400000)

    def test_process_snapshot(self):
        self._process_sample()
        snapshot = self.processor.handle_message(self.snapshot_message)
        self.assertEqual(snapshot["type"], "snapshot")
        self.assertEqual(snapshot["asOf"], 1672531200000)
        self.assertEqual(snapshot["stations"]["Foster Weather Station"]["high"], 37.1)
        self.assertEqual(snapshot["stations"]["Foster Weather Station"]["low"], 37.1)

    def test_process_reset(self):
        self._process_sample()
        reset = self.processor.handle_message(self.reset_message)
        self.assertEqual(reset["type"], "reset")
        self.assertEqual(reset["asOf"], 1672531200000)
        self.assertEqual(len(self.processor.stations), 0)
        self.assertIsNone(self.processor.latest_timestamp)

    def test_snapshot_after_reset(self):
        self._process_sample()
        self.processor.handle_message(self.reset_message)
        self._process_sample(
            {
                "type": "sample",
                "stationName": "New Station",
                "timestamp": 1672531500000,
                "temperature": 50.0,
            }
        )
        snapshot = self.processor.handle_message(self.snapshot_message)
        self.assertEqual(snapshot["type"], "snapshot")
        self.assertEqual(snapshot["asOf"], 1672531500000)
        self.assertEqual(snapshot["stations"]["New Station"]["high"], 50.0)
        self.assertEqual(snapshot["stations"]["New Station"]["low"], 50.0)

    def test_reset_without_samples(self):
        with self.assertRaises(Exception):
            self.processor.handle_message(self.reset_message)

    def test_snapshot_without_samples(self):
        with self.assertRaises(Exception):
            self.processor.handle_message(self.snapshot_message)

    def test_unknown_message_type(self):
        unknown_message = {"type": "unknown", "data": "test"}
        with self.assertRaises(ValueError):
            self.processor.handle_message(unknown_message)

    def test_unknown_control_command(self):
        unknown_command = {"type": "control", "command": "unknown"}
        with self.assertRaises(ValueError):
            self.processor.handle_message(unknown_command)

    def test_missing_message_type(self):
        missing_type_message = {
            "stationName": "Station 1",
            "timestamp": 1672531200000,
            "temperature": 37.1,
        }
        with self.assertRaises(ValueError):
            self.processor.handle_message(missing_type_message)

    def test_missing_control_command(self):
        missing_command_message = {"type": "control"}
        with self.assertRaises(ValueError):
            self.processor.handle_message(missing_command_message)

    def test_extreme_temperatures(self):
        self._process_sample(
            {
                "type": "sample",
                "stationName": "Extreme Station",
                "timestamp": 1672531200000,
                "temperature": -100.0,
            }
        )
        self._process_sample(
            {
                "type": "sample",
                "stationName": "Extreme Station",
                "timestamp": 1672531300000,
                "temperature": 150.0,
            }
        )
        station_data = self.processor.stations["Extreme Station"]
        self.assertEqual(station_data.high, 150.0)
        self.assertEqual(station_data.low, -100.0)

    def test_extreme_timestamps(self):
        self._process_sample(
            {
                "type": "sample",
                "stationName": "Timestamp Station",
                "timestamp": 0,
                "temperature": 0.0,
            }
        )
        self._process_sample(
            {
                "type": "sample",
                "stationName": "Timestamp Station",
                "timestamp": 9999999999999,
                "temperature": 100.0,
            }
        )
        station_data = self.processor.stations["Timestamp Station"]
        self.assertEqual(station_data.high, 100.0)
        self.assertEqual(station_data.low, 0.0)
        self.assertEqual(self.processor.latest_timestamp, 9999999999999)


class TestWeatherStation(unittest.TestCase):
    def setUp(self):
        self.station = WeatherStation("Test Station")

    def test_initial_values(self):
        self.assertEqual(self.station.name, "Test Station")
        self.assertEqual(self.station.high, float("-inf"))
        self.assertEqual(self.station.low, float("inf"))

    def test_update_single_temperature(self):
        self.station.update(20.5)
        self.assertEqual(self.station.high, 20.5)
        self.assertEqual(self.station.low, 20.5)

    def test_update_multiple_temperatures(self):
        temperatures = [20.5, 18.0, 22.5, 19.0]
        for temp in temperatures:
            self.station.update(temp)
        self.assertEqual(self.station.high, 22.5)
        self.assertEqual(self.station.low, 18.0)

    def test_to_dict(self):
        self.station.update(20.5)
        self.assertEqual(
            self.station.to_dict(),
            {"name": self.station.name, "high": 20.5, "low": 20.5},
        )

    def test_repr(self):
        self.station.update(20.5)
        expected_repr = "WeatherStation(name='Test Station', high=20.5, low=20.5)"
        self.assertEqual(repr(self.station), expected_repr)


if __name__ == "__main__":
    unittest.main()
