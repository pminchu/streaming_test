"""Module for processing weather data from multiple stations."""

import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Generator, Iterable
from .constants import MESSAGE_TYPE_SAMPLE, MESSAGE_TYPE_CONTROL, COMMAND_SNAPSHOT, COMMAND_RESET

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Weather sample keys
STATION_NAME = "stationName"
TEMPERATURE = "temperature"
TIMESTAMP = "timestamp"
SAMPLE_KEYS = {STATION_NAME, TEMPERATURE, TIMESTAMP}


@dataclass
class WeatherStation:
    """Represents a weather station with high and low temperature records."""

    name: str
    high: float = field(default=float("-inf"))
    low: float = field(default=float("inf"))

    def update(self, temperature: float) -> None:
        """Update the high and low temperatures for the station."""
        self.high = max(self.high, temperature)
        self.low = min(self.low, temperature)

        logger.info("Updated %s: high=%s, low=%s", self.name, self.high, self.low)

    def to_dict(self) -> dict[str, float]:
        """Convert the WeatherStation object to a dictionary."""
        return asdict(self)


class WeatherDataProcessor:
    """Processes weather data samples and generates snapshots."""

    def __init__(self) -> None:
        self.stations: dict[str, WeatherStation] = {}
        self.latest_timestamp: int | None = None

    def process_sample(self, sample: dict[str, Any]) -> None:
        """Process a sample message and update the corresponding weather station."""
        self.validate_weather_sample(sample)

        station_name = sample[STATION_NAME]
        temperature = sample[TEMPERATURE]
        timestamp = sample[TIMESTAMP]

        self.update_station(station_name, temperature)
        self.update_latest_timestamp(timestamp)

    def process_snapshot(self) -> dict[str, Any]:
        """Generate a snapshot of all weather stations."""
        if not self.stations:
            logger.warning("Attempted to create snapshot with no data")
            raise ValueError("No data to snapshot.")

        snapshot = {
            "type": COMMAND_SNAPSHOT,
            "asOf": self.latest_timestamp,
            "stations": {
                name: station.to_dict() for name, station in self.stations.items()
            },
        }

        logger.info("Generated snapshot: %s", snapshot)
        return snapshot

    def process_reset(self) -> dict[str, Any]:
        """Reset all weather station data."""
        if not self.stations:
            logger.warning("Attempted to reset with no data")
            raise ValueError("No data to reset.")

        response = {"type": COMMAND_RESET, "asOf": self.latest_timestamp}
        self.reset_weather_data()
        return response

    def reset_weather_data(self) -> None:
        """Clear all weather station data and reset the latest timestamp."""
        self.stations.clear()
        self.latest_timestamp = None
        logger.info("Reset all weather station data")

    def handle_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        """Handle incoming messages and route them to appropriate processing methods."""
        if "type" not in message:
            raise ValueError("Message is missing 'type' field")

        if message["type"] == MESSAGE_TYPE_SAMPLE:
            self.process_sample(message)
            return None
        if message["type"] == MESSAGE_TYPE_CONTROL:
            if "command" not in message:
                raise ValueError("Control message is missing 'command' field")
            if message["command"] == COMMAND_SNAPSHOT:
                return self.process_snapshot()
            if message["command"] == COMMAND_RESET:
                return self.process_reset()
            raise ValueError(f"Unknown control command: {message['command']}")
        raise ValueError(f"Unknown message type: {message['type']}")

    def update_station(self, station_name: str, temperature: int | float) -> None:
        """Update or create a weather station with new temperature data."""
        if station_name not in self.stations:
            self.stations[station_name] = WeatherStation(station_name)
            logger.info("Created new weather station: %s", station_name)

        self.stations[station_name].update(temperature)

    def update_latest_timestamp(self, timestamp: int) -> None:
        """Update the latest timestamp if the new timestamp is more recent."""
        if self.latest_timestamp is None or timestamp > self.latest_timestamp:
            self.latest_timestamp = timestamp
            logger.info("Updated latest timestamp to %s", self.latest_timestamp)

    def validate_weather_sample(self, sample: dict[str, Any]) -> None:
        """Validate the format and types of a weather sample."""
        if not all(key in sample for key in SAMPLE_KEYS):
            raise ValueError(
                f"Invalid sample format - missing one or more key(s) {SAMPLE_KEYS}."
            )

        if not isinstance(sample["temperature"], (int, float)):
            raise ValueError(
                f"Invalid temperature type - expected (int, float), "
                f"got {type(sample['temperature'])}"
            )
        if not isinstance(sample["timestamp"], int):
            raise ValueError(
                f"Invalid timestamp type - expected int, "
                f"got {type(sample['timestamp'])}"
            )


def process_events(
    events: Iterable[dict[str, Any]]
) -> Generator[dict[str, Any], None, None]:
    """Process a stream of events and yield results for control messages."""
    processor = WeatherDataProcessor()

    for event in events:
        result = processor.handle_message(event)
        if result:
            yield result
