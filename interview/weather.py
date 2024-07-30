import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Generator, Iterable

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Constants
class MessageTypes:
    SAMPLE_TYPE = "sample"
    CONTROL_TYPE = "control"
    SNAPSHOT_COMMAND = "snapshot"
    RESET_COMMAND = "reset"


# Keys
STATION_NAME = "stationName"
TEMPERATURE = "temperature"
TIMESTAMP = "timestamp"
SAMPLE_KEYS = {STATION_NAME, TEMPERATURE, TIMESTAMP}


@dataclass
class WeatherStation:
    name: str
    high: float = field(default=float("-inf"))
    low: float = field(default=float("inf"))

    def update(self, temperature: float) -> None:
        """Update the high and low temperatures for the station."""
        self.high = max(self.high, temperature)
        self.low = min(self.low, temperature)

        logger.info(f"Updated {self.name}: high={self.high}, low={self.low}")

    def to_dict(self) -> dict[str, float]:
        """Convert the WeatherStation object to a dictionary."""
        return asdict(self)


class WeatherDataProcessor:
    def __init__(self):
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
            "type": MessageTypes.SNAPSHOT_COMMAND,
            "asOf": self.latest_timestamp,
            "stations": {
                name: station.to_dict() for name, station in self.stations.items()
            },
        }

        logger.info(f"Generated snapshot: {snapshot}")
        return snapshot

    def process_reset(self) -> dict[str, Any]:
        """Reset all weather station data."""
        if not self.stations:
            logger.warning("Attempted to reset with no data")
            raise ValueError("No data to reset.")

        response = {"type": MessageTypes.RESET_COMMAND, "asOf": self.latest_timestamp}
        self.reset_weather_data()
        return response

    def reset_weather_data(self) -> None:
        self.stations.clear()
        self.latest_timestamp = None
        logger.info("Reset all weather station data")

    def handle_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        """Handle incoming messages and route them to appropriate processing methods."""
        match message:
            case {"type": MessageTypes.SAMPLE_TYPE}:
                self.process_sample(message)
                return None
            case {
                "type": MessageTypes.CONTROL_TYPE,
                "command": MessageTypes.SNAPSHOT_COMMAND,
            }:
                return self.process_snapshot()
            case {
                "type": MessageTypes.CONTROL_TYPE,
                "command": MessageTypes.RESET_COMMAND,
            }:
                return self.process_reset()
            case {"type": unknown_type}:
                msg = f"Unknown message type: {unknown_type}"
                logger.error(msg)
                raise ValueError(msg)
            case _:
                msg = f"Unhandled message format: {message}"
                logger.error(msg)
                raise ValueError(msg)

    def update_station(self, station_name: str, temperature: int | float) -> None:
        if station_name not in self.stations:
            self.stations[station_name] = WeatherStation(station_name)
            logger.info(f"Created new weather station: station_name")

        self.stations[station_name].update(temperature)

    def update_latest_timestamp(self, timestamp: int) -> None:
        if self.latest_timestamp is None or timestamp > self.latest_timestamp:
            self.latest_timestamp = timestamp
            logger.info(f"Updated latest timestamp to {self.latest_timestamp}")

    def validate_weather_sample(self, sample: dict[str, Any]) -> None:
        if not all(key in sample for key in SAMPLE_KEYS):
            raise ValueError(
                f"Invalid sample format - missing one or more key(s) {SAMPLE_KEYS}."
            )

        if not isinstance(sample["temperature"], (int, float)):
            raise ValueError(
                f"Invalid temperature type - expected (int, float), got {type(sample['temperature'])}"
            )
        if not isinstance(sample["timestamp"], int):
            raise ValueError(
                f"Invalid timestamp type - expected int, got {type(sample['timestamp'])}"
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
