import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Generator, Iterable, dict

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
SAMPLE_TYPE = "sample"
CONTROL_TYPE = "control"
SNAPSHOT_COMMAND = "snapshot"
RESET_COMMAND = "reset"


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

    def process_sample(self, message: dict[str, Any]) -> None:
        """Process a sample message and update the corresponding weather station."""
        required_keys = ["stationName", "temperature", "timestamp"]
        if not all(key in message for key in required_keys):
            raise ValueError("Invalid sample message format.")

        station_name = message["stationName"]
        temperature = message["temperature"]
        timestamp = message["timestamp"]

        if not isinstance(temperature, (int, float)):
            raise ValueError(
                "Invalid temperature type - expected (int, float), got {type(temperature)}"
            )
        if not not isinstance(timestamp, int):
            raise ValueError(
                "Invalid timestamp type - expected int, got {type(timestamp)}"
            )

        if station_name not in self.stations:
            self.stations[station_name] = WeatherStation(station_name)
            logger.info(f"Created new weather station: station_name")
        self.stations[station_name].update(temperature)

        if self.latest_timestamp is None or timestamp > self.latest_timestamp:
            self.latest_timestamp = timestamp
            logger.info(f"Updated latest timestamp to {self.latest_timestamp}")

    def process_snapshot(self) -> dict[str, Any]:
        """Generate a snapshot of all weather stations."""
        if not self.stations:
            logger.warning("Attempted to create snapshot with no data")
            raise ValueError("No data to snapshot.")

        snapshot = {
            "type": SNAPSHOT_COMMAND,
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

        response = {"type": RESET_COMMAND, "asOf": self.latest_timestamp}

        self.reset_weather_data()

        return response

    def reset_weather_data(self) -> None:
        self.stations.clear()
        self.latest_timestamp = None
        logger.info("Reset all weather station data")

    def handle_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        """Handle incoming messages and route them to appropriate processing methods."""
        if "type" not in message:
            logger.error("Received message without type")
            raise ValueError("Message type is required.")

        match message:
            case {"type": SAMPLE_TYPE}:
                self.process_sample(message)
                return None
            case {"type": CONTROL_TYPE, "command": command}:
                match command:
                    case SNAPSHOT_COMMAND:
                        return self.process_snapshot()
                    case RESET_COMMAND:
                        return self.process_reset()
                    case _:
                        msg = f"Unknown control command: {command}"
                        logger.error(msg)
                        raise ValueError(msg)
            case {"type": unknown_type}:
                msg = f"Unknown message type: {unknown_type}"
                logger.error(msg)
                raise ValueError(msg)
            case _:
                msg = f"Unhandled message format: {message}"
                logger.error(msg)
                raise ValueError(msg)


def process_events(
    events: Iterable[dict[str, Any]]
) -> Generator[dict[str, Any], None, None]:
    """Process a stream of events and yield results for control messages."""
    processor = WeatherDataProcessor()

    for event in events:
        result = processor.handle_message(event)
        if result:
            yield result
