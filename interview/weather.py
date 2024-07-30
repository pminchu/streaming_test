from typing import Any, Dict, Generator, Iterable, Optional
import json
import logging
from dataclasses import dataclass, field

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SAMPLE_TYPE = "sample"
CONTROL_TYPE = "control"
SNAPSHOT_COMMAND = "snapshot"
RESET_COMMAND = "reset"

@dataclass
class WeatherStation:
    name: str
    high: float = field(default=float('-inf'))
    low: float = field(default=float('inf'))

    def update(self, temperature: float) -> None:
        """Update the high and low temperatures for the station."""
        self.high = max(self.high, temperature)
        self.low = min(self.low, temperature)
        logger.info(f"Updated {self.name}: high={self.high}, low={self.low}")

    def to_dict(self) -> Dict[str, float]:
        """Convert the WeatherStation object to a dictionary."""
        return {"high": self.high, "low": self.low}

    def __repr__(self) -> str:
        """Return a string representation of the WeatherStation object."""
        return f"WeatherStation(name='{self.name}', high={self.high}, low={self.low})"

class WeatherDataProcessor:
    def __init__(self):
        self.stations: Dict[str, WeatherStation] = {}
        self.latest_timestamp: Optional[int] = None

    def process_sample(self, message: Dict[str, Any]) -> None:
        """Process a sample message and update the corresponding weather station."""
        required_keys = ["stationName", "temperature", "timestamp"]
        if not all(key in message for key in required_keys):
            raise ValueError("Invalid sample message format.")

        station_name = message["stationName"]
        temperature = message["temperature"]
        timestamp = message["timestamp"]

        if not isinstance(temperature, (int, float)) or not isinstance(timestamp, int):
            raise ValueError("Invalid temperature or timestamp type.")

        if station_name not in self.stations:
            self.stations[station_name] = WeatherStation(station_name)
            logger.info(f"Created new weather station: {station_name}")
        self.stations[station_name].update(temperature)
        
        if self.latest_timestamp is None or timestamp > self.latest_timestamp:
            self.latest_timestamp = timestamp
            logger.info(f"Updated latest timestamp to {self.latest_timestamp}")

    def process_snapshot(self) -> Dict[str, Any]:
        """Generate a snapshot of all weather stations."""
        if not self.stations:
            logger.warning("Attempted to create snapshot with no data")
            raise ValueError("No data to snapshot.")

        snapshot = {
            "type": SNAPSHOT_COMMAND,
            "asOf": self.latest_timestamp,
            "stations": {name: station.to_dict() for name, station in self.stations.items()}
        }
        logger.info(f"Generated snapshot: {snapshot}")
        return snapshot

    def process_reset(self) -> Dict[str, Any]:
        """Reset all weather station data."""
        if not self.stations:
            logger.warning("Attempted to reset with no data")
            raise ValueError("No data to reset.")

        response = {
            "type": RESET_COMMAND,
            "asOf": self.latest_timestamp
        }
        self.stations.clear()
        self.latest_timestamp = None
        logger.info("Reset all weather station data")
        return response

    def handle_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle incoming messages and route them to appropriate processing methods."""
        if 'type' not in message:
            logger.error("Received message without type")
            raise ValueError("Message type is required.")

        if message['type'] == SAMPLE_TYPE:
            self.process_sample(message)
            return None
        elif message['type'] == CONTROL_TYPE:
            if 'command' not in message:
                logger.error("Received control message without command")
                raise ValueError("Control command is required.")

            if message['command'] == SNAPSHOT_COMMAND:
                return self.process_snapshot()
            elif message['command'] == RESET_COMMAND:
                return self.process_reset()
            else:
                logger.error(f"Unknown control command: {message['command']}")
                raise ValueError(f"Unknown control command: {message['command']}.")
        else:
            logger.error(f"Unknown message type: {message['type']}")
            raise ValueError(f"Unknown message type: {message['type']}.")


def process_events(events: Iterable[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
    """Process a stream of events and yield results for control messages."""
    processor = WeatherDataProcessor()
    
    for event in events:
        result = processor.handle_message(event)
        if result:
            yield result