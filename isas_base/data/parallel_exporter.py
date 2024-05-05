import logging
import queue
import threading

from .data_manager import DataManager

logger = logging.getLogger(__name__)


class ParallelExporter(threading.Thread):
    """Dynamic data exporter in parallel processing for streaming."""

    def __init__(
            self,
            queue: queue,
            data_manager: DataManager,
            table_data_system: str = 'postgres',
            time_series_data_system: str = 'influx',
            ) -> None:
        """Initialize ParallelExporter.

        Args:
            queue: A queue in which data is stored.
            data_manager: An instance of DataManager.
            table_data_system: A data sytem for table data.
            time_series_data_system: A data sytem for time-series data.
        """
        logger.info(f'[ParallelExporter] Initializing with args: {table_data_system=}, {time_series_data_system=}')
        super().__init__()
        self.queue = queue
        self.data_manager = data_manager
        self.table_data_system = table_data_system
        self.time_series_data_system = time_series_data_system
        self.exit = False

    def run(self) -> None:
        """Export data."""
        logger.info('[ParallelExporter] Running.')
        while True:
            try:
                data_name, dynamic_data = self.queue.get(timeout=1)
                logger.debug(f'[ParallelExporter] Exporting {data_name}.')
                self.data_manager.export_dynamic_data(
                    dynamic_data=dynamic_data,
                    data_name=data_name,
                    table_data_system=self.table_data_system,
                    time_series_data_system=self.time_series_data_system,
                    )
                logger.debug(f'[ParallelExporter] Export {data_name} is finished.')
            except queue.Empty:
                pass
            if self.exit and self.queue.empty():
                logger.info('[ParallelExporter] Exit.')
                break
