from abc import ABC, abstractmethod
from pathlib import Path


class AbstractSource(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def import_file(self, path: Path):
        pass

    @abstractmethod
    def export_file(self, path: Path):
        pass
