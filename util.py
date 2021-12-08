from abc import ABC, abstractmethod
from pathlib import Path

class AbstractSource(ABC):
    @abstractmethod
    def import_file(self, path: Path, **kwargs):
        pass

    @abstractmethod
    def export_file(self, path: Path, **kwargs):
        pass