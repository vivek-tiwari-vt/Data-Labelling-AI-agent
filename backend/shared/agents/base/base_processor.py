
from abc import ABC, abstractmethod
from typing import Dict, Any

class BaseProcessor(ABC):
    """Abstract base class for all agent processors."""

    @abstractmethod
    async def process(self, content: str, job_id: str, task_id: str) -> Dict[str, Any]:
        """Processes the given content and returns a result."""
        pass


