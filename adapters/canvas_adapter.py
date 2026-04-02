"""Canvas adapter for formatting output."""
from typing import Any, Dict

from domain.content.canvas_builder import CanvasBuilder


class CanvasAdapter:
    """Adapter for Canvas operations."""

    def __init__(self):
        """Initialize canvas adapter."""
        self.builder = CanvasBuilder()

    def build_gm_review(self, account_data: Dict[str, Any]) -> str:
        """Build GM review canvas content."""
        return self.builder.build_gm_review(account_data)
