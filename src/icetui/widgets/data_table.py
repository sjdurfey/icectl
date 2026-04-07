"""Filterable data table widget for TUI."""

import logging
from typing import List, Dict, Any, Optional, Set
from textual.widgets import DataTable
from textual.reactive import reactive

logger = logging.getLogger(__name__)


class FilterableDataTable(DataTable):
    """Data table with built-in filtering capability."""

    filter_text: reactive[str] = reactive("")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._all_rows: List[Dict[str, Any]] = []
        self._filtered_rows: List[Dict[str, Any]] = []
        self._highlighted_row_indices: Set[int] = set()
        self.cursor_type = "row"
        
    def set_data(
        self,
        columns: List[str],
        rows: List[Dict[str, Any]],
        column_widths: Optional[Dict[str, int]] = None,
    ) -> None:
        """Set the data for the table."""
        self._all_rows = rows.copy()
        self.clear(columns=True)  # Clear both rows and columns

        # Add columns, optionally with explicit widths to avoid Rich Text sizing issues
        for col in columns:
            width = column_widths.get(col) if column_widths else None
            self.add_column(col, key=col, width=width)

        # Apply current filter
        self.apply_filter()
    
    @staticmethod
    def _coerce_cell(value: Any) -> Any:
        """Return Rich Text as-is; stringify everything else."""
        from rich.text import Text
        if isinstance(value, Text):
            return value
        return str(value) if value is not None else ""

    def _get_column_keys(self) -> List[str]:
        """Return the string key for each column in display order."""
        keys = []
        for col in self.columns:
            keys.append(col.value if hasattr(col, "value") else str(col))
        return keys

    def apply_filter(self) -> None:
        """Apply current filter to the data."""
        if not self.filter_text:
            self._filtered_rows = self._all_rows.copy()
        else:
            # Case-insensitive search across all string values
            filter_lower = self.filter_text.lower()
            self._filtered_rows = []

            for row in self._all_rows:
                # Check if filter matches any string value in the row
                for value in row.values():
                    if isinstance(value, str) and filter_lower in value.lower():
                        self._filtered_rows.append(row)
                        break

        # Reset highlights whenever the row set changes
        self._highlighted_row_indices = set()

        # Update table display
        self.clear(columns=False)  # Keep columns, clear rows

        column_keys = self._get_column_keys()
        for i, row in enumerate(self._filtered_rows):
            row_values = [self._coerce_cell(row.get(key, "")) for key in column_keys]
            self.add_row(*row_values, key=str(i))
    
    def highlight_cells(self, coords: Set[tuple]) -> None:
        """Highlight specific cells with a distinct style.

        coords is a set of (row_index, col_key) pairs. Pass an empty set to clear.
        """
        from rich.text import Text

        # Reset previously highlighted cells
        for row_i, col_key in self._highlighted_row_indices:
            if row_i < len(self._filtered_rows):
                value = self._coerce_cell(self._filtered_rows[row_i].get(col_key, ""))
                try:
                    self.update_cell(str(row_i), col_key, value, update_width=False)
                except Exception:
                    pass

        self._highlighted_row_indices = coords

        for row_i, col_key in coords:
            if row_i < len(self._filtered_rows):
                value = str(self._filtered_rows[row_i].get(col_key, ""))
                try:
                    self.update_cell(
                        str(row_i), col_key,
                        Text.assemble((value, "bold on dark_orange")),
                        update_width=False,
                    )
                except Exception as exc:
                    logger.error("update_cell failed row=%d col=%s: %s", row_i, col_key, exc)

    def set_filter(self, filter_text: str) -> None:
        """Set filter text and refresh display."""
        self.filter_text = filter_text
        self.apply_filter()
    
    def get_selected_row(self) -> Optional[Dict[str, Any]]:
        """Get the currently selected row data."""
        if not self._filtered_rows or self.cursor_row < 0:
            return None
        
        row_index = self.cursor_row
        if row_index >= len(self._filtered_rows):
            return None
            
        return self._filtered_rows[row_index]