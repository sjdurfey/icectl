"""Filterable data table widget for TUI."""

from typing import List, Dict, Any, Optional
from textual.widgets import DataTable
from textual.reactive import reactive


class FilterableDataTable(DataTable):
    """Data table with built-in filtering capability."""
    
    filter_text: reactive[str] = reactive("")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._all_rows: List[Dict[str, Any]] = []
        self._filtered_rows: List[Dict[str, Any]] = []
        self.cursor_type = "row"
        
    def set_data(self, columns: List[str], rows: List[Dict[str, Any]]) -> None:
        """Set the data for the table."""
        self._all_rows = rows.copy()
        self.clear(columns=True)  # Clear both rows and columns
        
        # Add columns
        for col in columns:
            self.add_column(col, key=col)
        
        # Apply current filter
        self.apply_filter()
    
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
        
        # Update table display
        self.clear(columns=False)  # Keep columns, clear rows
        
        
        for i, row in enumerate(self._filtered_rows):
            # Get column keys - the columns ARE the ColumnKey objects, not col.key
            column_keys = []
            for col in self.columns:
                # The column itself is the ColumnKey object with a .value attribute
                if hasattr(col, 'value'):
                    column_keys.append(col.value)
                else:
                    column_keys.append(str(col))
            
            row_values = [str(row.get(key, "")) for key in column_keys]
            self.add_row(*row_values)
    
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