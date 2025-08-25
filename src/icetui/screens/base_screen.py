"""Base screen with common navigation and search functionality."""

from typing import List, Dict, Any, Optional
from textual.screen import Screen
from textual.containers import Vertical, Horizontal
from textual.widgets import Header, Footer, Static
from textual.message import Message
from textual.binding import Binding

from ..widgets.data_table import FilterableDataTable
from ..widgets.search_input import SearchInput


class BaseListScreen(Screen):
    """Base screen for list-based navigation with filtering."""
    
    BINDINGS = [
        Binding("escape", "go_back", "Back", priority=True),
        Binding("enter", "select_item", "Select", priority=True),
        ("q", "quit", "Quit"),
    ]
    
    class ItemSelected(Message):
        """Message sent when an item is selected."""
        
        def __init__(self, item_data: Dict[str, Any]) -> None:
            super().__init__()
            self.item_data = item_data
    
    class GoBack(Message):
        """Message sent when user wants to go back."""
        pass
    
    def __init__(self, title: str, **kwargs):
        super().__init__(**kwargs)
        self.title = title
        self.data_table: Optional[FilterableDataTable] = None
        self.search_input: Optional[SearchInput] = None
        
    def compose(self):
        """Compose the screen layout."""
        with Vertical():
            yield Header()
            yield Static(self.title, id="screen-title")
            yield SearchInput(id="search-input")
            yield FilterableDataTable(id="data-table")
            yield Footer()
    
    def on_mount(self) -> None:
        """Initialize screen components after mount."""
        self.data_table = self.query_one("#data-table", FilterableDataTable)
        self.search_input = self.query_one("#search-input", SearchInput)
        
        # Focus search input by default
        self.search_input.focus()
        
        # Load initial data
        self.load_data()
    
    def on_search_input_filter_changed(self, event: SearchInput.FilterChanged) -> None:
        """Handle search filter changes."""
        if self.data_table:
            self.data_table.set_filter(event.filter_text)
    
    def action_select_item(self) -> None:
        """Handle item selection."""
        if self.data_table:
            selected_row = self.data_table.get_selected_row()
            if selected_row:
                self.post_message(self.ItemSelected(selected_row))
    
    def action_go_back(self) -> None:
        """Handle going back."""
        self.post_message(self.GoBack())
    
    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()
    
    def load_data(self) -> None:
        """Load data for this screen. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement load_data()")
    
    def set_data(self, columns: List[str], rows: List[Dict[str, Any]]) -> None:
        """Set the data for the table."""
        if self.data_table:
            self.data_table.set_data(columns, rows)
            
    def on_key(self, event) -> None:
        """Handle key events."""
        # If search input is focused, let it handle typing
        if self.search_input and self.search_input.has_focus:
            return
            
        # If it's a regular character, focus search and pass the key
        if event.character and event.character.isprintable():
            self.search_input.focus()
            # Let the search input handle the character
            return
            
        # Handle arrow keys for table navigation
        if event.key == "down":
            if self.data_table:
                self.data_table.focus()
        elif event.key == "up": 
            if self.data_table:
                self.data_table.focus()