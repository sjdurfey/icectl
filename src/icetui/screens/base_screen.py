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
        ("/", "focus_search", "Search"),
        ("s", "focus_search", "Search"),
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
        
        # Focus data table by default for navigation
        self.data_table.focus()
        
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
    
    def action_focus_search(self) -> None:
        """Focus the search input."""
        if self.search_input:
            self.search_input.focus()
    
    def load_data(self) -> None:
        """Load data for this screen. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement load_data()")
    
    def set_data(self, columns: List[str], rows: List[Dict[str, Any]]) -> None:
        """Set the data for the table."""
        if self.data_table:
            self.data_table.set_data(columns, rows)
            
    def on_key(self, event) -> None:
        """Handle key events."""
        # If search input is focused, let it handle everything except Escape
        if self.search_input and self.search_input.has_focus:
            if event.key == "escape":
                # Escape from search input goes back to table
                self.data_table.focus()
                event.prevent_default()
            return
            
        # If data table is focused, handle special keys
        if self.data_table and self.data_table.has_focus:
            # Forward slash or 's' to focus search
            if event.key == "/" or event.key == "s":
                self.search_input.focus()
                event.prevent_default()
                return
            # Regular printable characters also focus search (except single letters that might be shortcuts)
            elif event.character and event.character.isprintable() and len(event.character) == 1:
                # Don't steal single letter shortcuts like 'q', but do steal typing
                if not event.character.isalpha() or event.character.isdigit():
                    self.search_input.focus()
                    # Pass the character to search input
                    return
        
        # If neither is focused, default to table
        if self.data_table and not self.data_table.has_focus and not (self.search_input and self.search_input.has_focus):
            self.data_table.focus()