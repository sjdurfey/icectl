"""Search input widget for filtering data."""

from textual.widgets import Input
from textual.message import Message


class SearchInput(Input):
    """Input widget specialized for search/filtering."""
    
    class FilterChanged(Message):
        """Message sent when filter text changes."""
        
        def __init__(self, filter_text: str) -> None:
            super().__init__()
            self.filter_text = filter_text
    
    def __init__(self, **kwargs):
        super().__init__(placeholder="Type to filter...", **kwargs)
    
    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle input changes and emit filter message."""
        self.post_message(self.FilterChanged(event.value))