"""Main TUI application with global exception handling."""

import logging
from typing import Optional

from textual.app import App
from textual.message import Message

from icelib.config import load_config

from .command_parser import CommandParser, CommandType
from .models.navigation_state import NavigationState
from .screens.catalog_screen import CatalogScreen
from .screens.base_screen import BaseListScreen


logger = logging.getLogger(__name__)


class TUIApp(App):
    """Main TUI application for Iceberg catalog exploration."""
    
    TITLE = "icectl TUI"
    SUB_TITLE = "Apache Iceberg Catalog Explorer"
    
    CSS = """
    Screen {
        layout: vertical;
    }
    
    Header {
        dock: top;
    }
    
    Footer {
        dock: bottom;
    }
    
    .error-dialog {
        border: thick red;
        background: $panel;
        color: $text;
        width: 60%;
        height: 50%;
    }
    """
    
    def __init__(self):
        super().__init__()
        self.config = None
        self.navigation_state = NavigationState()
        self.command_parser = CommandParser()
        self.current_screen_name = "catalogs"
    
    def on_mount(self) -> None:
        """Initialize the app on mount."""
        try:
            self.config = load_config()
            logger.info("TUI app initialized successfully")
            
            # Start with catalog screen
            catalog_screen = CatalogScreen()
            self.push_screen(catalog_screen)
            
        except Exception as e:
            self.show_error_dialog(
                title="Configuration Error",
                message=f"Failed to load configuration: {e}"
            )
    
    async def on_exception(self, exception: Exception) -> None:
        """Global exception handler - never crash."""
        self.show_error_dialog(
            title="Unexpected Error", 
            message=f"An error occurred: {str(exception)}"
        )
        logger.error(f"TUI exception: {exception}", exc_info=True)
    
    def show_error_dialog(self, title: str, message: str, details: Optional[str] = None) -> None:
        """Show error dialog modal."""
        # For now, just bell and log - we'll implement proper modal later
        self.bell()
        logger.error(f"{title}: {message}")
        if details:
            logger.error(f"Details: {details}")
    
    def show_notification(self, message: str) -> None:
        """Show a notification message."""
        # For now, just update the subtitle to show feedback
        self.sub_title = f"Apache Iceberg Catalog Explorer - {message}"
        logger.info(f"Notification: {message}")
    
    def on_base_list_screen_item_selected(self, message: BaseListScreen.ItemSelected) -> None:
        """Handle item selection from list screens."""
        logger.info(f"Item selected: {message.item_data}")
        
        # Handle catalog selection - navigate to databases/namespaces
        if '_catalog_name' in message.item_data:
            catalog_name = message.item_data['_catalog_name']
            self.navigation_state.current_catalog = catalog_name
            logger.info(f"Selected catalog: {catalog_name}")
            # TODO: Navigate to database list screen
            self.show_notification(f"Selected catalog: {catalog_name}")
        else:
            # For other selections, just show the data
            self.show_notification(f"Selected: {message.item_data.get('NAME', 'Unknown')}")
    
    def on_base_list_screen_go_back(self, message: BaseListScreen.GoBack) -> None:
        """Handle go back from list screens."""
        logger.info("Going back")
        self.pop_screen()


def run_tui() -> None:
    """Entry point for running the TUI."""
    # Configure logging to file only for debugging
    import logging
    import os
    
    log_file = "/tmp/icetui_debug.log"
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode='w')
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting TUI, debug log at: {log_file}")
    
    app = TUIApp()
    app.run()