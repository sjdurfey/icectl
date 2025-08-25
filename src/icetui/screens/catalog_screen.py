"""Catalog screen for listing available catalogs."""

from typing import List, Dict, Any
import logging

from .base_screen import BaseListScreen

logger = logging.getLogger(__name__)


class CatalogScreen(BaseListScreen):
    """Screen for listing and selecting catalogs."""
    
    def __init__(self):
        super().__init__(title="Catalogs")
    
    def on_mount(self) -> None:
        """Initialize screen components and reload data after mount."""
        super().on_mount()
        # Call load_data again after mount to ensure app config is available
        self.call_after_refresh(self.load_data)
        
    def load_data(self) -> None:
        """Load catalog data from configuration."""
        try:
            # Get catalogs from app config
            config = getattr(self.app, 'config', None)
            if not config:
                logger.warning("App config not yet loaded, setting empty data")
                self.set_data(["NAME"], [])
                return
                
            if not hasattr(config, 'catalogs') or not config.catalogs:
                logger.warning("No catalogs found in config")
                self.set_data(["NAME"], [])
                return
                
            columns = ["NAME", "TYPE", "URI", "DEFAULT"]
            rows = []
            
            for name, catalog_config in config.catalogs.items():
                rows.append({
                    "NAME": name,
                    "TYPE": catalog_config.type or "unknown",
                    "URI": catalog_config.uri or "",
                    "DEFAULT": "yes" if name == config.default_catalog else "no",
                    "_catalog_name": name  # Hidden field for selection
                })
            
            # Sort by default first, then by name
            rows.sort(key=lambda x: (x["DEFAULT"] != "yes", x["NAME"]))
            
            self.set_data(columns, rows)
            logger.info(f"Loaded {len(rows)} catalogs")
            
            # DEBUG: Force a simple test row if no data appears
            if not rows:
                test_rows = [{"NAME": "TEST", "TYPE": "test", "URI": "test://uri", "DEFAULT": "no"}]
                self.set_data(columns, test_rows)
                logger.info("DEBUG: Added test row")
            
        except Exception as e:
            logger.error(f"Failed to load catalogs: {e}")
            self.set_data(["ERROR"], [{"ERROR": str(e)}])