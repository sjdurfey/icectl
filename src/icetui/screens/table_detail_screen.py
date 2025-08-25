"""Table details screen showing schema and sample data."""

from typing import List, Dict, Any, Optional
import logging

from textual.screen import Screen
from textual.containers import Vertical, Horizontal, Container
from textual.widgets import Header, Footer, Static, TabbedContent, TabPane, LoadingIndicator
from textual.binding import Binding
from textual.message import Message
from textual.worker import Worker, get_current_worker, WorkerState

from ..widgets.data_table import FilterableDataTable
import icelib.clients as clients

logger = logging.getLogger(__name__)


class TableDetailScreen(Screen):
    """Screen for showing detailed table information including schema and sample data."""
    
    BINDINGS = [
        Binding("escape", "go_back", "Back", priority=True),
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("l", "load_sample", "Load Sample"),
    ]
    
    class GoBack(Message):
        """Message sent when user wants to go back."""
        pass
    
    def __init__(self, catalog_name: str, namespace: str, table_name: str):
        super().__init__()
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.table_name = table_name
        self.full_table_name = f"{namespace}.{table_name}"
        self.schema_table: Optional[FilterableDataTable] = None
        self.sample_table: Optional[FilterableDataTable] = None
        self.snapshots_table: Optional[FilterableDataTable] = None
        self.branches_table: Optional[FilterableDataTable] = None
        
    def compose(self):
        """Compose the screen layout."""
        with Vertical():
            yield Header()
            yield Static(f"Table: {self.catalog_name}.{self.full_table_name}", id="screen-title")
            
            with TabbedContent(initial="schema"):
                with TabPane("Schema", id="schema"):
                    yield FilterableDataTable(id="schema-table")
                    
                with TabPane("Sample Data", id="sample"):
                    yield FilterableDataTable(id="sample-table")
                    
                with TabPane("Properties", id="properties"):
                    yield FilterableDataTable(id="properties-table")
                    
                with TabPane("Snapshots", id="snapshots"):
                    yield FilterableDataTable(id="snapshots-table")
                    
                with TabPane("Branches", id="branches"):
                    yield FilterableDataTable(id="branches-table")
            
            yield Footer()
    
    def on_mount(self) -> None:
        """Initialize screen components after mount."""
        self.schema_table = self.query_one("#schema-table", FilterableDataTable)
        self.sample_table = self.query_one("#sample-table", FilterableDataTable) 
        self.properties_table = self.query_one("#properties-table", FilterableDataTable)
        self.snapshots_table = self.query_one("#snapshots-table", FilterableDataTable)
        self.branches_table = self.query_one("#branches-table", FilterableDataTable)
        
        # Focus the first table by default
        self.schema_table.focus()
        
        # Load initial data
        self.load_all_data()
    
    def load_all_data(self) -> None:
        """Load schema and properties immediately, sample data on demand."""
        # Show loading indicators
        self.schema_table.set_data(["LOADING"], [{"LOADING": "Loading schema..."}])
        self.properties_table.set_data(["LOADING"], [{"LOADING": "Loading properties..."}])
        self.sample_table.set_data(["INFO"], [{"INFO": "Press 'l' to load sample data"}])
        self.snapshots_table.set_data(["LOADING"], [{"LOADING": "Loading snapshots..."}])
        self.branches_table.set_data(["LOADING"], [{"LOADING": "Loading branches..."}])
        
        # Load schema, properties, snapshots and branches immediately for fast response
        self.run_worker(self.load_schema_data_worker, name="schema", thread=True)
        self.run_worker(self.load_properties_data_worker, name="properties", thread=True)
        self.run_worker(self.load_snapshots_data_worker, name="snapshots", thread=True)
        self.run_worker(self.load_branches_data_worker, name="branches", thread=True)
        
    def start_sample_worker(self) -> None:
        """Start the sample data worker with lower priority."""
        self.run_worker(self.load_sample_data_worker, name="sample", thread=True)
    
    def load_schema_data_worker(self) -> Dict[str, Any]:
        """Load table schema information in background worker."""
        try:
            # Get config from app
            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
                
            # Get the catalog configuration
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}
            
            # Load schema information
            logger.info(f"Loading schema for table '{self.full_table_name}'")
            schema_info = clients.get_table_schema(catalog_config, self.full_table_name)
            
            if not schema_info or not schema_info.get("columns"):
                return {"columns": [], "rows": []}
            
            columns = ["COLUMN", "TYPE", "NULLABLE", "COMMENT"]
            rows = []
            
            for col in schema_info["columns"]:
                rows.append({
                    "COLUMN": col.get("name", ""),
                    "TYPE": col.get("type", ""),
                    "NULLABLE": "optional" if col.get("optional", True) else "required", 
                    "COMMENT": col.get("comment", "—")
                })
            
            logger.info(f"Loaded schema with {len(rows)} columns")
            return {"columns": columns, "rows": rows}
            
        except Exception as e:
            import traceback
            logger.error(f"Failed to load schema for table '{self.full_table_name}': {e}")
            logger.error(f"Schema error traceback: {traceback.format_exc()}")
            return {"error": f"Failed to load schema: {str(e)}"}
    
    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        """Handle worker completion."""
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            
            if event.worker.name == "schema":
                if "error" in result:
                    self.schema_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self.schema_table.set_data(result["columns"], result["rows"])
                    
            elif event.worker.name == "sample":
                if "error" in result:
                    self.sample_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self.sample_table.set_data(result["columns"], result["rows"])
                    
            elif event.worker.name == "properties":
                if "error" in result:
                    self.properties_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self.properties_table.set_data(result["columns"], result["rows"])
                    
            elif event.worker.name == "snapshots":
                if "error" in result:
                    self.snapshots_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self.snapshots_table.set_data(result["columns"], result["rows"])
                    
            elif event.worker.name == "branches":
                if "error" in result:
                    self.branches_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self.branches_table.set_data(result["columns"], result["rows"])
        
        elif event.state == WorkerState.ERROR:
            error_msg = f"Worker failed: {str(event.worker.error)}"
            if event.worker.name == "schema":
                self.schema_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "sample":
                self.sample_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "properties":
                self.properties_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "snapshots":
                self.snapshots_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "branches":
                self.branches_table.set_data(["ERROR"], [{"ERROR": error_msg}])
    
    def load_sample_data_worker(self) -> Dict[str, Any]:
        """Load table sample data in background worker."""
        try:
            # Get config from app
            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
                
            # Get the catalog configuration
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}
            
            # Load sample data
            logger.info(f"Loading sample data for table '{self.full_table_name}'")
            sample_data = clients.sample_table_data(catalog_config, self.full_table_name, limit=10)
            
            if not sample_data or not sample_data.get("rows"):
                return {"columns": ["NO DATA"], "rows": []}
            
            columns = sample_data.get("columns", [])
            rows_data = sample_data.get("rows", [])
            
            # Convert rows to dict format for FilterableDataTable
            rows = []
            for row in rows_data:
                row_dict = {}
                for i, col in enumerate(columns):
                    value = row[i] if i < len(row) else ""
                    # Truncate long values for display
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:47] + "..."
                    row_dict[col] = str(value) if value is not None else ""
                rows.append(row_dict)
            
            logger.info(f"Loaded {len(rows)} sample rows")
            return {"columns": columns, "rows": rows}
            
        except Exception as e:
            logger.error(f"Failed to load sample data for table '{self.full_table_name}': {e}")
            return {"error": f"Failed to load sample data: {str(e)}"}
    
    def load_properties_data_worker(self) -> Dict[str, Any]:
        """Load table properties and metadata in background worker."""
        try:
            # Get config from app
            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
                
            # Get the catalog configuration
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}
            
            # Load table description/properties (filtered for key properties only)
            logger.info(f"Loading properties for table '{self.full_table_name}'")
            table_info = clients.describe_table(catalog_config, self.full_table_name)
            
            columns = ["PROPERTY", "VALUE"]
            rows = []
            
            # Show only the most important properties for faster loading
            important_props = [
                "name", "format", "table_type", "location", "num_files", 
                "total_size", "num_rows", "last_updated_ms", "created_at",
                "owner", "provider", "snapshot_id", "manifest_list"
            ]
            
            for key, value in table_info.items():
                if isinstance(value, (dict, list)):
                    # Skip complex objects for table view
                    continue
                # Prioritize important properties, but show others too
                rows.append({
                    "PROPERTY": key,
                    "VALUE": str(value) if value is not None else "—"
                })
            
            # Sort to show important properties first
            rows.sort(key=lambda x: (x["PROPERTY"].lower() not in important_props, x["PROPERTY"].lower()))
            
            logger.info(f"Loaded {len(rows)} table properties")
            return {"columns": columns, "rows": rows}
            
        except Exception as e:
            import traceback
            logger.error(f"Failed to load properties for table '{self.full_table_name}': {e}")
            logger.error(f"Properties error traceback: {traceback.format_exc()}")
            return {"error": f"Failed to load properties: {str(e)}"}
    
    def load_snapshots_data_worker(self) -> Dict[str, Any]:
        """Load table snapshots in background worker."""
        try:
            # Get config from app
            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
                
            # Get the catalog configuration
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}
            
            # Load snapshots
            logger.info(f"Loading snapshots for table '{self.full_table_name}'")
            snapshots = clients.get_table_snapshots(catalog_config, self.full_table_name)
            
            columns = ["SNAPSHOT_ID", "PARENT_ID", "TIMESTAMP", "OPERATION", "TOTAL_RECORDS", "IS_CURRENT"]
            rows = []
            
            for snapshot in snapshots:
                rows.append({
                    "SNAPSHOT_ID": snapshot.get("snapshot_id", "—"),
                    "PARENT_ID": snapshot.get("parent_id", "—"),
                    "TIMESTAMP": snapshot.get("timestamp", "—"),
                    "OPERATION": snapshot.get("operation", "—"),
                    "TOTAL_RECORDS": str(snapshot.get("total_records", "—")),
                    "IS_CURRENT": "Yes" if snapshot.get("is_current", False) else "No"
                })
            
            logger.info(f"Loaded {len(rows)} snapshots")
            return {"columns": columns, "rows": rows}
            
        except Exception as e:
            import traceback
            logger.error(f"Failed to load snapshots for table '{self.full_table_name}': {e}")
            logger.error(f"Snapshots error traceback: {traceback.format_exc()}")
            return {"error": f"Failed to load snapshots: {str(e)}"}
    
    def load_branches_data_worker(self) -> Dict[str, Any]:
        """Load table branches in background worker."""
        try:
            # Get config from app
            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
                
            # Get the catalog configuration
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}
            
            # Load branches
            logger.info(f"Loading branches for table '{self.full_table_name}'")
            branches = clients.get_table_branches(catalog_config, self.full_table_name)
            
            columns = ["NAME", "SNAPSHOT_ID", "PARENT_REF", "ACTION", "TYPE", "IS_CURRENT"]
            rows = []
            
            for branch in branches:
                rows.append({
                    "NAME": branch.get("name", "—"),
                    "SNAPSHOT_ID": branch.get("snapshot_id", "—"),
                    "PARENT_REF": branch.get("parent_ref", "—"),
                    "ACTION": branch.get("action", "—"),
                    "TYPE": branch.get("type", "—"),
                    "IS_CURRENT": "Yes" if branch.get("is_current", False) else "No"
                })
            
            logger.info(f"Loaded {len(rows)} branches")
            return {"columns": columns, "rows": rows}
            
        except Exception as e:
            import traceback
            logger.error(f"Failed to load branches for table '{self.full_table_name}': {e}")
            logger.error(f"Branches error traceback: {traceback.format_exc()}")
            return {"error": f"Failed to load branches: {str(e)}"}
    
    def action_go_back(self) -> None:
        """Handle going back."""
        self.post_message(self.GoBack())
    
    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()
    
    def action_refresh(self) -> None:
        """Refresh all table data."""
        logger.info(f"Refreshing data for table '{self.full_table_name}'")
        self.load_all_data()
    
    def action_load_sample(self) -> None:
        """Load sample data on demand."""
        logger.info(f"Loading sample data for table '{self.full_table_name}'")
        self.sample_table.set_data(["LOADING"], [{"LOADING": "Loading sample data..."}])
        self.run_worker(self.load_sample_data_worker, name="sample", thread=True)