"""Table details screen showing schema and sample data."""

from typing import Dict, Any, List, Optional
import logging

from datetime import datetime

from textual.screen import Screen, ModalScreen
from textual.app import ComposeResult
from textual.containers import Vertical, VerticalScroll, Horizontal
from textual.widgets import Header, Footer, Static, TabbedContent, TabPane, DataTable, Select, Button, TextArea
from textual.binding import Binding
from textual.message import Message
from textual.worker import Worker, WorkerState
from textual import events

from ..widgets.data_table import FilterableDataTable
import icelib.clients as clients

logger = logging.getLogger(__name__)


class SnapshotGraph(Static, can_focus=False):
    """DAG graph widget that emits a message when a node row is clicked."""

    class NodeClicked(Message):
        def __init__(self, row_index: int) -> None:
            super().__init__()
            self.row_index = row_index

    def on_click(self, event: events.Click) -> None:
        # Each snapshot occupies 2 lines (node + connector), except the last.
        self.post_message(self.NodeClicked(event.y // 2))


class SnapshotDetailModal(ModalScreen):
    """Pop-up showing details for a single snapshot."""

    DEFAULT_CSS = """
    SnapshotDetailModal {
        align: center middle;
    }
    #snapshot-modal {
        width: 60;
        height: auto;
        background: $surface;
        border: round $primary;
        padding: 1 2;
    }
    #snapshot-modal-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }
    #snapshot-modal-body {
        margin-bottom: 1;
    }
    #snapshot-modal-close {
        width: 100%;
    }
    """

    BINDINGS = [("escape", "dismiss", "Close")]

    def __init__(self, snapshot: Dict[str, Any], branch_names: List[str], ancestor_names: List[str] = []) -> None:
        super().__init__()
        self._snapshot = snapshot
        self._branch_names = branch_names
        self._ancestor_names = ancestor_names

    def compose(self) -> ComposeResult:
        with Vertical(id="snapshot-modal"):
            yield Static("Snapshot Details", id="snapshot-modal-title")
            yield Static(self._build_content(), id="snapshot-modal-body")
            yield Button("Close  [Esc]", id="snapshot-modal-close", variant="primary")

    def _build_content(self) -> str:
        snap = self._snapshot
        ts = snap.get("timestamp_ms") or snap.get("timestamp")
        if isinstance(ts, (int, float)):
            ts_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(ts, str) and ts not in ("—", ""):
            ts_str = ts
        else:
            ts_str = "—"

        if self._branch_names:
            refs = ", ".join(self._branch_names)
        elif self._ancestor_names:
            refs = "on: " + ", ".join(self._ancestor_names)
        else:
            refs = "—"
        parent = snap.get("parent_id") or "—"

        rows = [
            ("Snapshot ID", snap.get("snapshot_id", "—")),
            ("Parent ID",   parent),
            ("Timestamp",   ts_str),
            ("Operation",   snap.get("operation", "—")),
            ("Refs",        refs),
            ("Records",     str(snap.get("total_records", "—"))),
            ("Is Current",  "yes" if snap.get("is_current") else "no"),
        ]
        label_w = max(len(r[0]) for r in rows)
        return "\n".join(f"{label:<{label_w}}  {value}" for label, value in rows)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss()


class TableDetailScreen(Screen):
    """Screen for showing detailed table information including schema and sample data."""
    
    BINDINGS = [
        Binding("escape", "go_back", "Back", priority=True),
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("l", "load_sample", "Load Sample"),
        Binding("ctrl+enter", "execute_query", "Execute Query", show=False),
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
        self.schema_selector: Optional[Select] = None
        self._all_partition_spec_rows: list = []  # cached for schema-filtered re-render
        self._branch_map: Dict[str, List[str]] = {}   # snapshot_id → [direct ref names]
        self._ancestor_map: Dict[str, List[str]] = {} # snapshot_id → [branch names it's on]
        self._snapshots_raw: List[Dict] = []          # raw snapshot dicts (newest-first)
        self.snapshot_graph_widget: Optional[Static] = None
        self.query_table: Optional[FilterableDataTable] = None
        
    def compose(self):
        """Compose the screen layout."""
        with Vertical():
            yield Header()
            yield Static(f"Table: {self.catalog_name}.{self.full_table_name}", id="screen-title")
            
            with TabbedContent(initial="schema"):
                with TabPane("Schema", id="schema"):
                    with Vertical():
                        yield Select([], id="schema-selector", prompt="Loading schemas...")
                        yield FilterableDataTable(id="schema-table")
                        yield Static("Partition Specs", id="partition-specs-label", classes="section-label")
                        yield FilterableDataTable(id="schema-partition-specs")

                with TabPane("Sample Data", id="sample"):
                    yield FilterableDataTable(id="sample-table")

                with TabPane("Properties", id="properties"):
                    yield FilterableDataTable(id="properties-table")

                with TabPane("Snapshots", id="snapshots"):
                    yield FilterableDataTable(id="snapshots-table")

                with TabPane("Branches", id="branches"):
                    with Horizontal(id="branches-split"):
                        with Vertical(id="branches-list-pane"):
                            yield FilterableDataTable(id="branches-table")
                        with VerticalScroll(id="branches-graph-pane"):
                            yield SnapshotGraph("", id="snapshot-dag", markup=False)

                with TabPane("Query", id="query"):
                    with Vertical(id="query-pane"):
                        yield TextArea("", id="query-input", language="sql")
                        with Horizontal(id="query-controls"):
                            yield Button("Execute  [Ctrl+Enter]", id="query-execute", variant="primary")
                            yield Static("", id="query-status")
                        yield FilterableDataTable(id="query-table")

            yield Footer()
    
    def on_mount(self) -> None:
        """Initialize screen components after mount."""
        self.schema_table = self.query_one("#schema-table", FilterableDataTable)
        self.sample_table = self.query_one("#sample-table", FilterableDataTable) 
        self.properties_table = self.query_one("#properties-table", FilterableDataTable)
        self.snapshots_table = self.query_one("#snapshots-table", FilterableDataTable)
        self.branches_table = self.query_one("#branches-table", FilterableDataTable)
        self.schema_partition_specs = self.query_one("#schema-partition-specs", FilterableDataTable)
        self.schema_selector = self.query_one("#schema-selector", Select)
        self.snapshot_graph_widget = self.query_one("#snapshot-dag", Static)
        self.query_table = self.query_one("#query-table", FilterableDataTable)

        # Pre-populate SQL editor with a sensible default
        query_input = self.query_one("#query-input", TextArea)
        query_input.load_text(
            f"SELECT *\nFROM {self.catalog_name}.{self.full_table_name}\nLIMIT 100"
        )

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
        self.schema_partition_specs.set_data(["LOADING"], [{"LOADING": "Loading partition specs..."}])

        # Load schema, properties, snapshots, branches, and partition specs immediately
        self.run_worker(self.load_schema_data_worker, name="schema", thread=True)
        self.run_worker(self.load_properties_data_worker, name="properties", thread=True)
        self.run_worker(self.load_snapshots_data_worker, name="snapshots", thread=True)
        self.run_worker(self.load_branches_data_worker, name="branches", thread=True)
        self.run_worker(self.load_partition_specs_worker, name="partition_specs", thread=True)
        
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
                    "COMMENT": col.get("comment") or "—",
                    "_field_id": col.get("id"),
                    "_schema_id": schema_info.get("schema_id"),
                })
            
            logger.info(f"Loaded schema with {len(rows)} columns")
            return {
                "columns": columns,
                "rows": rows,
                "schema_id": schema_info.get("schema_id"),
                "available_schemas": schema_info.get("available_schemas", []),
                "field_ids": {col.get("id") for col in schema_info.get("columns", []) if col.get("id") is not None},
            }
            
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
                    # Populate schema version selector
                    available = result.get("available_schemas", [])
                    if len(available) > 1:
                        def _schema_label(s: Dict[str, Any]) -> str:
                            sid = s["schema_id"]
                            date = s.get("first_used_date")
                            label = f"Schema {sid}"
                            if date:
                                label += f" ({date})"
                            if s["is_current"]:
                                label += " ★"
                            return label

                        options = [(_schema_label(s), s["schema_id"]) for s in available]
                        self.schema_selector.set_options(options)
                        self.schema_selector.value = result.get("schema_id", Select.BLANK)
                    else:
                        self.schema_selector.set_options([])
                    # Filter partition specs for the current schema's fields
                    if self._all_partition_spec_rows:
                        self._apply_partition_spec_filter(result.get("field_ids", set()))

            elif event.worker.name == "partition_specs":
                if "error" in result:
                    self.schema_partition_specs.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self._all_partition_spec_rows = result["rows"]
                    # Get field IDs from schema table if already loaded, else show all specs
                    field_ids = {
                        row.get("_field_id")
                        for row in self.schema_table._filtered_rows
                        if row.get("_field_id") is not None
                    }
                    if field_ids:
                        self._apply_partition_spec_filter(field_ids)
                    else:
                        # Schema not ready yet — show unfiltered; schema handler will re-filter
                        self.schema_partition_specs.set_data(
                            ["SPEC", "FIELD", "TRANSFORM"],
                            result["rows"],
                            column_widths={"SPEC": 28, "FIELD": 22, "TRANSFORM": 15},
                        )
                    
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
                    self._branch_map = result["branch_map"]
                    self._ancestor_map = result["ancestor_map"]
                    self._snapshots_raw = result["snapshots_raw"]
                    self.snapshots_table.set_data(
                        result["columns"],
                        result["rows"],
                        column_widths={
                            "SNAPSHOT_ID": 20, "PARENT_ID": 20, "TIMESTAMP": 22,
                            "OPERATION": 11, "TOTAL_RECORDS": 14,
                            "IS_CURRENT": 10, "BRANCHES": 20,
                        },
                    )
                    self._render_snapshot_graph(selected_snapshot_id=None)
                    
            elif event.worker.name == "branches":
                if "error" in result:
                    self.branches_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self.branches_table.set_data(result["columns"], result["rows"])

            elif event.worker.name == "query":
                status = self.query_one("#query-status", Static)
                if "error" in result:
                    status.update(f"[red]{result['error']}[/red]")
                    self.query_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    n = result["row_count"]
                    elapsed = result.get("elapsed_s", "?")
                    status.update(f"[green]{n} row{'s' if n != 1 else ''} in {elapsed}s[/green]")
                    if result["columns"]:
                        rows = [
                            {col: str(v) if v is not None else "" for col, v in row.items()}
                            for row in result["rows"]
                        ]
                        self.query_table.set_data(result["columns"], rows)
                    else:
                        self.query_table.set_data(["INFO"], [{"INFO": "Query returned no rows"}])

            elif event.worker.name.startswith("schema_"):
                if "error" in result:
                    self.schema_table.set_data(["ERROR"], [{"ERROR": result["error"]}])
                else:
                    self.schema_table.set_data(result["columns"], result["rows"])
                    # Re-filter partition specs for the newly selected schema
                    field_ids = {
                        row.get("_field_id")
                        for row in self.schema_table._filtered_rows
                        if row.get("_field_id") is not None
                    }
                    self._apply_partition_spec_filter(field_ids)

        elif event.state == WorkerState.ERROR:
            error_msg = f"Worker failed: {str(event.worker.error)}"
            if event.worker.name in ("schema",) or event.worker.name.startswith("schema_"):
                self.schema_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "sample":
                self.sample_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "properties":
                self.properties_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "snapshots":
                self.snapshots_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "branches":
                self.branches_table.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "partition_specs":
                self.schema_partition_specs.set_data(["ERROR"], [{"ERROR": error_msg}])
            elif event.worker.name == "query":
                self.query_one("#query-status", Static).update(f"[red]Error: {error_msg}[/red]")
                self.query_table.set_data(["ERROR"], [{"ERROR": error_msg}])

    def _apply_partition_spec_filter(self, schema_field_ids: set) -> None:
        """Re-render partition specs showing only those compatible with the given field IDs."""
        if not self._all_partition_spec_rows:
            return

        # Determine which spec IDs are fully compatible (all source_ids present in schema)
        spec_source_ids: Dict[str, set] = {}
        for row in self._all_partition_spec_rows:
            if row.get("_is_header"):
                spec_source_ids.setdefault(row.get("_spec_id", ""), set())
            else:
                sid = row.get("_source_id")
                spec_id = row.get("_spec_id", "")
                spec_source_ids.setdefault(spec_id, set())
                if sid is not None and sid != "—":
                    spec_source_ids[spec_id].add(int(sid))

        compatible_spec_ids = {
            spec_id
            for spec_id, source_ids in spec_source_ids.items()
            if source_ids <= schema_field_ids
        }

        filtered = [
            row for row in self._all_partition_spec_rows
            if row.get("_spec_id") in compatible_spec_ids
        ]
        self.schema_partition_specs.set_data(
            ["SPEC", "FIELD", "TRANSFORM"],
            filtered,
            column_widths={"SPEC": 28, "FIELD": 22, "TRANSFORM": 15},
        )

    def on_select_changed(self, event: Select.Changed) -> None:
        """Handle schema version selection."""
        if event.select.id != "schema-selector":
            return
        schema_id = event.value
        if schema_id is Select.BLANK:
            return
        # Skip if already showing this schema
        displayed_rows = self.schema_table._filtered_rows
        if displayed_rows and displayed_rows[0].get("_schema_id") == schema_id:
            return
        self.schema_table.set_data(["LOADING"], [{"LOADING": f"Loading schema {schema_id}..."}])
        self.run_worker(
            lambda sid=schema_id: self._fetch_schema_by_id_worker(sid),
            name=f"schema_{schema_id}",
            thread=True,
        )

    def _fetch_schema_by_id_worker(self, schema_id: int) -> Dict[str, Any]:
        """Load a specific schema version on demand."""
        try:
            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}

            logger.info(f"Loading schema {schema_id} for '{self.full_table_name}'")
            result = clients.get_schema_by_id(catalog_config, self.full_table_name, schema_id)
            if "error" in result:
                return result

            columns = ["COLUMN", "TYPE", "NULLABLE", "COMMENT"]
            rows = [
                {
                    "COLUMN": col.get("name", ""),
                    "TYPE": col.get("type", ""),
                    "NULLABLE": "optional" if col.get("optional", True) else "required",
                    "COMMENT": col.get("comment") or "—",
                    "_field_id": col.get("id"),
                    "_schema_id": schema_id,
                }
                for col in result["columns"]
            ]
            return {"columns": columns, "rows": rows}

        except Exception as e:
            import traceback
            logger.error(f"Failed to load schema {schema_id}: {e}")
            logger.error(traceback.format_exc())
            return {"error": f"Failed to load schema {schema_id}: {str(e)}"}

    def _render_snapshot_graph(self, selected_snapshot_id: Optional[str]) -> None:
        """Render ASCII DAG of snapshot history into the graph Static widget."""
        from rich.text import Text

        if self.snapshot_graph_widget is None:
            return

        if not self._snapshots_raw:
            self.snapshot_graph_widget.update(Text("No snapshots", style="dim"))
            return

        snapshots = self._snapshots_raw  # newest-first
        ordered_old_first = list(reversed(snapshots))

        # Build parent → [children] adjacency (in time order, oldest child first)
        children: Dict[str, List[str]] = {}
        for snap in ordered_old_first:
            pid = snap.get("parent_id", "—")
            sid = snap.get("snapshot_id", "")
            if pid and pid != "—":
                children.setdefault(pid, []).append(sid)

        # Assign display lanes (topological, left-to-right for forks)
        lanes: List[Optional[str]] = []
        snap_lane: Dict[str, int] = {}

        def first_free(start: int = 0) -> int:
            for i in range(start, len(lanes)):
                if lanes[i] is None:
                    return i
            lanes.append(None)
            return len(lanes) - 1

        for snap in ordered_old_first:
            sid = snap.get("snapshot_id", "")
            pid = snap.get("parent_id", "—")
            if pid == "—":
                pid = None
            snap_children = children.get(pid, []) if pid else []

            if pid is None or pid not in snap_lane:
                idx = first_free()
            elif snap_children and snap_children[0] == sid:
                idx = snap_lane[pid]  # first child inherits parent's lane
            else:
                idx = first_free(snap_lane[pid] + 1)  # branch: new lane to the right

            lanes[idx] = sid
            snap_lane[sid] = idx

            # Free parent lane once its last child has been placed
            if pid and snap_children and snap_children[-1] == sid:
                parent_lane_idx = snap_lane.get(pid)
                if parent_lane_idx is not None:
                    lanes[parent_lane_idx] = None

        num_lanes = max(snap_lane.values(), default=0) + 1

        # Compute display row indices (newest-first)
        row_index_map = {snap.get("snapshot_id", ""): i for i, snap in enumerate(snapshots)}

        # Compute lane_active_until[lane] = last connector row_index that needs a pipe.
        # A lane is active at the connector after row R if a node in that lane at row <= R
        # has a same-lane parent at row > R.  For node at row r with same-lane parent at
        # row p, the last active connector is after row p-1, so we store p-1.
        lane_active_until: Dict[int, int] = {}
        for snap in snapshots:
            sid = snap.get("snapshot_id", "")
            lane = snap_lane.get(sid, 0)
            pid = snap.get("parent_id", "—")
            if pid and pid != "—" and pid in snap_lane and snap_lane[pid] == lane:
                parent_row = row_index_map.get(pid, 0)
                lane_active_until[lane] = max(lane_active_until.get(lane, -1), parent_row - 1)

        # Precompute first/last display row for each lane, and the parent lane of each
        # lane's last node (for diagonal connectors).
        lane_first_row: Dict[int, int] = {}
        lane_last_row: Dict[int, int] = {}
        lane_last_sid: Dict[int, str] = {}
        for row_i, snap in enumerate(snapshots):
            sid = snap.get("snapshot_id", "")
            lane = snap_lane.get(sid, 0)
            if lane not in lane_first_row:
                lane_first_row[lane] = row_i
            lane_last_row[lane] = row_i
            lane_last_sid[lane] = sid

        lane_last_parent_lane: Dict[int, int] = {}
        sid_to_parent: Dict[str, str] = {
            s.get("snapshot_id", ""): s.get("parent_id", "—") for s in snapshots
        }
        for lj, last_sid in lane_last_sid.items():
            pid = sid_to_parent.get(last_sid, "—")
            if pid and pid != "—" and pid in snap_lane:
                p_lane = snap_lane[pid]
                if p_lane != lj:
                    lane_last_parent_lane[lj] = p_lane

        # Render: one node row + one connector row per snapshot (except last)
        result = Text()
        for row_i, snap in enumerate(snapshots):
            sid = snap.get("snapshot_id", "")
            lane = snap_lane.get(sid, 0)
            is_selected = sid == selected_snapshot_id
            is_current = snap.get("is_current", False)
            branch_names = self._branch_map.get(sid, [])

            # Node symbol and style
            if is_selected:
                node_char, node_style = ">", "bold bright_yellow"
            elif is_current:
                node_char, node_style = "*", "bold green"
            else:
                node_char, node_style = "o", "cyan"

            # Build node row: pipes for active lanes, node symbol for this lane
            line = Text()
            for li in range(num_lanes):
                if li == lane:
                    line.append(node_char, style=node_style)
                elif (li in lane_active_until and lane_active_until[li] >= row_i
                      and lane_first_row.get(li, row_i) < row_i):
                    line.append("|", style="dim white")
                else:
                    line.append(" ")
                if li < num_lanes - 1:
                    line.append(" ")

            # Label: prefer branch/tag names, fall back to short snapshot ID
            if branch_names:
                label_style = "bold bright_cyan" if is_current else "bright_cyan"
                line.append("  ")
                for i, name in enumerate(branch_names):
                    if i:
                        line.append(" ", style="default")
                    line.append(f"[{name}]", style=label_style)
            # unlabeled intermediate nodes: no label

            result.append_text(line)

            # Connector row between nodes (pipes + diagonals for forks/merges)
            if row_i < len(snapshots) - 1:
                result.append("\n")
                connector = Text()
                for li in range(num_lanes):
                    # Show | only if the lane has already started (first node at or
                    # above this row) and is still active (same-lane parent below).
                    # Using <= row_i (not < row_i) so a node's own connector row is
                    # also covered; the \ case (first_row == row_i+1) naturally
                    # evaluates to False here, so no separate suppression needed.
                    is_pipe = (
                        li in lane_active_until
                        and lane_active_until[li] >= row_i
                        and lane_first_row.get(li, row_i + 1) <= row_i
                    )
                    connector.append("|" if is_pipe else " ", style="dim white" if is_pipe else "default")
                    if li < num_lanes - 1:
                        next_lane = li + 1
                        if (lane_first_row.get(next_lane) == row_i + 1
                                and lane_first_row.get(li, row_i + 1) <= row_i
                                and lane_active_until.get(li, -1) >= row_i):
                            # Only draw \ if the source lane (li) has a pipe here;
                            # otherwise the diagonal has nothing to branch from.
                            connector.append("\\", style="dim white")
                        elif (lane_last_row.get(next_lane) == row_i and
                              lane_last_parent_lane.get(next_lane, next_lane) < next_lane):
                            connector.append("/", style="dim white")
                        else:
                            connector.append(" ")
                result.append_text(connector)
                result.append("\n")

        self.snapshot_graph_widget.update(result)

    def load_partition_specs_worker(self) -> Dict[str, Any]:
        """Load partition spec history in background worker."""
        try:
            from rich.text import Text as RichText

            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}

            logger.info(f"Loading partition specs for table '{self.full_table_name}'")
            spec_rows = clients.get_partition_specs(catalog_config, self.full_table_name)

            # Group by spec_id (preserving order from client)
            grouped: Dict[str, list] = {}
            for r in spec_rows:
                grouped.setdefault(r["spec_id"], []).append(r)

            columns = ["SPEC", "FIELD", "TRANSFORM"]
            rows = []
            for spec_id, fields in grouped.items():
                is_active = fields[0]["is_current"]

                # Header row for this spec
                if is_active:
                    label = RichText.assemble(
                        ("  Spec ", "bold"), (spec_id, "bold cyan"), ("  ★ active", "bold green")
                    )
                else:
                    label = RichText.assemble(
                        ("  Spec ", "dim"), (spec_id, "dim cyan"), ("  superseded", "dim")
                    )
                rows.append({
                    "SPEC": label,
                    "FIELD": RichText("", style="dim"),
                    "TRANSFORM": RichText("", style="dim"),
                    "_is_header": True,
                    "_source_id": None,
                    "_is_active": is_active,
                    "_spec_id": spec_id,
                })

                # Field rows
                field_style = "bold" if is_active else "dim"
                for f in fields:
                    rows.append({
                        "SPEC": RichText("", style=field_style),
                        "FIELD": RichText(f"  {f['field_name']}", style=field_style),
                        "TRANSFORM": RichText(f["transform"], style=field_style),
                        "_is_header": False,
                        "_source_id": f["source_id"],
                        "_is_active": is_active,
                        "_spec_id": spec_id,
                    })

            logger.info(f"Loaded partition specs: {len(grouped)} spec(s)")
            return {"columns": columns, "rows": rows}

        except Exception as e:
            import traceback
            logger.error(f"Failed to load partition specs for '{self.full_table_name}': {e}")
            logger.error(traceback.format_exc())
            return {"error": f"Failed to load partition specs: {str(e)}"}

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
            
            if sample_data.get("error") and not sample_data.get("rows"):
                return {"error": sample_data["error"]}

            if not sample_data or not sample_data.get("rows"):
                return {"columns": ["INFO"], "rows": [{"INFO": "No data found"}]}
            
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
        """Load table snapshots and branch refs in background worker."""
        try:
            config = getattr(self.app, 'config', None)
            if not config:
                return {"error": "App config not yet loaded"}
            catalog_config = config.catalogs.get(self.catalog_name)
            if not catalog_config:
                return {"error": f"Catalog '{self.catalog_name}' not found in config"}

            logger.info(f"Loading snapshots for table '{self.full_table_name}'")
            snapshots = clients.get_table_snapshots(catalog_config, self.full_table_name)
            branches = clients.get_table_branches(catalog_config, self.full_table_name)

            # Build snapshot_id → [ref_names] map; current branch first
            branch_map: Dict[str, List[str]] = {}
            for ref in branches:
                sid = ref["snapshot_id"]
                name = f"tag:{ref['name']}" if ref.get("type") == "tag" else ref["name"]
                bucket = branch_map.setdefault(sid, [])
                if ref.get("is_current"):
                    bucket.insert(0, name)
                else:
                    bucket.append(name)

            columns = ["SNAPSHOT_ID", "PARENT_ID", "TIMESTAMP", "OPERATION",
                       "TOTAL_RECORDS", "IS_CURRENT", "BRANCHES"]
            rows = []
            for snap in snapshots:
                sid = snap.get("snapshot_id", "—")
                branch_names = branch_map.get(sid, [])
                rows.append({
                    "SNAPSHOT_ID": sid,
                    "PARENT_ID": snap.get("parent_id", "—"),
                    "TIMESTAMP": snap.get("timestamp", "—"),
                    "OPERATION": snap.get("operation", "—"),
                    "TOTAL_RECORDS": str(snap.get("total_records", "—")),
                    "IS_CURRENT": "Yes" if snap.get("is_current", False) else "No",
                    "BRANCHES": ", ".join(branch_names) if branch_names else "—",
                    "_snapshot_id": sid,
                    "_branch_names": branch_names,
                })

            # Build ancestor_map: for each branch (not tag), walk parent chain
            # and record which branches each snapshot belongs to.
            parent_map = {
                str(s.get("snapshot_id", "")): str(s.get("parent_id", ""))
                for s in snapshots
                if s.get("parent_id") and s.get("parent_id") != "—"
            }
            ancestor_map: Dict[str, List[str]] = {}
            for ref in branches:
                if ref.get("type") == "tag":
                    continue
                branch_name = ref.get("name", "")
                current = ref.get("snapshot_id", "—")
                while current and current != "—":
                    bucket = ancestor_map.setdefault(current, [])
                    if branch_name not in bucket:
                        bucket.append(branch_name)
                    current = parent_map.get(current, "")

            logger.info(f"Loaded {len(rows)} snapshots, {len(branch_map)} refs")
            return {
                "columns": columns,
                "rows": rows,
                "branch_map": branch_map,
                "ancestor_map": ancestor_map,
                "snapshots_raw": snapshots,
            }

        except Exception as e:
            import traceback
            logger.error(f"Failed to load snapshots for table '{self.full_table_name}': {e}")
            logger.error(traceback.format_exc())
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
            
            columns = ["NAME", "TYPE", "SNAPSHOT_ID", "IS_CURRENT"]
            rows = []

            for branch in branches:
                sid = branch.get("snapshot_id", "—")
                rows.append({
                    "NAME": branch.get("name", "—"),
                    "TYPE": branch.get("type", "—"),
                    "SNAPSHOT_ID": sid,
                    "IS_CURRENT": "Yes" if branch.get("is_current", False) else "No",
                    "_snapshot_id": sid,
                })
            
            logger.info(f"Loaded {len(rows)} branches")
            return {"columns": columns, "rows": rows}
            
        except Exception as e:
            import traceback
            logger.error(f"Failed to load branches for table '{self.full_table_name}': {e}")
            logger.error(f"Branches error traceback: {traceback.format_exc()}")
            return {"error": f"Failed to load branches: {str(e)}"}
    
    def on_snapshot_graph_node_clicked(self, event: SnapshotGraph.NodeClicked) -> None:
        """Show a detail popup when a DAG node is clicked."""
        snapshots = self._snapshots_raw  # newest-first
        idx = event.row_index
        if 0 <= idx < len(snapshots):
            snap = snapshots[idx]
            sid = str(snap.get("snapshot_id", ""))
            branch_names = self._branch_map.get(sid, [])
            ancestor_names = self._ancestor_map.get(sid, [])
            self.app.push_screen(SnapshotDetailModal(snap, branch_names, ancestor_names))

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Handle row highlight events across all tables."""
        # Branches list: highlight corresponding node in the DAG graph
        if event.data_table is self.branches_table:
            rows = self.branches_table._filtered_rows
            row_index = event.cursor_row
            selected_sid = rows[row_index].get("_snapshot_id") if rows and row_index < len(rows) else None
            self._render_snapshot_graph(selected_snapshot_id=selected_sid)
            return

        # Snapshots list: highlight parent row in table
        if event.data_table is self.snapshots_table:
            rows = self.snapshots_table._filtered_rows
            row_index = event.cursor_row
            parent_id = rows[row_index].get("PARENT_ID", "—") if rows and row_index < len(rows) else "—"
            if parent_id != "—":
                parent_indices = {
                    i for i, r in enumerate(rows)
                    if r.get("SNAPSHOT_ID") == parent_id
                }
                self.snapshots_table.highlight_cells({(i, "SNAPSHOT_ID") for i in parent_indices})
            else:
                self.snapshots_table.highlight_cells(set())
            return

        # Partition spec → schema cross-link
        if event.data_table is self.schema_partition_specs:
            rows = self.schema_partition_specs._filtered_rows
            row_index = event.cursor_row
            if not rows or row_index >= len(rows):
                self.schema_table.highlight_cells(set())
                return
            row = rows[row_index]
            if row.get("_is_header"):
                self.schema_table.highlight_cells(set())
                return
            source_id = row.get("_source_id")
            if source_id is not None:
                schema_rows = self.schema_table._filtered_rows
                targets = {
                    i for i, sr in enumerate(schema_rows)
                    if str(sr.get("_field_id", "")) == str(source_id)
                }
                self.schema_table.highlight_cells({(i, "COLUMN") for i in targets})
            else:
                self.schema_table.highlight_cells(set())

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

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "query-execute":
            self.action_execute_query()
        elif event.button.id == "snapshot-modal-close":
            pass  # handled by SnapshotDetailModal itself

    def action_execute_query(self) -> None:
        """Execute the SQL in the query editor."""
        query_input = self.query_one("#query-input", TextArea)
        query = query_input.text.strip()
        if not query:
            return
        status = self.query_one("#query-status", Static)
        status.update("[dim]Running…[/dim]")
        self.query_table.set_data(["LOADING"], [{"LOADING": "Running query..."}])
        self.run_worker(
            lambda q=query: self._run_query_worker(q),
            name="query",
            thread=True,
        )

    def _run_query_worker(self, query: str) -> Dict[str, Any]:
        config = getattr(self.app, "config", None)
        if not config:
            return {"error": "App config not yet loaded"}
        catalog_config = config.catalogs.get(self.catalog_name)
        if not catalog_config:
            return {"error": f"Catalog '{self.catalog_name}' not found in config"}
        logger.info(f"Running DuckDB query for '{self.full_table_name}'")
        return clients.run_duckdb_query(catalog_config, query)