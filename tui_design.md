# icectl TUI Design Document

## Overview

The icectl TUI (Text User Interface) provides an interactive terminal-based interface for exploring Apache Iceberg catalogs. Built with Textual, it offers vim-style command mode navigation and real-time search capabilities.

## Core Philosophy

- **Simple and Fast**: Prioritize quick navigation and exploration
- **Keyboard-First**: Vim-style commands with mouse support
- **Context-Aware**: Smart refresh and navigation based on current scope
- **Error-Resilient**: Never crash, gracefully handle all exceptions

## User Interface Design

### Navigation Flow

```
Catalogs → Databases → Tables → Table Details
```

### k9s-style Interface (Phase 1)

```
┌─ icectl TUI ─────────────────────────────────────────────────────────┐
│ Context: prod > analytics > events                    [q]uit [?]help │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  NAME         TYPE      RECORDS    LAST_COMMIT           SIZE        │
│ ❯events       table     3          2025-08-24T23:11     1.2KB       │
│  sessions     table     —          —                    —            │
│                                                                      │
│                                                                      │
├─ Details: analytics.events ─────────────────────────────────────────┤
│ Schema: event_id(string), user_id(long), ts(timestamp)              │
│ Location: s3://warehouse/lakekeeper/.../                            │
│ Last Modified: 2025-08-24T23:11:19.322000Z                         │
└─ [Enter] view [s] sample [↑↓] navigate [Esc] back ───────────────────┘
```

## Command System

### Command Mode (`:` prefix)

| Command | Shortcut | Action | Target Screen |
|---------|----------|---------|---------------|
| `:table <name>` | `:t <name>` | Jump to table details | Detail Screen |
| `:catalog <name>` | `:c <name>` | Switch catalogs | Database Screen |
| `:database <name>` | `:d <name>` | Select database | Tables Screen |
| `:refresh` | `:r` | Context-aware refresh | Current Screen |
| `:quit` | `:q` | Exit application | - |
| `:help` | `:?` | Show help | Help Modal |

### Search Mode (no `:` prefix)

- Type directly to filter current view by name
- Real-time filtering as you type
- Scope-aware (catalogs/databases/tables)
- Case-insensitive matching

### Examples

```
# Command Mode
:table events          # Jump directly to events table details
:c prod               # Switch to prod catalog, show databases
:d analytics          # Select analytics database, show tables
:r                    # Refresh current scope

# Search Mode  
events                # Filter tables containing "events"
prod                  # Filter catalogs containing "prod"
analyt                # Filter databases containing "analyt"
```

## Navigation & Refresh Logic

### Context-Aware Refresh

Refresh scope matches current navigation context:

| Current Context | Refresh Scope | Action |
|----------------|---------------|---------|
| Catalog Screen | Catalogs | Reload catalog list |
| Database Screen | Databases | Reload databases within current catalog |
| Tables Screen | Tables | Reload tables within current catalog/database |
| Detail Screen | Table Metadata | Reload selected table schema/metadata |

### Navigation State

```python
@dataclass
class NavigationState:
    catalog: Optional[str] = None    # Current catalog
    database: Optional[str] = None   # Current database  
    table: Optional[str] = None      # Current table
```

## Technical Architecture

### Project Structure

```
src/icetui/
├── __init__.py
├── app.py              # Main Textual app with command mode
├── command_parser.py   # Parse and execute commands
├── screens/
│   ├── __init__.py
│   ├── base_screen.py     # Base class with search/command support
│   ├── catalog_screen.py  # List catalogs
│   ├── database_screen.py # List databases within catalog
│   ├── table_screen.py    # List tables within database
│   └── detail_screen.py   # Show table schema + sample data
├── widgets/
│   ├── __init__.py
│   ├── command_input.py   # Command/search input widget
│   ├── data_table.py      # Filterable data table
│   ├── breadcrumb.py      # Navigation breadcrumb
│   └── error_dialog.py    # Error handling dialog
└── models/
    ├── __init__.py
    └── navigation_state.py # Track current context
```

### Key Components

**TUIApp**: Main application class with global exception handling
**BaseScreen**: Abstract base with command/search functionality  
**CommandParser**: Parse and execute vim-style commands
**NavigationState**: Track current context for scope-aware operations

### Integration with CLI

Reuse existing icelib functionality:

```python
from icelib.clients import get_catalogs_client, get_tables_client
from icelib.config import load_config

class TUIApp(App):
    def __init__(self):
        super().__init__()
        self.config = load_config()  # XDG-compliant config loading
        self.catalogs_client = get_catalogs_client(self.config)
        self.tables_client = get_tables_client(self.config)
```

## Configuration

### XDG Specification Support

Following Unix XDG Base Directory Specification:

- **Config**: `$XDG_CONFIG_HOME/icectl/config.yaml` or `~/.config/icectl/config.yaml`
- **Cache**: `$XDG_CACHE_HOME/icectl/` or `~/.cache/icectl/`

### TUI-Specific Settings

```yaml
tui:
  sample_limit: 10        # Hard limit for data sampling
  auto_refresh: false     # Manual refresh only
  mouse_support: true     # Enable mouse interactions
  vim_keys: true          # Enable vim-style navigation (hjkl)
```

## Error Handling

### Principles

1. **Never crash**: All exceptions caught and handled gracefully
2. **User feedback**: Clear error messages via modal dialogs
3. **Recovery**: Allow user to continue after errors
4. **Logging**: Log errors for debugging without exposing to user

### Implementation

```python
class TUIApp(App):
    async def on_exception(self, exception: Exception):
        """Global exception handler - never crash"""
        self.show_error_dialog(
            title="Unexpected Error",
            message=f"An error occurred: {str(exception)}"
        )
        logger.error(f"TUI exception: {exception}", exc_info=True)
```

### Error Dialog

Modal dialog for error display:
- **Title**: Error category
- **Message**: User-friendly description
- **Details**: Technical details (expandable)
- **Actions**: OK to dismiss, Help for troubleshooting

## Key Features

### Phase 1 (MVP)

- ✅ k9s-style navigation interface
- ✅ Vim-style command mode (`:table`, `:catalog`, etc.)
- ✅ Real-time search filtering
- ✅ Context-aware refresh (`:r`)
- ✅ Keyboard navigation (↑↓, Enter, Esc)
- ✅ Mouse support (click to select)
- ✅ Schema display for tables
- ✅ Data sampling (10 rows max)
- ✅ Error handling via modal dialogs
- ✅ XDG configuration support

### Phase 2 (Future Enhancement)

- Enhanced sidebar design with three-pane layout
- Advanced filtering options
- Export functionality
- Query builder interface
- Multiple catalog connections

## Entry Point

The TUI is launched via:

```bash
icectl tui
```

This integrates seamlessly with the existing CLI while providing an interactive alternative for exploration-focused workflows.

## Development Roadmap

### Phase 1a: Basic TUI + Command Infrastructure
- Textual app setup and basic screens
- Command input widget and parser
- Navigation state management
- XDG config integration

### Phase 1b: Command Mode Implementation  
- Jump commands (`:table`, `:catalog`, `:database`)
- Context-aware refresh command (`:r`)
- Help system (`:?`)

### Phase 1c: Search Mode Implementation
- Real-time filtering by name
- Search highlighting
- Performance optimization for large datasets

### Phase 1d: Polish & Error Handling
- Comprehensive error dialogs
- Loading states and progress indicators
- Keyboard shortcuts help overlay
- Testing and documentation

This design provides a powerful yet intuitive interface for Iceberg catalog exploration while maintaining the simplicity and speed that users expect from terminal-based tools.