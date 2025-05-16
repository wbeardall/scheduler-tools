from typing import Dict
import paramiko
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, ListView, ListItem, Label, DataTable, Input, Button, ProgressBar, Static
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen

from schedtools.interfaces.manager.state import Job, ManagerState
from schedtools.shell_handler import ShellHandler
from schedtools.managers import get_workload_manager
from schedtools.core import PBSJob # Assuming PBSJob is dict-like as per context

#./.venv/bin/python src/schedtools/interfaces/manager

class HostSelectionScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    state: ManagerState

    def __init__(self, *args, state: ManagerState, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = state
        self.hosts = {f"host-{i}": host for i, host in enumerate(state.hosts)}


    def compose(self) -> ComposeResult:
        yield Header()
        if self.hosts:
            yield Label("Select an SSH Host:")
            list_items = []
            for k, v in self.hosts.items():
                list_items.append(ListItem(Label(v.name), id=k))
            yield ListView(*list_items)
        else:
            yield Label("No configured SSH hosts found in ~/.ssh/config")
        yield Footer()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        selected_host = event.item.id
        host = self.hosts[selected_host]
        self.state.set_selected_host(host)
        if host.requires_password:
            self.app.push_screen(PasswordPromptScreen(state=self.state))
        else:
            self.state.connect()
            self.app.push_screen(JobBrowserScreen(state=self.state))

class PasswordPromptScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    state: ManagerState

    def __init__(self, state: ManagerState) -> None:
        super().__init__()
        self.state = state

    def compose(self) -> ComposeResult:
        self.styles.width = "50%"
        yield Header()
        yield Label(f"Enter password for host '{self.state.selected_host.name}':", id="host_password_label")
        yield Input(placeholder='password', password=True, id="host_password_input")
        yield Label("(Press Enter to continue)", id="host_password_hint")
        yield Footer()

    def on_mount(self) -> None:
        """Focus the input when the screen is mounted."""
        self.query_one("#host_password_input", Input).focus()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "host_password_input":
            self.notify("Connecting...")
            try:
                self.state.connect(event.input.value)
                self.app.pop_screen()
                self.app.push_screen(JobBrowserScreen(state=self.state))
            except paramiko.ssh_exception.AuthenticationException:
                self.query_one("#host_password_hint", Label).update(f"Invalid password for host '{self.state.selected_host.name}'. Please try again.")
                self.query_one("#host_password_input", Input).focus()

class JobBrowserScreen(Screen):
    # BINDINGS = [("escape", "app.pop_screen", "Back to host selection")]
    state: ManagerState
    def __init__(self, state: ManagerState, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = state

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label(f"Job Queue for {self.state.selected_host.name}")
        yield DataTable(id="job_table", cursor_type='row')
        yield Footer()

    async def on_mount(self) -> None:
        """Load and display job data."""
        columns = [
            # "id",
            "name",
            # "owner",
            # "queue",
            "status",
            "walltime",
            "start_time",
            "percent_completion",
        ]
        try:
            self.query_one('#job_table', DataTable).loading = True
            # This is a blocking call, consider running in a worker thread
            # For now, doing it directly for simplicity in prototyping
            queued_jobs = self.state.job_data

            table = self.query_one('#job_table', DataTable)

            if not len(queued_jobs): # Check if the Queue is empty
                table.add_row("No jobs found.")
                self.app.bell()
                return
            
            table.add_columns(*[prettify(col) for col in columns])

            table.loading = False

            for job in queued_jobs:
                table.add_row(
                    *[getattr(job, col) for col in columns],
                    key=job.id
                )

        except Exception as e:
            self.notify(f"Error loading jobs: {e}")

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.app.push_screen(JobDetailScreen(state=self.state, job_id=event.row_key))


class JobDetailScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    state: ManagerState
    job: Job
    def __init__(self, *args, state: ManagerState, job_id: str, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = state
        self.job = self.state.get_job(job_id)

    def compose(self) -> ComposeResult:
        yield Header()
        
        with Container(id="job-details-container"):
            yield Label(f"📋 Job Details: {self.job.name}", id="job-title", classes="title")
            
            with Vertical(id="job-info", classes="info-panel"):
                with Horizontal(classes="info-row"):
                    yield Label("Status:", classes="label")
                    yield Label(prettify(self.job.status), classes="value")
                
                with Horizontal(classes="info-row"):
                    yield Label("Walltime:", classes="label")
                    yield Label(str(self.job.walltime or '-'), classes="value")
                
                with Horizontal(classes="info-row"):
                    yield Label("Start Time:", classes="label")
                    yield Label(str(self.job.start_time or '-'), classes="value")
                
                with Horizontal(classes="progress-section"):
                    yield Label("Completion:", classes="label")
                    yield ProgressBar(id="completion-bar", show_eta=False, show_bar=True, show_percentage=True)
                
                with Horizontal(classes="info-row"):
                    yield Label("Owner:", classes="label")
                    yield Label(self.job.owner, classes="value")
            
            
            with Horizontal(id="action-buttons", classes="button-container"):
                yield Button("⬅️ Back", variant="primary", id="back-button", classes="left-button")
                yield Static()
                yield Button("🗑️ Delete Job", variant="error", id="delete-button", classes="right-button")
        
        yield Footer()



    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "delete-button":
            # TODO: Implement job deletion
            self.notify("Job deletion not yet implemented")
        elif event.button.id == "back-button":
            self.app.pop_screen()

    def on_mount(self) -> None:
        """Add some styling when the screen mounts."""
        self.query_one("#completion-bar", ProgressBar).update(
            progress=self.job.percent_completion, 
            total=100
        )
        # self.query_one("#job-details-container", Container).styles.padding = (1, 2)
        # self.query_one("#job-title", Label).styles.color = "blue"
        # self.query_one("#completion-bar", ProgressBar).styles.width = "100%"

class ManagerApp(App):
    CSS_PATH = "manager.tcss" # We'll create this later
    SCREENS = {"host_select": HostSelectionScreen, "job_browser": JobBrowserScreen}
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = ManagerState()

    def on_mount(self) -> None:
        self.push_screen(HostSelectionScreen(state=self.state))


def prettify(s: str) -> str:
    return s.replace("_", " ").title()

if __name__ == "__main__":
    app = ManagerApp()
    app.run()
