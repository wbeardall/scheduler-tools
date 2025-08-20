from enum import Enum
from functools import partial
from typing import Any, Callable, Optional, Union

import paramiko
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Center, Container, Horizontal, ScrollableContainer
from textual.screen import Screen
from textual.widgets import (
    Button,
    ContentSwitcher,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    Log,
    Pretty,
    ProgressBar,
    RadioButton,
    RadioSet,
    Static,
)
from textual.worker import Worker, WorkerState

from schedtools.interfaces.queue_manager.state import (
    Job,
    JobFilter,
    JobLog,
    ManagerState,
    can_elevate_job,
    can_resubmit_job,
    get_live_icon,
)
from schedtools.interfaces.queue_manager.utils import prettify
from schedtools.schemas import JobState
from schedtools.utils import get_any_identifier


class ConnectorScreen(Screen):
    state: ManagerState

    def __init__(self, *args, state: ManagerState, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = state

    def attempt_connection(self, password: Union[str, None] = None) -> None:
        self.notify(f"Connecting to '{self.state.selected_host.name}'...")
        if password is None:
            return self.connect_worker()
        pw_key = "pw_key"
        self.state.kv_store.push(pw_key, password)
        self.connect_worker(pw_key)

    @work(thread=True, exclusive=True)
    def connect_worker(self, pw_key: Union[str, None] = None) -> None:
        timeout_millis = 2_000
        try:
            self.state.connect(pw_key=pw_key, timeout_seconds=timeout_millis / 1000)
            if pw_key is not None:
                # Remove the password from the KV store
                self.state.kv_store.pop(pw_key)
            return {"success": True}
        except Exception as e:
            if isinstance(e, paramiko.ssh_exception.AuthenticationException):
                return {
                    "success": False,
                    "msg": f"Invalid password for host '{self.state.selected_host.name}'",
                }
            elif isinstance(e, TimeoutError):
                return {
                    "success": False,
                    "msg": f"Connection to host '{self.state.selected_host.name}' timed out ({int(timeout_millis)}ms)",
                }
            else:
                return {
                    "success": False,
                    "msg": f"Error connecting to host '{self.state.selected_host.name}': {e}",
                }


class HostSelectionScreen(ConnectorScreen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]

    def __init__(self, *args, state: ManagerState, **kwargs) -> None:
        super().__init__(*args, state=state, **kwargs)
        self.hosts = {f"host-{i}": host for i, host in enumerate(state.hosts)}

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="host-selection-screen", classes="screen-container"):
            with Center():
                with Container(
                    id="host-selection-container", classes="centered-container"
                ):
                    with Container():
                        with Center():
                            yield Label(
                                "Select an SSH Host:",
                                id="host-selection-title",
                                classes="screen-title",
                            )
                            if self.hosts:
                                list_items = []
                                for k, v in self.hosts.items():
                                    list_items.append(ListItem(Label(v), id=k))
                                yield ListView(
                                    *list_items, id="host-selection-list-view"
                                )
                            else:
                                yield Label(
                                    "No configured SSH hosts found in ~/.ssh/config"
                                )
                    with Horizontal(
                        id="host-selection-buttons", classes="button-container"
                    ):
                        yield Button(
                            "✏️ Enter Host",
                            variant="warning",
                            id="host-selection-manual-button",
                            classes="left-button",
                        )
                        yield Static()
                        yield Button(
                            "🔗 Connect",
                            variant="primary",
                            id="host-selection-connect-button",
                            classes="right-button",
                        )
        yield Footer()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        selected_host = event.item.id
        self.select_and_connect(self.hosts[selected_host])

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "host-selection-manual-button":
            self.app.push_screen(URLPromptScreen(state=self.state))
        elif event.button.id == "host-selection-connect-button":
            list_view: ListView = self.query_one("#host-selection-list-view", ListView)
            self.select_and_connect(list_view.highlighted_child.id)

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            if result["success"]:
                self.app.pop_screen()
                self.app.push_screen(JobBrowserScreen(state=self.state))
            else:
                self.notify_connection_failure(result["msg"])
        elif event.state == WorkerState.ERROR:
            self.notify_connection_failure(event.error)

    def notify_connection_failure(self, msg: str) -> None:
        self.notify(
            f"❌ Error connecting to host '{self.state.selected_host.name}': {msg}"
        )

    def select_and_connect(self, alias: str) -> None:
        self.state.set_selected_host(alias)
        if self.state.selected_host.requires_password:
            self.app.push_screen(PasswordPromptScreen(state=self.state))
        else:
            self.attempt_connection()


class PasswordPromptScreen(ConnectorScreen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="password-prompt-screen", classes="screen-container"):
            with Center():
                with Container(
                    id="password-prompt-container", classes="centered-container"
                ):
                    yield Label(
                        f"Enter password for host '{self.state.selected_host.name}':",
                        id="host-password-label",
                        classes="screen-title",
                    )
                    yield Input(
                        placeholder="password", password=True, id="host-password-input"
                    )
                    with Horizontal(id="password-buttons", classes="button-container"):
                        yield Button(
                            "⬅️ Back",
                            variant="error",
                            id="password-prompt-back-button",
                            classes="left-button",
                        )
                        yield Static()
                        yield Button(
                            "🔑 Enter",
                            variant="primary",
                            id="password-prompt-enter-button",
                            classes="right-button",
                        )
        yield Footer()

    def on_mount(self) -> None:
        """Focus the input when the screen is mounted."""
        self.query_one("#host-password-input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "password-prompt-back-button":
            self.app.pop_screen()
        elif event.button.id == "password-prompt-enter-button":
            input_field = self.query_one("#host-password-input", Input)
            self.attempt_connection(input_field.value)

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.SUCCESS:
            result = event.worker.result
            if result["success"]:
                self.app.pop_screen()
                self.app.push_screen(JobBrowserScreen(state=self.state))
            else:
                self.notify_and_reset_password(result["msg"])
        elif event.state == WorkerState.ERROR:
            self.notify_and_reset_password(
                f"Error connecting to host '{self.state.selected_host.name}': {event.error}"
            )

    def notify_and_reset_password(self, msg: str) -> None:
        self.notify(f"❌ {msg}. Please try again.")
        password_input = self.query_one("#host-password-input", Input)
        password_input.value = ""
        password_input.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "host-password-input":
            self.attempt_connection(event.input.value)


class URLPromptScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    state: ManagerState

    def __init__(self, *args, state: ManagerState, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = state

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="url-prompt-screen", classes="screen-container"):
            with Center():
                with Container(id="url-prompt-container", classes="centered-container"):
                    yield Label(
                        "Enter SSH URL:", id="host-url-label", classes="screen-title"
                    )
                    yield Input(
                        placeholder="ssh://user@host[:port]", id="host-url-input"
                    )
                    with Horizontal(id="url-buttons", classes="button-container"):
                        yield Button(
                            "⬅️ Back",
                            variant="primary",
                            id="url-prompt-back-button",
                            classes="left-button",
                        )
                        yield Static()
                        yield Button(
                            "✅ Confirm",
                            variant="primary",
                            id="url-prompt-enter-button",
                            classes="right-button",
                        )
        yield Footer()

    def on_mount(self) -> None:
        """Focus the input when the screen is mounted."""
        self.query_one("#host-url-input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "url-prompt-back-button":
            self.app.pop_screen()
        elif event.button.id == "url-prompt-enter-button":
            input_field = self.query_one("#host-url-input", Input)
            self.prompt_for_password(input_field.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "host-url-input":
            self.prompt_for_password(event.input.value)

    def prompt_for_password(self, url: str) -> None:
        try:
            host = self.state.register_host_from_url(url)
            self.state.set_selected_host(host)
            self.app.push_screen(PasswordPromptScreen(state=self.state))
        except Exception as e:
            self.notify(f"❌ Error registering host: {e}")
            self.query_one("#host-url-input", Input).value = ""
            self.query_one("#host-url-input", Input).focus()


class JobBrowserScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Back to host selection")]
    state: ManagerState
    _columns_set: bool = False

    def __init__(self, state: ManagerState, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = state

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="job-browser-screen", classes="screen-container"):
            yield Label(
                self.screen_title,
                id="job-browser-title",
                classes="screen-title",
            )
            yield DataTable(id="job-table", cursor_type="row")
            with Horizontal(id="job-browser-buttons", classes="button-container"):
                yield Button(
                    "⬅️ Back",
                    variant="primary",
                    id="job-browser-back-button",
                    classes="left-button",
                )
                yield Button(
                    "🔍 Filter",
                    variant="primary",
                    id="job-browser-filter-button",
                    classes="action-button",
                )
                yield Button(
                    "⚠️ Resubmit Filtered Jobs",
                    variant="warning",
                    id="job-browser-resubmit-filtered-button",
                    classes="action-button",
                )
                yield Button(
                    "❌ Delete Filtered Jobs",
                    variant="error",
                    id="job-browser-delete-filtered-button",
                    classes="action-button",
                )
                yield Static()
                yield Button(
                    "🔄 Refresh",
                    variant="primary",
                    id="job-browser-refresh-button",
                    classes="right-button",
                )

        yield Footer()

    @work(thread=True, exclusive=True, name="fetch-jobs")
    def fetch_jobs(self) -> None:
        _ = self.state.job_data
        # on_worker_state_changed will now fire

    @work(thread=True, exclusive=True, name="resubmit-filtered-jobs")
    def resubmit_filtered_jobs(
        self, queue: Optional[str] = None, project: Optional[str] = None
    ) -> None:
        self.state.resubmit_filtered_jobs(queue=queue, project=project)

    @work(thread=True, exclusive=True, name="delete-filtered-jobs")
    def delete_filtered_jobs(self, expected_count: int) -> None:
        self.state.delete_filtered_jobs(expected_count)

    @property
    def screen_title(self) -> str:
        return f"Job Queue for {self.state.selected_host.name} ({self.state.filter})"

    def on_mount(self) -> None:
        self.populate_table()

    def populate_table(self) -> None:
        table = self.query_one("#job-table", DataTable)
        table.loading = True
        table.refresh()
        title = self.query_one("#job-browser-title", Label)
        title.update(self.screen_title)
        self.fetch_jobs()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.worker.name == "fetch-jobs":
            self.fetch_jobs_hook(event)
        elif event.worker.name == "resubmit-filtered-jobs":
            self.resubmit_filtered_jobs_hook(event)
        elif event.worker.name == "delete-filtered-jobs":
            self.delete_filtered_jobs_hook(event)

    def fetch_jobs_hook(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.SUCCESS:
            self.display_table()
        elif event.state == WorkerState.ERROR:
            table = self.query_one("#job-table", DataTable)
            table.loading = False
            table.refresh()
            self.notify(f"Error fetching jobs: {event.error}")

    def resubmit_filtered_jobs_hook(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.SUCCESS:
            self.notify(f"Resubmitted {len(self.state.job_data)} jobs.")
            self.state.evict_current_queue()
            self.populate_table()
        elif event.state == WorkerState.ERROR:
            self.notify(f"Error resubmitting filtered jobs: {event.error}")

    def delete_filtered_jobs_hook(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.SUCCESS:
            self.notify(f"Deleted {len(self.state.job_data)} jobs.")
            self.state.evict_current_queue()
            self.populate_table()
        elif event.state == WorkerState.ERROR:
            self.notify(f"Error deleting filtered jobs: {event.error}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "job-browser-back-button":
                self.app.pop_screen()
            case "job-browser-resubmit-filtered-button":
                if self.state.filter.is_empty:
                    self.notify(
                        "No filter set. Blanket resubmission not allowed for safety reasons."
                    )
                else:
                    self.app.push_screen(
                        JobResubmitScreen(
                            callback=self.resubmit_filtered_jobs,
                            message=f"Resubmit {len(self.state.job_data)} jobs?",
                        )
                    )
            case "job-browser-delete-filtered-button":
                if self.state.filter.is_empty:
                    self.notify(
                        "No filter set. Blanket deletion not allowed for safety reasons."
                    )
                else:
                    self.app.push_screen(
                        ConfirmationScreen(
                            callback=partial(
                                self.delete_filtered_jobs, len(self.state.job_data)
                            ),
                            message=f"Delete {len(self.state.job_data)} jobs?",
                            required_phrase="confirm delete jobs",
                        )
                    )
            case "job-browser-filter-button":
                self.app.push_screen(
                    JobFilterScreen(state=self.state, browser_handle=self)
                )
            case "job-browser-refresh-button":
                self.state.evict_current_queue()
                self.populate_table()

    def display_table(self) -> None:
        table = self.query_one("#job-table", DataTable)
        table.clear()
        """Load and display job data."""
        columns = [
            # "id",
            "name",
            # "owner",
            # "queue",
            "state",
            "walltime",
            "start_time",
            "percent_completion",
        ]
        # This is a blocking call, consider running in a worker thread
        # For now, doing it directly for simplicity in prototyping
        queued_jobs = self.state.job_data

        if not self._columns_set:
            table.add_columns("Live", *[prettify(col) for col in columns])
            self._columns_set = True

        table.loading = False

        if not len(queued_jobs):  # Check if the Queue is empty
            self.notify(f"No jobs found in host '{self.state.selected_host.name}'.")
            return

        for job in queued_jobs:
            table.add_row(
                # If the job is a Job object, it is live (i.e. queued with the scheduler as opposed to
                # simply being registered as a job spec)
                get_live_icon(job),
                *[
                    self.prettify_enum_only(get_attr_or(job, col, "-"))
                    for col in columns
                ],
                key=get_any_identifier(job),
            )

    @staticmethod
    def prettify_enum_only(s: Union[str, Enum]) -> str:
        if isinstance(s, Enum):
            return prettify(s)
        return s

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.app.push_screen(
            JobDetailScreen(
                state=self.state,
                browser_handle=self,
                job_id=event.row_key.value,
            )
        )


class JobFilterScreen(Screen):
    state: ManagerState
    browser_handle: JobBrowserScreen

    def __init__(
        self, *args, state: ManagerState, browser_handle: JobBrowserScreen, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.state = state
        self.browser_handle = browser_handle

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="job-filter-screen", classes="screen-container"):
            with Center():
                with Container(id="job-filter-content", classes="centered-container"):
                    yield Label(
                        "Filter Jobs", id="job-filter-title", classes="screen-title"
                    )

                    with Horizontal(
                        id="job-filter-buttons", classes="button-container"
                    ):
                        yield Button(
                            "State",
                            id="job-filter-state-button",
                            classes="filter-button",
                        )
                        yield Button(
                            "Name",
                            id="job-filter-name-button",
                            classes="filter-button",
                        )
                    with ContentSwitcher(initial="job-filter-state-radio-set"):
                        yield RadioSet(
                            *[
                                RadioButton(
                                    label=prettify(state), value=False, name=state.value
                                )
                                for state in JobState
                            ],
                            id="job-filter-state-radio-set",
                        )
                        yield Input(
                            placeholder="Filter by name",
                            id="job-filter-name-input",
                        )
                    with Horizontal(id="action-buttons", classes="button-container"):
                        yield Button(
                            "⬅️ Back",
                            variant="primary",
                            id="job-filter-back-button",
                            classes="left-button",
                        )
                        yield Button(
                            "🔄 Reset",
                            variant="primary",
                            id="job-filter-reset-button",
                            classes="action-button",
                        )
                        yield Button(
                            "✅ Apply",
                            variant="primary",
                            id="job-filter-apply-button",
                            classes="right-button",
                        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "job-filter-state-button":
                self.query_one(ContentSwitcher).current = "job-filter-state-radio-set"
            case "job-filter-name-button":
                self.query_one(ContentSwitcher).current = "job-filter-name-input"
            case "job-filter-reset-button":
                self.reset_filter()
            case "job-filter-back-button":
                self.app.pop_screen()
            case "job-filter-apply-button":
                self.apply_filter()
                # Populate the table with the filtered jobs (NOTE: do not evict the queue)
                self.browser_handle.populate_table()
                self.app.pop_screen()

    def reset_filter(self) -> None:
        state_radio = self.query_one("#job-filter-state-radio-set", RadioSet)
        # Following https://github.com/Textualize/textual/blob/main/src/textual/widgets/_radio_set.py
        buttons = state_radio.query(RadioButton)
        with state_radio.prevent(RadioButton.Changed):
            for button in buttons:
                button.value = False
            state_radio._pressed_button = None
        self.query_one("#job-filter-name-input", Input).value = ""

    def apply_filter(self) -> None:
        state_button = self.query_one(
            "#job-filter-state-radio-set", RadioSet
        ).pressed_button
        state = None if state_button is None else JobState(state_button.name)
        name = self.query_one("#job-filter-name-input", Input).value
        self.state.set_filter(JobFilter(state=state, name=name))


class ConfirmationScreen(Screen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    callback: Callable[[], None]
    message: str
    required_phrase: str

    def __init__(
        self,
        *args,
        callback: Callable[[], None],
        message: str,
        required_phrase: str = "confirm",
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.callback = callback
        self.message = message
        self.required_phrase = required_phrase

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="confirmation-screen", classes="screen-container"):
            with Center():
                with Container(id="confirmation-content", classes="centered-container"):
                    yield Label(
                        self.message, id="confirmation-message", classes="screen-title"
                    )
                    yield Input(
                        placeholder=f"Type '{self.required_phrase}' to proceed",
                        id="confirmation-input",
                    )
                    with Horizontal(
                        id="confirmation-buttons", classes="button-container"
                    ):
                        yield Button(
                            "✅ Confirm",
                            variant="primary",
                            id="confirmation-confirm-button",
                            classes="right-button",
                        )
                        yield Button(
                            "❌ Cancel",
                            variant="primary",
                            id="confirmation-cancel-button",
                            classes="left-button",
                        )
        yield Footer()

    def submit_if_confirmed(self) -> None:
        input_field = self.query_one("#confirmation-input", Input)
        if input_field.value == self.required_phrase:
            self.callback()
            self.app.pop_screen()
        else:
            self.notify(f"Please type '{self.required_phrase}' to proceed.")
            input_field.value = ""
            input_field.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.submit_if_confirmed()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "confirmation-confirm-button":
                self.submit_if_confirmed()
            case "confirmation-cancel-button":
                self.app.pop_screen()


class JobResubmitScreen(ConfirmationScreen):
    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="confirmation-screen", classes="screen-container"):
            with Center():
                with Container(id="confirmation-content", classes="centered-container"):
                    yield Label(
                        self.message, id="confirmation-message", classes="screen-title"
                    )
                    yield Input(
                        placeholder=f"Type '{self.required_phrase}' to proceed",
                        id="confirmation-input",
                    )
                    with Horizontal(
                        id="confirmation-buttons", classes="button-container"
                    ):
                        yield Button(
                            "✅ Confirm",
                            variant="primary",
                            id="confirmation-confirm-button",
                            classes="right-button",
                        )
                        yield Button(
                            "⚡️ Submit exp-00077",
                            variant="warning",
                            id="confirmation-express-button",
                            classes="action-button",
                        )
                        yield Button(
                            "🐢 Submit Default",
                            variant="warning",
                            id="confirmation-standard-button",
                            classes="action-button",
                        )
                        yield Button(
                            "❌ Cancel",
                            variant="primary",
                            id="confirmation-cancel-button",
                            classes="left-button",
                        )
        yield Footer()

    def submit_if_confirmed(
        self, queue: Optional[str] = None, project: Optional[str] = None
    ) -> None:
        input_field = self.query_one("#confirmation-input", Input)
        if input_field.value == self.required_phrase:
            self.callback(queue=queue, project=project)
            self.app.pop_screen()
        else:
            self.notify(f"Please type '{self.required_phrase}' to proceed.")
            input_field.value = ""
            input_field.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.submit_if_confirmed()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "confirmation-confirm-button":
                self.submit_if_confirmed()
            case "confirmation-express-button":
                self.submit_if_confirmed(queue="express", project="exp-00077")
            case "confirmation-standard-button":
                self.submit_if_confirmed(queue="default")
            case "confirmation-cancel-button":
                self.app.pop_screen()


class JobScriptScreen(Screen):
    state: ManagerState
    browser_handle: JobBrowserScreen
    job: Job

    def __init__(
        self,
        *args,
        state: ManagerState,
        browser_handle: JobBrowserScreen,
        job_id: str,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.state = state
        self.job = self.state.get_job(job_id)
        self.browser_handle = browser_handle


def get_attr_or(obj: Any, attr: str, default: Any) -> Any:
    return getattr(obj, attr, default) or default


class JobDetailScreen(JobScriptScreen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()

        with Container(id="job-details-container", classes="screen-container"):
            yield Label(
                f"📋 Job Details: {self.job.name}",
                id="job-title",
                classes="screen-title",
            )

            with ScrollableContainer(id="job-info", classes="info-panel"):
                with Horizontal(classes="info-row"):
                    yield Label("State:", classes="label")
                    yield Label(prettify(self.job.state), classes="value")

                with Horizontal(classes="info-row"):
                    yield Label("Queue:", classes="label")
                    yield Label(get_attr_or(self.job, "queue", "-"), classes="value")

                with Horizontal(classes="info-row"):
                    yield Label("Walltime:", classes="label")
                    yield Label(
                        str(get_attr_or(self.job, "walltime", "-")), classes="value"
                    )

                with Horizontal(classes="info-row"):
                    yield Label("Start Time:", classes="label")
                    yield Label(
                        str(get_attr_or(self.job, "start_time", "-")), classes="value"
                    )

                with Horizontal(classes="progress-section"):
                    yield Label("Completion:", classes="label")
                    yield ProgressBar(
                        id="completion-bar",
                        show_eta=False,
                        show_bar=True,
                        show_percentage=True,
                    )

                with Horizontal(classes="info-row"):
                    yield Label("Owner:", classes="label")
                    yield Label(get_attr_or(self.job, "owner", "-"), classes="value")

                with Horizontal(classes="info-row"):
                    yield Label("Script path:", classes="label")
                    yield Label(self.job.jobscript_path, classes="value")

                with Horizontal(classes="info-row"):
                    yield Label("Experiment path:", classes="label")
                    yield Label(self.job.experiment_path, classes="value")

                with Horizontal(classes="info-row"):
                    yield Label("Comment:", classes="label")
                    yield Label(get_attr_or(self.job, "comment", "-"), classes="value")

                yield Label("Details:", classes="label")
                yield Pretty(
                    get_attr_or(self.job, "job_details", {}), id="pretty-container"
                )

            with Horizontal(id="action-buttons", classes="button-container"):
                yield Button(
                    "⬅️ Back",
                    variant="primary",
                    id="job-detail-back-button",
                    classes="left-button",
                )
                yield Button(
                    "🔍 Logs",
                    variant="primary",
                    id="job-detail-log-button",
                    classes="action-button",
                )
                if can_elevate_job(self.job):
                    yield Button(
                        "⚡️ Elevate Job",
                        variant="warning",
                        id="job-detail-elevate-button",
                        classes="action-button",
                    )
                if can_resubmit_job(self.job):
                    yield Button(
                        "🔄 Resubmit Job",
                        variant="primary",
                        id="job-detail-resubmit-button",
                        classes="action-button",
                    )
                yield Static()
                yield Button(
                    "🗑️ Delete Job",
                    variant="error",
                    id="job-detail-delete-button",
                    classes="right-button",
                )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        match event.button.id:
            case "job-detail-delete-button":
                try:
                    response = self.state.shell_handler.delete_jobs([self.job.id])
                    if response.returncode != 0:
                        self.notify(
                            f"❌ Error deleting job: {str(response.stderr)}"[:300]
                        )
                    self.state.evict_current_queue()
                    self.browser_handle.populate_table()
                    self.app.pop_screen()
                except Exception as e:
                    self.notify(f"❌ Error deleting job: {str(e)}"[:300])
            case "job-detail-elevate-button":
                self.notify("Job elevation is not implemented.")
                # self.app.push_screen(
                #     ExpressQueuePromptScreen(
                #         state=self.state,
                #         browser_handle=self.browser_handle,
                #         job_id=self.job.id,
                #     )
                # )
            case "job-detail-resubmit-button":
                if can_resubmit_job(self.job):
                    try:
                        self.state.workload_manager.resubmit_job(self.job)
                        self.state.evict_current_queue()
                        self.browser_handle.populate_table()
                        self.app.pop_screen()
                    except Exception as e:
                        self.notify(f"❌ Error resubmitting job: {e}")
                else:
                    self.notify(
                        f"❌ Job in state '{self.job.state}' cannot be resubmitted."
                    )
            case "job-detail-back-button":
                self.app.pop_screen()
            case "job-detail-log-button":
                self.app.push_screen(
                    JobLogScreen(
                        state=self.state,
                        browser_handle=self.browser_handle,
                        job_id=self.job.id,
                    )
                )

    def on_mount(self) -> None:
        """Add some styling when the screen mounts."""
        self.query_one("#completion-bar", ProgressBar).update(
            progress=get_attr_or(self.job, "percent_completion", 0), total=100
        )


class JobLogScreen(JobScriptScreen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]
    job_log: JobLog

    def compose(self) -> ComposeResult:
        yield Header()

        with Container(id="job-log-screen", classes="screen-container"):
            yield Label(
                f"📋 Job Log: {self.job.name}",
                id="job-title",
                classes="screen-title",
            )

            yield Log(id="job-log", classes="log-panel")

            with Horizontal(id="action-buttons", classes="button-container"):
                yield Button(
                    "⬅️ Back",
                    variant="primary",
                    id="job-log-back-button",
                    classes="left-button",
                )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "job-log-back-button":
            self.app.pop_screen()

    def on_mount(self) -> None:
        self.populate_log()

    def populate_log(self) -> None:
        log = self.query_one("#job-log", Log)
        log.loading = True
        self.fetch_job_log()

    @work(thread=True, exclusive=True)
    def fetch_job_log(self) -> None:
        self.job_log = self.state.get_job_log(self.job.id)
        # on_worker_state_changed will now fire

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.SUCCESS:
            if self.job_log.notify is not None:
                self.notify(self.job_log.notify)
            if self.job_log.close:
                self.app.pop_screen()
            else:
                self.display_log()
        elif event.state == WorkerState.ERROR:
            log = self.query_one("#job-log", Log)
            log.loading = False
            self.notify(f"Error fetching jobs: {event.error}")

    def display_log(self) -> None:
        log = self.query_one("#job-log", Log)
        log.write_lines(self.job_log.log.split("\n"))
        log.loading = False
        log.refresh()


class ExpressQueuePromptScreen(JobScriptScreen):
    BINDINGS = [("escape", "app.pop_screen", "Back")]

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="express-queue-prompt-screen", classes="screen-container"):
            with Center():
                with Container(
                    id="express-queue-prompt-container", classes="centered-container"
                ):
                    yield Label(
                        f"Enter express account to elevate job '{self.job.name}':",
                        id="express-queue-label",
                        classes="screen-title",
                    )
                    yield Input(placeholder="exp-xxxxx", id="express-queue-input")
                    with Horizontal(
                        id="express-queue-buttons", classes="button-container"
                    ):
                        yield Button(
                            "⬅️ Back",
                            variant="primary",
                            id="express-queue-prompt-back-button",
                            classes="left-button",
                        )
                        yield Static()
                        yield Button(
                            "✅ Confirm",
                            variant="primary",
                            id="express-queue-prompt-enter-button",
                            classes="right-button",
                        )
        yield Footer()

    def on_mount(self) -> None:
        """Focus the input when the screen is mounted."""
        self.query_one("#express-queue-input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "express-queue-prompt-back-button":
            self.app.pop_screen()
        elif event.button.id == "express-queue-prompt-enter-button":
            input_field = self.query_one("#express-queue-input", Input)
            self.elevate_job(input_field.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "express-queue-input":
            self.elevate_job(event.input.value)

    def elevate_job(self, express_account: str) -> None:
        try:
            # TODO: Implement job elevation
            self.notify(f"Elevating job with express account '{express_account}'...")
            self.state.workload_manager.elevate_job(self.job, express_account)
            self.state.job_data.pop(self.job.id)
            self.browser_handle.populate_table()
            # Pop twice to return to the job browser screen
            self.notify(f"✅ Elevated job '{self.job.name}'")
            self.app.pop_screen()
            self.app.pop_screen()
        except Exception as e:
            self.notify(f"❌ Error elevating job: {e}")
            self.query_one("#express-queue-input", Input).value = ""
            self.query_one("#express-queue-input", Input).focus()


class ManagerApp(App):
    TITLE = "PBS Job Manager"
    CSS_PATH = "manager.tcss"  # We'll create this later
    SCREENS = {"host_select": HostSelectionScreen, "job_browser": JobBrowserScreen}

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.state = ManagerState()

    def on_mount(self) -> None:
        self.push_screen(HostSelectionScreen(state=self.state))


def main():
    app = ManagerApp()
    app.run()


if __name__ == "__main__":
    main()
