package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"kairo/internal/id"
	"kairo/internal/store"
)

// taskRef names a task: PROJECT [QUEUE] TASK. The queue is "" until given or
// looked up (resolveQueue); routes under /api/projects/{p}/tasks/{t} do not
// need it.
type taskRef struct {
	project, queue, task string
}

func parseTaskRef(command string, positional []string) (taskRef, error) {
	switch len(positional) {
	case 2:
		return taskRef{project: positional[0], task: positional[1]}, nil
	case 3:
		return taskRef{project: positional[0], queue: positional[1], task: positional[2]}, nil
	}
	return taskRef{}, usageError("%s requires PROJECT [QUEUE] TASK", command)
}

func (r taskRef) notFound() error {
	return fmt.Errorf("task %s not found in project %s; give PROJECT QUEUE TASK for tasks submitted without a ProjectSpec", r.task, r.project)
}

// resolveQueue looks the task's queue up in the project's status.
func (r *taskRef) resolveQueue(client *apiClient) error {
	if r.queue != "" {
		return nil
	}
	resp, err := fetchProject(client, r.project, projectQuery{tasks: []string{r.task}})
	if notFound(err) {
		return r.notFound()
	}
	if err != nil {
		return err
	}
	for _, t := range resp.Project.Tasks {
		if t.Name == r.task {
			r.queue = t.Queue
			return nil
		}
	}
	return r.notFound()
}

func (r taskRef) path() string {
	return "/api/projects/" + url.PathEscape(r.project) + "/tasks/" + url.PathEscape(r.task)
}

// fetchTask is GET /api/projects/{p}/tasks/{t}: the parsed detail and the body.
func fetchTask(client *apiClient, ref taskRef) (*taskDetail, []byte, error) {
	payload, err := client.call(http.MethodGet, ref.path(), nil)
	if notFound(err) {
		return nil, nil, fmt.Errorf("%w (%v)", ref.notFound(), err)
	}
	if err != nil {
		return nil, nil, err
	}
	// The executions come in ordinal order.
	var wire struct {
		Task taskDetail `json:"task"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		return nil, nil, fmt.Errorf("decode task: %w", err)
	}
	return &wire.Task, payload, nil
}

// sinceStamp turns --since (a duration ago, "3d", or an RFC 3339 time) into
// an RFC 3339 time.
func sinceStamp(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t.UTC().Format(time.RFC3339), nil
	}
	d, err := time.ParseDuration(value)
	if err != nil && strings.HasSuffix(value, "d") {
		var days int
		if days, err = strconv.Atoi(strings.TrimSuffix(value, "d")); err == nil {
			d = time.Duration(days) * 24 * time.Hour
		}
	}
	if err != nil || d < 0 {
		return "", usageError("--since %q: want a duration (90m, 24h, 3d) or an RFC 3339 time", value)
	}
	return nowFunc().Add(-d).UTC().Format(time.RFC3339), nil
}

// taskList: kairo task list PROJECT [filters].
func taskList(args []string) error {
	fs := newFlags("task list")
	api := apiFlag(fs)
	state := fs.String("state", "", "only tasks in these states (comma separated: "+strings.Join(store.TaskStates, ",")+")")
	match := fs.String("match", "", "only tasks whose name starts with PREFIX")
	live := fs.Bool("live", false, "only pending and running tasks")
	since := fs.String("since", "", "only tasks updated since DURATION ago (90m, 24h, 3d) or an RFC 3339 time")
	all := fs.Bool("all", false, "every task (default without filters: live ones and those updated in the last 24h)")
	asJSON := fs.Bool("json", false, "print the filtered API response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("task list requires PROJECT")
	}
	project := positional[0]
	q := projectQuery{match: *match, states: splitList(*state), live: *live, raw: *asJSON}
	if q.since, err = sinceStamp(*since); err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	var resp *projectResponse
	if *all || len(q.states) > 0 || q.live || q.since != "" {
		resp, err = fetchProject(client, project, q)
	} else {
		// Default: what is live plus what changed in the last day.
		resp, err = fetchLiveAndRecent(client, project, q, nowFunc().Add(-24*time.Hour))
	}
	if notFound(err) {
		return fmt.Errorf("project %s not found (%v)", project, err)
	}
	if err != nil {
		return err
	}
	sortTasks(resp.Project.Tasks)
	if *asJSON {
		body, err := resp.json()
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(append(body, '\n'))
		return err
	}
	now := nowFunc()
	if len(resp.Project.Tasks) > 0 {
		rows := []string{"NAME\tQUEUE\tSTATE\tEXEC\tSTARTED\tELAPSED\tNOTE"}
		for _, t := range resp.Project.Tasks {
			exec, started := "-", "-"
			if e := t.latest(); e != nil {
				exec = "#" + strconv.Itoa(e.Ordinal)
				if at, ok := e.startedAt(); ok {
					started = shortTime(at)
				}
			}
			rows = append(rows, strings.Join([]string{t.Name, t.Queue, t.State, exec, started, dash(elapsed(t, now)), taskNote(t)}, "\t"))
		}
		if err := writeTable(os.Stdout, rows); err != nil {
			return err
		}
	} else {
		fmt.Println("no tasks match (--all shows every task)")
	}
	counts := resp.Project.StateCounts
	if counts == nil {
		counts = countStates(resp.Project.Tasks)
	}
	name := resp.Project.Name
	if name == "" {
		name = project
	}
	fmt.Printf("%s: %s (%d shown)\n", name, stateSummary(counts), len(resp.Project.Tasks))
	return nil
}

// writeTable aligns tab-separated rows and drops the padding after an empty
// last column.
func writeTable(w io.Writer, rows []string) error {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
	for _, row := range rows {
		fmt.Fprintln(tw, row)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	var out bytes.Buffer
	for _, line := range strings.SplitAfter(buf.String(), "\n") {
		if strings.HasSuffix(line, "\n") {
			line = strings.TrimRight(line[:len(line)-1], " ") + "\n"
		}
		out.WriteString(line)
	}
	_, err := w.Write(out.Bytes())
	return err
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// sortTasks: running first, then pending, then the rest by most recent update.
func sortTasks(tasks []taskStatus) {
	rank := func(state string) int {
		switch state {
		case "running":
			return 0
		case "pending":
			return 1
		}
		return 2
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		a, b := tasks[i], tasks[j]
		if rank(a.State) != rank(b.State) {
			return rank(a.State) < rank(b.State)
		}
		if rank(a.State) == 2 && a.UpdatedAt != b.UpdatedAt {
			ta, _ := parseStamp(&a.UpdatedAt)
			tb, _ := parseStamp(&b.UpdatedAt)
			return ta.After(tb)
		}
		return a.Name < b.Name
	})
}

// taskShow: kairo task show PROJECT [QUEUE] TASK [--json].
func taskShow(args []string) error {
	fs := newFlags("task show")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the API response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	ref, err := parseTaskRef("task show", positional)
	if err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	task, payload, err := fetchTask(client, ref)
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	renderTask(os.Stdout, ref.project, task, nowFunc())
	return nil
}

// taskCancel: kairo task cancel PROJECT [QUEUE] TASK [--dry-run] cancels a
// task that is not running: every execution of it that has not started is
// withdrawn (gang ranks included) and the task ends cancelled, also while it
// still waits for its dependencies. A running task is refused: pause it, or
// end it with `task pause --force`.
func taskCancel(args []string) error {
	fs := newFlags("task cancel")
	api := apiFlag(fs)
	dryRun := fs.Bool("dry-run", false, "list what would be withdrawn")
	actor := fs.String("actor", defaultActor(), "audit actor")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	ref, err := parseTaskRef("task cancel", positional)
	if err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	var result store.CancelResult
	err = client.callJSON(http.MethodPost, ref.path()+"/cancel", map[string]any{"actor": *actor, "dry_run": *dryRun}, &result)
	switch {
	case routeMissing(err):
		fmt.Fprintf(os.Stderr, "kairo: the daemon does not know task cancel: %s; withdrawing the task's executions instead\n", olderDaemonHint())
		if err := ref.resolveQueue(client); err != nil {
			return err
		}
		return withdrawTaskExecutions(client, ref, *dryRun)
	case notFound(err) && ref.queue != "":
		// Not a declared task: a task of plain executions.
		return withdrawTaskExecutions(client, ref, *dryRun)
	case notFound(err):
		return fmt.Errorf("%w (%v)", ref.notFound(), err)
	case err != nil:
		return err
	}
	if result.Task == "" {
		result.Task = ref.task
	}
	for _, execution := range result.Withdrawn {
		if *dryRun {
			fmt.Println("would withdraw", execution)
		} else {
			fmt.Println("withdrawn", execution)
		}
	}
	if *dryRun {
		fmt.Println("would cancel", result.Task)
	} else {
		fmt.Println("cancelled", result.Task)
	}
	return nil
}

// withdrawTaskExecutions withdraws every unstarted execution of a task
// through the execution API (daemons without the cancel route, and tasks of
// plain executions).
func withdrawTaskExecutions(client *apiClient, ref taskRef, dryRun bool) error {
	query := url.Values{"project": {ref.project}, "queue": {ref.queue}, "task": {ref.task}, "limit": {"1000"}}
	var listed struct {
		Executions []struct {
			ID        string  `json:"id"`
			State     string  `json:"state"`
			StartedAt *string `json:"started_at"`
		} `json:"executions"`
	}
	if err := client.callJSON(http.MethodGet, withQuery("/api/executions", query), nil, &listed); err != nil {
		return err
	}
	var todo []string
	for _, e := range listed.Executions {
		if e.State == "terminal" {
			continue
		}
		if e.StartedAt != nil || e.State == "started" {
			return fmt.Errorf("execution %s of %s/%s/%s is running (%s); pause the task first, or end it with `task pause --force`", e.ID, ref.project, ref.queue, ref.task, e.State)
		}
		todo = append(todo, e.ID)
	}
	if len(todo) == 0 {
		fmt.Println("nothing to withdraw")
		return nil
	}
	for _, id := range todo {
		if dryRun {
			fmt.Println("would withdraw", id)
			continue
		}
		if _, err := client.call(http.MethodPost, "/api/executions/"+url.PathEscape(id)+"/withdraw", nil); err != nil {
			return fmt.Errorf("withdraw %s: %w", id, err)
		}
		fmt.Println("withdrawn", id)
	}
	return nil
}

// taskRerun: kairo task rerun PROJECT [QUEUE] TASK runs an ended task that
// did not succeed again, as the same declaration.
func taskRerun(args []string) error {
	fs := newFlags("task rerun")
	api := apiFlag(fs)
	actor := fs.String("actor", defaultActor(), "audit actor")
	requestID := fs.String("request-id", id.New("cli"), "idempotency request ID")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	ref, err := parseTaskRef("task rerun", positional)
	if err != nil {
		return err
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	var result store.RerunResult
	err = client.callJSON(http.MethodPost, ref.path()+"/rerun", map[string]string{"actor": *actor, "request_id": *requestID}, &result)
	if notFound(err) {
		return fmt.Errorf("%w (%v)", ref.notFound(), err)
	}
	if err != nil {
		return err
	}
	if result.Task == "" {
		result.Task = ref.task
	}
	fmt.Printf("rerun %s (execution #%d)\n", result.Task, result.Ordinal)
	for _, name := range result.Unblocked {
		fmt.Println("unblocked", name)
	}
	return nil
}
