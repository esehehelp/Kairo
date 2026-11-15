package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"kairo/internal/store"
)

// storeStatus: kairo store status [--json].
func storeStatus(args []string) error {
	fs := newFlags("store status")
	api := apiFlag(fs)
	asJSON := fs.Bool("json", false, "print the daemon's response")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return usageError("store status accepts no arguments")
	}
	client, err := newAPIClient(*api)
	if err != nil {
		return err
	}
	payload, err := client.call(http.MethodGet, "/api/stores", nil)
	if routeMissing(err) {
		return fmt.Errorf("the daemon does not know stores: %s", olderDaemonHint())
	}
	if err != nil {
		return err
	}
	if *asJSON {
		_, err = os.Stdout.Write(payload)
		return err
	}
	var body struct {
		Stores []store.StoreStatus `json:"stores"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return fmt.Errorf("decode stores: %w", err)
	}
	fmt.Print(formatStores(body.Stores))
	return nil
}

// formatStores is one line a store (its filesystem's total and free, Kairo's
// bytes by class and what else the filesystem holds), then each store's kept
// scratch and its missing scratch.
func formatStores(stores []store.StoreStatus) string {
	if len(stores) == 0 {
		return "no stores declared ([[stores]] in kairo machine apply)\n"
	}
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "STORE\tCLASS\tNODE\tSTATE\tROOT\tFILESYSTEM\tTOTAL\tFREE\tACTIVE\tKEPT\tOUTPUTS\tPACKS\tCOPIES\tPINNED\tCACHE\tEXTERNAL")
	counted := func(bytes int64, n int) string {
		if n > 0 {
			return fmt.Sprintf("%s (%d)", formatSize(bytes), n)
		}
		return formatSize(bytes)
	}
	for _, s := range stores {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.ID, dash(s.Class), dash(storeNodeLabel(s)), s.State, dash(s.Root), dash(s.Filesystem),
			optionalSize(s.TotalBytes), optionalSize(s.FreeBytes), formatSize(s.ScratchActiveBytes), formatSize(s.ScratchKeptBytes), counted(s.OutputBytes, s.Outputs),
			counted(s.PackBytes, s.Packs), counted(s.CopyBytes, s.Copies), formatSize(s.PinnedBytes), formatSize(s.CacheBytes), optionalSize(s.ExternalBytes))
	}
	w.Flush()
	for _, s := range stores {
		if s.Problem != nil {
			fmt.Fprintf(&b, "%s: %s\n", s.ID, *s.Problem)
		}
	}
	for _, s := range stores {
		if len(s.Kept) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\nkept scratch on %s:\n", s.ID)
		w = tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  ATTEMPT\tTASK\tBYTES\tKEPT\tREASON\tPATH")
		for _, k := range s.Kept {
			kept, reason := "-", "-"
			if k.KeptAt != nil {
				kept = *k.KeptAt
			}
			if k.Reason != nil {
				reason = *k.Reason
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%s\n", k.AttemptID, dash(k.Task), optionalSize(k.Bytes), kept, reason, k.Path)
		}
		w.Flush()
	}
	for _, s := range stores {
		if len(s.Missing) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\nmissing scratch on %s (no longer found under the root; Kairo did not delete it):\n", s.ID)
		w = tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  ATTEMPT\tTASK\tSINCE\tPATH")
		for _, k := range s.Missing {
			since := "-"
			if k.MissingAt != nil {
				since = *k.MissingAt
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", k.AttemptID, dash(k.Task), since, k.Path)
		}
		w.Flush()
	}
	for _, s := range stores {
		if len(s.Damaged) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\ndamaged copies on %s (read back unlike their pack; nothing was deleted):\n", s.ID)
		w = tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  OBJECT\tCHECKED\tPATH\tPROBLEM")
		for _, d := range s.Damaged {
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", d.ObjectID, d.CheckedAt, d.Path, d.Problem)
		}
		w.Flush()
	}
	return b.String()
}

// storeNodeLabel is a store's node, with its executor when it has one.
func storeNodeLabel(s store.StoreStatus) string {
	if s.Executor != "" {
		return s.Node + "/" + s.Executor
	}
	return s.Node
}

func optionalSize(bytes *int64) string {
	if bytes == nil {
		return "-"
	}
	return formatSize(*bytes)
}

// storeCatalogue: kairo store catalogue ROOT [--json]. It reads a durable
// store's directory alone, no daemon: the description beside each copy
// (<root>/objects/<month>/<object id>.tar.zst.object.json), one line each,
// then how many copies there have none.
func storeCatalogue(args []string) error {
	fs := newFlags("store catalogue")
	asJSON := fs.Bool("json", false, "print the descriptions as a JSON array")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError("store catalogue takes the durable store's root directory")
	}
	root := positional[0]
	months, err := os.ReadDir(filepath.Join(root, "objects"))
	if err != nil {
		return err
	}
	var out []store.ObjectDescription
	undescribed := 0
	for _, month := range months {
		if !month.IsDir() {
			continue
		}
		dir := filepath.Join(root, "objects", month.Name())
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		have := map[string]bool{}
		for _, e := range entries {
			have[e.Name()] = true
		}
		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".tar.zst") {
				continue
			}
			if !have[name+".object.json"] {
				undescribed++
				continue
			}
			body, err := os.ReadFile(filepath.Join(dir, name+".object.json"))
			if err != nil {
				return err
			}
			var d store.ObjectDescription
			if err = json.Unmarshal(body, &d); err != nil {
				return fmt.Errorf("%s: %w", filepath.Join(dir, name+".object.json"), err)
			}
			out = append(out, d)
		}
	}
	if *asJSON {
		if out == nil {
			out = []store.ObjectDescription{}
		}
		body, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Println(string(body))
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "OBJECT\tKIND\tPROJECT\tNAME\tTASK\tSOURCE\tPACK\tCREATED")
	str := func(s *string) string {
		if s == nil {
			return "-"
		}
		return *s
	}
	for _, d := range out {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", d.ObjectID, d.Kind, str(d.Project), str(d.Name), str(d.Task), d.SourcePath, byteSize(d.PackBytes), d.CreatedAt)
	}
	if err = w.Flush(); err != nil {
		return err
	}
	fmt.Printf("%d described, %d without a description (made before descriptions, or not described yet)\n", len(out), undescribed)
	return nil
}
