package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/heedle-ai/loom"
	"github.com/heedle-ai/warp"
)

// Costs (roadmap 10.4): what runs used of Claude, as recorded, and its
// estimated cost.

// defaultPrices is your prices file: ~/.heedle/prices.json.
func defaultPrices() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".heedle", "prices.json")
}

// budgetFlags reads -budget and -resume-next-month.
func budgetFlags(usd string, resume bool) (loom.Budget, error) {
	if usd == "" {
		if resume {
			return loom.Budget{}, errors.New("-resume-next-month goes with -budget")
		}
		return loom.Budget{}, nil
	}
	v, err := loom.ParseBudget(usd)
	if err != nil {
		return loom.Budget{}, err
	}
	if v == 0 && resume {
		return loom.Budget{}, errors.New("-resume-next-month goes with a budget above 0")
	}
	return loom.Budget{USD: v, ResumeNextMonth: resume}, nil
}

// describeBudget is a schedule's budget for its line, if it has one.
func describeBudget(b *loom.Budget) string {
	if b == nil || b.USD == 0 {
		return ""
	}
	s := fmt.Sprintf("  budget $%.2f a month", b.USD)
	if b.ResumeNextMonth {
		s += ", resumed by a new month"
	}
	return s
}

// usd is an estimate in dollars, to the cent, or below one.
func usd(v float64) string {
	if v > 0 && v < 0.005 {
		return "<$0.01"
	}
	return fmt.Sprintf("$%.2f", v)
}

// costs prints the month's runs by schedule and model: their tokens, as
// recorded, and the estimated cost of those that can be priced.
func costs(ctx context.Context, out io.Writer, store, month, zone, pricesFile string, now time.Time) error {
	loc := time.UTC
	if zone != "" {
		var err error
		if loc, err = time.LoadLocation(zone); err != nil || zone == "Local" {
			return fmt.Errorf("time zone %q: use an IANA name such as Europe/Lisbon", zone)
		}
	}
	at := now
	if month != "" {
		m, err := time.ParseInLocation("2006-01", month, loc)
		if err != nil {
			return fmt.Errorf("month %q: use YYYY-MM, such as 2026-11", month)
		}
		at = m
	}
	from, to := loom.MonthOf(at, loc)
	s, err := warp.Open(ctx, store)
	if err != nil {
		return err
	}
	prices, err := loom.ReadPrices(pricesFile)
	if err != nil {
		fmt.Fprintln(out, err)
	}
	runs, err := loom.AllRunsBetween(ctx, s, from, to)
	if err != nil {
		return err
	}
	type key struct{ schedule, model string }
	type row struct {
		runs   map[string]bool
		tokens loom.Tokens
		usage  loom.Usage
	}
	rows := map[key]*row{}
	var all loom.Usage
	for _, r := range runs {
		all = all.Add(r.Usage)
		for _, st := range r.Usage.Steps {
			k := key{r.Schedule, st.Model}
			if st.Missing != "" {
				k.model = "(no counts)"
			}
			if rows[k] == nil {
				rows[k] = &row{runs: map[string]bool{}}
			}
			rows[k].runs[r.Run] = true
			rows[k].tokens = rows[k].tokens.Add(st.Tokens)
			rows[k].usage = rows[k].usage.Add(loom.Usage{Steps: []loom.StepUsage{st}})
		}
	}
	fmt.Fprintf(out, "Claude usage in %s (%s), as recorded; dollars are estimates (est.) with prices as of %s, %s\n", from.Format("January 2006"), loc, prices.AsOf, prices.Source)
	if len(prices.Yours) > 0 {
		fmt.Fprintf(out, "your prices (%s) for: %v\n", pricesFile, prices.Yours)
	}
	if len(rows) == 0 {
		fmt.Fprintf(out, "no runs asked Claude in %s\n", from.Format("January 2006"))
		return nil
	}
	keys := make([]key, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].schedule != keys[j].schedule {
			return keys[i].schedule < keys[j].schedule
		}
		return keys[i].model < keys[j].model
	})
	tw := tabwriter.NewWriter(out, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "SCHEDULE\tMODEL\tRUNS\tINPUT\tOUTPUT\tEST.")
	for _, k := range keys {
		r := rows[k]
		sched := k.schedule
		if sched == "" {
			sched = "(not scheduled)"
		}
		c := loom.Cost(r.usage, prices)
		est := "no estimate"
		switch _, priced := prices.Models[k.model]; {
		case c.Complete:
			est = usd(c.USD)
		case k.model == "(no counts)":
			est = "-"
		case !priced:
			est = "no price"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%s\n", sched, k.model, len(r.runs), r.tokens.Input, r.tokens.Output, est)
	}
	tw.Flush()
	c := loom.Cost(all, prices)
	switch priced := len(all.Steps) - len(c.Unpriced); {
	case c.Complete:
		fmt.Fprintf(out, "total: %s est.\n", usd(c.USD))
	case priced == 0:
		// Never "$0.00" for what had no price (review 48).
		fmt.Fprintf(out, "total: no estimate; %d steps have none (tokens only)\n", len(c.Unpriced))
	default:
		fmt.Fprintf(out, "total: %s est. for the steps with a price; %d steps have no estimate (tokens only)\n", usd(c.USD), len(c.Unpriced))
	}
	return nil
}

// printProposalUsage says what proposing used of Claude, as counted.
func printProposalUsage(out io.Writer, u *loom.Usage) {
	if u == nil || len(u.Steps) == 0 {
		return
	}
	models := u.ByModel()
	names := make([]string, 0, len(models))
	for m := range models {
		names = append(names, m)
	}
	sort.Strings(names)
	for _, m := range names {
		fmt.Fprintf(out, "\nProposing this used %d input and %d output tokens of %s.\n", models[m].Input, models[m].Output, m)
	}
}
