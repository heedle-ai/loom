package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/heedle-ai/treadle"
	"github.com/heedle-ai/warp"
)

// publishSkill is `loom skills publish NAME -repo DIR -store DIR`
// (roadmap 10.6): treadle's Publish, which commits in your clone and
// never pushes. Flags may come before publish or after NAME.
func publishSkill(ctx context.Context, out io.Writer, fs *flag.FlagSet, store string, repo *string) error {
	rest := fs.Args()[1:]
	if len(rest) == 0 || rest[0] == "" || rest[0][0] == '-' {
		return errors.New("skills publish needs the skill's name: loom skills publish NAME -repo DIR -store DIR")
	}
	name := rest[0]
	if err := fs.Parse(rest[1:]); err != nil {
		return err
	}
	if s := fs.Lookup("store").Value.String(); s != "" {
		store = s
	}
	if fs.NArg() != 0 || store == "" || *repo == "" {
		return errors.New("skills publish takes NAME, -repo DIR (a clone of your repository) and -store DIR, and nothing else")
	}
	other := ""
	fs.Visit(func(f *flag.Flag) {
		if f.Name != "store" && f.Name != "repo" {
			other = f.Name
		}
	})
	if other != "" {
		return fmt.Errorf("skills publish doesn't take -%s", other)
	}
	s, err := warp.Open(ctx, store)
	if err != nil {
		return err
	}
	p, err := (&treadle.Library{Store: s}).Publish(ctx, name, *repo)
	if err != nil {
		return err
	}
	if p.Commit == "" {
		fmt.Fprintf(out, "%s at %s is already in %s; nothing to commit\n", name, p.Skill.Version, *repo)
	} else {
		fmt.Fprintf(out, "published %s at %s to %s/skills/%s, commit %s\n", name, p.Skill.Version, *repo, name, p.Commit)
	}
	fmt.Fprintf(out, "push it yourself: %s\n", p.Push)
	return nil
}
