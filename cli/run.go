package main

import (
	"fmt"
	"sort"
	"strings"
)

const runsDir = ".manta/logs/sessions"

// RunTarget holds the parsed components of a run target string.
type RunTarget struct {
	Pipeline string
	Stage    string // empty = all stages
	Work     string // empty = all works in stage
}

func parseRunTarget(s string) (RunTarget, error) {
	parts := strings.Split(s, ".")
	if len(parts) == 0 || len(parts) > 3 {
		return RunTarget{}, fmt.Errorf("invalid target %q: expected <pipeline>[.<stage>[.<work>]]", s)
	}
	t := RunTarget{Pipeline: parts[0]}
	if len(parts) >= 2 {
		t.Stage = parts[1]
	}
	if len(parts) == 3 {
		t.Work = parts[2]
	}
	return t, nil
}

// ResolvedWork carries the full context path for a work item.
type ResolvedWork struct {
	Pipeline string
	Stage    string
	Work     Work
}

func (r ResolvedWork) Context() string {
	return r.Pipeline + "." + r.Stage + "." + r.Work.Name
}

// resolveWorks returns execution steps for the target. Each step contains
// works with no dependency on each other and can run in parallel. Steps are
// ordered so all dependencies of a step are satisfied by prior steps.
// Graph validity is guaranteed by build — no re-validation needed here.
func resolveWorks(p *Pipeline, t RunTarget) ([][]ResolvedWork, error) {
	if p.Name != t.Pipeline {
		return nil, fmt.Errorf("pipeline %q not found (loaded: %q)", t.Pipeline, p.Name)
	}

	// Single work: no graph computation needed.
	if t.Work != "" {
		for _, stage := range p.Stages {
			if t.Stage != "" && stage.Name != t.Stage {
				continue
			}
			for _, work := range stage.Works {
				if work.Name == t.Work {
					return [][]ResolvedWork{{{Pipeline: p.Name, Stage: stage.Name, Work: work}}}, nil
				}
			}
		}
		return nil, fmt.Errorf("work %q not found", t.Work)
	}

	// Full pipeline: run each stage sequentially, concatenate their steps.
	if t.Stage == "" {
		var allSteps [][]ResolvedWork
		for _, stage := range p.Stages {
			allSteps = append(allSteps, stageSteps(p, stage.Name)...)
		}
		if len(allSteps) == 0 {
			return nil, fmt.Errorf("no works found in pipeline %q", t.Pipeline)
		}
		return allSteps, nil
	}

	// Single stage: compute steps for that stage only.
	for _, stage := range p.Stages {
		if stage.Name == t.Stage {
			steps := stageSteps(p, stage.Name)
			if len(steps) == 0 {
				return nil, fmt.Errorf("no works found in stage %q", t.Stage)
			}
			return steps, nil
		}
	}
	return nil, fmt.Errorf("stage %q not found in pipeline %q", t.Stage, t.Pipeline)
}

// stageSteps computes execution steps for a single stage using Kahn's algorithm.
// after edges pointing outside the stage are ignored (treated as satisfied).
func stageSteps(p *Pipeline, stageName string) [][]ResolvedWork {
	byName := make(map[string]ResolvedWork)
	for _, stage := range p.Stages {
		if stage.Name != stageName {
			continue
		}
		for _, work := range stage.Works {
			if work.ManualWork {
				continue
			}
			byName[work.Name] = ResolvedWork{Pipeline: p.Name, Stage: stage.Name, Work: work}
		}
	}
	if len(byName) == 0 {
		return nil
	}

	inDegree := make(map[string]int)
	dependents := make(map[string][]string)
	for name := range byName {
		inDegree[name] = 0
	}
	for name, rw := range byName {
		for _, dep := range rw.Work.After {
			if dep == "root" {
				continue
			}
			if _, inScope := byName[dep]; !inScope {
				continue
			}
			inDegree[name]++
			dependents[dep] = append(dependents[dep], name)
		}
	}

	queue := []string{}
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var steps [][]ResolvedWork
	for len(queue) > 0 {
		sort.Strings(queue)
		step := make([]ResolvedWork, 0, len(queue))
		for _, name := range queue {
			step = append(step, byName[name])
		}
		steps = append(steps, step)

		next := []string{}
		for _, name := range queue {
			for _, dependent := range dependents[name] {
				inDegree[dependent]--
				if inDegree[dependent] == 0 {
					next = append(next, dependent)
				}
			}
		}
		queue = next
	}

	return steps
}
