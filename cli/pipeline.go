package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v3"
)

type ParamEntry struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type Work struct {
	Name       string
	Entrypoint string
	Params     []ParamEntry
	After      []string
	LocalMode  bool
	ManualWork bool
}

type Stage struct {
	Name  string
	Works []Work
}

type Pipeline struct {
	Name         string
	Root         string
	SrcDir       string
	Dependencies string
	Stages       []Stage
}

type Mantafile struct {
	Cluster   string // empty == local mode
	Pipelines []*Pipeline
}

func parseMantafile(path string) (*Mantafile, error) {
	var raw map[string]interface{}
	meta, err := toml.DecodeFile(path, &raw)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	cluster, _ := raw["cluster"].(string)

	byName := map[string]*Pipeline{}
	var order []string

	// meta.Keys() returns all table keys in file order, as []string paths.
	// Scalars (cluster, src_dir, entrypoint) are not included.
	// key[0] is always the pipeline name at all depths.
	// depth 1 = pipeline, depth 2 = stage, depth 3 = work
	for _, key := range meta.Keys() {
		switch len(key) {
		case 1:
			name := key[0]
			pipelineMap, ok := raw[name].(map[string]interface{})
			if !ok {
				// Top-level scalar (e.g. "cluster"); not a pipeline table.
				continue
			}
			if _, has := pipelineMap["cluster"]; has {
				return nil, fmt.Errorf("pipeline %q: per-pipeline 'cluster' is no longer supported — move 'cluster = ...' to the top of mantafile.toml; omit it for local mode", name)
			}
			if _, has := pipelineMap["local_only"]; has {
				return nil, fmt.Errorf("pipeline %q: 'local_only' is no longer supported — omit the top-level 'cluster' for local mode", name)
			}
			p := &Pipeline{Name: name}
			p.SrcDir, _ = pipelineMap["src_dir"].(string)
			p.Dependencies, _ = pipelineMap["dependencies"].(string)
			byName[name] = p
			order = append(order, name)

		case 2:
			p := byName[key[0]]
			pipelineMap, _ := raw[key[0]].(map[string]interface{})
			if _, isTable := pipelineMap[key[1]].(map[string]interface{}); isTable {
				p.Stages = append(p.Stages, Stage{Name: key[1]})
			}

		case 3:
			p := byName[key[0]]
			stageName := key[1]
			workName := key[2]
			stageMap, _ := raw[key[0]].(map[string]interface{})
			sMap, _ := stageMap[stageName].(map[string]interface{})
			wMap, _ := sMap[workName].(map[string]interface{})
			entrypoint, _ := wMap["entrypoint"].(string)
			localMode, _ := wMap["local_mode"].(bool)
			manualWork, _ := wMap["manual_work"].(bool)

			var params []ParamEntry
			if arr, ok := wMap["params"].([]interface{}); ok {
				for _, item := range arr {
					if m, ok := item.(map[string]interface{}); ok {
						params = append(params, ParamEntry{
							Name:  m["name"].(string),
							Value: m["value"],
						})
					}
				}
			}

			var after []string
			if arr, ok := wMap["after"].([]interface{}); ok {
				for _, v := range arr {
					if s, ok := v.(string); ok {
						after = append(after, s)
					}
				}
			}

			for i := range p.Stages {
				if p.Stages[i].Name == stageName {
					p.Stages[i].Works = append(p.Stages[i].Works, Work{
						Name:       workName,
						Entrypoint: entrypoint,
						Params:     params,
						After:      after,
						LocalMode:  localMode,
						ManualWork: manualWork,
					})
					break
				}
			}
		}
	}

	if len(order) == 0 {
		return nil, fmt.Errorf("no pipelines found in %s", path)
	}
	pipelines := make([]*Pipeline, len(order))
	for i, name := range order {
		pipelines[i] = byName[name]
	}
	return &Mantafile{Cluster: cluster, Pipelines: pipelines}, nil
}

// validateGraph checks that all after references name existing works and that
// there are no cycles in the full pipeline dependency graph.
func validateGraph(p *Pipeline) error {
	allWorks := make(map[string]bool)
	for _, stage := range p.Stages {
		for _, work := range stage.Works {
			allWorks[work.Name] = true
		}
	}

	inDegree := make(map[string]int)
	dependents := make(map[string][]string)
	for name := range allWorks {
		inDegree[name] = 0
	}

	for _, stage := range p.Stages {
		for _, work := range stage.Works {
			for _, dep := range work.After {
				if dep == "root" {
					continue
				}
				if !allWorks[dep] {
					return fmt.Errorf("work %q: after references unknown work %q", work.Name, dep)
				}
				inDegree[work.Name]++
				dependents[dep] = append(dependents[dep], work.Name)
			}
		}
	}

	queue := []string{}
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	processed := 0
	for len(queue) > 0 {
		next := []string{}
		for _, name := range queue {
			processed++
			for _, dependent := range dependents[name] {
				inDegree[dependent]--
				if inDegree[dependent] == 0 {
					next = append(next, dependent)
				}
			}
		}
		queue = next
	}

	if processed < len(allWorks) {
		var cycled []string
		for name, deg := range inDegree {
			if deg > 0 {
				cycled = append(cycled, name)
			}
		}
		sort.Strings(cycled)
		return fmt.Errorf("cycle detected among works: %s", strings.Join(cycled, ", "))
	}

	return nil
}

type ClusterConfig struct {
	Provider struct {
		HeadIP string `yaml:"head_ip"`
	} `yaml:"provider"`
	Auth struct {
		SSHUser       string `yaml:"ssh_user"`
		SSHPrivateKey string `yaml:"ssh_private_key"`
	} `yaml:"auth"`
}

func parseClusterConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster config %s: %w", path, err)
	}
	var cfg ClusterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse cluster config %s: %w", path, err)
	}
	if cfg.Provider.HeadIP == "" {
		return nil, fmt.Errorf("cluster config %s: missing provider.head_ip", path)
	}
	if cfg.Auth.SSHUser == "" {
		return nil, fmt.Errorf("cluster config %s: missing auth.ssh_user", path)
	}
	if cfg.Auth.SSHPrivateKey == "" {
		return nil, fmt.Errorf("cluster config %s: missing auth.ssh_private_key", path)
	}
	return &cfg, nil
}

// loadMantafile reads .manta/pipeline.json and returns the built Mantafile.
func loadMantafile() (*Mantafile, error) {
	data, err := os.ReadFile(pipelineFile)
	if err != nil {
		return nil, fmt.Errorf("read pipeline: %w", err)
	}
	var mf Mantafile
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("parse pipeline: %w", err)
	}
	return &mf, nil
}
