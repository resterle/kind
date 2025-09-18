/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or impliep.
See the License for the specific language governing permissions and
limitations under the License.
*/

package container

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/errors"
	"sigs.k8s.io/kind/pkg/exec"
	"sigs.k8s.io/kind/pkg/log"

	"sigs.k8s.io/kind/pkg/cluster/internal/providers"
	"sigs.k8s.io/kind/pkg/cluster/internal/providers/common"
	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
	"sigs.k8s.io/kind/pkg/internal/apis/config"
	"sigs.k8s.io/kind/pkg/internal/cli"
	"sigs.k8s.io/kind/pkg/internal/sets"
)

// NewProvider returns a new provider based on executing `docker ...`
func NewProvider(logger log.Logger) providers.Provider {
	return &provider{
		logger: logger,
	}
}

// Provider implements provider.Provider
// see NewProvider
type provider struct {
	logger log.Logger
	info   *providers.ProviderInfo
}

// String implements fmt.Stringer
// NOTE: the value of this should not currently be relied upon for anything!
// This is only used for setting the Node's providerID
func (p *provider) String() string {
	return "container"
}

// Provision is part of the providers.Provider interface
func (p *provider) Provision(status *cli.Status, cfg *config.Cluster) (err error) {
	// TODO: validate cfg
	// ensure node images are pulled before actually provisioning
	if err := ensureNodeImages(p.logger, status, cfg); err != nil {
		return err
	}

	// ensure the pre-requisite network exists
	networkName := fixedNetworkName
	if err := ensureNetwork(networkName); err != nil {
		return errors.Wrap(err, "failed to ensure network")
	}

	// actually provision the cluster
	icons := strings.Repeat("📦 ", len(cfg.Nodes))
	status.Start(fmt.Sprintf("Preparing nodes %s", icons))
	defer func() { status.End(err == nil) }()

	// plan creating the containers
	createContainerFuncs, err := planCreation(cfg, networkName)
	if err != nil {
		return err
	}

	// actually create nodes
	return errors.UntilErrorConcurrent(createContainerFuncs)
}

// ListClusters is part of the providers.Provider interface
func (p *provider) ListClusters() ([]string, error) {
	lines := []string{}

	containers, err := getAllContainers()
	if err != nil {
		return lines, err
	}

	for _, c := range containers {
		if name, ok := c.Configuration.Labels[clusterLabelKey]; ok {
			lines = append(lines, name)
		}
	}

	return sets.NewString(lines...).List(), nil
}

// ListNodes is part of the providers.Provider interface
func (p *provider) ListNodes(cluster string) ([]nodes.Node, error) {
	nodes := []nodes.Node{}

	containers, err := getAllContainers()
	if err != nil {
		return nodes, err
	}

	for _, c := range containers {
		if c.Configuration.Labels[clusterLabelKey] == cluster {
			nodes = append(nodes, p.node(c.Configuration.Id))
		}
	}
	return nodes, nil
}

// DeleteNodes is part of the providers.Provider interface
func (p *provider) DeleteNodes(n []nodes.Node) error {
	if len(n) == 0 {
		return nil
	}
	const command = "container"
	args := make([]string, 0, len(n)+3) // allocate once
	args = append(args,
		"rm",
		"-f", // force the container to be delete now
	)
	for _, node := range n {
		args = append(args, node.String())
	}
	if err := exec.Command(command, args...).Run(); err != nil {
		return errors.Wrap(err, "failed to delete nodes")
	}
	// TODO: remove volumes
	return nil
}

// GetAPIServerEndpoint is part of the providers.Provider interface
func (p *provider) GetAPIServerEndpoint(cluster string) (string, error) {
	// locate the node that hosts this
	allNodes, err := p.ListNodes(cluster)
	if err != nil {
		return "", errors.Wrap(err, "failed to list nodes")
	}
	n, err := nodeutils.APIServerEndpointNode(allNodes)
	if err != nil {
		return "", errors.Wrap(err, "failed to get api server endpoint")
	}

	containers, err := getContainers(n.String())
	if err != nil {
		return "", err
	}
	container := containers[0]

	if networks := container.Networks; len(networks) == 1 {
		a := strings.SplitN(networks[0].Address, "/", 2)
		// join host and port
		return net.JoinHostPort(a[0], fmt.Sprintf("%d", common.APIServerInternalPort)), nil
	}

	return "", errors.New("Expected one network")
}

// GetAPIServerInternalEndpoint is part of the providers.Provider interface
func (p *provider) GetAPIServerInternalEndpoint(cluster string) (string, error) {
	// locate the node that hosts this
	allNodes, err := p.ListNodes(cluster)
	if err != nil {
		return "", errors.Wrap(err, "failed to list nodes")
	}
	n, err := nodeutils.APIServerEndpointNode(allNodes)
	if err != nil {
		return "", errors.Wrap(err, "failed to get api server endpoint")
	}
	// NOTE: we're using the nodes's hostnames which are their names
	return net.JoinHostPort(n.String(), fmt.Sprintf("%d", common.APIServerInternalPort)), nil
}

// node returns a new node handle for this provider
func (p *provider) node(name string) nodes.Node {
	return &node{
		name: name,
	}
}

// CollectLogs will populate dir with cluster logs and other debug files
func (p *provider) CollectLogs(dir string, nodes []nodes.Node) error {
	execToPathFn := func(cmd exec.Cmd, path string) func() error {
		return func() error {
			f, err := common.FileOnHost(path)
			if err != nil {
				return err
			}
			defer f.Close()
			return cmd.SetStdout(f).SetStderr(f).Run()
		}
	}
	// construct a slice of methods to collect logs
	fns := []func() error{
		// record info about the host
		execToPathFn(
			exec.Command("container", "system", "status"),
			filepath.Join(dir, "container-info.txt"),
		),
	}
	// inspect each node
	for _, n := range nodes {
		node := n // https://golang.org/doc/faq#closures_and_goroutines
		name := node.String()
		path := filepath.Join(dir, name)
		fns = append(fns,
			execToPathFn(exec.Command("container", "inspect", name), filepath.Join(path, "inspect.json")),
		)
	}
	// run and collect up all errors
	return errors.AggregateConcurrent(fns)
}

// Info returns the provider info.
// The info is cached on the first time of the execution.
func (p *provider) Info() (*providers.ProviderInfo, error) {
	var err error
	if p.info == nil {
		p.info, err = info()
	}
	return p.info, err
}

func info() (*providers.ProviderInfo, error) {
	info := providers.ProviderInfo{
		Rootless:            false,
		Cgroup2:             true,
		SupportsMemoryLimit: true,
		SupportsPidsLimit:   true,
		SupportsCPUShares:   true,
	}
	return &info, nil
}

type container struct {
	Status        string        `json:"status"`
	Networks      []network     `json:"networks"`
	Configuration configuration `json:"configuration"`
}

type configuration struct {
	Id     string            `json:"id"`
	Labels map[string]string `json:"labels"`
}

type network struct {
	Network  string `json:"network"`
	Hostname string `json:"hostname"`
	Address  string `json:"address"`
}

func getAllContainers() ([]container, error) {
	cmd := exec.Command("container",
		"list",
		"-a", // show stopped nodes
		"--format", "json",
	)
	output, err := exec.Output(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list clusters")
	}

	containers := []container{}
	err = json.Unmarshal(output, &containers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal container response")
	}
	return containers, nil
}

func getContainers(id string, ids ...string) ([]container, error) {
	args := []string{"inspect", id}
	args = append(args, ids...)
	cmd := exec.Command("container", args...)
	output, err := exec.Output(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list clusters")
	}

	containers := []container{}
	err = json.Unmarshal(output, &containers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal container response")
	}
	return containers, nil
}
