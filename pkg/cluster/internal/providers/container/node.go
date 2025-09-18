/*
Copyright 2019 The Kubernetes Authors.

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
	"context"
	"io"
	"strings"

	"sigs.k8s.io/kind/pkg/errors"
	"sigs.k8s.io/kind/pkg/exec"
)

// nodes.Node implementation for the docker provider
type node struct {
	name string
}

func (n *node) String() string {
	return n.name
}

func (n *node) Role() (string, error) {

	containers, err := getContainers(n.name)
	if err != nil {
		return "", err
	}
	if len(containers) != 1 {
		return "", errors.New("Container not found for name " + n.name)
	}

	return containers[0].Configuration.Labels[nodeRoleLabelKey], nil
}

func (n *node) IP() (ipv4 string, ipv6 string, err error) {
	containers, err := getContainers(n.String())
	if err != nil {
		return "", "", err
	}
	container := containers[0]

	if networks := container.Networks; len(networks) == 1 {
		a := strings.SplitN(networks[0].Address, "/", 2)
		// join host and port
		return a[0], "", nil
	}

	return "", "", errors.New("Expected one network for container with name " + n.String())
}

func (n *node) Command(command string, args ...string) exec.Cmd {
	return &nodeCmd{
		nameOrID: n.name,
		command:  command,
		args:     args,
	}
}

func (n *node) CommandContext(ctx context.Context, command string, args ...string) exec.Cmd {
	return &nodeCmd{
		nameOrID: n.name,
		command:  command,
		args:     args,
		ctx:      ctx,
	}
}

// nodeCmd implements exec.Cmd for docker nodes
type nodeCmd struct {
	nameOrID string // the container name or ID
	command  string
	args     []string
	env      []string
	stdin    io.Reader
	stdout   io.Writer
	stderr   io.Writer
	ctx      context.Context
}

func (c *nodeCmd) Run() error {
	args := []string{
		"exec",
	}
	if c.stdin != nil {
		args = append(args,
			"-i", // interactive so we can supply input
		)
	}
	// set env
	for _, env := range c.env {
		args = append(args, "-e", env)
	}
	// specify the container and command, after this everything will be
	// args the command in the container rather than to docker
	args = append(
		args,
		c.nameOrID, // ... against the container
		c.command,  // with the command specified
	)
	args = append(
		args,
		// finally, with the caller args
		c.args...,
	)
	var cmd exec.Cmd
	if c.ctx != nil {
		cmd = exec.CommandContext(c.ctx, "container", args...)
	} else {
		cmd = exec.Command("container", args...)
	}
	if c.stdin != nil {
		cmd.SetStdin(c.stdin)
	}
	if c.stderr != nil {
		cmd.SetStderr(c.stderr)
	}
	if c.stdout != nil {
		cmd.SetStdout(c.stdout)
	}
	return cmd.Run()
}

func (c *nodeCmd) SetEnv(env ...string) exec.Cmd {
	c.env = env
	return c
}

func (c *nodeCmd) SetStdin(r io.Reader) exec.Cmd {
	c.stdin = r
	return c
}

func (c *nodeCmd) SetStdout(w io.Writer) exec.Cmd {
	c.stdout = w
	return c
}

func (c *nodeCmd) SetStderr(w io.Writer) exec.Cmd {
	c.stderr = w
	return c
}

func (n *node) SerialLogs(w io.Writer) error {
	return exec.Command("container", "logs", n.name).SetStdout(w).SetStderr(w).Run()
}
