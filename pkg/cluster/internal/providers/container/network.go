/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package container

import (
	"encoding/json"
	"strings"

	"sigs.k8s.io/kind/pkg/errors"
	"sigs.k8s.io/kind/pkg/exec"
)

// This may be overridden by KIND_EXPERIMENTAL_DOCKER_NETWORK env,
// experimentally...
//
// By default currently picking a single network is equivalent to the previous
// behavior *except* that we moved from the default bridge to a user defined
// network because the default bridge is actually special versus any other
// docker network and lacks the embedded DNS
//
// For now this also makes it easier for apps to join the same network, and
// leaves users with complex networking desires to create and manage their own
// networks.
const fixedNetworkName = "kind"

// ensureNetwork checks if conatiner network by name exists, if not it creates it
func ensureNetwork(name string) error {
	// check if network exists already and remove any duplicate networks
	exists, err := checkIfNetworkExists(name)
	if err != nil {
		return err
	}

	// network already exists, we're good
	// TODO: the network might already exist and not have ipv6 ... :|
	// discussion: https://github.com/kubernetes-sigs/kind/pull/1508#discussion_r414594198
	if exists {
		return nil
	}

	return createNetwork(name)
}

func createNetwork(name string) error {
	return exec.Command("container", "network", "create", name).Run()
}

type networkInspectEntry struct {
	ID string `json:"id"`
}

func checkIfNetworkExists(name string) (bool, error) {
	out, err := exec.Output(exec.Command(
		"container", "network", "ls",
		"--format=json",
	))

	if err != nil {
		return false, errors.Wrap(err, "failed to get networks list")
	}

	networks := []networkInspectEntry{}
	if err := json.Unmarshal(out, &networks); err != nil {
		return false, errors.Wrap(err, "failed to decode networks list")
	}

	for _, network := range networks {
		if network.ID == name {
			return true, nil
		}
	}

	return false, nil
}

func isNetworkAlreadyExistsError(err error) bool {
	rerr := exec.RunErrorForError(err)
	return rerr != nil && strings.HasPrefix(string(rerr.Output), "Error: exists: \"network ") && strings.Contains(string(rerr.Output), " already exists\"")
}

func deleteNetworks(networks ...string) error {
	return exec.Command("container", append([]string{"network", "rm"}, networks...)...).Run()
}
