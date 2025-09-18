/*
Copyright 2019 The Kubernetes Authors.

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
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"sigs.k8s.io/kind/pkg/cluster/constants"
	"sigs.k8s.io/kind/pkg/errors"
	"sigs.k8s.io/kind/pkg/exec"
	"sigs.k8s.io/kind/pkg/fs"

	"sigs.k8s.io/kind/pkg/cluster/internal/loadbalancer"
	"sigs.k8s.io/kind/pkg/cluster/internal/providers/common"
	"sigs.k8s.io/kind/pkg/internal/apis/config"
)

// planCreation creates a slice of funcs that will create the containers
func planCreation(cfg *config.Cluster, networkName string) (createContainerFuncs []func() error, err error) {
	// we need to know all the names for NO_PROXY
	// compute the names first before any actual node details
	nodeNamer := common.MakeNodeNamer(cfg.Name)
	names := make([]string, len(cfg.Nodes))
	for i, node := range cfg.Nodes {
		name := nodeNamer(string(node.Role)) // name the node
		names[i] = name
	}
	haveLoadbalancer := config.ClusterHasImplicitLoadBalancer(cfg)
	if haveLoadbalancer {
		//names = append(names, nodeNamer(constants.ExternalLoadBalancerNodeRoleValue))
		panic("Not implemented: loadbalancer")
	}

	// these apply to all container creation
	genericArgs, err := commonArgs(cfg.Name, cfg, networkName, names)
	if err != nil {
		return nil, err
	}

	// only the external LB should reflect the port if we have multiple control planes
	apiServerPort := cfg.Networking.APIServerPort
	apiServerAddress := cfg.Networking.APIServerAddress
	if haveLoadbalancer {
		panic("Not implemented")
	}

	// plan normal nodes
	for i, node := range cfg.Nodes {
		node := node.DeepCopy() // copy so we can modify
		name := names[i]

		// fixup relative paths, docker can only handle absolute paths
		for m := range node.ExtraMounts {
			hostPath := node.ExtraMounts[m].HostPath
			if !fs.IsAbs(hostPath) {
				absHostPath, err := filepath.Abs(hostPath)
				if err != nil {
					return nil, errors.Wrapf(err, "unable to resolve absolute path for hostPath: %q", hostPath)
				}
				node.ExtraMounts[m].HostPath = absHostPath
			}
		}

		// plan actual creation based on role
		switch node.Role {
		case config.ControlPlaneRole:
			createContainerFuncs = append(createContainerFuncs, func() error {
				node.ExtraPortMappings = append(node.ExtraPortMappings,
					config.PortMapping{
						ListenAddress: apiServerAddress,
						HostPort:      apiServerPort,
						ContainerPort: common.APIServerInternalPort,
					},
				)
				args, err := runArgsForNode(node, cfg.Networking.IPFamily, name, genericArgs)
				if err != nil {
					return err
				}
				return createContainerWithWaitUntilSystemdReachesMultiUserSystem(name, args)
			})
		case config.WorkerRole:
			createContainerFuncs = append(createContainerFuncs, func() error {
				args, err := runArgsForNode(node, cfg.Networking.IPFamily, name, genericArgs)
				if err != nil {
					return err
				}
				return createContainerWithWaitUntilSystemdReachesMultiUserSystem(name, args)
			})
		default:
			return nil, errors.Errorf("unknown node role: %q", node.Role)
		}
	}
	return createContainerFuncs, nil
}

// commonArgs computes static arguments that apply to all containers
func commonArgs(cluster string, cfg *config.Cluster, networkName string, nodeNames []string) ([]string, error) {
	// standard arguments all nodes containers need, computed once
	args := []string{
		"--detach", // run the container detached
		"--tty",    // allocate a tty for entrypoint logs
		// label the node with the cluster ID
		"--label", fmt.Sprintf("%s=%s", clusterLabelKey, cluster),
		// user a user defined container network so we get embedded DNS
		"--network", networkName,
	}

	// enable IPv6 if necessary
	if config.ClusterHasIPv6(cfg) {
		panic("Not implemented")
	}

	// pass proxy environment variables
	proxyEnv, err := getProxyEnv(cfg, networkName, nodeNames)
	if err != nil {
		return nil, errors.Wrap(err, "proxy setup error")
	}
	for key, val := range proxyEnv {
		args = append(args, "-e", fmt.Sprintf("%s=%s", key, val))
	}

	if cfg.Networking.DNSSearch != nil {
		args = append(args, "-e", "KIND_DNS_SEARCH="+strings.Join(*cfg.Networking.DNSSearch, " "))
	}

	return args, nil
}

func runArgsForNode(node *config.Node, clusterIPFamily config.ClusterIPFamily, name string, args []string) ([]string, error) {
	args = append([]string{
		"--name", name, // make hostname match container name
		// label the node with the role ID
		"--label", fmt.Sprintf("%s=%s", nodeRoleLabelKey, node.Role),
		// runtime temporary storage
		"--tmpfs", "/tmp", // various things depend on working /tmp
		"--tmpfs", "/run", // systemd wants a writable /run
		// runtime persistent storage
		// this ensures that E.G. pods, logs etc. are not on the container
		// filesystem, which is not only better for performance, but allows
		// running kind in kind for "party tricks"
		// (please don't depend on doing this though!)
		//"--volume", "/var",
		// some k8s things want to read /lib/modules
		//"--volume", "/lib/modules:/lib/modules:ro",
		// propagate KIND_EXPERIMENTAL_CONTAINERD_SNAPSHOTTER to the entrypoint script
		"-e", "KIND_EXPERIMENTAL_CONTAINERD_SNAPSHOTTER",
		"--virtualization",
		"--memory", "8G",
	},
		args...,
	)

	// convert mounts and port mappings to container run args
	args = append(args, generateMountBindings(node.ExtraMounts...)...)
	mappingArgs, err := generatePortMappings(clusterIPFamily, node.ExtraPortMappings...)
	if err != nil {
		return nil, err
	}
	args = append(args, mappingArgs...)

	switch node.Role {
	case config.ControlPlaneRole:
		args = append(args, "-e", "KUBECONFIG=/etc/kubernetes/admin.conf")
	}

	// finally, specify the image to run
	return append(args, node.Image), nil
}

func runArgsForLoadBalancer(cfg *config.Cluster, name string, args []string) ([]string, error) {
	args = append([]string{
		"--hostname", name, // make hostname match container name
		// label the node with the role ID
		"--label", fmt.Sprintf("%s=%s", nodeRoleLabelKey, constants.ExternalLoadBalancerNodeRoleValue),
	},
		args...,
	)

	// load balancer port mapping
	mappingArgs, err := generatePortMappings(cfg.Networking.IPFamily,
		config.PortMapping{
			ListenAddress: cfg.Networking.APIServerAddress,
			HostPort:      cfg.Networking.APIServerPort,
			ContainerPort: common.APIServerInternalPort,
		},
	)
	if err != nil {
		return nil, err
	}
	args = append(args, mappingArgs...)

	// finally, specify the image to run
	return append(args, loadbalancer.Image), nil
}

func getProxyEnv(cfg *config.Cluster, networkName string, nodeNames []string) (map[string]string, error) {
	envs := common.GetProxyEnvs(cfg)
	// Specifically add the docker network subnets to NO_PROXY if we are using a proxy
	if len(envs) > 0 {
		panic("Not implemented: proxies")
	}
	return envs, nil
}

// generateMountBindings converts the mount list to a list of args for docker
// '<HostPath>:<ContainerPath>[:options]', where 'options'
// is a comma-separated list of the following strings:
// 'ro', if the path is read only
// 'Z', if the volume requires SELinux relabeling
func generateMountBindings(mounts ...config.Mount) []string {
	args := make([]string, 0, len(mounts))
	for _, m := range mounts {
		bind := fmt.Sprintf("%s:%s", m.HostPath, m.ContainerPath)
		var attrs []string
		if m.Readonly {
			attrs = append(attrs, "ro")
		}
		// Only request relabeling if the pod provides an SELinux context. If the pod
		// does not provide an SELinux context relabeling will label the volume with
		// the container's randomly allocated MCS label. This would restrict access
		// to the volume to the container which mounts it first.
		if m.SelinuxRelabel {
			attrs = append(attrs, "Z")
		}
		switch m.Propagation {
		case config.MountPropagationNone:
			// noop, private is default
		case config.MountPropagationBidirectional:
			attrs = append(attrs, "rshared")
		case config.MountPropagationHostToContainer:
			attrs = append(attrs, "rslave")
		default: // Falls back to "private"
		}
		if len(attrs) > 0 {
			bind = fmt.Sprintf("%s:%s", bind, strings.Join(attrs, ","))
		}
		args = append(args, fmt.Sprintf("--volume=%s", bind))
	}
	return args
}

// generatePortMappings converts the portMappings list to a list of args for docker
func generatePortMappings(clusterIPFamily config.ClusterIPFamily, portMappings ...config.PortMapping) ([]string, error) {
	args := make([]string, 0, len(portMappings))
	for _, pm := range portMappings {
		// do provider internal defaulting
		// in a future API revision we will handle this at the API level and remove this
		if pm.ListenAddress == "" {
			switch clusterIPFamily {
			case config.IPv4Family, config.DualStackFamily:
				pm.ListenAddress = "0.0.0.0" // this is the docker default anyhow
			case config.IPv6Family:
				panic("Not implemented")
			default:
				return nil, errors.Errorf("unknown cluster IP family: %v", clusterIPFamily)
			}
		}
		if string(pm.Protocol) == "" {
			pm.Protocol = config.PortMappingProtocolTCP // TCP is the default
		}

		// validate that the provider can handle this binding
		switch pm.Protocol {
		case config.PortMappingProtocolTCP:
		case config.PortMappingProtocolUDP:
		case config.PortMappingProtocolSCTP:
		default:
			return nil, errors.Errorf("unknown port mapping protocol: %v", pm.Protocol)
		}

		// get a random port if necessary (port = 0)
		hostPort, releaseHostPortFn, err := common.PortOrGetFreePort(pm.HostPort, pm.ListenAddress)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get random host port for port mapping")
		}
		if releaseHostPortFn != nil {
			defer releaseHostPortFn()
		}

		// generate the actual mapping arg
		protocol := string(pm.Protocol)
		hostPortBinding := net.JoinHostPort(pm.ListenAddress, fmt.Sprintf("%d", hostPort))
		args = append(args, fmt.Sprintf("--publish=%s:%d/%s", hostPortBinding, pm.ContainerPort, protocol))
	}
	return args, nil
}

func createContainer(name string, args []string) error {
	return exec.Command("container", append([]string{"run", "--name", name}, args...)...).Run()
}

func createContainerWithWaitUntilSystemdReachesMultiUserSystem(name string, args []string) error {
	if err := exec.Command("container", append([]string{"run", "--name", name}, args...)...).Run(); err != nil {
		return err
	}

	logCtx, logCancel := context.WithTimeout(context.Background(), 30*time.Second)
	logCmd := exec.CommandContext(logCtx, "container", "logs", "-f", name)
	defer logCancel()
	return common.WaitUntilLogRegexpMatches(logCtx, logCmd, common.NodeReachedCgroupsReadyRegexp())
}
