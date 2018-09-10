package hosttree

import (
	"fmt"
	"net"

	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/fastrand"
)

const (
	ipv4FilterRange = 24
	ipv6FilterRange = 54
)

// Resolver is an interface that allows resolving a hostname into IP
// addresses.
type Resolver interface {
	LookupIP(string) ([]net.IP, error)
}

// ProductionResolver is the hostname resolver used in production builds.
type ProductionResolver struct{}

// LookupIP is a passthrough function to net.LookupIP.
func (ProductionResolver) LookupIP(host string) ([]net.IP, error) {
	return net.LookupIP(host)
}

// TestingResolver is the default hostname resolver used in testing builds. It
// returns a different valid IP address every time it is called.
type TestingResolver struct {
	counter uint32
}

// LookupIP on the TestingResolver returns a random IPv6 address every time it
// is called. That way we are pretty much guaranteed to never have an address
// from the same subnet during testing.
func (tr TestingResolver) LookupIP(host string) ([]net.IP, error) {
	rawIP := make([]byte, 16)
	fastrand.Read(rawIP)
	return []net.IP{net.IP(rawIP)}, nil
}

// Filter filters host addresses which belong to the same subnet to
// avoid selecting hosts from the same region.
type Filter struct {
	filter   map[string]struct{}
	resolver Resolver
}

// NewFilter creates a new addressFilter object.
func NewFilter(resolver Resolver) *Filter {
	return &Filter{
		filter:   make(map[string]struct{}),
		resolver: resolver,
	}
}

// Add adds a host to the filter. This will resolve the hostname into one
// or more IP addresses, extract the subnets used by those addresses and
// add the subnets to the filter. Add doesn't return an error, but if the
// addresses of a host can't be resolved it will be handled as if the host
// had no addresses associated with it.
func (af *Filter) Add(host modules.NetAddress) {
	// Translate the hostname to one or multiple IPs. If the argument is an IP
	// address LookupIP will just return that IP.
	addresses, err := af.resolver.LookupIP(host.Host())
	if err != nil {
		return
	}
	// If any of the addresses is blocked we ignore the host.
	for _, ip := range addresses {
		// Set the filterRange according to the type of IP address.
		var filterRange int
		if ip.To4() != nil {
			filterRange = ipv4FilterRange
		} else {
			filterRange = ipv6FilterRange
		}
		// Get the subnet.
		_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", ip.String(), filterRange))
		if err != nil {
			continue
		}
		// Add the subnet to the map.
		af.filter[ipnet.String()] = struct{}{}
	}
}

// Filtered checks if a host uses a subnet that is already in use by a host
// that was previously added to the filter. If it is in use, or if the host is
// associated with 2 addresses of the same type (e.g. IPv4 and IPv4) or if it
// is associated with more than 2 addresses, Filtered will return 'true'.
func (af *Filter) Filtered(host modules.NetAddress) bool {
	// Translate the hostname to one or multiple IPs. If the argument is an IP
	// address LookupIP will just return that IP.
	addresses, err := af.resolver.LookupIP(host.Host())
	if err != nil {
		return true
	}
	// If the hostname is associated with more than 2 addresses we filter it
	if len(addresses) > 2 {
		return true
	}
	// If the hostname is associated with 2 addresses of the same type, we
	// filter it.
	if (len(addresses) == 2) && (len(addresses[0]) == len(addresses[1])) {
		return true
	}
	// If any of the addresses is blocked we ignore the host.
	for _, ip := range addresses {
		// Set the filterRange according to the type of IP address.
		var filterRange int
		if ip.To4() != nil {
			filterRange = ipv4FilterRange
		} else {
			filterRange = ipv6FilterRange
		}
		// Get the subnet.
		_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", ip.String(), filterRange))
		if err != nil {
			continue
		}
		// Check if the subnet is in the map. If it is, we filter the host.
		if _, exists := af.filter[ipnet.String()]; exists {
			return true
		}
	}
	return false
}

// Reset clears the filter's contents.
func (af *Filter) Reset() {
	af.filter = make(map[string]struct{})
}
