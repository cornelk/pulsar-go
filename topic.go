package pulsar

import (
	"errors"
	"fmt"
	"strings"
)

// ...
const (
	publicTenant     = "public"
	defaultNamespace = "default"
	// TODO support partitioning partitionedTopicSuffix = "-partition-"
	persistentDomain    = "persistent"
	nonPersistentDomain = "non-persistent"
	domainSeparator     = "://"
)

// Topic represents a Pulsar Topic.
type Topic struct {
	Domain       string
	Tenant       string
	Namespace    string
	LocalName    string
	CompleteName string
}

// NewTopic creates a new topic struct from the given topic name.
// The topic name can be in short form or a fully qualified topic name.
func NewTopic(name string) (*Topic, error) {
	if !strings.Contains(name, domainSeparator) {
		// The short topic name can be:
		// - <topic>
		// - <property>/<namespace>/<topic>
		parts := strings.Split(name, "/")
		switch len(parts) {
		case 3:
			name = persistentDomain + domainSeparator +
				name
		case 1:
			name = persistentDomain + domainSeparator +
				publicTenant + "/" + defaultNamespace + "/" + parts[0]
		default:
			return nil, errors.New("invalid topic short name format")
		}
	}

	parts := strings.Split(name, domainSeparator)
	if len(parts) != 2 {
		return nil, errors.New("invalid topic domain format")
	}

	domain := parts[0]
	if domain != persistentDomain && domain != nonPersistentDomain {
		return nil, errors.New("invalid topic domain")
	}

	parts = strings.Split(parts[1], "/")
	if len(parts) != 3 {
		return nil, errors.New("invalid topic name format")
	}

	t := &Topic{
		Domain:       domain,
		Tenant:       parts[0],
		Namespace:    parts[1],
		LocalName:    parts[2],
		CompleteName: "",
	}
	t.CompleteName = fmt.Sprintf("%s://%s/%s/%s", t.Domain, t.Tenant,
		t.Namespace, t.LocalName)
	return t, nil
}
