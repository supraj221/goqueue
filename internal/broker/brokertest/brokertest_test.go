package brokertest

import (
	"testing"

	"github.com/supraj221/goqueue/internal/broker"
)

// The suite must pass against the in-memory reference broker; this keeps the
// suite itself honest independent of any database.
func TestMemBrokerConformance(t *testing.T) {
	Run(t, func(t *testing.T) broker.Broker { return NewMemBroker() })
}
