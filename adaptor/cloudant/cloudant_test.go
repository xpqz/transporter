package cloudant

import (
	"testing"
)

func TestDescription(t *testing.T) {
	if CloudantAdaptor.Description() != description {
		t.Errorf("wrong description returned, expected %s, got %s", description, CloudantAdaptor.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	if CloudantAdaptor.SampleConfig() != sampleConfig {
		t.Errorf("wrong config returned, expected %s, got %s", sampleConfig, CloudantAdaptor.SampleConfig())
	}
}
