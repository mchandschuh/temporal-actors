package actor

import (
	"errors"
	"sync"
)

const defaultRouteVersion = "v2"

// TypeNameEncoder converts an actor definition type name into one safe segment
// of a Temporal Task Queue name. Implementations should be deterministic,
// stateless, and safe for concurrent use.
type TypeNameEncoder func(string) (string, error)

// TypeNameEncoderFunc documents conversion of a function into a configured
// type-name encoder. It is useful when supplying a definition-specific
// override.
func TypeNameEncoderFunc(fn func(string) (string, error)) TypeNameEncoder {
	return TypeNameEncoder(fn)
}

// Config contains immutable process-wide actor defaults. Configure must be
// called before definitions or workers are created. Definitions capture the
// effective configuration at construction time; workflow execution never
// reads mutable global configuration.
type Config struct {
	RouteVersion    string
	TypeNameEncoder TypeNameEncoder
}

type resolvedConfig struct {
	routeVersion    string
	typeNameEncoder TypeNameEncoder
}

var (
	configMu     sync.Mutex
	globalConfig = &resolvedConfig{
		routeVersion:    defaultRouteVersion,
		typeNameEncoder: TypeNameEncoder(DefaultTypeNameEncoder),
	}
	configFrozen bool
)

// Configure sets process-wide actor defaults. It must be called before the
// first Definition or RouteResolver is created. Configuration is intentionally
// immutable after first use so Temporal replay cannot observe a changed route.
func Configure(config Config) error {
	configMu.Lock()
	defer configMu.Unlock()
	if configFrozen {
		return errors.New("actor configuration is already frozen")
	}
	resolved, err := resolveConfig(config)
	if err != nil {
		return err
	}
	globalConfig = resolved
	return nil
}

func currentConfig() *resolvedConfig {
	configMu.Lock()
	defer configMu.Unlock()
	configFrozen = true
	return globalConfig
}

func resolveConfig(overrides Config) (*resolvedConfig, error) {
	return resolveConfigLocked(overrides.RouteVersion, overrides.TypeNameEncoder)
}
