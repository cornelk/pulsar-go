package pulsar

// ClientOption ...
type ClientOption func(*clientConfig)

type clientConfig struct {
	Logger Logger
}

// WithLogger sets a custom logger.
func WithLogger(logger Logger) ClientOption {
	return func(conf *clientConfig) {
		conf.Logger = logger
	}
}

func applyOptions(opts []ClientOption) clientConfig {
	var conf clientConfig
	for _, opt := range opts {
		opt(&conf)
	}
	return conf
}
