package pulsar

// ClientOption ...
type ClientOption func(*clientConfig)

type clientConfig struct {
	Logger Logger
	dialer dialer
}

// WithLogger sets a custom logger.
func WithLogger(logger Logger) ClientOption {
	return func(conf *clientConfig) {
		conf.Logger = logger
	}
}

// WithLogger sets a custom logger.
func withDialer(dialer dialer) ClientOption {
	return func(conf *clientConfig) {
		conf.dialer = dialer
	}
}

func applyOptions(opts []ClientOption) clientConfig {
	conf := clientConfig{
		dialer: defaultDialer,
	}
	for _, opt := range opts {
		opt(&conf)
	}
	return conf
}
