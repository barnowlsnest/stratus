package config

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ConfigSuite struct {
	suite.Suite
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

func (s *ConfigSuite) TestValidate() {
	cases := []struct {
		name     string
		cfg      Config
		expected error
	}{
		{name: "no host, no token, no tls", cfg: Config{}},
		{name: "loopback, no token, no tls", cfg: Config{Host: "127.0.0.1"}},
		{name: "all interfaces, no token, no tls", cfg: Config{Host: "0.0.0.0"}},
		{name: "all interfaces, with token, no tls", cfg: Config{Host: "0.0.0.0", AuthToken: "t"}},
		{
			name: "all interfaces, with token and tls",
			cfg: Config{
				Host: "0.0.0.0", AuthToken: "t",
				TLSCertFile: "c.pem", TLSKeyFile: "k.pem",
			},
		},
		{name: "cert without key", cfg: Config{TLSCertFile: "c.pem"}, expected: ErrIncompleteTLS},
		{name: "key without cert", cfg: Config{TLSKeyFile: "k.pem"}, expected: ErrIncompleteTLS},
	}

	for i := range cases {
		tc := &cases[i]
		s.Run(tc.name, func() {
			actual := tc.cfg.Validate()
			if tc.expected == nil {
				s.NoError(actual)
				return
			}

			s.ErrorIs(actual, tc.expected)
		})
	}
}
