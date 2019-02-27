package river

import "time"

// TomlDuration supports time codec for TOML format.
type TomlDuration struct {
	time.Duration
}

// UnmarshalText implementes TOML UnmarshalText
func (d *TomlDuration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
