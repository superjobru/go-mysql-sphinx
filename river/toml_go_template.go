package river

import "text/template"

// TomlGoTemplate supports template decoding for TOML format.
type TomlGoTemplate struct {
	template.Template
}

// UnmarshalText implementes TOML UnmarshalText
func (t *TomlGoTemplate) UnmarshalText(text []byte) error {
	var err error
	_, err = t.Delims("<<", ">>").Parse(string(text))
	return err
}
