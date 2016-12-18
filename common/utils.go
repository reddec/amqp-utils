package common

import (
	"errors"
	"flag"
	"strings"
)

type StringList []string

func (i *StringList) String() string {
	return strings.Join(*i, ", ")
}

func (i *StringList) Set(value string) error {
	(*i) = append(*i, value)
	return nil
}

func FlagStringList(name string, def StringList, help string) *StringList {
	v := def
	flag.Var(&v, name, help)
	return &v
}

type MapFlags map[string]string

func (i *MapFlags) String() string {
	s := ""
	for k, v := range *i {
		if s != "" {
			s += ", "
		}
		s += k + "=" + v
	}
	return s
}

func (i *MapFlags) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return errors.New("Key-value pairs requires key=value format")
	}
	(*i)[parts[0]] = parts[1]
	return nil
}

func FlagMapFlags(name string, def MapFlags, help string) *MapFlags {
	v := def
	flag.Var(&v, name, help)
	return &v
}
