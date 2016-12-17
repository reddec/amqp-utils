package common

import (
	"errors"
	"flag"
	"net/http"
	"strings"
)

type AuthFlags map[string]string

func (i *AuthFlags) String() string {
	s := ""
	for user, password := range *i {
		if s != "" {
			s += ", "
		}
		s += user + ":" + password
	}
	return s
}

func (i *AuthFlags) Set(value string) error {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		return errors.New("Auth pair requires username:password format")
	}
	(*i)[parts[0]] = parts[1]
	return nil
}

func FlagAuths(name string, def AuthFlags, help string) *AuthFlags {
	v := def
	flag.Var(&v, name, help)
	return &v
}

func (i *AuthFlags) CheckHTTP(req *http.Request) bool {
	if len(*i) == 0 {
		return true
	}
	user, password, ok := req.BasicAuth()
	if !ok {
		return false
	}
	return (*i)[user] == password
}
