package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type (
	Config struct {
		// maps directory name to region name
		Names map[string]string `json:"names,omitempty"`
	}
)

func ReadConfig(fileName string) (*Config, error) {
	var cfg Config
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	// glog.Infof("==> parsing data=%q", data)
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
