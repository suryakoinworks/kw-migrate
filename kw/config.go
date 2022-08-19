package kw

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type (
	Config struct {
		Version string  `yaml:"version"`
		Migrate Migrate `yaml:"migrate"`
	}

	Migrate struct {
		PgDump   string                         `yaml:"pg_dump"`
		Folder   string                         `yaml:"folder"`
		Database Database                       `yaml:"database"`
		Schemas  map[string]map[string][]string `yaml:"schemas"`
	}

	Database struct {
		Driver   string `yaml:"driver"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Name     string `yaml:"name"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	}
)

func Parse(path string) Config {
	config := Config{}
	c, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error occur: %s\n", err.Error())
	}

	err = yaml.Unmarshal(c, &config)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if config.Migrate.PgDump == "" {
		config.Migrate.PgDump = "pg_dump"
	}

	if config.Migrate.Folder == "" {
		config.Migrate.Folder = "migrations"
	}

	os.MkdirAll(config.Migrate.Folder, 0777)

	for k, v := range config.Migrate.Schemas {
		_, ok := v["excludes"]
		if !ok {
			v["excludes"] = []string{}
		}

		_, ok = v["with_data"]
		if !ok {
			v["with_data"] = []string{}
		}

		config.Migrate.Schemas[k] = v
	}

	return config
}
