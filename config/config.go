package config

import (
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/joho/godotenv"
	"github.com/pelletier/go-toml/v2"
	"github.com/pkg/errors"

	"github.com/DataDog/dd-trace-go/contrib/database/sql/parsedsn"
)

const (
	EnvVarPrefix          = "MMMBOP"
	CheckpointIndexSuffix = ".index"

	DefaultBatchSize          = 10
	DefaultNumWorkers         = 2
	DefaultCheckpointInterval = duration(5 * time.Second)
	DefaultCheckpointFile     = "checkpoint.json"

	MinBatchSize          = 1
	MaxBatchSize          = 10_000
	MinNumWorkers         = 1
	MaxNumWorkers         = 100
	MinCheckpointInterval = duration(1 * time.Millisecond)
	MaxCheckpointInterval = duration(1 * time.Hour)
)

var (
	// VERSION gets set during build
	VERSION = "0.0.0"

	validFileTypes = map[string]struct{}{
		"plain": {},
		"gzip":  {},
	}

	validFileContents = map[string]struct{}{
		"json": {},
		"csv":  {},
		"bson": {},
	}

	validConvs = map[string]struct{}{
		"string":    {},
		"int":       {},
		"float":     {},
		"bool":      {},
		"time":      {},
		"json":      {},
		"date":      {},
		"timestamp": {},
	}
)

type Config struct {
	CLI  *CLI
	TOML *TOML
}

type TOML struct {
	Config      *TOMLConfig      `toml:"config"`
	Source      *TOMLSource      `toml:"source"`
	Destination *TOMLDestination `toml:"destination"`
	Mapping     *TOMLMapping     `toml:"mapping"`
}

type TOMLConfig struct {
	LogLevel             string   `toml:"log_level"`
	NumProcessors        int      `toml:"num_processors"`
	NumWriters           int      `toml:"num_writers"`
	BatchSize            int      `toml:"batch_size"`
	CheckpointFile       string   `toml:"checkpoint_file"`
	CheckpointIndex      string   `toml:"checkpoint_index"`
	CheckpointInterval   duration `toml:"checkpoint_interval"`
	DisableCheckpointing bool     `toml:"disable_checkpointing"`
	DisableDupecheck     bool     `toml:"disable_dupecheck"`
}

type CheckpointFile struct {
	Index  []byte `json:"index"` // base64'd, gob data used by gzran
	Offset int64  `json:"offset"`
}

type TOMLSource struct {
	File         string `toml:"file"`
	FileType     string `toml:"file_type"`
	FileContents string `toml:"file_contents"`
}

type TOMLDestination struct {
	Type string `toml:"type"`
	DSN  string `toml:"dsn"`
}

type TOMLMapping struct {
	Mapping map[string][]*TOMLMappingEntry `toml:"mapping"`
}

type TOMLMappingEntry struct {
	Src       string `toml:"src"`
	Dst       string `toml:"dst"`
	Conv      string `toml:"conv"`
	Required  bool   `toml:"required"`
	DupeCheck bool   `toml:"dupe_check"`
}

type CLI struct {
	ConfigFile     string        `kong:"help='Path to the TOML config file',type='path',default='config.toml',short='c'"`
	DryRun         bool          `kong:"help='Enable dry-run mode',short='n'"`
	Migrate        bool          `kong:"help='Perform the migration',short='m'"`
	ReportInterval time.Duration `kong:"help='Interval to report progress',short='r',default='5s',short='r'"`
	ReportOutput   string        `kong:"help='Output file for progress reports',short='o'"`
	DisableResume  bool          `kong:"help='Disable resuming from checkpoint',short='R'"`
	DisableColor   bool          `kong:"help='Disable color output',short='C'"`

	Debug   bool             `kong:"help='Enable debug output',short='d'"`
	Quiet   bool             `kong:"help='Disable showing pre/post output',short='q'"`
	Version kong.VersionFlag `help:"Show version and exit" short:"v" env:"-"`

	// Internal bits
	Ctx *kong.Context `kong:"-"`
}

func NewConfig() (*Config, error) {
	// Attempt to load .env
	_ = godotenv.Load(".env")

	cli, err := readCLIArgs()
	if err != nil {
		return nil, errors.Wrap(err, "error parsing CLI args")
	}

	tomlConfig, err := readTOML(cli.ConfigFile)
	if err != nil {
		return nil, errors.Wrap(err, "error reading config file")
	}

	if err := validateTOML(tomlConfig); err != nil {
		return nil, errors.Wrap(err, "error validating TOML config")
	}

	return &Config{
		CLI:  cli,
		TOML: tomlConfig,
	}, nil
}

func setTOMLDefaults(t *TOML) error {
	if t == nil {
		return errors.New("toml config cannot be nil")
	}

	if t.Config == nil {
		t.Config = &TOMLConfig{}
	}

	if t.Source == nil {
		t.Source = &TOMLSource{}
	}

	if t.Destination == nil {
		t.Destination = &TOMLDestination{}
	}

	if t.Mapping == nil {
		t.Mapping = &TOMLMapping{}
	}

	// Set defaults for [config]
	if t.Config.BatchSize == 0 {
		t.Config.BatchSize = DefaultBatchSize
	}

	if t.Config.NumProcessors == 0 {
		t.Config.NumProcessors = DefaultNumWorkers
	}

	if t.Config.CheckpointInterval == 0 {
		t.Config.CheckpointInterval = DefaultCheckpointInterval
	}

	if t.Config.CheckpointFile == "" {
		t.Config.CheckpointFile = DefaultCheckpointFile
	}

	if t.Config.CheckpointIndex == "" {
		t.Config.CheckpointIndex = t.Config.CheckpointFile + CheckpointIndexSuffix
	}

	return nil
}

func Validate(c *Config) error {
	if err := validateCLIArgs(c.CLI); err != nil {
		return errors.Wrap(err, "error validating CLI args")
	}

	if err := validateTOML(c.TOML); err != nil {
		return errors.Wrap(err, "error validating toml config")
	}

	if err := validateDestinationMappings(c.TOML.Destination, c.TOML.Mapping); err != nil {
		return errors.Wrap(err, "error validating destination mappings")
	}

	return nil
}

// TODO: Implement
func validateDestinationMappings(d *TOMLDestination, m *TOMLMapping) error {
	// TODO: Validate that we can connect to the destination

	// TODO: Validate that the destination tables exist

	// TODO: Validate that the destination columns exist

	// TODO: Validate that the destination columns are the correct type

	return nil
}

func validateTOML(t *TOML) error {
	if t == nil {
		return errors.New("toml config cannot be nil")
	}

	// Validate [config]
	if err := validateTOMLConfig(t.Config); err != nil {
		return errors.Wrap(err, "config error(s)")
	}

	// Validate [source]
	if err := validateTOMLSource(t.Source); err != nil {
		return errors.Wrap(err, "error validating toml [source]")
	}

	// Validate [destination]
	if err := validateTOMLDestination(t.Destination); err != nil {
		return errors.Wrap(err, "destination error(s)")
	}

	// Validate mapping entries
	if err := validateTOMLMapping(t.Mapping); err != nil {
		return errors.Wrap(err, "mapping error(s)")
	}

	return nil
}

func validateTOMLConfig(c *TOMLConfig) error {
	if c == nil {
		return errors.New("config cannot be empty")
	}

	if c.BatchSize < MinBatchSize || c.BatchSize > MaxBatchSize {
		return errors.Errorf("config.batch_size must be between %d and %d", MinBatchSize, MaxBatchSize)
	}

	if c.NumProcessors < MinNumWorkers || c.NumProcessors > MaxNumWorkers {
		return errors.Errorf("config.num_workers must be between %d and %d", MinNumWorkers, MaxNumWorkers)
	}

	if c.CheckpointInterval < MinCheckpointInterval || c.CheckpointInterval > MaxCheckpointInterval {
		return errors.Errorf("config.checkpoint_interval must be between %s and %s", MinCheckpointInterval, MaxCheckpointInterval)
	}

	if c.CheckpointFile == "" {
		return errors.New("config.checkpoint_file cannot be empty")
	}

	if c.CheckpointIndex == "" {
		return errors.New("config.checkpoint_index cannot be empty")
	}

	return nil
}

func validateTOMLSource(s *TOMLSource) error {
	if s == nil {
		return errors.New("source cannot be empty")
	}

	if s.File == "" {
		return errors.New("source.file cannot be empty")
	}

	// Check if .File exists
	info, err := os.Stat(s.File)
	if os.IsNotExist(err) {
		return errors.Errorf("source.file %s does not exist", s.File)
	}

	if info.IsDir() {
		return errors.Errorf("source.file %s is a directory", s.File)
	}

	// Check if .FileType is valid
	if _, ok := validFileTypes[s.FileType]; !ok {
		return errors.Errorf("source.file_type %s is invalid", s.FileType)
	}

	// Check if .FileContents is valid
	if _, ok := validFileContents[s.FileContents]; !ok {
		return errors.Errorf("source.file_contents %s is invalid", s.FileContents)
	}

	return nil
}

func validateTOMLDestination(d *TOMLDestination) error {
	if d == nil {
		return errors.New("destination cannot be empty")
	}

	if d.DSN == "" {
		return errors.New("destination.dsn cannot be empty")
	}

	if d.Type == "" {
		return errors.New("destination.type cannot be empty")
	}

	var err error

	switch d.Type {
	case "mysql":
		_, err = parsedsn.MySQL(d.DSN)
	case "postgres":
		_, err = parsedsn.Postgres(d.DSN)
	default:
		return errors.Errorf("destination.type %s is invalid", d.Type)
	}

	if err != nil {
		return errors.Wrap(err, "error validating destination.dsn")
	}

	return nil
}

func validateTOMLMapping(m *TOMLMapping) error {
	if m == nil {
		return errors.New("mapping cannot be empty")
	}

	for name, entries := range m.Mapping {
		if err := validateMappingEntries(name, entries); err != nil {
			return errors.Wrap(err, "error validating mapping entries")
		}
	}

	return nil
}

func validateMappingEntries(name string, entries []*TOMLMappingEntry) error {
	// Shouldn't be possible, but might as well check
	if name == "" {
		return errors.New("mapping name cannot be empty")
	}

	for _, e := range entries {
		if err := validateMappingEntry(e); err != nil {
			return errors.Wrap(err, "error validating mapping entry")
		}
	}

	return nil
}

func validateMappingEntry(e *TOMLMappingEntry) error {
	if e == nil {
		return errors.New("mapping entry cannot be nil")
	}

	if e.Src == "" {
		return errors.New("mapping entry.src cannot be empty")
	}

	if e.Dst == "" {
		return errors.New("mapping entry.dst cannot be empty")
	}

	if e.Conv == "" {
		return errors.New("mapping entry.conv cannot be empty")
	}

	if _, ok := validConvs[e.Conv]; !ok {
		return errors.Errorf("mapping entry.conv %s is invalid", e.Conv)
	}

	return nil
}

func readCLIArgs() (*CLI, error) {
	cli := &CLI{}
	cli.Ctx = kong.Parse(cli,
		kong.Name("mmmbop"),
		kong.Description("Migrator tool for Mongo -> PostgreSQL"),
		kong.UsageOnError(),
		kong.DefaultEnvars(EnvVarPrefix),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
			Summary: true,
		}),
		kong.Vars{
			"version": VERSION,
		})

	if err := validateCLIArgs(cli); err != nil {
		return nil, errors.Wrap(err, "error validating args")
	}

	return cli, nil
}

func readTOML(file string) (*TOML, error) {
	// Attempt to load file
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, errors.Wrap(err, "error reading file")
	}

	tomlConfig := &TOML{}

	if err := toml.Unmarshal(data, tomlConfig); err != nil {
		return nil, errors.Wrap(err, "error parsing TOML config")
	}

	// Set defaults
	if err := setTOMLDefaults(tomlConfig); err != nil {
		return nil, errors.Wrap(err, "error setting TOML defaults")
	}

	// Validate loaded config
	if err := validateTOML(tomlConfig); err != nil {
		return nil, errors.Wrap(err, "error validating TOML config")
	}

	return tomlConfig, nil
}

func validateCLIArgs(cli *CLI) error {
	if cli == nil {
		return errors.New("config cannot be nil")
	}

	return nil
}

// Copied from https://www.kelche.co/blog/go/toml/
type duration time.Duration

func (d duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *duration) UnmarshalText(text []byte) error {
	dur, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = duration(dur)
	return nil
}
