package config

type Config struct {
	Pipeline PipelineConfig `yaml:"pipeline" json:"pipeline"`
	Runtime  RuntimeConfig  `yaml:"run_time" json:"run_time"`
}

type PipelineConfig struct {
	Receiver   ReceiverConfig    `yaml:"receiver" json:"receiver"`
	Processors []ProcessorConfig `yaml:"processors" json:"processors"`
	Exporter   ExporterConfig    `yaml:"exporter" json:"exporter"`
	Batcher    BatcherConfig     `yaml:"batcher" json:"batcher"`
}

type RuntimeConfig struct {
	NumOfWorkers int `yaml:"num_of_workers" json:"num_of_workers"`
	Buffersize   int `yaml:"buffer_size" json:"buffer_size"`
}

type ReceiverConfig struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config" json:"config"`
}

type ProcessorConfig struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config" json:"config"`
}

type BatcherConfig struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config" json:"config"`
}

type ExporterConfig struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config" json:"config"`
}
