package document_test

import (
	"reflect"
	"testing"

	"plastic-engine-core/internal/core/search/document"
)

func TestGenerateEdgeNgrams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		term   string
		config document.NgramConfig
		want   []string
	}{
		{
			name: "basic hello",
			term: "hello",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 2,
				MaxLength: 5,
			},
			want: []string{"he", "hel", "hell", "hello"},
		},
		{
			name: "term shorter than min",
			term: "a",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 2,
				MaxLength: 5,
			},
			want: nil,
		},
		{
			name: "term shorter than max",
			term: "hi",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 2,
				MaxLength: 10,
			},
			want: []string{"hi"},
		},
		{
			name: "disabled config",
			term: "hello",
			config: document.NgramConfig{
				Enabled:   false,
				MinLength: 2,
				MaxLength: 5,
			},
			want: nil,
		},
		{
			name: "min equals max",
			term: "hello",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 3,
				MaxLength: 3,
			},
			want: []string{"hel"},
		},
		{
			name: "longer term",
			term: "elasticsearch",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 2,
				MaxLength: 5,
			},
			want: []string{"el", "ela", "elas", "elast"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := document.GenerateEdgeNgrams(tt.term, tt.config)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateEdgeNgrams(%q) = %v, want %v", tt.term, got, tt.want)
			}
		})
	}
}

func TestGenerateEdgeNgramsUTF8(t *testing.T) {
	t.Parallel()

	// UTF-8 multi-byte characters
	tests := []struct {
		name   string
		term   string
		config document.NgramConfig
		want   []string
	}{
		{
			name: "spanish word",
			term: "niño",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 2,
				MaxLength: 4,
			},
			want: []string{"ni", "niñ", "niño"},
		},
		{
			name: "japanese",
			term: "日本語",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 1,
				MaxLength: 3,
			},
			want: []string{"日", "日本", "日本語"},
		},
		{
			name: "emoji",
			term: "👋hello",
			config: document.NgramConfig{
				Enabled:   true,
				MinLength: 2,
				MaxLength: 4,
			},
			want: []string{"👋h", "👋he", "👋hel"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := document.GenerateEdgeNgrams(tt.term, tt.config)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateEdgeNgrams(%q) = %v, want %v", tt.term, got, tt.want)
			}
		})
	}
}

func TestNgramConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  document.NgramConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  document.NgramConfig{Enabled: true, MinLength: 2, MaxLength: 10},
			wantErr: false,
		},
		{
			name:    "disabled is always valid",
			config:  document.NgramConfig{Enabled: false, MinLength: 0, MaxLength: 0},
			wantErr: false,
		},
		{
			name:    "min length zero",
			config:  document.NgramConfig{Enabled: true, MinLength: 0, MaxLength: 5},
			wantErr: true,
		},
		{
			name:    "max less than min",
			config:  document.NgramConfig{Enabled: true, MinLength: 5, MaxLength: 3},
			wantErr: true,
		},
		{
			name:    "max too large",
			config:  document.NgramConfig{Enabled: true, MinLength: 2, MaxLength: 100},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultNgramConfig(t *testing.T) {
	t.Parallel()

	cfg := document.DefaultNgramConfig()

	if !cfg.Enabled {
		t.Error("DefaultNgramConfig should be enabled")
	}
	if cfg.MinLength < 1 {
		t.Errorf("DefaultNgramConfig MinLength = %d, want >= 1", cfg.MinLength)
	}
	if cfg.MaxLength < cfg.MinLength {
		t.Errorf("DefaultNgramConfig MaxLength = %d < MinLength = %d", cfg.MaxLength, cfg.MinLength)
	}
}

func TestNgramConfigDisabled(t *testing.T) {
	t.Parallel()

	cfg := document.NgramConfigDisabled()

	if cfg.Enabled {
		t.Error("NgramConfigDisabled should not be enabled")
	}
}

