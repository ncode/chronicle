package wire

import "testing"

func TestDiscoveryStatusValidityAndCleanliness(t *testing.T) {
	tests := []struct {
		name      string
		status    DiscoveryStatus
		wantValid bool
		wantClean bool
	}{
		{"built-in ok", DiscoveryStatus{Builtin: map[string]string{"os": StatusOK}}, true, true},
		{"built-in error", DiscoveryStatus{Builtin: map[string]string{"os": StatusError}}, true, false},
		{"external ok", DiscoveryStatus{External: map[string]string{"/etc/facts.d/site.json": StatusOK}}, true, true},
		{"external error", DiscoveryStatus{External: map[string]string{"/etc/facts.d/site.json": StatusError}}, true, false},
		{"external absent", DiscoveryStatus{External: map[string]string{"/etc/facts.d/site.json": StatusAbsent}}, true, true},
		{"built-in absent", DiscoveryStatus{Builtin: map[string]string{"os": StatusAbsent}}, false, false},
		{"invalid external", DiscoveryStatus{External: map[string]string{"/etc/facts.d/site.json": "unknown"}}, false, false},
		{
			"mixed valid dirty",
			DiscoveryStatus{
				Builtin:  map[string]string{"os": StatusOK, "networking": StatusError},
				External: map[string]string{"/etc/facts.d/site.json": StatusAbsent},
			},
			true,
			false,
		},
		{
			"mixed invalid fails closed",
			DiscoveryStatus{
				Builtin:  map[string]string{"os": StatusOK},
				External: map[string]string{"/etc/facts.d/site.json": "unknown"},
			},
			false,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.Valid(); got != tt.wantValid {
				t.Errorf("Valid() = %v, want %v", got, tt.wantValid)
			}
			if got := tt.status.Clean(); got != tt.wantClean {
				t.Errorf("Clean() = %v, want %v", got, tt.wantClean)
			}
		})
	}
}
