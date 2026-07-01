package classify

import "testing"

func TestPolicy(t *testing.T) {
	c, err := New([]string{"uptime", "memory.system.*", "load*"})
	if err != nil {
		t.Fatal(err)
	}
	volatile := []string{"uptime", "memory.system.available_bytes", "load", "load.1m"}
	durable := []string{"os.name", "uptimed", "memory.total", "networking.interfaces.eth0.address"}
	for _, p := range volatile {
		if !c.IsVolatile(p) {
			t.Errorf("%s should be volatile", p)
		}
	}
	for _, p := range durable {
		if c.IsVolatile(p) {
			t.Errorf("%s should be durable", p)
		}
	}
}
