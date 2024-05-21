package client

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func Test_consistentHashSelector_Select(t *testing.T) {
	servers := map[string]string{
		"tcp@192.168.1.16:9392": "",
		"tcp@192.168.1.16:9393": "",
	}
	s := newConsistentHashSelector(servers).(*consistentHashSelector)

	key := uint64(9280147620691907957)
	selected, _ := s.h.Get(key).(string)

	for i := 0; i < 10000; i++ {
		selected2, _ := s.h.Get(key).(string)
		if selected != selected2 {
			t.Errorf("expected %s but got %s", selected, selected2)
		}
	}
}

func Test_consistentHashSelector_UpdateServer(t *testing.T) {
	servers := map[string]string{
		"tcp@192.168.1.16:9392": "",
		"tcp@192.168.1.16:9393": "",
	}
	s := newConsistentHashSelector(servers).(*consistentHashSelector)
	if len(s.h.All()) != len(servers) {
		t.Errorf("NewSelector: expected %d server but got %d", len(servers), len(s.h.All()))
	}
	s.UpdateServer(servers)
	if len(s.h.All()) != len(servers) {
		t.Errorf("UpdateServer: expected %d server but got %d", len(servers), len(s.h.All()))
	}
}

func Test_createLatencyWeighted_UpdateServer(t *testing.T) {
	testCases := []struct {
		name    string
		servers map[string]string
		results []*Weighted
	}{
		{
			name: "test_all_empty",
			servers: map[string]string{
				"tcp@192.168.1.16:9392": "",
				"tcp@192.168.1.16:9393": "",
				"tcp@192.168.1.16:9394": "",
			},
			results: []*Weighted{
				{
					Server:          "tcp@192.168.1.16:9392",
					Weight:          100,
					EffectiveWeight: 100,
				},
				{
					Server:          "tcp@192.168.1.16:9393",
					Weight:          100,
					EffectiveWeight: 100,
				},
				{
					Server:          "tcp@192.168.1.16:9394",
					Weight:          100,
					EffectiveWeight: 100,
				},
			},
		},
		{
			name: "test_all_equal",
			servers: map[string]string{
				"tcp@192.168.1.16:9392": "latency=30",
				"tcp@192.168.1.16:9393": "latency=30",
				"tcp@192.168.1.16:9394": "latency=30",
			},
			results: []*Weighted{
				{
					Server:          "tcp@192.168.1.16:9392",
					Weight:          100,
					EffectiveWeight: 100,
				},
				{
					Server:          "tcp@192.168.1.16:9393",
					Weight:          100,
					EffectiveWeight: 100,
				},
				{
					Server:          "tcp@192.168.1.16:9394",
					Weight:          100,
					EffectiveWeight: 100,
				},
			},
		},
		{
			name: "test_compound_1",
			servers: map[string]string{
				"tcp@192.168.1.16:9392": "latency=60",
				"tcp@192.168.1.16:9393": "latency=40",
				"tcp@192.168.1.16:9394": "",
			},
			results: []*Weighted{
				{
					Server:          "tcp@192.168.1.16:9392",
					Weight:          83,
					EffectiveWeight: 83,
				},
				{
					Server:          "tcp@192.168.1.16:9393",
					Weight:          125,
					EffectiveWeight: 125,
				},
				{
					Server:          "tcp@192.168.1.16:9394",
					Weight:          100,
					EffectiveWeight: 100,
				},
			},
		},
		{
			name: "test_compound_2",
			servers: map[string]string{
				"tcp@192.168.1.16:9392": "latency=60",
				"tcp@192.168.1.16:9393": "latency=40",
				"tcp@192.168.1.16:9394": "latency=50",
			},
			results: []*Weighted{
				{
					Server:          "tcp@192.168.1.16:9392",
					Weight:          83,
					EffectiveWeight: 83,
				},
				{
					Server:          "tcp@192.168.1.16:9393",
					Weight:          125,
					EffectiveWeight: 125,
				},
				{
					Server:          "tcp@192.168.1.16:9394",
					Weight:          100,
					EffectiveWeight: 100,
				},
			},
		},
	}

	s := newWeightedLatencySelector(nil).(*weightedLatencySelector)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s.UpdateServer(tc.servers)
			sort.Slice(s.servers, func(i, j int) bool {
				return s.servers[i].Server < s.servers[j].Server
			})
			assert.Equal(t, tc.results, s.servers)
		})
	}
}
