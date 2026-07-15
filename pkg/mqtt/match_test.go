package mqtt

import "testing"

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		matched string
		ok      bool
	}{
		// single-level +
		{"a/+/c", "a/b/c", "b", true},
		{"a/+/c", "a/b/d", "", false},
		{"a/+/c", "a/b", "", false},   // + requires a level and lengths differ
		{"a/+", "a", "", false},       // + needs a segment
		{"a/+", "a/b/c", "", false},   // + is single-level; lengths differ
		{"a/+/+", "a/b/c", "b/c", true},
		// trailing #
		{"a/#", "a/b/c", "b/c", true},
		{"a/#", "a/b", "b", true},
		{"a/#", "a", "", true}, // # matches zero remaining levels
		{"a/b/#", "a", "", false},
		// no wildcard = exact equality
		{"a/b/c", "a/b/c", "", true},
		{"a/b/c", "a/b/d", "", false},
		// StatsPump-shaped
		{
			"mqtt/PUMP/solace1025/POLLER_STAT/VPN/+/queue_rates",
			"mqtt/PUMP/solace1025/POLLER_STAT/VPN/vpn3/queue_rates",
			"vpn3", true,
		},
		{
			"mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/+",
			"mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/stats_client",
			"stats_client", true,
		},
		{
			"mqtt/PUMP/solace1025/POLLER_STAT/SYSTEM/+",
			"mqtt/PUMP/solace1025/POLLER_STAT/VPN/queue_rates",
			"", false, // different level 4
		},
	}
	for _, c := range cases {
		matched, ok := MatchTopic(c.pattern, c.topic)
		if ok != c.ok || matched != c.matched {
			t.Errorf("MatchTopic(%q, %q) = (%q, %v), want (%q, %v)",
				c.pattern, c.topic, matched, ok, c.matched, c.ok)
		}
	}
}

func TestIsWildcard(t *testing.T) {
	for _, c := range []struct {
		s    string
		want bool
	}{
		{"a/b/c", false},
		{"a/+/c", true},
		{"a/#", true},
		{"", false},
	} {
		if IsWildcard(c.s) != c.want {
			t.Errorf("IsWildcard(%q) = %v, want %v", c.s, !c.want, c.want)
		}
	}
}
