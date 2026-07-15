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

func TestRootPrefix(t *testing.T) {
	for _, c := range []struct {
		root string
		want string
	}{
		{"mqtt/PUMP/#", "mqtt/PUMP/"},
		{"a/+/c", "a/"},
		{"mqtt/PUMP/solace1025/POLLER_STAT/VPN/+/queue_rates", "mqtt/PUMP/solace1025/POLLER_STAT/VPN/"},
		{"no/wildcard/here", "no/wildcard/here"}, // no wildcard -> unchanged
		{"#", ""},
	} {
		if got := rootPrefix(c.root); got != c.want {
			t.Errorf("rootPrefix(%q) = %q, want %q", c.root, got, c.want)
		}
	}
}

func TestUnderRoot(t *testing.T) {
	for _, c := range []struct {
		topic string
		roots []string
		want  bool
	}{
		// no roots configured -> unrestricted
		{"anything/goes", nil, true},
		{"anything/goes", []string{}, true},
		// single root
		{"mqtt/PUMP/solace1025/x", []string{"mqtt/PUMP/#"}, true},
		{"mqtt/OTHER/x", []string{"mqtt/PUMP/#"}, false},
		// exact prefix boundary
		{"mqtt/PUMP/", []string{"mqtt/PUMP/#"}, true},
		// multiple roots -> under any
		{"sensors/kitchen/temp", []string{"mqtt/PUMP/#", "sensors/#"}, true},
		{"other/thing", []string{"mqtt/PUMP/#", "sensors/#"}, false},
		// no-wildcard root behaves as exact/prefix
		{"a/b/c", []string{"a/b/c"}, true},
		{"a/b/cd", []string{"a/b/c"}, true}, // prefix guardrail, not exact
		{"a/b", []string{"a/b/c"}, false},
	} {
		if got := UnderRoot(c.topic, c.roots); got != c.want {
			t.Errorf("UnderRoot(%q, %v) = %v, want %v", c.topic, c.roots, got, c.want)
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
