package mqtt

import "strings"

// IsWildcard reports whether an MQTT topic filter contains a wildcard (`+` or `#`).
func IsWildcard(pattern string) bool {
	return strings.ContainsAny(pattern, "+#")
}

// rootPrefix returns the non-wildcard leading portion of a root filter — everything
// up to the first `+` or `#` (e.g. `mqtt/PUMP/#` -> `mqtt/PUMP/`). A root with no
// wildcard returns itself unchanged.
func rootPrefix(root string) string {
	if i := strings.IndexAny(root, "+#"); i >= 0 {
		return root[:i]
	}
	return root
}

// UnderRoot reports whether a topic falls under any of the given root filters, using a
// prefix check against each root's non-wildcard portion. This is a tidiness guardrail
// (not a rigorous MQTT filter-subset check). With no roots configured it returns true,
// so an enabled restriction with no scope doesn't reject everything.
func UnderRoot(topic string, roots []string) bool {
	if len(roots) == 0 {
		return true
	}
	for _, r := range roots {
		if strings.HasPrefix(topic, rootPrefix(r)) {
			return true
		}
	}
	return false
}

// MatchTopic reports whether a concrete topic matches an MQTT topic filter that may
// contain wildcards (`+` = one level, `#` = trailing multi-level), and returns the
// concrete segment(s) at the wildcard positions joined by "/". Those matched segments
// are what varies across matches, so they make the natural series label for wildcard
// graphing (e.g. filter `a/+/c` on topic `a/b/c` -> matched "b").
//
// For a filter with no wildcards, matched is "" and ok is whether pattern == topic.
func MatchTopic(pattern, topic string) (matched string, ok bool) {
	if !IsWildcard(pattern) {
		return "", pattern == topic
	}

	pp := strings.Split(pattern, "/")
	tp := strings.Split(topic, "/")

	var segments []string
	for i := 0; i < len(pp); i++ {
		switch pp[i] {
		case "#":
			// Multi-level wildcard: valid only as the final segment; matches the rest
			// (including zero remaining levels).
			if i != len(pp)-1 {
				return "", false
			}
			if i < len(tp) {
				segments = append(segments, tp[i:]...)
			}
			return strings.Join(segments, "/"), true
		case "+":
			if i >= len(tp) {
				return "", false
			}
			segments = append(segments, tp[i])
		default:
			if i >= len(tp) || pp[i] != tp[i] {
				return "", false
			}
		}
	}

	// No trailing '#': the topic must have exactly as many levels as the filter.
	if len(pp) != len(tp) {
		return "", false
	}
	return strings.Join(segments, "/"), true
}

// MatchesFilter reports whether a concrete topic passes a comma-separated include/exclude
// filter (case-sensitive substring matching). Each comma term is an INCLUDE unless it starts
// with "!", which makes it an EXCLUDE. A topic passes iff it contains NONE of the excludes AND
// (there are no includes, or it contains AT LEAST ONE include). An empty filter passes everything.
// Examples: "!prod" (all but prod); "a,b" (contains a OR b); "a,!b" (contains a AND not b).
// This is mirrored byte-for-byte by matchesFilter() in src/wildcard.ts — keep them in sync.
func MatchesFilter(topic, filter string) bool {
	var includes, excludes []string
	for _, term := range strings.Split(filter, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		if strings.HasPrefix(term, "!") {
			if e := strings.TrimSpace(term[1:]); e != "" {
				excludes = append(excludes, e)
			}
			continue
		}
		includes = append(includes, term)
	}
	for _, e := range excludes {
		if strings.Contains(topic, e) {
			return false
		}
	}
	if len(includes) == 0 {
		return true
	}
	for _, in := range includes {
		if strings.Contains(topic, in) {
			return true
		}
	}
	return false
}
