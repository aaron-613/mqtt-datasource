// MQTT topic-filter matching, ported from the Go backend (pkg/mqtt/match.go). Used by the
// query editor to show how many discovered topics a typed wildcard pattern would match.

// isWildcard reports whether an MQTT topic filter contains a wildcard (`+` or `#`).
export const isWildcard = (pattern: string): boolean => /[+#]/.test(pattern);

// matchesTopic reports whether a concrete topic matches an MQTT topic filter that may
// contain wildcards (`+` = one level, `#` = trailing multi-level). This is the boolean
// half of the backend's MatchTopic (which also returns the matched segments for labeling).
//
// Segments are split on "/" with no trimming or empty-segment collapsing, matching Go's
// strings.Split so behavior is identical to the backend.
export const matchesTopic = (pattern: string, topic: string): boolean => {
  if (!isWildcard(pattern)) {
    return pattern === topic;
  }

  const pp = pattern.split('/');
  const tp = topic.split('/');

  for (let i = 0; i < pp.length; i++) {
    const seg = pp[i];
    if (seg === '#') {
      // Multi-level wildcard: valid only as the final segment; matches the rest
      // (including zero remaining levels).
      return i === pp.length - 1;
    }
    if (seg === '+') {
      // Single-level wildcard: consumes exactly one topic level, which must exist.
      if (i >= tp.length) {
        return false;
      }
      continue;
    }
    // Literal segment: must exist and be equal.
    if (i >= tp.length || seg !== tp[i]) {
      return false;
    }
  }

  // No trailing '#': the topic must have exactly as many levels as the filter.
  return pp.length === tp.length;
};
