use std::cmp::Ordering;

/// Compares two keys using Oxia's slash-aware ordering.
///
/// This must match the server and every other Oxia client — it is a direct port
/// of the Go reference `common/compare.CompareWithSlash`. Comparison proceeds
/// segment by segment, splitting on `/`; at any level a key with no further `/`
/// sorts *before* one that has a deeper segment, regardless of the remaining
/// bytes. `list` and `range_scan` merge per-shard results with this order, so it
/// has to agree with the server exactly.
pub fn compare(mut a: &str, mut b: &str) -> Ordering {
    while !a.is_empty() && !b.is_empty() {
        match (a.find('/'), b.find('/')) {
            // Neither has a further separator: plain byte-wise comparison.
            (None, None) => return a.cmp(b),
            // The side that ends first (no more `/`) sorts before the deeper one,
            // independent of the bytes — this is the crux of the ordering.
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            // Both have another segment: compare this segment, else descend.
            (Some(ia), Some(ib)) => {
                let span = a[..ia].cmp(&b[..ib]);
                if span != Ordering::Equal {
                    return span;
                }
                a = &a[ia + 1..];
                b = &b[ib + 1..];
            }
        }
    }
    // One side is exhausted: the shorter key sorts first.
    a.len().cmp(&b.len())
}
#[cfg(test)]
mod tests {
    use crate::key::compare;
    use std::cmp::Ordering;

    #[test]
    fn test_simple_strings() {
        assert_eq!(compare("apple", "banana"), Ordering::Less);
        assert_eq!(compare("zoo", "zebra"), Ordering::Greater);
        assert_eq!(compare("test", "test"), Ordering::Equal);
    }

    #[test]
    fn test_single_slash_paths() {
        assert_eq!(compare("a/b", "a/c"), Ordering::Less);
        assert_eq!(compare("a/b", "a/b"), Ordering::Equal);
        assert_eq!(compare("a/c", "a/b"), Ordering::Greater);
        assert_eq!(compare("apple/fruit", "banana/fruit"), Ordering::Less);
        assert_eq!(compare("apple/fruit", "apple/grape"), Ordering::Less);
    }

    #[test]
    fn test_multi_slash_paths() {
        assert_eq!(compare("a/b/c", "a/b/d"), Ordering::Less);
        assert_eq!(compare("a/b/c", "a/b/c"), Ordering::Equal);
        assert_eq!(compare("a/b/c", "a/c/b"), Ordering::Less);
        assert_eq!(compare("a/b/c/d", "a/b/e/f"), Ordering::Less);
    }

    #[test]
    fn test_prefix_paths() {
        assert_eq!(compare("a/b", "a/b/c"), Ordering::Less);
        assert_eq!(compare("a/b/c", "a/b"), Ordering::Greater);
        assert_eq!(compare("a", "a/b"), Ordering::Less);
        assert_eq!(compare("a/b", "a"), Ordering::Greater);
    }

    #[test]
    fn test_empty_segments() {
        assert_eq!(compare("a//c", "a/b/c"), Ordering::Less);
        assert_eq!(compare("a/b", "a/b/"), Ordering::Less);
        // "a//" descends to "/" vs "b": "/" has a further segment, "b" does not,
        // so "a//" sorts after "a/b". (Matches the Go reference; the pre-fix
        // implementation wrongly returned Less by comparing bytes first.)
        assert_eq!(compare("a//", "a/b"), Ordering::Greater);
        assert_eq!(compare("", "a/b"), Ordering::Less);
    }

    /// Vectors copied verbatim from the Go reference
    /// (`common/compare/compare_with_slash_test.go`). If these diverge, this
    /// client orders `list`/`range_scan` results differently from the server and
    /// every other Oxia client.
    #[test]
    fn matches_go_compare_with_slash_vectors() {
        assert_eq!(compare("aaaaa", "aaaaa"), Ordering::Equal);
        assert_eq!(compare("aaaaa", "zzzzz"), Ordering::Less);
        assert_eq!(compare("bbbbb", "aaaaa"), Ordering::Greater);
        assert_eq!(compare("aaaaa", ""), Ordering::Greater);
        assert_eq!(compare("", "aaaaaa"), Ordering::Less);
        assert_eq!(compare("", ""), Ordering::Equal);
        assert_eq!(compare("aaaaa", "aaaaaaaaaaa"), Ordering::Less);
        assert_eq!(compare("aaaaaaaaaaa", "aaa"), Ordering::Greater);
        assert_eq!(compare("a", "/"), Ordering::Less);
        assert_eq!(compare("/", "a"), Ordering::Greater);
        assert_eq!(compare("/aaaa", "/bbbbb"), Ordering::Less);
        assert_eq!(compare("/aaaa", "/aa/a"), Ordering::Less);
        assert_eq!(compare("/aaaa/a", "/aaaa/b"), Ordering::Less);
        assert_eq!(compare("/aaaa/a/a", "/bbbbbbbbbb"), Ordering::Greater);
        assert_eq!(compare("/aaaa/a/a", "/aaaa/bbbbbbbbbb"), Ordering::Greater);
        assert_eq!(compare("/a/b/a/a/a", "/a/b/a/b"), Ordering::Greater);
    }

    #[test]
    fn test_empty_strings() {
        assert_eq!(compare("", ""), Ordering::Equal);
        assert_eq!(compare("a", ""), Ordering::Greater);
        assert_eq!(compare("", "a"), Ordering::Less);
        assert_eq!(compare("", "/"), Ordering::Less);
    }

    #[test]
    fn test_different_prefixes() {
        assert_eq!(compare("c/a", "a/b"), Ordering::Greater);
        assert_eq!(compare("a/b", "c/a"), Ordering::Less);
    }

    #[test]
    fn test_paths_with_complex_characters() {
        assert_eq!(compare("a-1/b", "a-2/b"), Ordering::Less);
        assert_eq!(compare("a_1/b", "a_2/b"), Ordering::Less);
        assert_eq!(compare("1/2/3", "1/2/4"), Ordering::Less);
    }

    #[test]
    fn test_leading_trailing_slashes() {
        assert_eq!(compare("/a/b", "a/b"), Ordering::Less);
        assert_eq!(compare("a/b/", "a/b"), Ordering::Greater);
        assert_eq!(compare("/a/b/", "a/b"), Ordering::Less);
        assert_eq!(compare("/a/b/", "/a/b"), Ordering::Greater);
    }

    #[test]
    fn test_root_vs_subdirectory() {
        assert_eq!(compare("/", "a/b"), Ordering::Less);
        assert_eq!(compare("a/b", "/"), Ordering::Greater);
    }
}
