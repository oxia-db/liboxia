use std::cmp::Ordering;

pub fn compare(mut a: &str, mut b: &str) -> Ordering {
    loop {
        let idx_a = a.find('/');
        let idx_b = b.find('/');
        match (idx_a, idx_b) {
            (None, None) => {
                return a.cmp(b);
            }
            (None, Some(_)) => {
                let b_span = &b[..idx_b.unwrap()];
                let span_res = a.cmp(b_span);
                if span_res != Ordering::Equal {
                    return span_res;
                }
                return Ordering::Less;
            }
            (Some(_), None) => {
                let a_span = &a[..idx_a.unwrap()];
                let span_res = a_span.cmp(b);
                if span_res != Ordering::Equal {
                    return span_res;
                }
                return Ordering::Greater;
            }
            (Some(ia), Some(ib)) => {
                let span_a = &a[..ia];
                let span_b = &b[..ib];
                let span_res = span_a.cmp(span_b);
                if span_res != Ordering::Equal {
                    return span_res;
                }
                a = &a[ia + 1..];
                b = &b[ib + 1..];
            }
        }
    }
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
        assert_eq!(compare("a//", "a/b"), Ordering::Less);
        assert_eq!(compare("", "a/b"), Ordering::Less);
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
