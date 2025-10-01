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
