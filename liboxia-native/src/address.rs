pub(crate) fn ensure_protocol(raw_address: String) -> String {
    if raw_address.starts_with("http://") || raw_address.starts_with("https://") {
        return raw_address;
    }
    format!("http://{}", raw_address)
}
