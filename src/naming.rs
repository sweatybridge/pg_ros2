//! Validation for fully qualified ROS names, shared by subscriptions and publishing.

/// Check that `name` is a fully qualified ROS name with valid segments.
///
/// Only the ROS name rules are enforced here. The PostgreSQL notification
/// channel length limit is subscription-specific and applied by its caller.
pub(crate) fn validate_topic(name: &str) -> Result<(), &'static str> {
    let Some(relative) = name.strip_prefix('/') else {
        return Err("topic must be a fully qualified ROS name starting with '/'");
    };
    if relative.split('/').any(|part| {
        let mut bytes = part.bytes();
        !bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
            || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }) {
        return Err("topic must contain nonempty ROS name segments (letters, digits, underscores; no leading digits)");
    }
    Ok(())
}
