use dashmap::DashSet;
use once_cell::sync::Lazy;
/// Set of users currently under maintenance (WAL recovery)
static LOCKED_USERS: Lazy<DashSet<String>> = Lazy::new(DashSet::new);
/// Put a specific user into maintenance mode
pub fn enter_user_maintenance(user_id: &str) -> UserMaintenanceGuard {
    LOCKED_USERS.insert(user_id.to_string());
    yh_console_log::yhlog(
        "warn",
        &format!("User {} entered maintenance mode (LOCKED)", user_id),
    );
    UserMaintenanceGuard {
        user_id: user_id.to_string(),
    }
}
/// User Maintenance Mode Auto-Unlock Guard (RAII)
pub struct UserMaintenanceGuard {
    user_id: String,
}
impl Drop for UserMaintenanceGuard {
    fn drop(&mut self) {
        LOCKED_USERS.remove(&self.user_id);
        yh_console_log::yhlog(
            "info",
            &format!(
                "User {} exited maintenance mode (RAII UNLOCKED)",
                self.user_id
            ),
        );
    }
}
/// Take a specific user out of maintenance mode (manual explicit call)
pub fn exit_user_maintenance(user_id: &str) {
    LOCKED_USERS.remove(user_id);
    yh_console_log::yhlog(
        "info",
        &format!("User {} exited maintenance mode (MANUAL UNLOCKED)", user_id),
    );
}
/// Force clear all maintenance locks
pub fn clear_all_maintenance() {
    LOCKED_USERS.clear();
    yh_console_log::yhlog("warn", "ALL VFS MAINTENANCE LOCKS CLEARED BY ADMIN");
}
/// Check if a specific user is under maintenance
pub fn is_user_under_maintenance(user_id: &str) -> bool {
    LOCKED_USERS.contains(user_id)
}
/// Get a list of all users in maintenance mode
pub fn get_all_locked_users() -> Vec<String> {
    LOCKED_USERS
        .iter()
        .map(|item| item.key().to_owned())
        .collect()
}
pub fn is_maintenance_mode() -> bool {
    !LOCKED_USERS.is_empty()
}
