pub fn derive_nextcloud_file_numeric_id(user_id: &str, path: &str) -> u64 {
    stable_hash_u64(format!("{}:{}", user_id, path).as_bytes()) & 0x7fff_ffff_ffff_ffff
}

pub fn derive_nextcloud_share_numeric_id(share_token: &str) -> u64 {
    stable_hash_u64(share_token.as_bytes()) & 0x7fff_ffff
}

pub fn derive_nextcloud_remote_id(user_id: &str, path: &str) -> String {
    format!(
        "{:08}fileuni",
        derive_nextcloud_file_numeric_id(user_id, path)
    )
}

fn stable_hash_u64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    if hash == 0 {
        1
    } else {
        hash
    }
}
