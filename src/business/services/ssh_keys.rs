use super::super::entities::ssh_keys;
use super::{VfsCommonError, VfsCommonResult};
use base64::{Engine as _, engine::general_purpose};
use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, ModelTrait, QueryFilter, Set,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SshKeyInfo {
    pub id: String,
    pub user_id: String,
    pub key_name: String,
    pub public_key: String,
    pub fingerprint: String,
    pub key_type: String,
    pub comment: Option<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
}

pub struct SshKeyService;

impl SshKeyService {
    /// SSH
    pub async fn add_key(
        db: &DatabaseConnection,
        user_id: &str,
        key_name: &str,
        public_key: &str,
        comment: Option<String>,
    ) -> VfsCommonResult<SshKeyInfo> {
        let (fingerprint, key_type) = Self::parse_public_key(public_key)?;
        let id = Uuid::now_v7().to_string();
        let now = Utc::now();
        let key = ssh_keys::ActiveModel {
            id: Set(id),
            user_id: Set(user_id.to_string()),
            key_name: Set(key_name.to_string()),
            public_key: Set(public_key.to_string()),
            fingerprint: Set::<String>(fingerprint),
            key_type: Set::<String>(key_type),
            comment: Set(comment),
            created_at: Set(now),
            last_used_at: Set(None),
        };
        let inserted = key.insert(db).await?;
        Ok(SshKeyInfo {
            id: inserted.id,
            user_id: inserted.user_id,
            key_name: inserted.key_name,
            public_key: inserted.public_key,
            fingerprint: inserted.fingerprint,
            key_type: inserted.key_type,
            comment: inserted.comment,
            created_at: inserted.created_at.to_rfc3339(),
            last_used_at: inserted.last_used_at.map(|dt| dt.to_rfc3339()),
        })
    }

    /// SSH
    pub async fn delete_key(
        db: &DatabaseConnection,
        user_id: &str,
        key_id: &str,
    ) -> VfsCommonResult<()> {
        let key = ssh_keys::Entity::find_by_id(key_id.to_string())
            .filter(ssh_keys::Column::UserId.eq(user_id))
            .one(db)
            .await?
            .ok_or(VfsCommonError::Internal("SSH key not found".to_string()))?;
        key.delete(db).await?;
        Ok(())
    }

    /// SSH
    pub async fn list_keys(
        db: &DatabaseConnection,
        user_id: &str,
    ) -> VfsCommonResult<Vec<SshKeyInfo>> {
        let keys = ssh_keys::Entity::find()
            .filter(ssh_keys::Column::UserId.eq(user_id))
            .all(db)
            .await?;
        Ok(keys
            .into_iter()
            .map(|k| SshKeyInfo {
                id: k.id,
                user_id: k.user_id,
                key_name: k.key_name,
                public_key: k.public_key,
                fingerprint: k.fingerprint,
                key_type: k.key_type,
                comment: k.comment,
                created_at: k.created_at.to_rfc3339(),
                last_used_at: k.last_used_at.map(|dt| dt.to_rfc3339()),
            })
            .collect())
    }

    pub async fn update_last_used(db: &DatabaseConnection, key_id: &str) -> VfsCommonResult<()> {
        let key = ssh_keys::Entity::find_by_id(key_id.to_string())
            .one(db)
            .await?
            .ok_or(VfsCommonError::Internal("SSH key not found".to_string()))?;
        let active_key: ssh_keys::ActiveModel = key.into();
        let mut active_key = active_key;
        active_key.last_used_at = Set(Some(Utc::now()));
        active_key.update(db).await?;
        Ok(())
    }

    /// SSH (, )
    fn parse_public_key(public_key: &str) -> VfsCommonResult<(String, String)> {
        let parts: Vec<&str> = public_key.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(VfsCommonError::Internal(
                "Invalid SSH public key format".to_string(),
            ));
        }
        let key_type = parts
            .first()
            .ok_or_else(|| VfsCommonError::Internal("Missing key type".to_string()))?
            .to_string();
        let key_data = parts
            .get(1)
            .ok_or_else(|| VfsCommonError::Internal("Missing key data".to_string()))?;

        use sha2::{Digest, Sha256};
        let decoded = general_purpose::STANDARD
            .decode(key_data)
            .map_err(|_| VfsCommonError::Internal("Invalid base64 encoding".to_string()))?;
        let hash = Sha256::digest(&decoded);
        let fingerprint = hash
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join(":");
        Ok((fingerprint, key_type))
    }

    pub fn validate_public_key(public_key: &str) -> VfsCommonResult<()> {
        let parts: Vec<&str> = public_key.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(VfsCommonError::Internal(
                "Invalid SSH public key format".to_string(),
            ));
        }
        let key_type = parts
            .first()
            .ok_or_else(|| VfsCommonError::Internal("Missing key type".to_string()))?;
        let valid_types = [
            "ssh-rsa",
            "ssh-ed25519",
            "ecdsa-sha2-nistp256",
            "ecdsa-sha2-nistp384",
            "ecdsa-sha2-nistp521",
        ];
        if !valid_types.contains(key_type) {
            return Err(VfsCommonError::Internal(format!(
                "Unsupported SSH key type: {}",
                key_type
            )));
        }

        let key_data = parts
            .get(1)
            .ok_or_else(|| VfsCommonError::Internal("Missing key data".to_string()))?;
        general_purpose::STANDARD
            .decode(key_data)
            .map_err(|_| VfsCommonError::Internal("Invalid base64 encoding".to_string()))?;
        Ok(())
    }
}
