use aes::Aes256;
use bytes::{Bytes, BytesMut};
use ctr::cipher::{KeyIvInit, StreamCipher};
use hmac::{Hmac, Mac};
use rand::RngCore;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PROTECTED_STORAGE_DIR: &str = "/.protected";
const PROTECTED_MAGIC: [u8; 4] = *b"FUPR";
const PROTECTED_VERSION: u8 = 1;
pub const PROTECTED_HEADER_LEN: usize = 36;
pub const PROTECTED_INTEGRITY_NONE: &str = "none";
pub const PROTECTED_INTEGRITY_HMAC_SHA256_CHUNKED: &str = "hmac-sha256-chunked";
pub const PROTECTED_MAC_LEN: usize = 32;
pub const DEFAULT_ENCRYPT_INTEGRITY_CHUNK_SIZE: u32 = 1024 * 1024;

type Aes256Ctr = ctr::Ctr128BE<Aes256>;
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectedMode {
    Obfuscate,
    Encrypt,
}

impl ProtectedMode {
    pub fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "obfuscate" => Some(Self::Obfuscate),
            "encrypt" => Some(Self::Encrypt),
            _ => None,
        }
    }

    fn to_byte(self) -> u8 {
        match self {
            Self::Obfuscate => 1,
            Self::Encrypt => 2,
        }
    }

    fn from_byte(raw: u8) -> Result<Self, String> {
        match raw {
            1 => Ok(Self::Obfuscate),
            2 => Ok(Self::Encrypt),
            _ => Err(format!("unsupported protected mode byte: {}", raw)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectedPrng {
    Xorshift,
    Pcg,
}

impl ProtectedPrng {
    pub fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "xorshift" => Some(Self::Xorshift),
            "pcg" => Some(Self::Pcg),
            _ => None,
        }
    }

    fn to_byte(self) -> u8 {
        match self {
            Self::Xorshift => 1,
            Self::Pcg => 2,
        }
    }

    fn from_byte(raw: u8) -> Result<Self, String> {
        match raw {
            1 => Ok(Self::Xorshift),
            2 => Ok(Self::Pcg),
            _ => Err(format!("unsupported protected PRNG byte: {}", raw)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProtectedPathPlan {
    pub root: String,
    pub mode: ProtectedMode,
    pub key_slot_id: String,
    pub block_size: usize,
    pub prng: ProtectedPrng,
    pub encrypt_key: Option<[u8; 32]>,
    pub workers: usize,
}

#[derive(Debug, Clone)]
pub struct ProtectedHeader {
    pub mode: ProtectedMode,
    pub prng: ProtectedPrng,
    pub block_size: u32,
    pub logical_size: u64,
    pub seed_or_nonce: [u8; 16],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtectedMetaRecord {
    pub version: u8,
    pub mode: String,
    pub prng: String,
    pub block_size: u32,
    pub logical_size: u64,
    pub seed_or_nonce_hex: String,
    pub integrity: String,
    pub integrity_chunk_size: Option<u32>,
}

impl ProtectedHeader {
    pub fn encode(&self) -> [u8; PROTECTED_HEADER_LEN] {
        let mut out = [0u8; PROTECTED_HEADER_LEN];
        out[..4].copy_from_slice(&PROTECTED_MAGIC);
        out[4] = PROTECTED_VERSION;
        out[5] = self.mode.to_byte();
        out[6] = self.prng.to_byte();
        out[7] = 0;
        out[8..12].copy_from_slice(&self.block_size.to_le_bytes());
        out[12..20].copy_from_slice(&self.logical_size.to_le_bytes());
        out[20..36].copy_from_slice(&self.seed_or_nonce);
        out
    }

    pub fn decode(raw: &[u8]) -> Result<Self, String> {
        if raw.len() < PROTECTED_HEADER_LEN {
            return Err("protected payload header too short".to_string());
        }
        if raw[..4] != PROTECTED_MAGIC {
            return Err("protected payload magic mismatch".to_string());
        }
        if raw[4] != PROTECTED_VERSION {
            return Err(format!("unsupported protected payload version: {}", raw[4]));
        }
        let mode = ProtectedMode::from_byte(raw[5])?;
        let prng = ProtectedPrng::from_byte(raw[6])?;
        let block_size =
            u32::from_le_bytes(raw[8..12].try_into().map_err(|_| "invalid block size")?);
        let logical_size =
            u64::from_le_bytes(raw[12..20].try_into().map_err(|_| "invalid logical size")?);
        let mut seed_or_nonce = [0u8; 16];
        seed_or_nonce.copy_from_slice(&raw[20..36]);
        Ok(Self {
            mode,
            prng,
            block_size,
            logical_size,
            seed_or_nonce,
        })
    }
}

pub fn next_blob_logical_path(key_slot_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"fileuni:protected:blob-slot:");
    hasher.update(key_slot_id.as_bytes());
    let digest = hasher.finalize();
    let slot_prefix = hex::encode(&digest[..4]);
    let blob_id = uuid::Uuid::now_v7().to_string();
    format!("{}/{}/{}.bin", PROTECTED_STORAGE_DIR, slot_prefix, blob_id)
}

pub fn encode_payload(
    user_id: &str,
    plan: &ProtectedPathPlan,
    plaintext: Bytes,
) -> Result<(Bytes, ProtectedHeader, String), String> {
    let mut seed_or_nonce = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut seed_or_nonce);
    let logical_size = plaintext.len() as u64;
    let mut payload = plaintext.to_vec();
    match plan.mode {
        ProtectedMode::Obfuscate => obfuscate_in_place(
            &mut payload,
            user_id,
            &plan.key_slot_id,
            seed_or_nonce,
            plan.block_size.max(1),
            plan.prng,
            plan.workers,
        ),
        ProtectedMode::Encrypt => encrypt_in_place(
            &mut payload,
            plan.encrypt_key
                .as_ref()
                .ok_or_else(|| "Protected encrypt key is missing".to_string())?,
            seed_or_nonce,
        ),
    }
    let header = ProtectedHeader {
        mode: plan.mode,
        prng: plan.prng,
        block_size: plan.block_size as u32,
        logical_size,
        seed_or_nonce,
    };
    let mut meta_record = ProtectedMetaRecord::from_header(&header);
    let mut mac_table = Vec::new();
    if plan.mode == ProtectedMode::Encrypt {
        let encrypt_key = plan
            .encrypt_key
            .as_ref()
            .ok_or_else(|| "Protected encrypt key is missing".to_string())?;
        mac_table = build_encrypt_mac_table(
            encrypt_key,
            DEFAULT_ENCRYPT_INTEGRITY_CHUNK_SIZE as usize,
            &payload,
        )?;
        meta_record.integrity = PROTECTED_INTEGRITY_HMAC_SHA256_CHUNKED.to_string();
        meta_record.integrity_chunk_size = Some(DEFAULT_ENCRYPT_INTEGRITY_CHUNK_SIZE);
    }
    let mut out = BytesMut::with_capacity(PROTECTED_HEADER_LEN + payload.len() + mac_table.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&payload);
    if !mac_table.is_empty() {
        out.extend_from_slice(&mac_table);
    }
    let meta_json = serde_json::to_string(&meta_record)
        .map_err(|e| format!("serialize protected meta failed: {}", e))?;
    Ok((out.freeze(), header, meta_json))
}

pub fn header_from_meta_json(raw: &str) -> Result<ProtectedHeader, String> {
    let meta: ProtectedMetaRecord =
        serde_json::from_str(raw).map_err(|e| format!("parse protected meta failed: {}", e))?;
    meta.into_header()
}

pub fn meta_from_json(raw: &str) -> Result<ProtectedMetaRecord, String> {
    serde_json::from_str(raw).map_err(|e| format!("parse protected meta failed: {}", e))
}

pub fn encrypt_mac_table_len(logical_size: u64, chunk_size: u32) -> usize {
    integrity_chunk_count(logical_size, chunk_size) * PROTECTED_MAC_LEN
}

pub fn integrity_chunk_count(logical_size: u64, chunk_size: u32) -> usize {
    if logical_size == 0 {
        0
    } else {
        logical_size.div_ceil(chunk_size.max(1) as u64) as usize
    }
}

pub fn decode_range(
    user_id: &str,
    key_slot_id: &str,
    encrypt_key: Option<[u8; 32]>,
    workers: usize,
    header: &ProtectedHeader,
    payload: Bytes,
    payload_logical_start: u64,
    start: u64,
    end: u64,
) -> Result<Bytes, String> {
    let logical_end = end
        .min(header.logical_size)
        .max(start.min(header.logical_size));
    if start >= logical_end {
        return Ok(Bytes::new());
    }
    match header.mode {
        ProtectedMode::Obfuscate => decode_obfuscate_range(
            user_id,
            key_slot_id,
            workers,
            header,
            payload,
            payload_logical_start,
            start,
            logical_end,
        ),
        ProtectedMode::Encrypt => decode_encrypt_range(
            encrypt_key,
            header,
            payload,
            payload_logical_start,
            start,
            logical_end,
        ),
    }
}

pub fn slice_logical_range(data: Bytes, start: u64, end: u64) -> Bytes {
    let len = data.len() as u64;
    let clamped_start = start.min(len);
    let clamped_end = end.min(len).max(clamped_start);
    data.slice(clamped_start as usize..clamped_end as usize)
}

impl ProtectedMetaRecord {
    pub fn from_header(header: &ProtectedHeader) -> Self {
        Self {
            version: PROTECTED_VERSION,
            mode: match header.mode {
                ProtectedMode::Obfuscate => "obfuscate".to_string(),
                ProtectedMode::Encrypt => "encrypt".to_string(),
            },
            prng: match header.prng {
                ProtectedPrng::Xorshift => "xorshift".to_string(),
                ProtectedPrng::Pcg => "pcg".to_string(),
            },
            block_size: header.block_size,
            logical_size: header.logical_size,
            seed_or_nonce_hex: hex::encode(header.seed_or_nonce),
            integrity: PROTECTED_INTEGRITY_NONE.to_string(),
            integrity_chunk_size: None,
        }
    }

    pub fn into_header(self) -> Result<ProtectedHeader, String> {
        let mode = ProtectedMode::from_str(&self.mode)
            .ok_or_else(|| format!("unsupported protected mode meta: {}", self.mode))?;
        let prng = ProtectedPrng::from_str(&self.prng)
            .ok_or_else(|| format!("unsupported protected prng meta: {}", self.prng))?;
        let seed = hex::decode(&self.seed_or_nonce_hex)
            .map_err(|e| format!("decode protected seed failed: {}", e))?;
        if seed.len() != 16 {
            return Err("invalid protected seed size".to_string());
        }
        let mut seed_or_nonce = [0u8; 16];
        seed_or_nonce.copy_from_slice(&seed);
        Ok(ProtectedHeader {
            mode,
            prng,
            block_size: self.block_size,
            logical_size: self.logical_size,
            seed_or_nonce,
        })
    }
}

fn encrypt_in_place(buf: &mut [u8], key: &[u8; 32], nonce: [u8; 16]) {
    let mut cipher = Aes256Ctr::new(key.into(), (&nonce).into());
    cipher.apply_keystream(buf);
}

fn encrypt_range_in_place(buf: &mut [u8], key: &[u8; 32], nonce: [u8; 16], logical_offset: u64) {
    let block_offset = logical_offset / 16;
    let intra_offset = (logical_offset % 16) as usize;
    let counter = u128::from_be_bytes(nonce).wrapping_add(block_offset as u128);
    let iv = counter.to_be_bytes();
    let mut cipher = Aes256Ctr::new(key.into(), (&iv).into());
    if intra_offset > 0 {
        let mut skip = vec![0u8; intra_offset];
        cipher.apply_keystream(&mut skip);
    }
    cipher.apply_keystream(buf);
}

fn derive_encrypt_mac_key(encrypt_key: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"fileuni:protected:encrypt:mac:v1:");
    hasher.update(encrypt_key);
    hasher.finalize().into()
}

fn compute_encrypt_chunk_mac(
    encrypt_key: &[u8; 32],
    chunk_index: u64,
    ciphertext: &[u8],
) -> Result<[u8; PROTECTED_MAC_LEN], String> {
    let mac_key = derive_encrypt_mac_key(encrypt_key);
    let mut mac =
        HmacSha256::new_from_slice(&mac_key).map_err(|e| format!("invalid mac key: {}", e))?;
    mac.update(&chunk_index.to_le_bytes());
    mac.update(ciphertext);
    let out = mac.finalize().into_bytes();
    let mut tag = [0u8; PROTECTED_MAC_LEN];
    tag.copy_from_slice(&out);
    Ok(tag)
}

fn build_encrypt_mac_table(
    encrypt_key: &[u8; 32],
    chunk_size: usize,
    ciphertext: &[u8],
) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(
        integrity_chunk_count(ciphertext.len() as u64, chunk_size as u32) * PROTECTED_MAC_LEN,
    );
    for (chunk_index, chunk) in ciphertext.chunks(chunk_size.max(1)).enumerate() {
        let tag = compute_encrypt_chunk_mac(encrypt_key, chunk_index as u64, chunk)?;
        out.extend_from_slice(&tag);
    }
    Ok(out)
}

fn obfuscate_in_place(
    buf: &mut [u8],
    user_id: &str,
    key_slot_id: &str,
    seed: [u8; 16],
    block_size: usize,
    prng: ProtectedPrng,
    workers: usize,
) {
    obfuscate_in_place_from_block(
        buf,
        user_id,
        key_slot_id,
        seed,
        block_size,
        prng,
        0,
        workers,
    )
}

fn effective_parallel_workers(requested: usize) -> usize {
    if requested == 0 {
        std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1)
    } else {
        requested.max(1)
    }
}

fn obfuscate_in_place_from_block(
    buf: &mut [u8],
    user_id: &str,
    key_slot_id: &str,
    seed: [u8; 16],
    block_size: usize,
    prng: ProtectedPrng,
    start_block_index: u64,
    workers: usize,
) {
    let block_size = block_size.max(1);
    let chunk_count = buf.len().div_ceil(block_size);
    let effective_workers = effective_parallel_workers(workers);
    if effective_workers <= 1 || chunk_count < 4 {
        for (block_index, chunk) in buf.chunks_mut(block_size).enumerate() {
            let absolute_index = start_block_index + block_index as u64;
            match prng {
                ProtectedPrng::Xorshift => {
                    apply_xorshift_mask(chunk, user_id, key_slot_id, seed, absolute_index)
                }
                ProtectedPrng::Pcg => {
                    apply_pcg_mask(chunk, user_id, key_slot_id, seed, absolute_index)
                }
            }
        }
        return;
    }

    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(effective_workers)
        .build()
        .map(|pool| {
            pool.install(|| {
                buf.par_chunks_mut(block_size)
                    .enumerate()
                    .for_each(|(block_index, chunk)| {
                        let absolute_index = start_block_index + block_index as u64;
                        match prng {
                            ProtectedPrng::Xorshift => apply_xorshift_mask(
                                chunk,
                                user_id,
                                key_slot_id,
                                seed,
                                absolute_index,
                            ),
                            ProtectedPrng::Pcg => {
                                apply_pcg_mask(chunk, user_id, key_slot_id, seed, absolute_index)
                            }
                        }
                    });
            })
        });
}

fn derive_block_material(
    user_id: &str,
    key_slot_id: &str,
    seed: [u8; 16],
    block_index: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"fileuni:protected:obfuscate:v1:");
    hasher.update(user_id.as_bytes());
    hasher.update(b":");
    hasher.update(key_slot_id.as_bytes());
    hasher.update(b":");
    hasher.update(seed);
    hasher.update(block_index.to_le_bytes());
    hasher.finalize().into()
}

fn apply_xorshift_mask(
    chunk: &mut [u8],
    user_id: &str,
    key_slot_id: &str,
    seed: [u8; 16],
    block_index: u64,
) {
    let material = derive_block_material(user_id, key_slot_id, seed, block_index);
    let mut state = u64::from_le_bytes(material[..8].try_into().unwrap_or([1u8; 8]));
    if state == 0 {
        state = 0x9e37_79b9_7f4a_7c15;
    }
    let mut offset = 0usize;
    while offset < chunk.len() {
        let next = xorshift64star_next(&mut state).to_le_bytes();
        let take = (chunk.len() - offset).min(next.len());
        for (dst, mask) in chunk[offset..offset + take].iter_mut().zip(next.iter()) {
            *dst ^= *mask;
        }
        offset += take;
    }
}

fn apply_pcg_mask(
    chunk: &mut [u8],
    user_id: &str,
    key_slot_id: &str,
    seed: [u8; 16],
    block_index: u64,
) {
    let material = derive_block_material(user_id, key_slot_id, seed, block_index);
    let mut state = u64::from_le_bytes(material[..8].try_into().unwrap_or([1u8; 8]));
    let mut inc = u64::from_le_bytes(material[8..16].try_into().unwrap_or([3u8; 8])) | 1;
    if state == 0 {
        state = 0x853c_49e6_748f_ea9b;
    }
    let mut offset = 0usize;
    while offset < chunk.len() {
        let next = pcg32_next(&mut state, &mut inc).to_le_bytes();
        let take = (chunk.len() - offset).min(next.len());
        for (dst, mask) in chunk[offset..offset + take].iter_mut().zip(next.iter()) {
            *dst ^= *mask;
        }
        offset += take;
    }
}

fn decode_obfuscate_range(
    user_id: &str,
    key_slot_id: &str,
    workers: usize,
    header: &ProtectedHeader,
    payload: Bytes,
    payload_logical_start: u64,
    start: u64,
    end: u64,
) -> Result<Bytes, String> {
    let block_size = (header.block_size as usize).max(1);
    let aligned_start = payload_logical_start as usize;
    let clamped_end = (payload_logical_start as usize).saturating_add(payload.len());
    let mut buf = payload.to_vec();
    let first_block_index = aligned_start / block_size;
    obfuscate_in_place_from_block(
        &mut buf,
        user_id,
        key_slot_id,
        header.seed_or_nonce,
        block_size,
        header.prng,
        first_block_index as u64,
        workers,
    );
    let slice_start = (start as usize).saturating_sub(aligned_start);
    let slice_end = slice_start + (end - start) as usize;
    let logical_window = &buf[..clamped_end.saturating_sub(aligned_start)];
    Ok(Bytes::copy_from_slice(
        &logical_window[slice_start..slice_end],
    ))
}

fn decode_encrypt_range(
    encrypt_key: Option<[u8; 32]>,
    header: &ProtectedHeader,
    payload: Bytes,
    payload_logical_start: u64,
    start: u64,
    end: u64,
) -> Result<Bytes, String> {
    let mut buf = payload.to_vec();
    let key = encrypt_key.ok_or_else(|| "Protected encrypt key is missing".to_string())?;
    encrypt_range_in_place(&mut buf, &key, header.seed_or_nonce, payload_logical_start);
    let slice_start = (start - payload_logical_start) as usize;
    let slice_end = slice_start + (end - start) as usize;
    Ok(Bytes::copy_from_slice(&buf[slice_start..slice_end]))
}

pub fn verify_encrypt_window(
    encrypt_key: &[u8; 32],
    chunk_size: u32,
    payload_logical_start: u64,
    ciphertext_window: &[u8],
    mac_table: &[u8],
) -> Result<(), String> {
    let chunk_size = chunk_size.max(1) as u64;
    let start_chunk = payload_logical_start / chunk_size;
    let expected_chunks = integrity_chunk_count(ciphertext_window.len() as u64, chunk_size as u32);
    if mac_table.len() != expected_chunks * PROTECTED_MAC_LEN {
        return Err("protected MAC table window size mismatch".to_string());
    }
    for (idx, chunk) in ciphertext_window.chunks(chunk_size as usize).enumerate() {
        let chunk_index = start_chunk + idx as u64;
        let expected = compute_encrypt_chunk_mac(encrypt_key, chunk_index, chunk)?;
        let offset = idx * PROTECTED_MAC_LEN;
        let actual = &mac_table[offset..offset + PROTECTED_MAC_LEN];
        if actual != expected {
            return Err(format!(
                "protected MAC verification failed at chunk {}",
                chunk_index
            ));
        }
    }
    Ok(())
}

fn xorshift64star_next(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

fn pcg32_next(state: &mut u64, inc: &mut u64) -> u32 {
    let oldstate = *state;
    *state = oldstate
        .wrapping_mul(6364136223846793005)
        .wrapping_add(*inc | 1);
    let xorshifted = (((oldstate >> 18) ^ oldstate) >> 27) as u32;
    let rot = (oldstate >> 59) as u32;
    xorshifted.rotate_right(rot)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obfuscate_plan(workers: usize) -> ProtectedPathPlan {
        ProtectedPathPlan {
            root: "/".to_string(),
            mode: ProtectedMode::Obfuscate,
            key_slot_id: "slot-a".to_string(),
            block_size: 64,
            prng: ProtectedPrng::Xorshift,
            encrypt_key: None,
            workers,
        }
    }

    fn encrypt_plan() -> ProtectedPathPlan {
        ProtectedPathPlan {
            root: "/".to_string(),
            mode: ProtectedMode::Encrypt,
            key_slot_id: "slot-b".to_string(),
            block_size: 256 * 1024,
            prng: ProtectedPrng::Xorshift,
            encrypt_key: Some([7u8; 32]),
            workers: 1,
        }
    }

    #[test]
    fn obfuscate_roundtrip_and_range_work() {
        let plaintext = Bytes::from_static(
            b"hello protected storage this payload should cross blocks for range reading",
        );
        let (encoded, header, _meta) =
            encode_payload("user-a", &obfuscate_plan(1), plaintext.clone()).expect("encode");
        let payload = encoded.slice(PROTECTED_HEADER_LEN..);
        let all = decode_range(
            "user-a",
            "slot-a",
            None,
            1,
            &header,
            payload.clone(),
            0,
            0,
            plaintext.len() as u64,
        )
        .expect("decode all");
        assert_eq!(all, plaintext);

        let range = decode_range("user-a", "slot-a", None, 1, &header, payload, 0, 7, 31)
            .expect("decode range");
        assert_eq!(range, plaintext.slice(7..31));
    }

    #[test]
    fn obfuscate_multithread_is_stable() {
        let seed = [9u8; 16];
        let input = vec![0x5a; 4096];
        let mut single = input.clone();
        let mut parallel = input.clone();
        obfuscate_in_place_from_block(
            &mut single,
            "user-a",
            "slot-a",
            seed,
            256,
            ProtectedPrng::Pcg,
            0,
            1,
        );
        obfuscate_in_place_from_block(
            &mut parallel,
            "user-a",
            "slot-a",
            seed,
            256,
            ProtectedPrng::Pcg,
            0,
            4,
        );
        assert_eq!(single, parallel);
    }

    #[test]
    fn encrypt_roundtrip_and_chunk_mac_work() {
        let plaintext = Bytes::from(vec![0x2a; 2 * 1024 * 1024 + 321]);
        let plan = encrypt_plan();
        let (encoded, header, meta_json) =
            encode_payload("user-b", &plan, plaintext.clone()).expect("encode");
        let meta = meta_from_json(&meta_json).expect("meta");
        assert_eq!(meta.integrity, PROTECTED_INTEGRITY_HMAC_SHA256_CHUNKED);
        let chunk_size = meta.integrity_chunk_size.expect("chunk size");
        let mac_len = encrypt_mac_table_len(header.logical_size, chunk_size);
        let payload_end = encoded.len() - mac_len;
        let payload = encoded.slice(PROTECTED_HEADER_LEN..payload_end);
        let mac_table = encoded.slice(payload_end..);
        verify_encrypt_window(
            &plan.encrypt_key.expect("encrypt key"),
            chunk_size,
            0,
            payload.as_ref(),
            mac_table.as_ref(),
        )
        .expect("verify mac");
        let range = decode_range(
            "user-b",
            "slot-b",
            plan.encrypt_key,
            1,
            &header,
            payload,
            0,
            123,
            4567,
        )
        .expect("decode range");
        assert_eq!(range, plaintext.slice(123..4567));
    }
}
