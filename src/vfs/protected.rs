use aes::Aes256;
use bytes::{Bytes, BytesMut};
use ctr::cipher::{KeyIvInit, StreamCipher};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PROTECTED_STORAGE_DIR: &str = "/.protected";
const PROTECTED_MAGIC: [u8; 4] = *b"FUPR";
const PROTECTED_VERSION: u8 = 1;
pub const PROTECTED_HEADER_LEN: usize = 36;

type Aes256Ctr = ctr::Ctr128BE<Aes256>;

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
        ),
        ProtectedMode::Encrypt => {
            encrypt_in_place(&mut payload, user_id, &plan.key_slot_id, seed_or_nonce)
        }
    }
    let header = ProtectedHeader {
        mode: plan.mode,
        prng: plan.prng,
        block_size: plan.block_size as u32,
        logical_size,
        seed_or_nonce,
    };
    let mut out = BytesMut::with_capacity(PROTECTED_HEADER_LEN + payload.len());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&payload);
    let meta_json = serde_json::to_string(&ProtectedMetaRecord::from_header(&header))
        .map_err(|e| format!("serialize protected meta failed: {}", e))?;
    Ok((out.freeze(), header, meta_json))
}

pub fn decode_payload(
    user_id: &str,
    key_slot_id: &str,
    data: Bytes,
) -> Result<(ProtectedHeader, Bytes), String> {
    if data.len() < PROTECTED_HEADER_LEN {
        return Err("protected payload is too short".to_string());
    }
    let header = ProtectedHeader::decode(&data[..PROTECTED_HEADER_LEN])?;
    let mut payload = data.slice(PROTECTED_HEADER_LEN..).to_vec();
    match header.mode {
        ProtectedMode::Obfuscate => obfuscate_in_place(
            &mut payload,
            user_id,
            key_slot_id,
            header.seed_or_nonce,
            (header.block_size as usize).max(1),
            header.prng,
        ),
        ProtectedMode::Encrypt => {
            encrypt_in_place(&mut payload, user_id, key_slot_id, header.seed_or_nonce)
        }
    }
    if payload.len() as u64 != header.logical_size {
        return Err("protected payload size mismatch".to_string());
    }
    Ok((header, Bytes::from(payload)))
}

pub fn header_from_meta_json(raw: &str) -> Result<ProtectedHeader, String> {
    let meta: ProtectedMetaRecord =
        serde_json::from_str(raw).map_err(|e| format!("parse protected meta failed: {}", e))?;
    meta.into_header()
}

pub fn decode_range(
    user_id: &str,
    key_slot_id: &str,
    header: &ProtectedHeader,
    payload: Bytes,
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
        ProtectedMode::Obfuscate => {
            decode_obfuscate_range(user_id, key_slot_id, header, payload, start, logical_end)
        }
        ProtectedMode::Encrypt => {
            decode_encrypt_range(user_id, key_slot_id, header, payload, start, logical_end)
        }
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

fn encrypt_in_place(buf: &mut [u8], user_id: &str, key_slot_id: &str, nonce: [u8; 16]) {
    let key = derive_encrypt_key(user_id, key_slot_id);
    let mut cipher = Aes256Ctr::new((&key).into(), (&nonce).into());
    cipher.apply_keystream(buf);
}

fn encrypt_range_in_place(
    buf: &mut [u8],
    user_id: &str,
    key_slot_id: &str,
    nonce: [u8; 16],
    logical_offset: u64,
) {
    let key = derive_encrypt_key(user_id, key_slot_id);
    let block_offset = logical_offset / 16;
    let intra_offset = (logical_offset % 16) as usize;
    let counter = u128::from_be_bytes(nonce).wrapping_add(block_offset as u128);
    let iv = counter.to_be_bytes();
    let mut cipher = Aes256Ctr::new((&key).into(), (&iv).into());
    if intra_offset > 0 {
        let mut skip = vec![0u8; intra_offset];
        cipher.apply_keystream(&mut skip);
    }
    cipher.apply_keystream(buf);
}

fn obfuscate_in_place(
    buf: &mut [u8],
    user_id: &str,
    key_slot_id: &str,
    seed: [u8; 16],
    block_size: usize,
    prng: ProtectedPrng,
) {
    for (block_index, chunk) in buf.chunks_mut(block_size).enumerate() {
        match prng {
            ProtectedPrng::Xorshift => {
                apply_xorshift_mask(chunk, user_id, key_slot_id, seed, block_index as u64)
            }
            ProtectedPrng::Pcg => {
                apply_pcg_mask(chunk, user_id, key_slot_id, seed, block_index as u64)
            }
        }
    }
}

fn derive_encrypt_key(user_id: &str, key_slot_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"fileuni:protected:encrypt:v1:");
    hasher.update(user_id.as_bytes());
    hasher.update(b":");
    hasher.update(key_slot_id.as_bytes());
    hasher.finalize().into()
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
    header: &ProtectedHeader,
    payload: Bytes,
    start: u64,
    end: u64,
) -> Result<Bytes, String> {
    let block_size = (header.block_size as usize).max(1);
    let aligned_start = ((start as usize) / block_size) * block_size;
    let aligned_end = (((end as usize) + block_size - 1) / block_size) * block_size;
    let clamped_end = aligned_end.min(header.logical_size as usize);
    let mut buf = payload.to_vec();
    let first_block_index = aligned_start / block_size;
    for (idx, chunk) in buf.chunks_mut(block_size).enumerate() {
        let block_index = first_block_index as u64 + idx as u64;
        match header.prng {
            ProtectedPrng::Xorshift => apply_xorshift_mask(
                chunk,
                user_id,
                key_slot_id,
                header.seed_or_nonce,
                block_index,
            ),
            ProtectedPrng::Pcg => apply_pcg_mask(
                chunk,
                user_id,
                key_slot_id,
                header.seed_or_nonce,
                block_index,
            ),
        }
    }
    let slice_start = (start as usize).saturating_sub(aligned_start);
    let slice_end = slice_start + (end - start) as usize;
    let logical_window = &buf[..clamped_end.saturating_sub(aligned_start)];
    Ok(Bytes::copy_from_slice(
        &logical_window[slice_start..slice_end],
    ))
}

fn decode_encrypt_range(
    user_id: &str,
    key_slot_id: &str,
    header: &ProtectedHeader,
    payload: Bytes,
    start: u64,
    end: u64,
) -> Result<Bytes, String> {
    let mut buf = payload.to_vec();
    encrypt_range_in_place(&mut buf, user_id, key_slot_id, header.seed_or_nonce, start);
    let expected = (end - start) as usize;
    if buf.len() != expected {
        return Err("protected encrypt range size mismatch".to_string());
    }
    Ok(Bytes::from(buf))
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
