use blake2::{Blake2s, Digest};
use std::collections::HashMap;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::path::Path;
use std::error::Error;
use std::fs::{File, metadata};
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::time::Duration;
use separator::Separatable;
use pbr::ProgressBar;

pub type PkHashMap = HashMap<u64, Vec<u8>>;

fn get_hash_size() -> usize {
    let hasher = Blake2s::new();
    hasher.result().len()
}

pub fn get_hash<'a, T: Iterator<Item = &'a str>>(iter: T) -> Vec<u8> {
    let mut hasher = Blake2s::new();
    for item in iter {
        hasher.input(item);
    }
    Vec::from(hasher.result().as_slice())
}

pub fn create_pk_map() -> PkHashMap {
    HashMap::new()
}

fn build_pb(total: u64) -> ProgressBar<std::io::Stdout> {
    let mut pb = ProgressBar::new(total);
    pb.show_counter = false;
    pb.show_speed = false;
    pb.set_max_refresh_rate(Some(Duration::from_millis(100)));
    pb
}

pub fn read_pk_map(map: &mut PkHashMap, path: &Path) -> Result<(), Box<Error>> {
    let rawfile = File::open(path)?;
    let mut file = BufReader::new(rawfile);
    let u64_size = 8;
    let hash_size = get_hash_size();
    let entry_size = (u64_size + hash_size) as u64;
    let total_entries = metadata(path)?.len() / entry_size;
    println!("Loading log cache with {} entries...", total_entries.separated_string());
    let mut pb = build_pb(total_entries);
    for _ in 0..total_entries {
        let number = file.read_u64::<LittleEndian>()?;
        let mut hash = vec![0; hash_size];
        file.read_exact(&mut hash)?;
        map.insert(number, hash);
        pb.inc();
    }
    pb.finish_println("");
    Ok(())
}

pub fn write_pk_map(map: &mut PkHashMap, path: &Path) -> Result<(), Box<Error>> {
    let rawfile = File::create(path)?;
    let mut file = BufWriter::new(rawfile);
    let total_entries = map.len();
    println!("Saving log cache with {} entries...", total_entries.separated_string());
    let mut pb = build_pb(total_entries as u64);
    for (key, value) in map.iter() {
        file.write_u64::<LittleEndian>(*key)?;
        file.write(value)?;
        pb.inc();
    }
    file.flush()?;
    pb.finish_println("");
    Ok(())
}
