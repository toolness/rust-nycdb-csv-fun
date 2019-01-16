use md5::{Md5, Digest};
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

const HASH_SIZE: usize = 16;

pub enum UpdateType {
    Added,
    Changed
}

fn build_progress_bar(total: u64) -> ProgressBar<std::io::Stdout> {
    let mut pb = ProgressBar::new(total);
    pb.show_counter = false;
    pb.show_speed = false;
    pb.set_max_refresh_rate(Some(Duration::from_millis(100)));
    pb
}

pub struct PkHashMap {
    map: HashMap<u64, Vec<u8>>,
    temp_hash: Md5
}

impl PkHashMap {
    pub fn new() -> Self {
        PkHashMap {
            map: HashMap::new(),
            temp_hash: Md5::new()
        }
    }

    pub fn update<'a, T: Iterator<Item = &'a [u8]>>(&mut self, pk: u64, iter_to_hash: T) -> Option<UpdateType> {
        for item in iter_to_hash {
            self.temp_hash.input(item);
        }
        let hash = self.temp_hash.result_reset();
        let result = if let Some(existing_hash) = self.map.get(&pk) {
            if hash.as_slice() == existing_hash.as_slice() {
                None
            } else {
                Some(UpdateType::Changed)
            }
        } else {
            Some(UpdateType::Added)
        };
        if result.is_some() {
            self.map.insert(pk, Vec::from(hash.as_slice()));
        }
        result
    }

    pub fn deserialize(&mut self, path: &Path) -> Result<(), Box<Error>> {
        let rawfile = File::open(path)?;
        let mut file = BufReader::new(rawfile);
        let u64_size = 8;
        let entry_size = (u64_size + HASH_SIZE) as u64;
        let total_entries = metadata(path)?.len() / entry_size;
        println!("Loading log cache with {} entries...", total_entries.separated_string());
        let mut pb = build_progress_bar(total_entries);
        for _ in 0..total_entries {
            let number = file.read_u64::<LittleEndian>()?;
            let mut hash = vec![0; HASH_SIZE];
            file.read_exact(&mut hash)?;
            self.map.insert(number, hash);
            pb.inc();
        }
        pb.finish_println("");
        Ok(())
    }

    pub fn serialize(&mut self, path: &Path) -> Result<(), Box<Error>> {
        let rawfile = File::create(path)?;
        let mut file = BufWriter::new(rawfile);
        let total_entries = self.map.len();
        println!("Saving log cache with {} entries...", total_entries.separated_string());
        let mut pb = build_progress_bar(total_entries as u64);
        for (key, value) in self.map.iter() {
            file.write_u64::<LittleEndian>(*key)?;
            file.write(value)?;
            pb.inc();
        }
        file.flush()?;
        pb.finish_println("");
        Ok(())
    }
}
