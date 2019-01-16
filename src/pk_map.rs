use blake2::VarBlake2b;
use blake2::digest::{Input, VariableOutput};
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

// We're effectively using id-blake2b160. For more details, see:
// https://tools.ietf.org/html/rfc7693
const HASH_SIZE: usize = 20;

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
    temp_hash: Vec<u8>
}

impl PkHashMap {
    pub fn new() -> Self {
        PkHashMap {
            map: HashMap::new(),
            temp_hash: Vec::with_capacity(HASH_SIZE)
        }
    }

    fn fill_temp_hash<'a, T: Iterator<Item = &'a [u8]>>(&mut self, iter: T) {
        // Note that it'd be nice to be able to have this hasher be part
        // of the struct so that we wouldn't have to re-allocate it every
        // call, but this doesn't seem to be possible with the variable
        // hasher interface, since getting the result always consumes the
        // hasher.
        let mut hasher = VarBlake2b::new(HASH_SIZE).unwrap();
        for item in iter {
            hasher.input(item);
        }
        self.temp_hash.clear();
        hasher.variable_result(|res| {
            self.temp_hash.extend_from_slice(res);
        });
    }

    pub fn update<'a, T: Iterator<Item = &'a [u8]>>(&mut self, pk: u64, iter_to_hash: T) -> Option<UpdateType> {
        self.fill_temp_hash(iter_to_hash);
        let result = if let Some(existing_hash) = self.map.get(&pk) {
            if &self.temp_hash == existing_hash {
                None
            } else {
                Some(UpdateType::Changed)
            }
        } else {
            Some(UpdateType::Added)
        };
        if result.is_some() {
            self.map.insert(pk, self.temp_hash.clone());
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
