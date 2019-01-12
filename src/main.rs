extern crate csv;
extern crate blake2;

use blake2::{Blake2s, Digest};
use std::collections::HashMap;
use std::error::Error;
use std::mem;
use std::env;
use std::process;
use std::fs::File;
use std::path::Path;

const VIOLATION_ID_INDEX: usize = 0;
const ROW_REPORT_INTERVAL: usize = 100000;

fn validate_headers(headers: &csv::StringRecord) {
    assert_eq!(headers.get(VIOLATION_ID_INDEX), Some("ViolationID"));
}

fn process_csv(filename: &str) -> Result<(), Box<Error>> {
    let mut violation_map = HashMap::new();
    let path = Path::new(filename);
    let total_bytes = std::fs::metadata(path)?.len();
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);
    validate_headers(rdr.headers()?);
    let mut num_rows: usize = 0;
    let mut record_iter = rdr.into_records();
    loop {
        match record_iter.next() {
            Some(result) => {
                let record = result?;
                let violation_id_str = record.get(VIOLATION_ID_INDEX).unwrap();
                let violation_id: u64 = violation_id_str.parse().unwrap();
                let mut hasher = Blake2s::new();
                for item in record.iter() {
                    hasher.input(item);
                }
                let hash = hasher.result();
                if violation_map.insert(violation_id, hash).is_some() {
                    panic!("Multiple entries for violation id {} found!", violation_id);
                }
                num_rows += 1;
                if num_rows % ROW_REPORT_INTERVAL == 0 {
                    let byte = record_iter.reader().position().byte();
                    let pct: u32 = ((byte as f32 / total_bytes as f32) * 100.0) as u32;
                    println!("{}% complete.", pct);
                }
            }
            None => break
        }
    }
    println!("Finished processing {} records.", num_rows);
    println!("Approximate memory used by violation_map: {} bytes",
             mem::size_of_val(&violation_map) * num_rows);
    Ok(())
}

fn main() {
    let mut args = env::args();

    if args.len() < 2 {
        let executable = args.nth(0).unwrap();
        println!("usage: {} <csv-file>", executable);
        process::exit(1);
    }

    let filename = args.nth(1).unwrap();

    if let Err(err) = process_csv(&filename) {
        println!("error: {}", err);
        process::exit(1);
    }
}
