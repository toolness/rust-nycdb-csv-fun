extern crate serde_derive;
extern crate bincode;
extern crate csv;
extern crate blake2;
extern crate separator;
extern crate byteorder;

use separator::Separatable;
use blake2::{Blake2s, Digest};
use std::collections::HashMap;
use std::error::Error;
use std::env;
use std::process;
use std::fs::File;
use std::path::Path;

const VIOLATION_ID_INDEX: usize = 0;
const ROW_REPORT_INTERVAL: usize = 100000;

type ViolationMap = HashMap<u64, Vec<u8>>;

fn validate_headers(headers: &csv::StringRecord) {
    assert_eq!(headers.get(VIOLATION_ID_INDEX), Some("ViolationID"));
}

fn get_hash<'a, T: Iterator<Item = &'a str>>(iter: T) -> Vec<u8> {
    let mut hasher = Blake2s::new();
    for item in iter {
        hasher.input(item);
    }
    Vec::from(hasher.result().as_slice())
}

fn create_empty_logfile(path: &Path, headers: &csv::StringRecord) -> Result<(), Box<Error>> {
    let logfile = File::create(path)?;
    let mut writer = csv::Writer::from_writer(logfile);
    writer.write_record(headers)?;
    writer.flush()?;
    Ok(())
}

fn process_csv(
    rdr: &mut csv::Reader<File>,
    path: &Path,
    violation_map: &mut ViolationMap,
    logfile: Option<&mut csv::Writer<File>>
) -> Result<(), Box<Error>> {
    validate_headers(rdr.headers()?);
    let total_bytes = std::fs::metadata(path)?.len();
    let mut num_rows: usize = 0;
    let mut record_iter = rdr.records();
    let mut additions = 0;
    let mut updates = 0;
    let mut my_logfile = logfile;
    println!("Processing {}...", path.display());
    loop {
        match record_iter.next() {
            Some(result) => {
                let record = result?;
                let violation_id_str = record.get(VIOLATION_ID_INDEX).unwrap();
                let violation_id: u64 = violation_id_str.parse().unwrap();
                let hash = get_hash(record.iter());
                let is_changed = match violation_map.insert(violation_id, hash) {
                    Some(existing_hash) => if violation_map.get(&violation_id).unwrap() != &existing_hash {
                        updates += 1;
                        true
                    } else {
                        false
                    },
                    None => {
                        additions += 1;
                        true
                    }
                };
                num_rows += 1;
                if is_changed {
                    if let Some(ref mut writer) = my_logfile {
                        writer.write_record(&record)?;
                    }
                }
                if num_rows % ROW_REPORT_INTERVAL == 0 {
                    let byte = record_iter.reader().position().byte();
                    let pct: u32 = ((byte as f32 / total_bytes as f32) * 100.0) as u32;
                    println!("{}% complete.", pct);
                }
            }
            None => break
        }
    }
    if let Some(ref mut writer) = my_logfile {
        writer.flush()?;
    }
    println!("Finished processing {} records with {} additions and {} updates.",
             num_rows.separated_string(), additions.separated_string(), updates.separated_string());
    Ok(())
}

fn process_logfile(path: &Path, violation_map: &mut ViolationMap) -> Result<(), Box<Error>> {
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);
    process_csv(&mut rdr, path, violation_map, None)?;
    Ok(())
}

fn process_logfile_and_csv(log_filename: &str, filename: &str) -> Result<(), Box<Error>> {
    let mut violation_map = HashMap::new();
    let path = Path::new(filename);
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);

    let logfile_path = Path::new(log_filename);
    if !logfile_path.exists() {
        create_empty_logfile(logfile_path, rdr.headers()?)?;
    }
    process_logfile(logfile_path, &mut violation_map)?;
    let logfile = std::fs::OpenOptions::new().write(true).append(true).open(logfile_path)?;
    let mut logfile_writer = csv::Writer::from_writer(logfile);

    process_csv(&mut rdr, path, &mut violation_map, Some(&mut logfile_writer))?;

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

    if let Err(err) = process_logfile_and_csv("log.csv", &filename) {
        println!("error: {}", err);
        process::exit(1);
    }
}
