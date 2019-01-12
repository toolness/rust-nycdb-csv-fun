#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate csv;
extern crate blake2;
extern crate separator;
extern crate byteorder;

use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use separator::Separatable;
use blake2::{Blake2s, Digest};
use std::collections::HashMap;
use std::error::Error;
use std::io::prelude::*;
use std::env;
use std::process;
use std::fs::File;
use std::path::Path;

const VIOLATION_ID_INDEX: usize = 0;
const ROW_REPORT_INTERVAL: usize = 100000;

#[derive(Serialize, Deserialize, Debug)]
enum LogRecord<K, V> {
    Add(K, V),
    Update(K, V),
    Remove(K),
}

type ViolationLogRecord = LogRecord<u64, Vec<String>>;

fn validate_headers(headers: &csv::StringRecord) {
    assert_eq!(headers.get(VIOLATION_ID_INDEX), Some("ViolationID"));
}

fn process_csv(filename: &str) -> Result<(), Box<Error>> {
    let mut violation_map = HashMap::new();
    let path = Path::new(filename);
    let total_bytes = std::fs::metadata(path)?.len();
    let file = File::open(path)?;
    let logfile_path = Path::new("log.dat");
    let mut logfile = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).open(logfile_path)?;
    let mut records_read = 0;
    loop {
        match logfile.read_u16::<LittleEndian>() {
            Ok(size) => {
                let mut buf: Vec<u8> = Vec::with_capacity(size as usize);
                let mut handle = &logfile;
                handle.take(size as u64).read_to_end(&mut buf)?;
                let log_record = bincode::deserialize::<ViolationLogRecord>(&buf).unwrap();
                println!("{:?}", log_record);
                records_read += 1;
                // TODO: Actually modify violation_map based on the log record.
            },
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(Box::new(err));
            }
        }
    }
    println!("Read {} existing records from logfile.", records_read);
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
                let hash: Vec<u8> = Vec::from(hasher.result().as_slice());
                if violation_map.insert(violation_id, hash).is_some() {
                    panic!("Multiple entries for violation id {} found!", violation_id);
                }
                let mut row: Vec<String> = Vec::with_capacity(record.len());
                for item in record.iter() {
                    row.push(String::from(item));
                }
                let log_record: ViolationLogRecord = LogRecord::Add(violation_id, row);
                let encoded = bincode::serialize(&log_record).unwrap();
                logfile.write_u16::<LittleEndian>(encoded.len() as u16).unwrap();
                logfile.write(encoded.as_slice())?;
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
    println!("Finished processing {} records.", num_rows.separated_string());

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
