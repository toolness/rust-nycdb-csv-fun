extern crate serde;
extern crate pbr;
extern crate csv;
extern crate blake2;
extern crate separator;
extern crate byteorder;
extern crate docopt;

#[macro_use]
extern crate serde_derive;

pub mod pk_map;

use separator::Separatable;
use pbr::{ProgressBar, Units};
use std::error::Error;
use std::process;
use std::fs::{File, metadata, OpenOptions};
use std::path::Path;
use std::time::SystemTime;
use pk_map::PkHashMap;

const USAGE: &'static str = "
Proof-of-concept CSV experiment for NYC-DB.

Usage:
  nycsv add <filename>
  nycsv export <revision>

Options:
  -h, --help    Show this screen.
";

const PRIMARY_KEY_INDEX: usize = 0;

const ROW_REPORT_INTERVAL: usize = 100000;

#[derive(Deserialize)]
struct Args {
    arg_filename: String,
    arg_revision: u64,
    cmd_add: bool,
    cmd_export: bool
}

#[derive(Serialize, Deserialize)]
struct Revision {
    id: u64,
    byte_offset: u64,
    rows: u64
}

fn create_empty_logfile(path: &Path, headers: &csv::StringRecord) -> Result<(), Box<Error>> {
    let logfile = File::create(path)?;
    let mut writer = csv::Writer::from_writer(logfile);
    writer.write_record(headers)?;
    writer.flush()?;
    Ok(())
}

fn process_csv<F>(
    rdr: &mut csv::Reader<File>,
    path: &Path,
    pk_map: &mut PkHashMap,
    on_change: &mut F
) -> Result<(), Box<Error>> where F: FnMut(&csv::StringRecord) -> Result<(), Box<Error>> {
    let total_bytes = metadata(path)?.len();
    let mut num_rows: usize = 0;
    let mut record_iter = rdr.records();
    let mut additions = 0;
    let mut updates = 0;
    println!("Processing {}...", path.display());
    let mut pb = ProgressBar::new(total_bytes);
    pb.set_units(Units::Bytes);
    loop {
        match record_iter.next() {
            Some(result) => {
                let record = result?;
                let pk_str = record.get(PRIMARY_KEY_INDEX).unwrap();
                let pk: u64 = pk_str.parse().unwrap();
                let hash = pk_map::get_hash(record.iter());
                let is_changed = match pk_map.insert(pk, hash) {
                    Some(existing_hash) => if pk_map.get(&pk).unwrap() != &existing_hash {
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
                    on_change(&record)?;
                }
                if num_rows % ROW_REPORT_INTERVAL == 0 {
                    pb.set(record_iter.reader().position().byte());
                }
            }
            None => break
        }
    }
    pb.finish_println("");
    if num_rows > 0 {
        println!(
            "Finished processing {} records with {} additions and {} updates.",
            num_rows.separated_string(),
            additions.separated_string(),
            updates.separated_string()
        );
    }
    Ok(())
}

fn process_logfile(path: &Path, pk_map: &mut PkHashMap) -> Result<(), Box<Error>> {
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);
    process_csv(&mut rdr, path, pk_map, &mut |_| Ok(()))?;
    Ok(())
}

struct LogInfo {
    basename: String,
    filename: String,
    index_filename: String
}

impl LogInfo {
    fn new(basename: &str) -> Self {
        LogInfo {
            basename: String::from(basename),
            filename: format!("{}.csv", basename),
            index_filename: format!("{}.revisions.csv", basename)
        }
    }
}

fn process_logfile_and_csv(loginfo: &LogInfo, filename: &str) -> Result<(), Box<Error>> {
    let start_time = SystemTime::now();
    let vmap_filename = format!("{}.cache.dat", loginfo.basename);
    let vmap_path = Path::new(&vmap_filename);
    let mut pk_map = pk_map::create_pk_map();
    let path = Path::new(filename);
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);

    let logfile_path = Path::new(&loginfo.filename);
    if !logfile_path.exists() {
        create_empty_logfile(logfile_path, rdr.headers()?)?;
    }
    let revision_byte_offset = metadata(logfile_path)?.len();
    if vmap_path.exists() {
        pk_map::read_pk_map(&mut pk_map, vmap_path)?;
    } else {
        process_logfile(logfile_path, &mut pk_map)?;
    }
    let logfile = OpenOptions::new()
        .write(true).append(true).open(logfile_path)?;
    let mut logfile_writer = csv::Writer::from_writer(logfile);
    let mut rows = 0;

    process_csv(&mut rdr, path, &mut pk_map, &mut |record| {
        rows += 1;
        logfile_writer.write_record(record)?;
        Ok(())
    })?;

    if rows > 0 {
        let logfile_index_path = Path::new(&loginfo.index_filename);
        logfile_writer.flush()?;
        let id = write_logfile_index_revision(logfile_index_path, revision_byte_offset, rows)?;
        pk_map::write_pk_map(&mut pk_map, vmap_path)?;
        println!("Wrote revision {}.", id);
    } else {
        println!("No changes found.");
    }

    let elapsed_secs = start_time.elapsed().unwrap().as_secs();
    if elapsed_secs > 1 {
        println!("Finished in {} seconds.", elapsed_secs);
    }

    Ok(())
}

fn create_empty_logfile_index(path: &Path) -> Result<(), Box<Error>> {
    let logfile = File::create(path)?;
    let mut writer = csv::Writer::from_writer(logfile);
    writer.write_record(vec!["id", "byte_offset", "rows"])?;
    writer.flush()?;
    Ok(())
}

fn get_latest_logfile_index_revision(path: &Path) -> Result<u64, Box<Error>> {
    let file = File::open(path)?;
    let mut reader = csv::Reader::from_reader(file);
    let mut latest = 0;

    for result in reader.deserialize() {
        let rev: Revision = result?;
        if rev.id > latest {
            latest = rev.id;
        }
    }

    Ok(latest)
}

fn write_logfile_index_revision(path: &Path, byte_offset: u64, rows: u64) -> Result<u64, Box<Error>> {
    if !path.exists() {
        create_empty_logfile_index(path)?;
    }

    let id = get_latest_logfile_index_revision(path)? + 1;
    let logfile_index = OpenOptions::new()
        .write(true).append(true).open(path)?;
    let mut logfile_index_writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(logfile_index);
    logfile_index_writer.serialize(Revision {
        id: id,
        byte_offset: byte_offset,
        rows: rows
    })?;

    Ok(id)
}

fn export_revision(loginfo: &LogInfo, revision: u64) -> Result<(), Box<Error>> {
    let logfile_index = File::open(&loginfo.index_filename)?;
    let mut logfile_index_reader = csv::Reader::from_reader(logfile_index);

    for index_result in logfile_index_reader.deserialize() {
        let rev: Revision = index_result?;
        if rev.id != revision {
            continue;
        }
        let logfile = File::open(&loginfo.filename)?;
        let mut logfile_reader = csv::Reader::from_reader(logfile);
        let mut writer = csv::Writer::from_writer(std::io::stdout());
        let mut pos = csv::Position::new();
        let mut rows = 0;

        writer.write_record(logfile_reader.headers()?)?;

        pos.set_byte(rev.byte_offset);
        logfile_reader.seek(pos).unwrap();
        for result in logfile_reader.records() {
            let record = result?;
            writer.write_record(&record)?;
            rows += 1;
            if rows == rev.rows {
                break;
            }
        }
        return Ok(());
    }

    println!("Revision {} does not exist!", revision);
    process::exit(1);
}

fn exit_on_error(result: Result<(), Box<Error>>) {
    if let Err(err) = result {
        println!("error: {}", err);
        process::exit(1);
    }
}

fn main() {
    let args: Args = docopt::Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let loginfo = LogInfo::new("log");

    if args.cmd_add {
        exit_on_error(process_logfile_and_csv(&loginfo, &args.arg_filename));
    }

    if args.cmd_export {
        exit_on_error(export_revision(&loginfo, args.arg_revision));
    }
}
