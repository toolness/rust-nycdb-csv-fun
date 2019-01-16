extern crate serde;
extern crate pbr;
extern crate csv;
extern crate md5;
extern crate separator;
extern crate byteorder;
extern crate docopt;

#[macro_use]
extern crate serde_derive;

pub mod pk_map;
pub mod log;

use separator::Separatable;
use pbr::{ProgressBar, Units};
use std::error::Error;
use std::process;
use std::fs::{File, metadata};
use std::path::Path;
use std::time::SystemTime;

use pk_map::PkHashMap;
use log::CsvLog;

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

fn process_csv<F>(
    rdr: &mut csv::Reader<File>,
    path: &str,
    pk_map: &mut PkHashMap,
    on_change: &mut F
) -> Result<(), Box<Error>> where F: FnMut(&csv::ByteRecord) -> Result<(), Box<Error>> {
    let total_bytes = metadata(path)?.len();
    let mut num_rows: usize = 0;
    let mut additions = 0;
    let mut updates = 0;
    println!("Processing {}...", path);
    let mut pb = ProgressBar::new(total_bytes);
    pb.set_units(Units::Bytes);
    let mut record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut record)? {
        let pk_bytes = record.get(PRIMARY_KEY_INDEX).unwrap();
        let pk: u64 = std::str::from_utf8(pk_bytes).unwrap().parse().unwrap();
        let result = pk_map.update(pk, record.iter());
        match result {
            Some(update) => {
                match update {
                    pk_map::UpdateType::Added => { additions += 1; },
                    pk_map::UpdateType::Changed => { updates += 1; }
                }
                on_change(&record)?;
            },
            None => {}
        }
        num_rows += 1;
        if num_rows % ROW_REPORT_INTERVAL == 0 {
            pb.set(rdr.position().byte());
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

fn process_logfile(path: &str, pk_map: &mut PkHashMap) -> Result<(), Box<Error>> {
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);
    process_csv(&mut rdr, path, pk_map, &mut |_| Ok(()))?;
    Ok(())
}

fn process_logfile_and_csv(csvlog: &mut CsvLog, filename: &str) -> Result<(), Box<Error>> {
    let start_time = SystemTime::now();
    let vmap_filename = format!("{}.cache.dat", csvlog.basename);
    let vmap_path = Path::new(&vmap_filename);
    let mut pk_map = PkHashMap::new();
    let file = File::open(filename)?;
    let mut rdr = csv::Reader::from_reader(file);

    if vmap_path.exists() {
        pk_map.deserialize(vmap_path)?;
    } else if Path::new(&csvlog.filename).exists() {
        process_logfile(&csvlog.filename, &mut pk_map)?;
    }

    let mut rev_writer = csvlog.create_revision(rdr.byte_headers()?)?;

    process_csv(&mut rdr, filename, &mut pk_map, &mut |record| {
        rev_writer.write(record)
    })?;

    if let Some(rev) = rev_writer.complete()? {
        pk_map.serialize(vmap_path)?;
        println!("Wrote revision {}.", rev.id);
    } else {
        println!("No changes found.");
    }

    let elapsed_secs = start_time.elapsed().unwrap().as_secs();
    if elapsed_secs > 1 {
        println!("Finished in {} seconds.", elapsed_secs);
    }

    Ok(())
}

fn export_revision(csvlog: &CsvLog, revision: u64) -> Result<(), Box<Error>> {
    let mut writer = csv::Writer::from_writer(std::io::stdout());
    let rows_written = csvlog.export_revision(revision, &mut writer)?;
    if rows_written == 0 {
        println!("Revision {} does not exist!", revision);
        process::exit(1);
    }

    Ok(())
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

    let mut csvlog = CsvLog::new("log");

    if args.cmd_add {
        exit_on_error(process_logfile_and_csv(&mut csvlog, &args.arg_filename));
    }

    if args.cmd_export {
        exit_on_error(export_revision(&csvlog, args.arg_revision));
    }
}
