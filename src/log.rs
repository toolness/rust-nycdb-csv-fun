use std::fs::{File, OpenOptions};
use std::path::Path;
use std::error::Error;


#[derive(Serialize, Deserialize)]
struct Revision {
    id: u64,
    byte_offset: u64,
    rows: u64
}

pub fn create_empty_logfile(path: &Path, headers: &csv::StringRecord) -> Result<(), Box<Error>> {
    let logfile = File::create(path)?;
    let mut writer = csv::Writer::from_writer(logfile);
    writer.write_record(headers)?;
    writer.flush()?;
    Ok(())
}

pub struct LogInfo {
    pub basename: String,
    pub filename: String,
    pub index_filename: String
}

impl LogInfo {
    pub fn new(basename: &str) -> Self {
        LogInfo {
            basename: String::from(basename),
            filename: format!("{}.csv", basename),
            index_filename: format!("{}.revisions.csv", basename)
        }
    }
}

pub fn create_empty_logfile_index(path: &Path) -> Result<(), Box<Error>> {
    let logfile = File::create(path)?;
    let mut writer = csv::Writer::from_writer(logfile);
    writer.write_record(vec!["id", "byte_offset", "rows"])?;
    writer.flush()?;
    Ok(())
}

pub fn get_latest_logfile_index_revision(path: &Path) -> Result<u64, Box<Error>> {
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

pub fn write_logfile_index_revision(path: &Path, byte_offset: u64, rows: u64) -> Result<u64, Box<Error>> {
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

pub fn export_revision<T: std::io::Write>(loginfo: &LogInfo, revision: u64, writer: &mut csv::Writer<T>) -> Result<u64, Box<Error>> {
    let logfile_index = File::open(&loginfo.index_filename)?;
    let mut logfile_index_reader = csv::Reader::from_reader(logfile_index);

    for index_result in logfile_index_reader.deserialize() {
        let rev: Revision = index_result?;
        if rev.id != revision {
            continue;
        }
        let logfile = File::open(&loginfo.filename)?;
        let mut logfile_reader = csv::Reader::from_reader(logfile);
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
        return Ok(rows);
    }

    Ok(0)
}
