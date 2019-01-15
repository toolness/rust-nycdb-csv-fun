use std::fs::{File, metadata, OpenOptions};
use std::path::Path;
use std::error::Error;


#[derive(Serialize, Deserialize)]
pub struct Revision {
    pub id: u64,
    pub byte_offset: u64,
    pub rows: u64
}

#[derive(Clone)]
pub struct CsvLog {
    pub basename: String,
    pub filename: String,
    pub index_filename: String
}

impl CsvLog {
    pub fn new(basename: &str) -> Self {
        CsvLog {
            basename: String::from(basename),
            filename: format!("{}.csv", basename),
            index_filename: format!("{}.revisions.csv", basename)
        }
    }

    fn create_empty_logfile(&mut self, headers: &csv::StringRecord) -> Result<(), Box<Error>> {
        let logfile = File::create(&self.filename)?;
        let mut writer = csv::Writer::from_writer(logfile);
        writer.write_record(headers)?;
        writer.flush()?;
        Ok(())
    }

    pub fn create_revision(&mut self, headers: &csv::StringRecord) -> Result<LogRevisionWriter, Box<Error>> {
        if !Path::new(&self.filename).exists() {
            self.create_empty_logfile(headers)?;
        }
        LogRevisionWriter::new(self.clone())
    }

    pub fn export_revision<T: std::io::Write>(&self, revision: u64, writer: &mut csv::Writer<T>) -> Result<u64, Box<Error>> {
        let logfile_index = File::open(&self.index_filename)?;
        let mut logfile_index_reader = csv::Reader::from_reader(logfile_index);

        for index_result in logfile_index_reader.deserialize() {
            let rev: Revision = index_result?;
            if rev.id != revision {
                continue;
            }
            let logfile = File::open(&self.filename)?;
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
}

pub struct LogRevisionWriter {
    info: CsvLog,
    rev: Revision,
    logfile_writer: csv::Writer<File>
}

impl LogRevisionWriter {
    fn new(info: CsvLog) -> Result<Self, Box<Error>> {
        let byte_offset = metadata(&info.filename)?.len();
        let logfile = OpenOptions::new()
            .write(true).append(true).open(&info.filename)?;
        let logfile_writer = csv::Writer::from_writer(logfile);

        Ok(LogRevisionWriter {
            info,
            rev: Revision {
                id: 0,
                byte_offset,
                rows: 0
            },
            logfile_writer
        })
    }

    pub fn write(&mut self, record: &csv::StringRecord) -> Result<(), Box<Error>> {
        self.rev.rows += 1;
        self.logfile_writer.write_record(record)?;
        Ok(())
    }

    pub fn complete(mut self) -> Result<Option<Revision>, Box<Error>> {
        if self.rev.rows == 0 {
            return Ok(None);
        }
        let logfile_index_path = Path::new(&self.info.index_filename);
        self.logfile_writer.flush()?;
        let id = write_logfile_index_revision(logfile_index_path, self.rev.byte_offset, self.rev.rows)?;
        self.rev.id = id;
        Ok(Some(self.rev))
    }
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
