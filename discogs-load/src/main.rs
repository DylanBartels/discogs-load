use anyhow::Result;
use flate2::read::GzDecoder;
use log::info;
use quick_xml::{events::Event, Reader};
use std::{error::Error, fs::File, io::BufReader, path::PathBuf};
use structopt::StructOpt;

mod artist;
mod db;
mod label;
mod master;
mod parser;
mod release;

const BUF_SIZE: usize = 4096; // 4kb at once

#[derive(StructOpt, Debug)]
#[structopt(name = "discogs-load")]
struct Opt {
    /// Path to one or more discogs monthly data dump files, still compressed
    #[structopt(name = "FILE(S)", parse(from_os_str))]
    files: Vec<PathBuf>,

    // DB related arguments
    #[structopt(flatten)]
    dbopts: db::DbOpt,
}

fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    let opt = Opt::from_args();

    if let Err(e) = read_files(&opt) {
        println!("{:?}", e);
        std::process::exit(1);
    }
    Ok(())
}

fn read_files(opt: &Opt) -> Result<(), Box<dyn Error>> {
    for file in &opt.files {
        let gzfile = File::open(file.to_str().unwrap())?;
        let xmlfile = GzDecoder::new(gzfile);
        let xmlfile = BufReader::new(xmlfile);
        let mut xmlfile = Reader::from_reader(xmlfile);
        let mut buf = Vec::with_capacity(BUF_SIZE);

        // Parse fileinput on type (label/release/artist)
        let mut parser: Box<dyn parser::Parser> = loop {
            if let Event::Start(ref e) = xmlfile.read_event(&mut buf)? {
                match e.name() {
                    b"labels" => {
                        db::init(&opt.dbopts, "sql/tables/label.sql")?;
                        break Box::new(parser::Parser::new(
                            &label::LabelsParser::new(&opt.dbopts),
                            &opt.dbopts,
                        ));
                    }
                    b"releases" => {
                        db::init(&opt.dbopts, "sql/tables/release.sql")?;
                        break Box::new(parser::Parser::new(
                            &release::ReleasesParser::new(&opt.dbopts),
                            &opt.dbopts,
                        ));
                    }
                    b"artists" => {
                        db::init(&opt.dbopts, "sql/tables/artist.sql")?;
                        break Box::new(parser::Parser::new(
                            &artist::ArtistsParser::new(&opt.dbopts),
                            &opt.dbopts,
                        ));
                    }
                    b"masters" => {
                        db::init(&opt.dbopts, "sql/tables/master.sql")?;
                        break Box::new(parser::Parser::new(
                            &master::MastersParser::new(&opt.dbopts),
                            &opt.dbopts,
                        ));
                    }
                    _ => (),
                };
                buf.clear();
            };
            buf.clear();
        };

        // Parse and insert file
        let gzfile = File::open(file.to_str().unwrap())?;
        let xmlfile = GzDecoder::new(gzfile);
        let xmlfile = BufReader::new(xmlfile);
        let mut xmlfile = Reader::from_reader(xmlfile);
        let mut buf = Vec::with_capacity(BUF_SIZE);
        info!("Parsing and inserting: {:?}", file.file_name().unwrap());
        loop {
            match xmlfile.read_event(&mut buf)? {
                Event::Eof => break,
                ev => parser.process(ev)?,
            };
            buf.clear();
        }
    }

    if opt.dbopts.create_indexes {
        db::indexes(&opt.dbopts, "sql/indexes.sql")?;
    }

    Ok(())
}
