use std::{
    fs::File,
    error::Error,
    io::BufReader,
    path::PathBuf
};
use log::info;
use structopt::StructOpt;
use anyhow::Result;
use flate2::read::GzDecoder;
use encoding_rs_io::DecodeReaderBytes;
use quick_xml::{
    events::Event,
    Reader,
};

mod db;
mod releases;

const BUF_SIZE: usize = 4096; // 4kb at once

#[derive(StructOpt, Debug)]
#[structopt(name = "discogs-load")]
struct Opt {
    /// Path to the releases file, still compressed
    #[structopt(name = "FILE", parse(from_os_str))]
    files: Vec<PathBuf>,

    // DB related arguments
    #[structopt(flatten)]
    dbopts: db::DbOpt,
}

fn main() -> Result<()> { 
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    let opt = Opt::from_args();

    match load_releases(&opt) {
        Err(e) => println!("{:?}", e),
        _ => ()
    }
    Ok(())
}

fn load_releases(opt: &Opt) -> Result<(), Box<dyn Error>> {
    db::init(&opt.dbopts)?;

    let gzfile = File::open(&opt.files[0].to_str().unwrap())?;
    let xmlfile = GzDecoder::new(gzfile);

    let xmlfile = BufReader::new(DecodeReaderBytes::new(xmlfile));
    let mut xmlfile = Reader::from_reader(xmlfile);

    let mut releaseparser = releases::ReleasesParser::with_predicate();
    let mut buf = Vec::with_capacity(BUF_SIZE);
    
    info!("Parsing XML and inserting into database...");
    loop {
        match xmlfile.read_event(&mut buf)? {
            Event::Eof => break,
            ev => releaseparser.process(ev)?,
        };
        buf.clear();
    }

    // db::indexes(&opt.dbopts)?;

    Ok(())
}