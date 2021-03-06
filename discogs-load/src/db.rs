use anyhow::Result;
use log::info;
use postgres::types::{ToSql, Type};
use postgres::{binary_copy::BinaryCopyInWriter, Client, NoTls};
use std::{collections::HashMap, fs};
use structopt::StructOpt;

use crate::artist::Artist;
use crate::label::Label;
use crate::master::{Master, MasterArtist};
use crate::release::{Release, ReleaseLabel, ReleaseVideo};

#[derive(Debug, Clone, StructOpt)]
pub struct DbOpt {
    /// Creates indexes
    #[structopt(long = "create-indexes")]
    pub create_indexes: bool,
    /// Number of rows per insert
    #[structopt(long = "batch-size", default_value = "10000")]
    pub batch_size: usize,
    /// Database host
    #[structopt(long = "db-host", default_value = "localhost")]
    pub db_host: String,
    /// Database user
    #[structopt(long = "db-user", default_value = "dev")]
    pub db_user: String,
    /// Database password
    #[structopt(long = "db-password", default_value = "dev_pass")]
    pub db_password: String,
    /// Database name
    #[structopt(long = "db-name", default_value = "discogs")]
    pub db_name: String,
}

pub trait SqlSerialization {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)>;
}

/// Initialize schema and close connection.
pub fn init(db_opts: &DbOpt, schema_path: &str) -> Result<()> {
    info!("Creating the tables.");
    let db = Db::connect(db_opts);
    Db::execute_file(&mut db?, schema_path)?;
    Ok(())
}

/// Initialize indexes and close connection.
pub fn indexes(opts: &DbOpt, file_path: &str) -> Result<()> {
    info!("Creating the indexes.");
    let db = Db::connect(opts);
    Db::execute_file(&mut db?, file_path)?;
    Ok(())
}

pub fn write_releases(
    db_opts: &DbOpt,
    releases: &HashMap<i32, Release>,
    releases_labels: &HashMap<i32, ReleaseLabel>,
    releases_videos: &HashMap<i32, ReleaseVideo>,
) -> Result<()> {
    let mut db = Db::connect(db_opts)?;
    Db::write_rows(&mut db, releases, InsertCommand::new(
        "release",
        "(id, status, title, country, released, notes, genres, styles, master_id, data_quality)",
        &[
            Type::INT4,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT_ARRAY,
            Type::TEXT_ARRAY,
            Type::INT4,
            Type::TEXT,
        ],
    )?)?;
    Db::write_rows(
        &mut db,
        releases_labels,
        InsertCommand::new(
            "release_label",
            "(release_id, label, catno, label_id)",
            &[Type::INT4, Type::TEXT, Type::TEXT, Type::INT4],
        )?,
    )?;
    Db::write_rows(
        &mut db,
        releases_videos,
        InsertCommand::new(
            "release_video",
            "(release_id, duration, src, title)",
            &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT],
        )?,
    )?;
    Ok(())
}

pub fn write_labels(db_opts: &DbOpt, labels: &HashMap<i32, Label>) -> Result<()> {
    let mut db = Db::connect(db_opts)?;
    Db::write_rows(
        &mut db,
        labels,
        InsertCommand::new(
            "label",
            "(id, name, contactinfo, profile, parent_label, sublabels, urls, data_quality)",
            &[
                Type::INT4,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT_ARRAY,
                Type::TEXT_ARRAY,
                Type::TEXT,
            ],
        )?,
    )?;
    Ok(())
}

pub fn write_artists(db_opts: &DbOpt, artists: &HashMap<i32, Artist>) -> Result<()> {
    let mut db = Db::connect(db_opts)?;
    Db::write_rows(
        &mut db,
        artists,
        InsertCommand::new(
            "artist",
            "(id, name, real_name, profile, data_quality, name_variations, urls, aliases, members)",
            &[
                Type::INT4,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT_ARRAY,
                Type::TEXT_ARRAY,
                Type::TEXT_ARRAY,
                Type::TEXT_ARRAY,
            ],
        )?,
    )?;
    Ok(())
}

pub fn write_masters(
    db_opts: &DbOpt,
    masters: &HashMap<i32, Master>,
    masters_artists: &HashMap<i32, MasterArtist>,
) -> Result<()> {
    let mut db = Db::connect(db_opts)?;
    Db::write_rows(
        &mut db,
        masters,
        InsertCommand::new(
            "master",
            "(id, title, release_id, year, notes, genres, styles, data_quality)",
            &[
                Type::INT4,
                Type::TEXT,
                Type::INT4,
                Type::INT4,
                Type::TEXT,
                Type::TEXT_ARRAY,
                Type::TEXT_ARRAY,
                Type::TEXT,
            ],
        )?,
    )?;
    Db::write_rows(
        &mut db,
        masters_artists,
        InsertCommand::new(
            "master_artist",
            "(artist_id, master_id, name, anv, role)",
            &[Type::INT4, Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT],
        )?,
    )?;
    Ok(())
}

struct Db {
    db_client: Client,
}

impl Db {
    pub fn connect(db_opts: &DbOpt) -> Result<Self> {
        let connection_string = format!(
            "host={} user={} password={} dbname={}",
            db_opts.db_host, db_opts.db_user, db_opts.db_password, db_opts.db_name
        );
        let client = Client::connect(&connection_string, NoTls)?;

        Ok(Db { db_client: client })
    }

    fn write_rows<T: SqlSerialization>(
        &mut self,
        data: &HashMap<i32, T>,
        insert_cmd: InsertCommand,
    ) -> Result<()> {
        insert_cmd.execute(&mut self.db_client, data)?;
        Ok(())
    }

    fn execute_file(&mut self, schema_path: &str) -> Result<()> {
        let tables_structure = fs::read_to_string(schema_path).unwrap();
        self.db_client.batch_execute(&tables_structure).unwrap();
        Ok(())
    }
}

struct InsertCommand<'a> {
    col_types: &'a [Type],
    copy_stm: String,
}

impl<'a> InsertCommand<'a> {
    fn new(table_name: &str, column_name: &str, col_types: &'a [Type]) -> Result<Self> {
        Ok(Self {
            col_types,
            copy_stm: get_copy_statement(table_name, column_name),
        })
    }

    fn execute<T>(&self, client: &mut Client, data: &HashMap<i32, T>) -> Result<()>
    where
        T: SqlSerialization,
    {
        let sink = client.copy_in(&self.copy_stm)?;
        let mut writer = BinaryCopyInWriter::new(sink, self.col_types);

        for values in data.values() {
            writer.write(&values.to_sql())?;
        }

        writer.finish()?;
        Ok(())
    }
}

fn get_copy_statement(table: &str, columns: &str) -> String {
    format!("COPY {} {} FROM STDIN BINARY", table, columns)
}
