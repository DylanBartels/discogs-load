use anyhow::Result;
use log::info;
use postgres::{binary_copy::BinaryCopyInWriter, types::Type, Client, NoTls};
use std::{collections::HashMap, fs};
use structopt::StructOpt;

use crate::release::{Release, ReleaseLabel, ReleaseVideo, SqlSerialization};

#[derive(Debug, Clone, StructOpt)]
pub struct DbOpt {
    /// Number of rows per insert
    #[structopt(long = "batch-size", default_value = "1000")]
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

/// Initialize schema and close connection.
pub fn init(db_opts: &DbOpt) -> Result<()> {
    let db = Db::connect(db_opts);
    Db::create_schema(&mut db?)?;
    Ok(())
}

// /// Initialize indexes and close connection.
// pub fn indexes(opts: &DbOpt) -> Result<()> {
//     let db = Db::connect(opts);
//     Db::create_indexes(&mut db?)?;
//     Ok(())
// }

/// Write the batch size to db
pub fn write_releases(
    db_opts: &DbOpt,
    releases: &HashMap<i32, Release>,
    releases_labels: &HashMap<i32, ReleaseLabel>,
    releases_videos: &HashMap<i32, ReleaseVideo>,
) -> Result<()> {
    let mut db = Db::connect(db_opts)?;
    Db::write_release_rows(&mut db, releases)?;
    Db::write_release_labels_rows(&mut db, releases_labels)?;
    Db::write_release_videos_rows(&mut db, releases_videos)?;
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

    fn write_release_rows(&mut self, data: &HashMap<i32, Release>) -> Result<()> {
        let insert = InsertCommand::new(
            "release",
            "(status, title, country, released, notes, genres, styles, master_id, data_quality)",
        )?;
        insert.execute(
            &mut self.db_client,
            data,
            &[
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
        )?;
        Ok(())
    }

    fn write_release_labels_rows(&mut self, data: &HashMap<i32, ReleaseLabel>) -> Result<()> {
        let insert = InsertCommand::new("release_label", "(label, catno)")?;
        insert.execute(&mut self.db_client, data, &[Type::TEXT, Type::TEXT])?;
        Ok(())
    }

    fn write_release_videos_rows(&mut self, data: &HashMap<i32, ReleaseVideo>) -> Result<()> {
        let insert = InsertCommand::new("release_video", "(duration, src, title)")?;
        insert.execute(
            &mut self.db_client,
            data,
            &[Type::INT4, Type::TEXT, Type::TEXT],
        )?;
        Ok(())
    }

    fn create_schema(&mut self) -> Result<()> {
        info!("Creating the tables.");
        let tables_structure = fs::read_to_string("sql/tables/release.sql").unwrap();
        self.db_client.batch_execute(&tables_structure).unwrap();
        Ok(())
    }

    // fn create_indexes(&mut self) -> Result<()> {
    //     info!("Creating the indexes.");
    //     let tables_structure = fs::read_to_string("sql/indexes/release.sql").unwrap();
    //     self.db_client.batch_execute(&tables_structure).unwrap();
    //     Ok(())
    // }
}

struct InsertCommand {
    // Todo: get type from db?
    // https://github.com/sfackler/rust-postgres/issues/862#issuecomment-1014894748
    // col_types: String,
    copy_stm: String,
}

impl InsertCommand {
    fn new(table_name: &str, column_name: &str) -> Result<Self> {
        Ok(Self {
            // col_types: get_col_types(),
            copy_stm: get_copy_statement(table_name, column_name),
        })
    }

    fn execute<T>(&self, client: &mut Client, data: &HashMap<i32, T>, types: &[Type]) -> Result<()>
    where
        T: SqlSerialization,
    {
        let sink = client.copy_in(&self.copy_stm)?;
        let mut writer = BinaryCopyInWriter::new(sink, types);

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
