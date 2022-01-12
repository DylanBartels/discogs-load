use std::{
    collections::HashMap,
    fs
};
use log::info;
use anyhow::Result;
use structopt::StructOpt;
use postgres::{Client, NoTls, types::ToSql};

use crate::releases::{Release, ReleaseLabel, ReleaseVideo};

#[derive(Debug, Clone, StructOpt)]
pub struct DbOpt {
    // TODO: make these db config variables work

    // /// Number of rows per insert
    // #[structopt(long = "batch-size", default_value = "1000")]
    // pub batch_size: usize,
    // /// Database host
    // #[structopt(long = "db-host", default_value = "localhost")]
    // pub db_host: String,
    // /// Database user
    // #[structopt(long = "db-user", default_value = "dev")]
    // pub db_user: String,
    // /// Database password
    // #[structopt(long = "db-password", default_value = "dev_pass")]
    // pub db_password: String,
    // /// Database name
    // #[structopt(long = "db-name", default_value = "discogs")]
    // pub db_name: String,
}

/// Initialize schema and close connection.
pub fn init(opts: &DbOpt) -> Result<()> {
    let db = Db::connect(opts);
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
pub fn write_releases(data: &HashMap<i32, Release>) -> Result<()> {
    Db::write_release_rows(&data)?;
    Ok(())
}

pub fn write_release_labels(data: &HashMap<i32, ReleaseLabel>) -> Result<()> {
    Db::write_release_labels_rows(&data)?;
    Ok(())
}

pub fn write_release_videos(data: &HashMap<i32, ReleaseVideo>) -> Result<()> {
    Db::write_release_videos_rows(&data)?;
    Ok(())
}

struct Db {
    db_client: Client,
    // batch_size: usize,
}

impl Db {
    pub fn connect(_opts: &DbOpt) -> Result<Self> {
        let connection_string = "host=localhost user=dev password=dev_pass dbname=discogs";
        let client = Client::connect(connection_string, NoTls).unwrap();

        Ok(Db {
            db_client: client,
            // batch_size: opts.batch_size
        })
    }

    fn write_release_rows(data: &HashMap<i32, Release>) -> Result<()> {
        let query_begin = "INSERT INTO release (id, ";
        let fields = &Release::field_names().join(", ");
        let query_end = ") VALUES ";
        let query = [query_begin, fields, query_end].join("");

        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();

        for (key, value) in &*data {
            params_prep.push(Box::new(key));
            params_prep.push(Box::new(&value.status));
            params_prep.push(Box::new(&value.title));
            params_prep.push(Box::new(&value.country));
            params_prep.push(Box::new(&value.released));
            params_prep.push(Box::new(&value.notes));
            params_prep.push(Box::new(&value.genres));
            params_prep.push(Box::new(&value.styles));
            params_prep.push(Box::new(&value.master_id));
            params_prep.push(Box::new(&value.data_quality));
        }

        let params: Vec<&(dyn ToSql + Sync)> = params_prep
            .iter()
            .map(|x| x.as_ref() as &(dyn ToSql + Sync))
            .collect();
        
        let columns = (1..=data.len()*10)
            .step_by(10)
            .map(|c| format!("(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})", c, c+1, c+2, c+3, c+4, c+5, c+6, c+7, c+8, c+9))
            .collect::<Vec<_>>()
            .join(", ");
    
            Client::connect("host=localhost user=dev password=dev_pass dbname=discogs", NoTls).unwrap().execute(
            &format!("{}{}", query, columns),
            &params,
        ).unwrap();

        Ok(())
    }

    fn write_release_labels_rows(data: &HashMap<i32, ReleaseLabel>) -> Result<()> {
        let query_begin = "INSERT INTO release_label (id, ";
        let fields = &ReleaseLabel::field_names().join(", ");
        let query_end = ") VALUES ";
        let query = [query_begin, fields, query_end].join("");

        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();

        for (key, value) in &*data {
            params_prep.push(Box::new(key));
            params_prep.push(Box::new(&value.label));
            params_prep.push(Box::new(&value.catno));
        }

        let params: Vec<&(dyn ToSql + Sync)> = params_prep
            .iter()
            .map(|x| x.as_ref() as &(dyn ToSql + Sync))
            .collect();
        
        let columns = (1..=data.len()*3)
            .step_by(3)
            .map(|c| format!("(${}, ${}, ${})", c, c+1, c+2))
            .collect::<Vec<_>>()
            .join(", ");
    
            Client::connect("host=localhost user=dev password=dev_pass dbname=discogs", NoTls).unwrap().execute(
            &format!("{}{}", query, columns),
            &params,
        ).unwrap();

        Ok(())
    }

    fn write_release_videos_rows(data: &HashMap<i32, ReleaseVideo>) -> Result<()> {
        let query_begin = "INSERT INTO release_video (id, ";
        let fields = &ReleaseVideo::field_names().join(", ");
        let query_end = ") VALUES ";
        let query = [query_begin, fields, query_end].join("");

        let mut params_prep: Vec<Box<dyn ToSql + Sync>> = Vec::new();

        for (key, value) in &*data {
            params_prep.push(Box::new(key));
            params_prep.push(Box::new(&value.duration));
            params_prep.push(Box::new(&value.src));
            params_prep.push(Box::new(&value.title));
        }

        let params: Vec<&(dyn ToSql + Sync)> = params_prep
            .iter()
            .map(|x| x.as_ref() as &(dyn ToSql + Sync))
            .collect();
        
        let columns = (1..=data.len()*4)
            .step_by(4)
            .map(|c| format!("(${}, ${}, ${}, ${})", c, c+1, c+2, c+3))
            .collect::<Vec<_>>()
            .join(", ");
    
            Client::connect("host=localhost user=dev password=dev_pass dbname=discogs", NoTls).unwrap().execute(
            &format!("{}{}", query, columns),
            &params,
        ).unwrap();

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
