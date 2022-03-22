use indicatif::ProgressBar;
use postgres::types::ToSql;
use quick_xml::events::Event;
use std::{collections::HashMap, error::Error, str};

use crate::db::{write_masters, DbOpt, SqlSerialization};
use crate::parser::Parser;

#[derive(Clone, Debug)]
pub struct Master {
    pub id: i32,
    pub title: String,
    pub release_id: i32,
    pub year: i32,
    pub notes: String,
    pub genres: Vec<String>,
    pub styles: Vec<String>,
    pub data_quality: String,
}

impl SqlSerialization for Master {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> = vec![
            &self.id,
            &self.title,
            &self.release_id,
            &self.year,
            &self.notes,
            &self.genres,
            &self.styles,
            &self.data_quality,
        ];
        row
    }
}

impl Master {
    pub fn new() -> Self {
        Master {
            id: 0,
            title: String::new(),
            release_id: 0,
            year: 0,
            notes: String::new(),
            genres: Vec::new(),
            styles: Vec::new(),
            data_quality: String::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MasterArtist {
    pub id: i32,
    pub master_id: i32,
    pub name: String,
    pub anv: String,
    pub role: String,
}

impl SqlSerialization for MasterArtist {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> =
            vec![&self.id, &self.master_id, &self.name, &self.anv, &self.role];
        row
    }
}

impl MasterArtist {
    pub fn new() -> Self {
        MasterArtist {
            id: 0,
            master_id: 0,
            name: String::new(),
            anv: String::new(),
            role: String::new(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ParserReadState {
    // master
    Master,
    MainRelease,
    Artists,
    Title,
    DataQuality,
    // master_artists
    ArtistId,
    ArtistName,
    ArtistAnv,
    ArtistRole,
}

pub struct MastersParser<'a> {
    state: ParserReadState,
    masters: HashMap<i32, Master>,
    current_master: Master,
    current_artist: MasterArtist,
    current_master_id: i32,
    master_artists: HashMap<i32, MasterArtist>,
    pb: ProgressBar,
    db_opts: &'a DbOpt,
}

impl<'a> MastersParser<'a> {
    pub fn new(db_opts: &'a DbOpt) -> Self {
        MastersParser {
            state: ParserReadState::Master,
            masters: HashMap::new(),
            current_master: Master::new(),
            current_artist: MasterArtist::new(),
            current_master_id: 0,
            master_artists: HashMap::new(),
            pb: ProgressBar::new(1821993),
            db_opts,
        }
    }
}

impl<'a> Parser<'a> for MastersParser<'a> {
    fn new(&self, db_opts: &'a DbOpt) -> Self {
        MastersParser {
            state: ParserReadState::Master,
            masters: HashMap::new(),
            current_master: Master::new(),
            current_artist: MasterArtist::new(),
            current_master_id: 0,
            master_artists: HashMap::new(),
            pb: ProgressBar::new(1821993),
            db_opts,
        }
    }
    fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserReadState::Master => {
                match ev {
                    Event::Start(e) if e.local_name() == b"master" => {
                        self.current_master.genres = Vec::new();
                        self.current_master.styles = Vec::new();
                        self.current_master.id = str::parse(str::from_utf8(
                            &e.attributes().next().unwrap()?.unescaped_value()?,
                        )?)?;
                        ParserReadState::Master
                    }

                    Event::Start(e) => match e.local_name() {
                        b"main_release" => ParserReadState::MainRelease,
                        b"title" => ParserReadState::Title,
                        b"artists" => ParserReadState::Artists,
                        b"data_quality" => ParserReadState::DataQuality,
                        _ => ParserReadState::Master,
                    },

                    Event::End(e) if e.local_name() == b"master" => {
                        self.masters
                            .entry(self.current_master.id)
                            .or_insert(self.current_master.clone());
                        if self.masters.len() >= self.db_opts.batch_size {
                            write_masters(self.db_opts, &self.masters, &self.master_artists)?;
                            self.masters = HashMap::new();
                            self.master_artists = HashMap::new();
                        }
                        self.pb.inc(1);
                        ParserReadState::Master
                    }

                    Event::End(e) if e.local_name() == b"masters" => {
                        // write to db remainder of masters
                        write_masters(self.db_opts, &self.masters, &self.master_artists)?;
                        ParserReadState::Master
                    }

                    _ => ParserReadState::Master,
                }
            }

            ParserReadState::MainRelease => match ev {
                Event::Text(e) => {
                    self.current_master.release_id = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::MainRelease
                }

                Event::End(e) if e.local_name() == b"main_release" => ParserReadState::Master,

                _ => ParserReadState::MainRelease,
            },

            ParserReadState::Artists => match ev {
                Event::Start(e) => match e.local_name() {
                    b"artist" => {
                        self.current_artist = MasterArtist::new();
                        self.current_artist.master_id = self.current_master.id;
                        ParserReadState::Artists
                    }
                    b"id" => ParserReadState::ArtistId,
                    b"name" => ParserReadState::ArtistName,
                    b"anv" => ParserReadState::ArtistAnv,
                    b"role" => ParserReadState::ArtistRole,
                    _ => ParserReadState::Artists,
                },

                Event::End(e) => match e.local_name() {
                    b"artist" => {
                        self.master_artists
                            .entry(self.current_master_id)
                            .or_insert(self.current_artist.clone());
                        self.current_master_id += 1;
                        ParserReadState::Artists
                    }
                    b"artists" => ParserReadState::Master,
                    _ => ParserReadState::Artists,
                },

                _ => ParserReadState::Artists,
            },

            ParserReadState::ArtistId => match ev {
                Event::Text(e) => {
                    self.current_artist.id = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Artists
                }

                Event::End(e) if e.local_name() == b"id" => ParserReadState::Artists,

                _ => ParserReadState::ArtistId,
            },

            ParserReadState::ArtistName => match ev {
                Event::Text(e) => {
                    self.current_artist.name = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Artists
                }

                Event::End(e) if e.local_name() == b"name" => ParserReadState::Artists,

                _ => ParserReadState::ArtistName,
            },

            ParserReadState::ArtistAnv => match ev {
                Event::Text(e) => {
                    self.current_artist.anv = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Artists
                }

                Event::End(e) if e.local_name() == b"anv" => ParserReadState::Artists,

                _ => ParserReadState::ArtistAnv,
            },

            ParserReadState::ArtistRole => match ev {
                Event::Text(e) => {
                    self.current_artist.role = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Artists
                }

                Event::End(e) if e.local_name() == b"role" => ParserReadState::Artists,

                _ => ParserReadState::ArtistRole,
            },

            ParserReadState::Title => match ev {
                Event::Text(e) => {
                    self.current_master.title = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Title
                }

                Event::End(e) if e.local_name() == b"title" => ParserReadState::Master,

                _ => ParserReadState::Title,
            },

            ParserReadState::DataQuality => match ev {
                Event::Text(e) => {
                    self.current_master.data_quality =
                        str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::DataQuality
                }

                Event::End(e) if e.local_name() == b"data_quality" => ParserReadState::Master,

                _ => ParserReadState::DataQuality,
            },
        };

        Ok(())
    }
}
