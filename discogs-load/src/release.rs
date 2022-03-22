use indicatif::ProgressBar;
use postgres::types::ToSql;
use quick_xml::events::Event;
use std::{collections::HashMap, error::Error, str};

use crate::db::{write_releases, DbOpt, SqlSerialization};
use crate::parser::Parser;

#[derive(Clone, Debug)]
pub struct Release {
    pub id: i32,
    pub status: String,
    pub title: String,
    pub country: String,
    pub released: String,
    pub notes: String,
    pub genres: Vec<String>,
    pub styles: Vec<String>,
    pub master_id: i32,
    pub data_quality: String,
}

impl SqlSerialization for Release {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> = vec![
            &self.id,
            &self.status,
            &self.title,
            &self.country,
            &self.released,
            &self.notes,
            &self.genres,
            &self.styles,
            &self.master_id,
            &self.data_quality,
        ];
        row
    }
}

#[derive(Clone, Debug)]
pub struct ReleaseLabel {
    pub release_id: i32,
    pub label: String,
    pub catno: String,
    pub label_id: i32,
}

impl SqlSerialization for ReleaseLabel {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> =
            vec![&self.release_id, &self.label, &self.catno, &self.label_id];
        row
    }
}

#[derive(Clone, Debug)]
pub struct ReleaseVideo {
    pub release_id: i32,
    pub duration: i32,
    pub src: String,
    pub title: String,
}

impl SqlSerialization for ReleaseVideo {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> =
            vec![&self.release_id, &self.duration, &self.src, &self.title];
        row
    }
}

impl Release {
    pub fn new() -> Self {
        Release {
            id: 0,
            status: String::new(),
            title: String::new(),
            country: String::new(),
            released: String::new(),
            notes: String::new(),
            genres: Vec::new(),
            styles: Vec::new(),
            master_id: 0,
            data_quality: String::new(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ParserReadState {
    // release
    Release,
    Title,
    Country,
    Released,
    Notes,
    Genres,
    Genre,
    Styles,
    Style,
    MasterId,
    DataQuality,
    // release_label
    Labels,
    // release_video
    Videos,
}

pub struct ReleasesParser<'a> {
    state: ParserReadState,
    releases: HashMap<i32, Release>,
    current_release: Release,
    current_id: i32,
    release_labels: HashMap<i32, ReleaseLabel>,
    current_video_id: i32,
    release_videos: HashMap<i32, ReleaseVideo>,
    pb: ProgressBar,
    db_opts: &'a DbOpt,
}

impl<'a> ReleasesParser<'a> {
    pub fn new(db_opts: &'a DbOpt) -> Self {
        ReleasesParser {
            state: ParserReadState::Release,
            releases: HashMap::new(),
            current_release: Release::new(),
            current_id: 0,
            release_labels: HashMap::new(),
            current_video_id: 0,
            release_videos: HashMap::new(),
            pb: ProgressBar::new(14976967), // https://api.discogs.com/
            db_opts,
        }
    }
}

impl<'a> Parser<'a> for ReleasesParser<'a> {
    fn new(&self, db_opts: &'a DbOpt) -> Self {
        ReleasesParser {
            state: ParserReadState::Release,
            releases: HashMap::new(),
            current_release: Release::new(),
            current_id: 0,
            release_labels: HashMap::new(),
            current_video_id: 0,
            release_videos: HashMap::new(),
            pb: ProgressBar::new(14976967), // https://api.discogs.com/
            db_opts,
        }
    }

    fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserReadState::Release => {
                match ev {
                    Event::Start(e) if e.local_name() == b"release" => {
                        self.current_release.status = str::parse(str::from_utf8(
                            &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                        )?)?;
                        self.current_id = str::parse(str::from_utf8(
                            &e.attributes().next().unwrap()?.unescaped_value()?,
                        )?)?;
                        self.current_release.id = self.current_id;
                        self.current_release.genres = Vec::new();
                        self.current_release.styles = Vec::new();
                        ParserReadState::Release
                    }

                    Event::Start(e) => match e.local_name() {
                        b"title" => ParserReadState::Title,
                        b"country" => ParserReadState::Country,
                        b"released" => ParserReadState::Released,
                        b"notes" => ParserReadState::Notes,
                        b"genres" => ParserReadState::Genres,
                        b"styles" => ParserReadState::Styles,
                        b"master_id" => ParserReadState::MasterId,
                        b"data_quality" => ParserReadState::DataQuality,
                        b"labels" => ParserReadState::Labels,
                        b"videos" => ParserReadState::Videos,
                        _ => ParserReadState::Release,
                    },

                    Event::End(e) if e.local_name() == b"release" => {
                        self.releases
                            .entry(self.current_id)
                            .or_insert(self.current_release.clone());
                        if self.releases.len() >= self.db_opts.batch_size {
                            // write to db every 1000 records and clean the hashmaps
                            // use drain? https://doc.rust-lang.org/std/collections/struct.HashMap.html#examples-13
                            write_releases(
                                self.db_opts,
                                &self.releases,
                                &self.release_labels,
                                &self.release_videos,
                            )?;
                            self.releases = HashMap::new();
                            self.release_labels = HashMap::new();
                            self.release_videos = HashMap::new();
                        }
                        self.pb.inc(1);
                        ParserReadState::Release
                    }

                    Event::End(e) if e.local_name() == b"releases" => {
                        // write to db remainder of releases
                        write_releases(
                            self.db_opts,
                            &self.releases,
                            &self.release_labels,
                            &self.release_videos,
                        )?;
                        ParserReadState::Release
                    }

                    _ => ParserReadState::Release,
                }
            }

            ParserReadState::Title => match ev {
                Event::Text(e) => {
                    self.current_release.title = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Title
                }

                Event::End(e) if e.local_name() == b"title" => ParserReadState::Release,

                _ => ParserReadState::Title,
            },

            ParserReadState::Country => match ev {
                Event::Text(e) => {
                    self.current_release.country = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Country
                }

                Event::End(e) if e.local_name() == b"country" => ParserReadState::Release,

                _ => ParserReadState::Country,
            },

            ParserReadState::Released => match ev {
                Event::Text(e) => {
                    self.current_release.released = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Released
                }

                Event::End(e) if e.local_name() == b"released" => ParserReadState::Release,

                _ => ParserReadState::Released,
            },

            ParserReadState::Notes => match ev {
                Event::Text(e) => {
                    self.current_release.notes = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::Notes
                }

                Event::End(e) if e.local_name() == b"notes" => ParserReadState::Release,

                _ => ParserReadState::Notes,
            },

            ParserReadState::Genres => match ev {
                Event::Start(e) if e.local_name() == b"genre" => ParserReadState::Genre,

                Event::End(e) if e.local_name() == b"genres" => ParserReadState::Release,

                _ => ParserReadState::Genres,
            },

            ParserReadState::Genre => match ev {
                Event::Text(e) => {
                    self.current_release
                        .genres
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserReadState::Genres
                }

                _ => ParserReadState::Genres,
            },

            ParserReadState::Styles => match ev {
                Event::Start(e) if e.local_name() == b"style" => ParserReadState::Style,

                Event::End(e) if e.local_name() == b"styles" => ParserReadState::Release,

                _ => ParserReadState::Styles,
            },

            ParserReadState::Style => match ev {
                Event::Text(e) => {
                    self.current_release
                        .styles
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserReadState::Styles
                }

                _ => ParserReadState::Styles,
            },

            ParserReadState::MasterId => match ev {
                Event::Text(e) => {
                    self.current_release.master_id = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::MasterId
                }

                Event::End(e) if e.local_name() == b"master_id" => ParserReadState::Release,

                _ => ParserReadState::MasterId,
            },

            ParserReadState::DataQuality => match ev {
                Event::Text(e) => {
                    self.current_release.data_quality =
                        str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserReadState::DataQuality
                }

                Event::End(e) if e.local_name() == b"data_quality" => ParserReadState::Release,

                _ => ParserReadState::DataQuality,
            },

            ParserReadState::Labels => match ev {
                Event::Empty(e) => {
                    let label_id = str::parse(str::from_utf8(
                        &e.attributes().nth(2).unwrap()?.unescaped_value()?,
                    )?)?;
                    self.release_labels.entry(label_id).or_insert(ReleaseLabel {
                        release_id: self.current_release.id,
                        label: str::parse(str::from_utf8(
                            &e.attributes().next().unwrap()?.unescaped_value()?,
                        )?)?,
                        catno: str::parse(str::from_utf8(
                            &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                        )?)?,
                        label_id: str::parse(str::from_utf8(
                            &e.attributes().nth(2).unwrap()?.unescaped_value()?,
                        )?)?,
                    });
                    ParserReadState::Labels
                }

                Event::End(e) if e.local_name() == b"labels" => ParserReadState::Release,

                _ => ParserReadState::Labels,
            },

            ParserReadState::Videos => match ev {
                Event::Start(e) if e.local_name() == b"video" => {
                    self.release_videos
                        .entry(self.current_video_id)
                        .or_insert(ReleaseVideo {
                            release_id: self.current_release.id,
                            duration: str::parse(str::from_utf8(
                                &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                            )?)?,
                            src: str::parse(str::from_utf8(
                                &e.attributes().next().unwrap()?.unescaped_value()?,
                            )?)?,
                            title: String::new(),
                        });
                    self.current_video_id += 1;
                    ParserReadState::Videos
                }

                Event::End(e) if e.local_name() == b"videos" => ParserReadState::Release,

                _ => ParserReadState::Videos,
            },
        };

        Ok(())
    }
}
