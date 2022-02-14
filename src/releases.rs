use indicatif::ProgressBar;
use quick_xml::events::Event;
use std::{collections::HashMap, error::Error, str};

use crate::db::{write_release_labels, write_release_videos, write_releases, DbOpt};

// macro rule to dynamically get the names of a struct
macro_rules! get_struct_field_names {(
    // meta data about struct
    $(#[$meta:meta])*
    pub struct $name:ident {
        $(pub $fname:ident : $ftype:ty),
        *
    }) => {
        $(#[$meta])*
        pub struct $name {
            $(pub $fname : $ftype),
            *
        }

        impl $name {
            pub fn field_names() -> &'static [&'static str] {
                static NAMES: &'static [&'static str] = &[$(stringify!($fname)),*];
                NAMES
            }

            // pub fn field_count() -> usize {
            //     static COUNT: usize = [$(stringify!($fname)),*].len();
            //     COUNT
            // }
        }
    }
}

get_struct_field_names! {
    #[derive(Clone, Debug)]
    pub struct Release {
        pub status: String,
        pub title: String,
        pub country: String,
        pub released: String,
        pub notes: String,
        pub genres: Vec<String>,
        pub styles: Vec<String>,
        pub master_id: i32,
        pub data_quality: String
    }
}

get_struct_field_names! {
    #[derive(Clone, Debug)]
    pub struct ReleaseLabel {
        pub label: String,
        pub catno: String
    }
}

get_struct_field_names! {
    #[derive(Clone, Debug)]
    pub struct ReleaseVideo {
        pub duration: i32,
        pub src: String,
        pub title: String
    }
}

// use crate::db::write_batch;

impl Release {
    pub fn new() -> Self {
        Release {
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

impl ReleaseLabel {
    pub fn new() -> Self {
        ReleaseLabel {
            label: String::new(),
            catno: String::new(),
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
    current_release_label: ReleaseLabel,
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
            current_release_label: ReleaseLabel::new(),
            release_videos: HashMap::new(),
            pb: ProgressBar::new(14779645), // https://api.discogs.com/  - 14783275
            db_opts,
        }
    }

    pub fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
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
                            write_releases(self.db_opts, &self.releases)?;
                            write_release_labels(self.db_opts, &self.release_labels)?;
                            write_release_videos(self.db_opts, &self.release_videos)?;
                            self.releases = HashMap::new();
                            self.release_labels = HashMap::new();
                            self.release_videos = HashMap::new();
                        }
                        self.pb.inc(1);
                        ParserReadState::Release
                    }

                    Event::End(e) if e.local_name() == b"releases" => {
                        // write to db remainder of releases
                        write_releases(self.db_opts, &self.releases)?;
                        write_release_labels(self.db_opts, &self.release_labels)?;
                        write_release_videos(self.db_opts, &self.release_videos)?;
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
                    self.current_release_label.label = str::parse(str::from_utf8(
                        &e.attributes().next().unwrap()?.unescaped_value()?,
                    )?)?;
                    self.current_release_label.catno = str::parse(str::from_utf8(
                        &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                    )?)?;
                    let label_id = str::parse(str::from_utf8(
                        &e.attributes().nth(2).unwrap()?.unescaped_value()?,
                    )?)?;
                    self.release_labels
                        .entry(label_id)
                        .or_insert(self.current_release_label.clone());
                    ParserReadState::Labels
                }

                Event::End(e) if e.local_name() == b"labels" => ParserReadState::Release,

                _ => ParserReadState::Labels,
            },

            ParserReadState::Videos => match ev {
                Event::Start(e) if e.local_name() == b"video" => {
                    self.release_videos
                        .entry(self.current_id)
                        .or_insert(ReleaseVideo {
                            duration: str::parse(str::from_utf8(
                                &e.attributes().nth(1).unwrap()?.unescaped_value()?,
                            )?)?,
                            src: str::parse(str::from_utf8(
                                &e.attributes().next().unwrap()?.unescaped_value()?,
                            )?)?,
                            title: String::new(),
                        });
                    ParserReadState::Videos
                }

                Event::End(e) if e.local_name() == b"videos" => ParserReadState::Release,

                _ => ParserReadState::Videos,
            },
        };

        Ok(())
    }
}
