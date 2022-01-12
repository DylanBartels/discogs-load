use std::{
    collections::HashMap,
    error::Error,
    str
};
use indicatif::ProgressBar;
use quick_xml::{
    events::Event,
};

use crate::db::{write_releases, write_release_labels, write_release_videos};

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
            data_quality: String::new()
        }
    }
}

impl ReleaseLabel {
    pub fn new() -> Self {
        ReleaseLabel {
            label: String::new(),
            catno: String::new()
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ParserState {
    // release
    ReadRelease,
    ReadTitle,
    ReadCountry,
    ReadReleased,
    ReadNotes,
    ReadGenres,
    ReadGenre,
    ReadStyles,
    ReadStyle,
    ReadMasterId,
    ReadDataQuality,
    // release_label
    ReadLabels,
    // release_video
    ReadVideos,
}

pub struct ReleasesParser {
    state: ParserState,
    releases: HashMap<i32, Release>,
    current_release: Release,
    current_id: i32,
    release_labels: HashMap<i32, ReleaseLabel>,
    current_release_label: ReleaseLabel,
    release_videos: HashMap<i32, ReleaseVideo>,
    pb: ProgressBar
}

impl ReleasesParser {
    pub fn new() -> Self {
        ReleasesParser {
            state: ParserState::ReadRelease,
            releases: HashMap::new(),
            current_release: Release::new(),
            current_id: 0,
            release_labels: HashMap::new(),
            current_release_label: ReleaseLabel::new(),
            release_videos: HashMap::new(),
            pb: ProgressBar::new(14779645)
            // https://api.discogs.com/
            // releases: 14783275
        }
    }

    pub fn with_predicate() -> Self {
        let parser = ReleasesParser::new();
        parser
    }

    pub fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserState::ReadRelease => {
                match ev {
                    Event::Start(e) if e.local_name() == b"release" => {
                        self.current_release.status = str::parse(
                            str::from_utf8(&e.attributes().nth(1).unwrap()?.unescaped_value()?)?
                        )?;
                        self.current_id = str::parse(
                            str::from_utf8(&e.attributes().nth(0).unwrap()?.unescaped_value()?)?
                        )?;
                        self.current_release.genres = Vec::new();
                        self.current_release.styles = Vec::new();
                        ParserState::ReadRelease
                    },

                    Event::Start(e) => match e.local_name() {
                        b"title" => ParserState::ReadTitle,
                        b"country" => ParserState::ReadCountry,
                        b"released" => ParserState::ReadReleased,
                        b"notes" => ParserState::ReadNotes,
                        b"genres" => ParserState::ReadGenres,
                        b"styles" => ParserState::ReadStyles,
                        b"master_id" => ParserState::ReadMasterId,
                        b"data_quality" => ParserState::ReadDataQuality,
                        b"labels" => ParserState::ReadLabels,
                        b"videos" => ParserState::ReadVideos,
                        _ => ParserState::ReadRelease,
                    },

                    Event::End(e) if e.local_name() == b"release" => {
                        self.releases.entry(self.current_id).or_insert(self.current_release.clone());
                        if self.releases.len() > 999 {
                            // write to db every 1000 records and clean the hashmaps
                            // use drain? https://doc.rust-lang.org/std/collections/struct.HashMap.html#examples-13
                            write_releases(&self.releases)?;
                            write_release_labels(&self.release_labels)?;
                            write_release_videos(&self.release_videos)?;
                            self.releases = HashMap::new();
                            self.release_labels = HashMap::new();
                            self.release_videos = HashMap::new();
                        }
                        self.pb.inc(1);
                        ParserState::ReadRelease
                    }

                    Event::End(e) if e.local_name() == b"releases" => {
                        // write to db remainder of releases
                        write_releases(&self.releases)?;
                        write_release_labels(&self.release_labels)?;
                        write_release_videos(&self.release_videos)?;
                        ParserState::ReadRelease
                    }

                    _ => ParserState::ReadRelease,
                }
            },

            ParserState::ReadTitle => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.title = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadTitle
                    },

                    Event::End(e) if e.local_name() == b"title" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadTitle,
                }
            },

            ParserState::ReadCountry => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.country = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadCountry
                    },

                    Event::End(e) if e.local_name() == b"country" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadCountry,
                }
            },

            ParserState::ReadReleased => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.released = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadReleased
                    },

                    Event::End(e) if e.local_name() == b"released" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadReleased,
                }
            },

            ParserState::ReadNotes => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.notes = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadNotes
                    },

                    Event::End(e) if e.local_name() == b"notes" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadNotes,
                }
            },

            ParserState::ReadGenres => {
                match ev {
                    Event::Start(e) if e.local_name() == b"genre" =>
                        ParserState::ReadGenre,

                    Event::End(e) if e.local_name() == b"genres" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadGenres,
                }
            },

            ParserState::ReadGenre => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.genres.extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                        ParserState::ReadGenres
                    },

                    _ => ParserState::ReadGenres,
                }
            },

            ParserState::ReadStyles => {
                match ev {
                    Event::Start(e) if e.local_name() == b"style" =>
                        ParserState::ReadStyle,

                    Event::End(e) if e.local_name() == b"styles" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadStyles,
                }
            },

            ParserState::ReadStyle => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.styles.extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                        ParserState::ReadStyles
                    },

                    _ => ParserState::ReadStyles,
                }
            },

            ParserState::ReadMasterId => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.master_id = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadMasterId
                    },

                    Event::End(e) if e.local_name() == b"master_id" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadMasterId,
                }
            },

            ParserState::ReadDataQuality => {
                match ev {
                    Event::Text(e) => {
                        self.current_release.data_quality = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadDataQuality
                    },

                    Event::End(e) if e.local_name() == b"data_quality" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadDataQuality,
                }
            },

            ParserState::ReadLabels => {
                match ev {
                    Event::Empty(e) => {
                        self.current_release_label.label = str::parse(
                            str::from_utf8(&e.attributes().nth(0).unwrap()?.unescaped_value()?)?
                        )?;
                        self.current_release_label.catno = str::parse(
                            str::from_utf8(&e.attributes().nth(1).unwrap()?.unescaped_value()?)?
                        )?;
                        let label_id = str::parse(
                            str::from_utf8(&e.attributes().nth(2).unwrap()?.unescaped_value()?)?
                        )?;
                        self.release_labels.entry(label_id).or_insert(self.current_release_label.clone());
                        ParserState::ReadLabels
                    }

                    Event::End(e) if e.local_name() == b"labels" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadLabels,
                }
            },

            ParserState::ReadVideos => {
                match ev {
                    Event::Start(e) if e.local_name() == b"video" => {
                        self.release_videos.entry(self.current_id).or_insert(ReleaseVideo {
                            duration: str::parse(
                                str::from_utf8(&e.attributes().nth(1).unwrap()?.unescaped_value()?)?
                            )?,
                            src: str::parse(
                                str::from_utf8(&e.attributes().nth(0).unwrap()?.unescaped_value()?)?
                            )?,
                            title: String::new()
                        });
                        ParserState::ReadVideos
                    }

                    Event::End(e) if e.local_name() == b"videos" =>
                        ParserState::ReadRelease,

                    _ => ParserState::ReadVideos,
                }
            }
        };

        Ok(())
    }
}
