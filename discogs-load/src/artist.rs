use indicatif::ProgressBar;
use postgres::types::ToSql;
use quick_xml::events::Event;
use std::{collections::HashMap, error::Error, str};

use crate::db::{write_artists, DbOpt, SqlSerialization};
use crate::parser::Parser;

#[derive(Clone, Debug)]
pub struct Artist {
    pub id: i32,
    pub name: String,
    pub real_name: String,
    pub profile: String,
    pub data_quality: String,
    pub name_variations: Vec<String>,
    pub urls: Vec<String>,
    pub aliases: Vec<String>,
    pub members: Vec<String>,
}

impl SqlSerialization for Artist {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> = vec![
            &self.id,
            &self.name,
            &self.real_name,
            &self.profile,
            &self.data_quality,
            &self.name_variations,
            &self.urls,
            &self.aliases,
            &self.members,
        ];
        row
    }
}

impl Artist {
    pub fn new() -> Self {
        Artist {
            id: 0,
            name: String::new(),
            real_name: String::new(),
            profile: String::new(),
            data_quality: String::new(),
            name_variations: Vec::new(),
            urls: Vec::new(),
            aliases: Vec::new(),
            members: Vec::new(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ParserState {
    Artist,
    Id,
    Name,
    RealName,
    Profile,
    DataQuality,
    NameVariations,
    Url,
    Urls,
    Alias,
    Aliases,
    Member,
    Members,
}

pub struct ArtistsParser<'a> {
    state: ParserState,
    artists: HashMap<i32, Artist>,
    current_artist: Artist,
    pb: ProgressBar,
    db_opts: &'a DbOpt,
}

impl<'a> ArtistsParser<'a> {
    pub fn new(db_opts: &'a DbOpt) -> Self {
        ArtistsParser {
            state: ParserState::Artist,
            artists: HashMap::new(),
            current_artist: Artist::new(),
            pb: ProgressBar::new(7993954),
            db_opts,
        }
    }
}

impl<'a> Parser<'a> for ArtistsParser<'a> {
    fn new(&self, db_opts: &'a DbOpt) -> Self {
        ArtistsParser {
            state: ParserState::Artist,
            artists: HashMap::new(),
            current_artist: Artist::new(),
            pb: ProgressBar::new(7993954),
            db_opts,
        }
    }
    fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserState::Artist => {
                match ev {
                    Event::Start(e) if e.local_name() == b"artist" => {
                        self.current_artist.name_variations = Vec::new();
                        self.current_artist.urls = Vec::new();
                        self.current_artist.aliases = Vec::new();
                        self.current_artist.members = Vec::new();
                        ParserState::Artist
                    }

                    Event::Start(e) => match e.local_name() {
                        b"id" => ParserState::Id,
                        b"name" => ParserState::Name,
                        b"realname" => ParserState::RealName,
                        b"profile" => ParserState::Profile,
                        b"data_quality" => ParserState::DataQuality,
                        b"urls" => ParserState::Urls,
                        b"namevariations" => ParserState::NameVariations,
                        b"aliases" => ParserState::Aliases,
                        b"members" => ParserState::Members,
                        _ => ParserState::Artist,
                    },

                    Event::End(e) if e.local_name() == b"artist" => {
                        self.artists
                            .entry(self.current_artist.id)
                            .or_insert(self.current_artist.clone());
                        if self.artists.len() >= self.db_opts.batch_size {
                            // use drain? https://doc.rust-lang.org/std/collections/struct.HashMap.html#examples-13
                            write_artists(self.db_opts, &self.artists)?;
                            self.artists = HashMap::new();
                        }
                        self.pb.inc(1);
                        ParserState::Artist
                    }

                    Event::End(e) if e.local_name() == b"artists" => {
                        // write to db remainder of artists
                        write_artists(self.db_opts, &self.artists)?;
                        ParserState::Artist
                    }

                    _ => ParserState::Artist,
                }
            }

            ParserState::Id => match ev {
                Event::Text(e) => {
                    self.current_artist.id = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::Id
                }

                Event::End(e) if e.local_name() == b"id" => ParserState::Artist,

                _ => ParserState::Id,
            },

            ParserState::Name => match ev {
                Event::Text(e) => {
                    self.current_artist.name = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::Name
                }

                Event::End(e) if e.local_name() == b"name" => ParserState::Artist,

                _ => ParserState::Name,
            },

            ParserState::RealName => match ev {
                Event::Text(e) => {
                    self.current_artist.real_name = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::RealName
                }

                Event::End(e) if e.local_name() == b"realname" => ParserState::Artist,

                _ => ParserState::RealName,
            },

            ParserState::Profile => match ev {
                Event::Text(e) => {
                    self.current_artist.profile = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::Profile
                }

                Event::End(e) if e.local_name() == b"profile" => ParserState::Artist,

                _ => ParserState::Profile,
            },

            ParserState::DataQuality => match ev {
                Event::Text(e) => {
                    self.current_artist.data_quality =
                        str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::DataQuality
                }

                Event::End(e) if e.local_name() == b"data_quality" => ParserState::Artist,

                _ => ParserState::DataQuality,
            },

            ParserState::Urls => match ev {
                Event::Start(e) if e.local_name() == b"url" => ParserState::Url,

                Event::End(e) if e.local_name() == b"urls" => ParserState::Artist,

                _ => ParserState::Artist,
            },

            ParserState::Url => match ev {
                Event::Text(e) => {
                    self.current_artist
                        .urls
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserState::Urls
                }

                _ => ParserState::Urls,
            },

            ParserState::Aliases => match ev {
                Event::Start(e) if e.local_name() == b"alias" => ParserState::Alias,

                Event::End(e) if e.local_name() == b"aliases" => ParserState::Artist,

                _ => ParserState::Artist,
            },

            ParserState::Alias => match ev {
                Event::Text(e) => {
                    self.current_artist
                        .members
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserState::Aliases
                }

                _ => ParserState::Aliases,
            },

            ParserState::Members => match ev {
                Event::Start(e) if e.local_name() == b"member" => ParserState::Member,

                Event::End(e) if e.local_name() == b"members" => ParserState::Artist,

                _ => ParserState::Artist,
            },

            ParserState::Member => match ev {
                Event::Text(e) => {
                    self.current_artist
                        .members
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserState::Members
                }

                _ => ParserState::Members,
            },

            _ => ParserState::Members,
        };

        Ok(())
    }
}
