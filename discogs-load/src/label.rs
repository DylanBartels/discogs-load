use indicatif::ProgressBar;
use postgres::types::ToSql;
use quick_xml::events::Event;
use std::{collections::HashMap, error::Error, str};

use crate::db::{write_labels, DbOpt, SqlSerialization};
use crate::parser::Parser;

#[derive(Clone, Debug)]
pub struct Label {
    pub id: i32,
    pub name: String,
    pub contactinfo: String,
    pub profile: String,
    pub parent_label: String,
    pub sublabels: Vec<String>,
    pub urls: Vec<String>,
    pub data_quality: String,
}

impl SqlSerialization for Label {
    fn to_sql(&self) -> Vec<&'_ (dyn ToSql + Sync)> {
        let row: Vec<&'_ (dyn ToSql + Sync)> = vec![
            &self.id,
            &self.name,
            &self.contactinfo,
            &self.profile,
            &self.parent_label,
            &self.sublabels,
            &self.urls,
            &self.data_quality,
        ];
        row
    }
}

impl Label {
    pub fn new() -> Self {
        Label {
            id: 0,
            name: String::new(),
            contactinfo: String::new(),
            profile: String::new(),
            parent_label: String::new(),
            sublabels: Vec::new(),
            urls: Vec::new(),
            data_quality: String::new(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ParserState {
    Label,
    Name,
    Id,
    Contactinfo,
    Profile,
    ParentLabel,
    Sublabels,
    Sublabel,
    Urls,
    Url,
    DataQuality,
}

pub struct LabelsParser<'a> {
    state: ParserState,
    labels: HashMap<i32, Label>,
    current_label: Label,
    pb: ProgressBar,
    db_opts: &'a DbOpt,
}

impl<'a> LabelsParser<'a> {
    pub fn new(db_opts: &'a DbOpt) -> Self {
        LabelsParser {
            state: ParserState::Label,
            labels: HashMap::new(),
            current_label: Label::new(),
            pb: ProgressBar::new(1821993),
            db_opts,
        }
    }
}

impl<'a> Parser<'a> for LabelsParser<'a> {
    fn new(&self, db_opts: &'a DbOpt) -> Self {
        LabelsParser {
            state: ParserState::Label,
            labels: HashMap::new(),
            current_label: Label::new(),
            pb: ProgressBar::new(1821993),
            db_opts,
        }
    }
    fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserState::Label => {
                match ev {
                    Event::Start(e) if e.local_name() == b"label" => {
                        self.current_label.sublabels = Vec::new();
                        self.current_label.urls = Vec::new();
                        ParserState::Label
                    }

                    Event::Start(e) => match e.local_name() {
                        b"name" => ParserState::Name,
                        b"id" => ParserState::Id,
                        b"contactinfo" => ParserState::Contactinfo,
                        b"profile" => ParserState::Profile,
                        b"parent_label" => ParserState::ParentLabel,
                        b"sublabels" => ParserState::Sublabels,
                        b"urls" => ParserState::Urls,
                        b"data_quality" => ParserState::DataQuality,
                        _ => ParserState::Label,
                    },

                    Event::End(e) if e.local_name() == b"label" => {
                        self.labels
                            .entry(self.current_label.id)
                            .or_insert(self.current_label.clone());
                        if self.labels.len() >= self.db_opts.batch_size {
                            // use drain? https://doc.rust-lang.org/std/collections/struct.HashMap.html#examples-13
                            write_labels(self.db_opts, &self.labels)?;
                            self.labels = HashMap::new();
                        }
                        self.pb.inc(1);
                        ParserState::Label
                    }

                    Event::End(e) if e.local_name() == b"labels" => {
                        // write to db remainder of labels
                        write_labels(self.db_opts, &self.labels)?;
                        ParserState::Label
                    }

                    _ => ParserState::Label,
                }
            }

            ParserState::Id => match ev {
                Event::Text(e) => {
                    self.current_label.id = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::Id
                }

                Event::End(e) if e.local_name() == b"id" => ParserState::Label,

                _ => ParserState::Id,
            },

            ParserState::Name => match ev {
                Event::Text(e) => {
                    self.current_label.name = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::Name
                }

                Event::End(e) if e.local_name() == b"name" => ParserState::Label,

                _ => ParserState::Name,
            },

            ParserState::Contactinfo => match ev {
                Event::Text(e) => {
                    self.current_label.contactinfo = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::Contactinfo
                }

                Event::End(e) if e.local_name() == b"contactinfo" => ParserState::Label,

                _ => ParserState::Contactinfo,
            },

            ParserState::Profile => match ev {
                Event::Text(e) => {
                    self.current_label.profile = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::Profile
                }

                Event::End(e) if e.local_name() == b"profile" => ParserState::Label,

                _ => ParserState::Profile,
            },

            ParserState::ParentLabel => match ev {
                Event::Text(e) => {
                    self.current_label.parent_label = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::ParentLabel
                }

                Event::End(e) if e.local_name() == b"parent_label" => ParserState::Label,

                _ => ParserState::ParentLabel,
            },

            ParserState::Sublabels => match ev {
                Event::Start(e) if e.local_name() == b"label" => ParserState::Sublabel,

                Event::End(e) if e.local_name() == b"sublabels" => ParserState::Label,

                _ => ParserState::Sublabels,
            },

            ParserState::Sublabel => match ev {
                Event::Text(e) => {
                    self.current_label
                        .sublabels
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserState::Sublabels
                }

                _ => ParserState::Sublabels,
            },

            ParserState::Urls => match ev {
                Event::Start(e) if e.local_name() == b"url" => ParserState::Url,

                Event::End(e) if e.local_name() == b"urls" => ParserState::Label,

                _ => ParserState::Urls,
            },

            ParserState::Url => match ev {
                Event::Text(e) => {
                    self.current_label
                        .urls
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserState::Urls
                }

                _ => ParserState::Urls,
            },

            ParserState::DataQuality => match ev {
                Event::Text(e) => {
                    self.current_label.data_quality = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::DataQuality
                }

                Event::End(e) if e.local_name() == b"data_quality" => ParserState::Label,

                _ => ParserState::DataQuality,
            },
        };

        Ok(())
    }
}
