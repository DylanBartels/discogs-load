use indicatif::ProgressBar;
use postgres::types::ToSql;
use quick_xml::events::Event;
use std::{collections::HashMap, error::Error, str};

use crate::db::{write_labels, DbOpt, SqlSerialization};
use crate::parser::Parser;

#[derive(Clone, Debug)]
pub struct Label {
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
    ReadLabel,
    ReadName,
    ReadId,
    ReadContactinfo,
    ReadProfile,
    ReadParentLabel,
    ReadSublabels,
    ReadSublabel,
    ReadUrls,
    ReadUrl,
    ReadDataQuality,
}

pub struct LabelsParser<'a> {
    state: ParserState,
    labels: HashMap<i32, Label>,
    current_label: Label,
    current_id: i32,
    pb: ProgressBar,
    db_opts: &'a DbOpt,
}

impl<'a> LabelsParser<'a> {
    pub fn new(db_opts: &'a DbOpt) -> Self {
        LabelsParser {
            state: ParserState::ReadLabel,
            labels: HashMap::new(),
            current_label: Label::new(),
            current_id: 0,
            pb: ProgressBar::new(1821993),
            db_opts,
        }
    }
}

impl<'a> Parser<'a> for LabelsParser<'a> {
    fn new(&self, db_opts: &'a DbOpt) -> Self {
        LabelsParser {
            state: ParserState::ReadLabel,
            labels: HashMap::new(),
            current_label: Label::new(),
            current_id: 0,
            pb: ProgressBar::new(1821993),
            db_opts,
        }
    }
    fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserState::ReadLabel => {
                match ev {
                    Event::Start(e) if e.local_name() == b"label" => {
                        self.current_label.sublabels = Vec::new();
                        self.current_label.urls = Vec::new();
                        ParserState::ReadLabel
                    }

                    Event::Start(e) => match e.local_name() {
                        b"name" => ParserState::ReadName,
                        b"id" => ParserState::ReadId,
                        b"contactinfo" => ParserState::ReadContactinfo,
                        b"profile" => ParserState::ReadProfile,
                        b"parent_label" => ParserState::ReadParentLabel,
                        b"sublabels" => ParserState::ReadSublabels,
                        b"urls" => ParserState::ReadUrls,
                        b"data_quality" => ParserState::ReadDataQuality,
                        _ => ParserState::ReadLabel,
                    },

                    Event::End(e) if e.local_name() == b"label" => {
                        self.labels
                            .entry(self.current_id)
                            .or_insert(self.current_label.clone());
                        if self.labels.len() >= self.db_opts.batch_size {
                            // use drain? https://doc.rust-lang.org/std/collections/struct.HashMap.html#examples-13
                            write_labels(self.db_opts, &self.labels)?;
                            self.labels = HashMap::new();
                        }
                        self.pb.inc(1);
                        ParserState::ReadLabel
                    }

                    Event::End(e) if e.local_name() == b"labels" => {
                        // write to db remainder of labels
                        write_labels(self.db_opts, &self.labels)?;
                        ParserState::ReadLabel
                    }

                    _ => ParserState::ReadLabel,
                }
            }

            ParserState::ReadId => match ev {
                Event::Text(e) => {
                    self.current_id = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::ReadId
                }

                Event::End(e) if e.local_name() == b"id" => ParserState::ReadLabel,

                _ => ParserState::ReadId,
            },

            ParserState::ReadName => match ev {
                Event::Text(e) => {
                    self.current_label.name = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::ReadName
                }

                Event::End(e) if e.local_name() == b"name" => ParserState::ReadLabel,

                _ => ParserState::ReadName,
            },

            ParserState::ReadContactinfo => match ev {
                Event::Text(e) => {
                    self.current_label.contactinfo = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::ReadContactinfo
                }

                Event::End(e) if e.local_name() == b"contactinfo" => ParserState::ReadLabel,

                _ => ParserState::ReadContactinfo,
            },

            ParserState::ReadProfile => match ev {
                Event::Text(e) => {
                    self.current_label.profile = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::ReadProfile
                }

                Event::End(e) if e.local_name() == b"profile" => ParserState::ReadLabel,

                _ => ParserState::ReadProfile,
            },

            ParserState::ReadParentLabel => match ev {
                Event::Text(e) => {
                    self.current_label.parent_label = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::ReadParentLabel
                }

                Event::End(e) if e.local_name() == b"parent_label" => ParserState::ReadLabel,

                _ => ParserState::ReadParentLabel,
            },

            ParserState::ReadSublabels => match ev {
                Event::Start(e) if e.local_name() == b"label" => ParserState::ReadSublabel,

                Event::End(e) if e.local_name() == b"sublabels" => ParserState::ReadLabel,

                _ => ParserState::ReadSublabels,
            },

            ParserState::ReadSublabel => match ev {
                Event::Text(e) => {
                    self.current_label
                        .sublabels
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserState::ReadSublabels
                }

                _ => ParserState::ReadSublabels,
            },

            ParserState::ReadUrls => match ev {
                Event::Start(e) if e.local_name() == b"url" => ParserState::ReadUrl,

                Event::End(e) if e.local_name() == b"urls" => ParserState::ReadLabel,

                _ => ParserState::ReadUrls,
            },

            ParserState::ReadUrl => match ev {
                Event::Text(e) => {
                    self.current_label
                        .urls
                        .extend(str::parse(str::from_utf8(&e.unescaped()?)?));
                    ParserState::ReadUrls
                }

                _ => ParserState::ReadUrls,
            },

            ParserState::ReadDataQuality => match ev {
                Event::Text(e) => {
                    self.current_label.data_quality = str::parse(str::from_utf8(&e.unescaped()?)?)?;
                    ParserState::ReadDataQuality
                }

                Event::End(e) if e.local_name() == b"data_quality" => ParserState::ReadLabel,

                _ => ParserState::ReadDataQuality,
            },
        };

        Ok(())
    }
}
