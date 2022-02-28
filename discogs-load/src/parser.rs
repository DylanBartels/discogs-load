use quick_xml::events::Event;
use std::error::Error;

use crate::db::DbOpt;

pub trait Parser<'a> {
    fn new(&self, db_opts: &'a DbOpt) -> Self
    where
        Self: Sized;
    fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>>;
}
