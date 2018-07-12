extern crate docopt;
#[macro_use]
extern crate lazy_static;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use regex::Regex;
use rusoto_core::Region;
use rusoto_s3::{S3, S3Client};
use serde::de;
use serde::de::{Deserialize, Deserializer};
use std::fmt;
use std::str::FromStr;

const USAGE: &'static str = "
Move S3 objects from src to dest.

Usage:
  s3-bulk-move [options] <src-url> <dest-url>
  s3-bulk-move (-h | --help)
  s3-bulk-move --version

Options:
  -h --help       Show this screen.
  --version       Show version.
  --src-filter    Regex matched against src key parts trailing <src-url>.
  --dest-replace  Replacement pattern for the trailing key part. May refer to groups from
                  --src-filter
  --src-region=<region>    AWS region of src bucket. Defaults to $AWS_DEFAULT_REGION.
  --dest-region=<region>   AWS region of dest bucket. Defaults to $AWS_DEFAULT_REGION.
";

#[derive(Debug, Deserialize)]
struct Args {
    arg_src_url: S3Url,
    arg_dest_url: S3Url,
    flag_src_filter: Option<String>,
    flag_dest_replace: Option<String>,
    flag_src_region: Option<String>,
    flag_dest_region: Option<String>,
}

#[derive(Debug)]
struct S3Url {
    bucket: String,
    prefix: Option<String>,
}

struct S3UrlVisitor;

impl<'de> de::Visitor<'de> for S3UrlVisitor {
    type Value = S3Url;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a URL like s3://bucket/some/key")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: de::Error {
        lazy_static! {
            static ref S3_URL_REGEX: Regex = Regex::new("^s3://([^/]+)/(.*)").unwrap();
        }

        let matches = S3_URL_REGEX
            .captures(v)
            .ok_or_else(|| de::Error::invalid_value(de::Unexpected::Str(v), &self))?;
        let prefix = matches.get(2).unwrap().as_str().to_owned();
        Ok(S3Url {
            bucket: matches.get(1).unwrap().as_str().to_owned(),
            prefix: if prefix.len() > 0 { Some(prefix) } else { None },
        })
    }
}

impl<'de> Deserialize<'de> for S3Url {
    fn deserialize<D>(d: D) -> Result<S3Url, D::Error> where D: Deserializer<'de> {
        d.deserialize_str(S3UrlVisitor)
    }
}

fn main() {
    run().err().map(|err| {
        println!("{}", err);
        std::process::exit(1)
    });
}

fn get_region(region_flag: &Option<String>, default_region: &String) -> Result<Region, impl std::error::Error> {
    Region::from_str(
        region_flag.as_ref().unwrap_or(default_region).as_str()
    )
}

fn run() -> Result<(), Box<std::error::Error>> {
    let args: Args = docopt::Docopt::new(USAGE)
        .and_then(|d| d.deserialize())?;

    let default_region = std::env::var("AWS_DEFAULT_REGION")
        .unwrap_or("us-east-1".to_owned());
    let src_region = get_region(&args.flag_src_region, &default_region)?;
    let dest_region = get_region(&args.flag_dest_region, &default_region)?;

    let src_client = S3Client::simple(src_region);
    let dest_client = S3Client::simple(dest_region);

    let src_filter = args.flag_src_filter.map(|s| Regex::new(s.as_str()).unwrap());
    let dest_replace = args.flag_dest_replace.as_ref().map(String::as_str);

    let mut list_req = rusoto_s3::ListObjectsRequest::default();
    list_req.bucket = args.arg_src_url.bucket;
    list_req.prefix = args.arg_src_url.prefix;
    let mut next_marker = None;

    loop {
        list_req.marker = next_marker;
        let rsp = src_client.list_objects(&list_req).sync()?;
        if rsp.contents.is_none() || rsp.contents.as_ref().unwrap().is_empty() {
            break;
        }

        let mut objs: Box<Iterator<Item = (String, i64)>> = Box::new(rsp.contents.as_ref().unwrap().iter()
            .filter(|obj| obj.key.is_some() && obj.size.is_some())
            .map(|obj| {
                let mut key = obj.key.as_ref().unwrap().clone();
                let pfx_len = list_req.prefix
                    .as_ref()
                    .map(|pfx| pfx.as_str().len())
                    .unwrap_or(0);
                (key.split_off(pfx_len), obj.size.unwrap())
            }));

        if src_filter.is_some() {
            if dest_replace.is_some() {
                objs = Box::new(objs.filter_map(|(key, sz)| {
                    let re = src_filter.as_ref().unwrap();
                    let rep = dest_replace.unwrap();
                    re.captures(key.as_str())
                        .map(|x| {
                            let mut result = String::new();
                            x.expand(rep, &mut result);
                            (result, sz)
                        })
                }))
            } else {
                objs = Box::new(objs.filter(|(key, sz)| {
                    let re = src_filter.as_ref().unwrap();
                    re.is_match(key.as_str())
                }))
            }
        }

        for (key, sz) in objs {
            println!("{}\t{}", key, sz);
        }

        if rsp.is_truncated.unwrap() {
            next_marker = rsp.contents.as_ref().unwrap().last().unwrap().key.clone()
        } else {
            break
        }
    }


    Ok(())
}