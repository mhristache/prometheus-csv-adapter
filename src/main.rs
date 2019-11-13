use log::{debug, error, warn, info};
use regex::Regex;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Write};
use std::iter::Iterator;
use std::path::PathBuf;
use std::net::SocketAddr;
use tiny_http::{Server, Response, Method};

const USAGE: &'static str = "Usage: prometheus-csv-adapter <config>\n";

fn main() {
    env_logger::init();
    let mut cfg_path = None;
    for (i, arg) in std::env::args().enumerate() {
        if i == 1 {
            cfg_path = Some(arg);
        } else if i > 1 {
            cfg_path = None;
        }
    }
    if let Some(p) = cfg_path {
        match std::fs::read_to_string(p) {
            Ok(s) => {
                let cfg: Config = match serde_yaml::from_str(&s) {
                    Ok(c) => c,
                    Err(e) => {
                        error!("failed to parse the config: {}", e);
                        std::process::exit(1);
                    }
                };
                // start the server
                info!("starting the web server on {}", cfg.output.socket);
                let server = Server::http(cfg.output.socket).expect("failed to start the http server");

                for rq in server.incoming_requests() {
                    info!("received request! method: {:?}, url: {:?}", rq.method(), rq.url());
                    if rq.url() != "/metrics" {
                        let _ = rq.respond(Response::empty(404));
                    } else if rq.method() != &Method::Get {
                        let _ = rq.respond(Response::empty(405));
                    } else {
                        match gen_response(&cfg) {
                            Ok(s) => {
                                let _ = rq.respond(Response::from_string(s));
                            },
                            Err(_) => {
                                let _ = rq.respond(Response::empty(500));
                            },
                        }
                    }

                }
            }
            Err(e) => {
                error!("failed to read the config: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        write!(std::io::stderr(), "{}", USAGE).unwrap();
        std::process::exit(1);
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    input: Input,
    output: Output,
    fields: Option<Fields>,
}

#[derive(Debug, Deserialize)]
struct Input {
    file: PathBuf,
    delimiter: Option<char>,
    has_headers: bool,
}

#[derive(Debug, Deserialize)]
struct Output {
    socket: SocketAddr,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    numeric_values_only: bool,
    #[serde(default)]
    skip_duplicate_headers: bool,
}

#[derive(Debug, Deserialize)]
struct Fields {
    include: Vec<Field>,
    exclude: Vec<Field>,
}

#[derive(Debug, Deserialize)]
struct Field {
    #[serde(with = "serde_regex")]
    name: Regex,
}

// convert the input csv file into a string with prometheus metrics
fn gen_response(cfg: &Config) -> Result<String, Box<dyn Error>> {
    debug!("parsing {:?}", cfg.input.file);
    // Open the input file and read it line by line
    let ifile = File::open(&cfg.input.file)?;
    let reader = BufReader::new(ifile);

    // Build the CSV reader
    let delimiter = match cfg.input.delimiter {
        Some(c) => c as u8,
        None => b',',
    };

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(cfg.input.has_headers)
        .delimiter(delimiter)
        .from_reader(reader);

    let mut res = String::new();
    // we only care about the last line (newest records)
    if let Some(last) = rdr.records().last() {
        // The reader iterator yields Result<StringRecord, Error>, so we check the error here
        let records = last?;
        let headers = rdr.headers()?;
        let mut seen_headers: Vec<&str> = vec![];
        for (header, value) in headers.iter().zip(records.iter()) {
            if cfg.output.skip_duplicate_headers {
                if seen_headers.contains(&header) {
                    warn!("skipping duplicate header '{}'", header);
                    res.push_str(&*format!("# skipped: '{}' '{}'\n\n", header, value));
                    continue;
                }
                seen_headers.push(header);
            }
            if cfg.output.numeric_values_only {
                if value.parse::<f64>().is_err() {
                    warn!(
                        "skipping record '{}' as the corresponding value is not numeric: {}",
                        header, value
                    );
                    res.push_str(&*format!("# skipped: '{}' '{}'\n\n", header, value));
                    continue;
                }
            }
            res.push_str(&*format!("# {}\n", header));
            res.push_str(&*format!(
                "{}{}  {}\n\n",
                cfg.output.prefix,
                normalize_string(header),
                value
            ));
        }
    }
    Ok(res)
}

// replace spaces, - and () from the input string with _
fn normalize_string(s: &str) -> String {
    let mut v = vec![];
    let mut prev_c = '_';
    for c in s.chars() {
        if c.is_alphanumeric() {
            prev_c = c.to_ascii_lowercase();
            v.push(prev_c);
        } else {
            // don't replace at start and end of string and if there was a replacement before
            if prev_c != '_' {
                v.push('_');
                prev_c = '_';
            }
        }
    }
    // remove the trailing _
    if v.last() == Some(&'_') {
        v.pop();
    }
    v.iter().collect()
}
