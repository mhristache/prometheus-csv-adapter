use inotify::{Inotify, WatchMask};
use log::{debug, error, warn};
use regex::Regex;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Write};
use std::iter::Iterator;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

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
                if let Err(e) = run_once(&cfg) {
                    error!("first run failed: {}", e);
                }
                // continuously monitor the input file and
                // run the main logic when a change is detected
                run_when_file_is_modified(&cfg);
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
    file: PathBuf,
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

fn run_once(cfg: &Config) -> Result<(), Box<dyn Error>> {
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

    // we only care about the last line (newest records)
    if let Some(last) = rdr.records().last() {
        // The reader iterator yields Result<StringRecord, Error>, so we check the error here
        let records = last?;
        let headers = rdr.headers()?;
        let mut seen_headers: Vec<&str> = vec![];
        let mut ofile = File::create(&cfg.output.file)?;
        for (header, value) in headers.iter().zip(records.iter()) {
            if cfg.output.skip_duplicate_headers {
                if seen_headers.contains(&header) {
                    warn!("skipping duplicate header '{}'", header);
                    ofile.write_fmt(format_args!("# skipped: '{}' '{}'\n\n", header, value))?;
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
                    ofile.write_fmt(format_args!("# skipped: '{}' '{}'\n\n", header, value))?;
                    continue;
                }
            }
            ofile.write_fmt(format_args!("# {}\n", header))?;
            ofile.write_fmt(format_args!(
                "{}{}  {}\n\n",
                cfg.output.prefix,
                normalize_string(header),
                value
            ))?;
        }
        debug!("output saved to {:?}", cfg.output.file);
    }
    Ok(())
}

// monitor the input file using inotify and reconfigure bird when the config was changed
fn run_when_file_is_modified(cfg: &Config) {
    debug!("monitoring {:?} for changes", cfg.input.file);

    let mut inotify = Inotify::init().expect("failed to initialize inotify");
    let mut buffer = [0u8; 4096];

    loop {
        // add the watch inside a loop to avoid issues where
        // inotify reports only the first change
        match inotify.add_watch(&cfg.input.file, WatchMask::MODIFY) {
            Err(e) => {
                error!("failed to add inotify watch of {:?}: {}", cfg.input.file, e);
                sleep(Duration::from_secs(10));
            }
            _ => match inotify.read_events_blocking(&mut buffer) {
                Err(e) => {
                    error!("failed to read inotify events: {}", e);
                    sleep(Duration::from_secs(10));
                }
                _ => {
                    // update the running config when inotify received an event
                    // (the thread was unblocked)
                    debug!("change detected in {:?}", cfg.input.file);
                    if let Err(e) = run_once(&cfg) {
                        error!("failed to generate the output: {}", e);
                        sleep(Duration::from_secs(10));
                    }
                }
            },
        }
    }
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
