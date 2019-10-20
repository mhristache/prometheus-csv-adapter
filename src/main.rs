use std::error::Error;
use log::{error, warn};
use serde::Deserialize;
use std::io::{BufReader, Write};
use std::fs::File;
use regex::Regex;
use std::path::PathBuf;
use std::iter::Iterator;

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
                        error!("Failed to parse the config: {}", e);
                        std::process::exit(1);
                    }
                };
                if let Err(e) = run(&cfg) {
                    error!("{}", e);
                }
            }
            Err(e) => {
                error!("Failed to read the config: {}", e);
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
    prefix: String,
    numeric_values_only: bool,
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


fn run(cfg: &Config) -> Result<(), Box<dyn Error>> {
    // Open the input file and read it line by line
    let ifile = File::open(&cfg.input.file)?;
    let reader = BufReader::new(ifile);

    // Build the CSV reader
    let delimiter = match cfg.input.delimiter {
        Some(c) => c as u8,
        None => b',',
    };

    let mut rdr = csv::ReaderBuilder::new().has_headers(cfg.input.has_headers).delimiter(delimiter).from_reader(reader);

    // we only care about the last line (newest records)
    if let Some(last)= rdr.records().last() {
        // The reader iterator yields Result<StringRecord, Error>, so we check the error here.
        let records = last?;
        let headers = rdr.headers()?;

        let mut ofile = File::create(&cfg.output.file)?;
        for (header, value) in headers.iter().zip(records.iter()) {
            if cfg.output.numeric_values_only {
                if value.parse::<f64>().is_err() {
                    warn!("Skipping record '{}' as the corresponding value is not numeric: {}", header, value);
                    ofile.write_fmt(format_args!("# {} {}\n", header, value))?;
                    continue;
                }
            }
            ofile.write_fmt(format_args!("# {}\n", header))?;
            ofile.write_fmt(format_args!("{}{}  {}\n\n", cfg.output.prefix, normalize_string(header), value))?;
        }
    }
    Ok(())
}


// replace spaces, - and () from the input string with _
fn normalize_string(s: &str) -> String {
    let mut v = vec![];
    let mut prev_c = None;
    for (i, c) in s.chars().enumerate() {
        if c.is_alphanumeric() {
            v.push(c.to_ascii_lowercase());
            prev_c = None;
        } else {
            // don't replace at start of string and if there was a replacement before
            if i > 0 && prev_c != Some('_') {
                v.push('_');
                prev_c = Some('_')
            }
        }
    }
    v.iter().collect()
}