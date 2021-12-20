extern crate pbr;
extern crate serde_yaml;

use anyhow::Result;
use clap::Parser;
use colored::Colorize;
use pbr::ProgressBar;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct StageSteps {
    steps: Vec<ExecutionEntry>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ExecutionEntry {
    name: String,
    execute: String,
    check: String,
}

#[derive(PartialEq, Debug)]
enum ExecutionResult {
    CONTINUE,
    TERMINATE,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct DatabaseConfig {
    url: String,
}

fn load_entries(path: &str, stage: &str) -> Result<Vec<ExecutionEntry>> {
    let file_path = Path::new(path)
        .join("stages")
        .join(format!("{}.yaml", stage));
    println!("{}", file_path.as_path().to_str().unwrap());
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let stage_steps: StageSteps = serde_yaml::from_reader(reader)?;
    Ok(stage_steps.steps)
}

fn load_config(path: &str) -> Result<DatabaseConfig> {
    let file_path = Path::new(path).join("config.yaml");
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let config: DatabaseConfig = serde_yaml::from_reader(reader)?;
    Ok(config)
}

fn execute(config: &DatabaseConfig, entry: &ExecutionEntry) -> ExecutionResult {
    println!();
    println!(
        "We well execute [{}]: \n {}",
        entry.name.yellow(),
        entry.execute.red()
    );
    let input: &str = &read_input("\n (E)xecute or (S)kip?", HashSet::from(["E", "S"]));
    match input {
        "E" => do_execute(config, entry),
        "S" => {
            println!("{}", "User skipped.".yellow());
            ExecutionResult::CONTINUE
        }
        _ => {
            println!("{}", "Program has unknown error".red());
            ExecutionResult::TERMINATE
        }
    }
}

fn read_input(message: &str, values: HashSet<&str>) -> String {
    loop {
        println!("{}", message.green());
        let mut input: String = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let r: &str = input.trim_end();
        if values.contains(r) {
            return r.to_string();
        }
    }
}

fn do_execute(config: &DatabaseConfig, entry: &ExecutionEntry) -> ExecutionResult {
    loop {
        let success = execute_sql(config, &entry.execute);
        if success {
            break;
        }
        let _input = read_input("(R)etry or (I)gnore?", HashSet::from(["R", "I"]));
        if "I".eq(&_input) {
            break;
        }
    }
    loop {
        println!("{} \n", "Checking".yellow());
        execute_sql(config, &entry.check);
        let _input = read_input("(R)echeck or (N)next?", HashSet::from(["R", "N"]));
        if "N".eq(&_input) {
            break;
        }
    }
    ExecutionResult::CONTINUE
}

fn execute_sql(config: &DatabaseConfig, sql: &str) -> bool {
    println!("  {} \n", sql.blue());
    let (tx, rx) = mpsc::channel();
    let count = 100;
    let pb_ref = Arc::new(Mutex::new(ProgressBar::new(count)));
    let pb1 = pb_ref.clone();
    std::thread::spawn(move || {
        for _ in 0..count {
            match rx.try_recv() {
                Ok(_) | Err(TryRecvError::Disconnected) => {
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }
            pb1.lock().unwrap().inc();
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    let client = reqwest::blocking::Client::builder().build().unwrap();
    let resp = client
        .post(&config.url)
        .body(sql.to_string())
        .send()
        .unwrap();
    let _ = tx.send(());
    pb_ref.lock().unwrap().finish_println("\n");

    if resp.status().is_success() {
        println!("{}", resp.text().unwrap().green());
        true
    } else {
        println!("{}", resp.text().unwrap().red());
        false
    }
}

#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    /// Base dir
    path: String,
    /// Execute stage
    stage: String,
}

fn main() {
    let args: Args = Args::parse();
    let config = load_config(&args.path).unwrap();
    let entries = load_entries(&args.path, &args.stage).unwrap();
    for entry in entries {
        let _result: ExecutionResult = execute(&config, &entry);
        match _result {
            ExecutionResult::TERMINATE => break,
            _ => (),
        }
    }
}
