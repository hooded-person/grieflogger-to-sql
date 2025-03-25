use std::env;
use std::fs::File;
use std::io::prelude::*;
use dotenv::dotenv;

fn main() -> std::io::Result<()>{
    dotenv().ok();
    let path_to_file: String = env::var("PROGRESS_LOG").unwrap();
    let mut file = File::open(&path_to_file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    println!("{}", contents);
    Ok(())
}
