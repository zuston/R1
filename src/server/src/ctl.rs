#![allow(dead_code, unused)]

use bytes::{Buf, Bytes};
use clap::builder::Str;
use clap::{Parser, Subcommand};
use std::fs;
use uniffle_worker::util::get_crc;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    DataValidator {
        #[arg(short, long)]
        index_file_path: String,
        #[arg(short, long)]
        data_file_path: String,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.command.is_none() {
        return Ok(());
    }

    let command = args.command.unwrap();
    match command {
        Commands::DataValidator {
            index_file_path,
            data_file_path,
        } => {
            do_check_data_consistency(index_file_path, data_file_path)?;
        }
        _ => {}
    }

    Ok(())
}

fn do_check_data_consistency(index_path: String, data_path: String) -> anyhow::Result<()> {
    let index_data = fs::read(index_path)?;
    let mut index_data = Bytes::copy_from_slice(&index_data);

    let data = fs::read(data_path)?;
    let data = Bytes::copy_from_slice(&data);

    // check the length
    let mut index_clone = index_data.clone();
    let mut expected_len = 0;
    let batch = index_clone.len() / 40;
    for idx in 0..batch {
        let offset = index_clone.get_i64();
        let length = index_clone.get_i32();
        let uncompress_len = index_clone.get_i32();
        let crc = index_clone.get_i64();
        let block_id = index_clone.get_i64();
        let task_id = index_clone.get_i64();

        expected_len += length as usize;
    }

    if expected_len != data.len() {
        println!(
            "index recorded data len: {}. real: {}",
            expected_len,
            data.len()
        );
        return Ok(());
    }

    for idx in 0..batch {
        let offset = index_data.get_i64();
        let length = index_data.get_i32();
        let uncompress_len = index_data.get_i32();
        let crc = index_data.get_i64();
        let block_id = index_data.get_i64();
        let task_id = index_data.get_i64();

        let partial = data.slice((offset as usize..(offset + (length as i64)) as usize));
        let data_crc = get_crc(&partial);
        if crc != data_crc {
            println!(
                "blockId: {}, crc is not correct. expected: {}, real: {}. total batch: {}. batch index: {}",
                block_id, crc, data_crc, batch, idx
            );
        }
    }
    Ok(())
}
