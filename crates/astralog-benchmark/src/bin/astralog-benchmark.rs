use std::{env, fs, path::Path};

use astralog_benchmark::{
    BenchmarkManifest, RawBundle, generate_comparison_markdown, generate_report_markdown,
    run_benchmark,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("astralog-benchmark: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, manifest_path, raw_path, report_path] if command == "run" => {
            let manifest = BenchmarkManifest::parse(&fs::read_to_string(manifest_path)?)?;
            let bundle = run_benchmark(manifest).await?;
            let raw = bundle.canonical_text()?;
            write_parented(raw_path, raw.as_bytes())?;
            // Raw machine-readable evidence is durably written before the
            // human report is generated, as required by the P8 contract.
            let report = generate_report_markdown(&bundle)?;
            write_parented(report_path, report.as_bytes())?;
        }
        [command, raw_path, report_path] if command == "report" => {
            let bundle = RawBundle::parse(&fs::read_to_string(raw_path)?)?;
            let report = generate_report_markdown(&bundle)?;
            write_parented(report_path, report.as_bytes())?;
        }
        [command, baseline_path, contender_path, comparison_path] if command == "compare" => {
            let baseline = RawBundle::parse(&fs::read_to_string(baseline_path)?)?;
            let contender = RawBundle::parse(&fs::read_to_string(contender_path)?)?;
            let report = generate_comparison_markdown(&baseline, &contender)?;
            write_parented(comparison_path, report.as_bytes())?;
        }
        _ => {
            return Err(
                "usage: astralog-benchmark run <manifest> <raw-out> <report-out> | report <raw> <report-out> | compare <baseline-raw> <contender-raw> <comparison-out>"
                    .into(),
            );
        }
    }
    Ok(())
}

fn write_parented(path: &str, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(path);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    Ok(())
}
