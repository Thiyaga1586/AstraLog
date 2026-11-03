$ErrorActionPreference = "Stop"

cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

cargo clippy --workspace --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

cargo test --workspace --all-features
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

git diff --check
exit $LASTEXITCODE
