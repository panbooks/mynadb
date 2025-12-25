set -x

cargo build --release
cp -rf target/release/stats dist/
cp -rf config.toml dist/

[ -f dist.gz ] && rm -f dist.gz

tar cvfz dist.gz dist/
