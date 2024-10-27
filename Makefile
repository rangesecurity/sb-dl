.PHONY: cli
cli:
	rustup default 1.78
	cargo build --bin sb_dl
	cp target/debug/sb_dl .

.PHONY: cli-release
cli-release:
	rustup default 1.78
	cargo build --release --bin sb_dl
	cp target/release/sb_dl .

.PHONY: format
format:
	 find -type f -name "*.rs" -not -path "*target*" -not -path "*proto*" -exec rustfmt --edition 2021 {} \; 
