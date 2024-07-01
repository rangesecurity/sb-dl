.PHONY: format
format:
	 find -type f -name "*.rs" -not -path "*target*" -not -path "*proto*" -exec rustfmt --edition 2021 {} \; 
