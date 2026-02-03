# Contributing to ThorDB

First off, thank you for considering contributing to ThorDB! It's people like you that make ThorDB such a great tool.

## Code of Conduct

This project and everyone participating in it is governed by our commitment to providing a welcoming and inclusive environment. Please be respectful and constructive in all interactions.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When you create a bug report, include as many details as possible:

- **Use a clear and descriptive title**
- **Describe the exact steps to reproduce the problem**
- **Provide specific examples** (code snippets, test cases)
- **Describe the behavior you observed and what you expected**
- **Include your environment** (OS, Rust version, ThorDB version)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion:

- **Use a clear and descriptive title**
- **Provide a detailed description of the proposed enhancement**
- **Explain why this enhancement would be useful**
- **List any alternatives you've considered**

### Pull Requests

1. **Fork the repo** and create your branch from `main`
2. **Write tests** for any new functionality
3. **Ensure the test suite passes** (`cargo test`)
4. **Format your code** (`cargo fmt`)
5. **Run clippy** (`cargo clippy`)
6. **Write clear commit messages**

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/thordb.git
cd thordb

# Add upstream remote
git remote add upstream https://github.com/akram/thordb.git

# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build the project
cargo build

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run
```

## Project Structure

```
thordb/
├── core/                    # Core storage engine library
│   └── src/
│       ├── lsm/             # LSM-tree implementation
│       │   ├── memtable.rs  # In-memory sorted table
│       │   ├── sstable.rs   # Sorted string tables
│       │   ├── wal.rs       # Write-ahead log
│       │   ├── iterator.rs  # Merge iterators
│       │   ├── types.rs     # Key, Value, Entry types
│       │   └── lsm.rs       # Main coordinator
│       ├── bufferpool.rs    # Page buffer pool
│       ├── page.rs          # Page abstraction
│       ├── pagefile.rs      # File I/O
│       └── tuple/           # Tuple serialization
└── src/
    └── main.rs              # CLI entry point
```

## Coding Guidelines

### Rust Style

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `cargo fmt` to format code
- Use `cargo clippy` to catch common mistakes
- Write documentation for public APIs

### Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters
- Reference issues and pull requests when relevant

### Testing

- Write unit tests for new functionality
- Tests should be in `#[cfg(test)]` modules within the same file
- Integration tests go in the `tests/` directory
- Use descriptive test names: `test_memtable_handles_duplicate_keys`

## Areas We Need Help

### High Priority
- **Compaction** — Implement level-based or size-tiered compaction
- **Bloom Filters** — Add bloom filters for faster negative lookups
- **Benchmarks** — Create comprehensive performance benchmarks

### Medium Priority
- **Compression** — Add LZ4/Zstd compression support
- **Documentation** — Improve API documentation and tutorials
- **Examples** — Create example applications

### Good First Issues
- Add more unit tests
- Improve error messages
- Add logging/tracing statements
- Documentation improvements

## Questions?

Feel free to open an issue with your question or reach out to the maintainers.

Thank you for contributing! 🎉
