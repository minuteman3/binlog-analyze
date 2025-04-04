# Contributing to MySQL Binlog Analyzer

Thank you for your interest in contributing to MySQL Binlog Analyzer! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please be respectful and considerate of others when participating in this project.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Set up the development environment
4. Create a new branch for your changes

## Development Environment

Make sure you have Go 1.19 or higher installed.

```bash
# Clone your fork
git clone https://github.com/your-username/binlog-analyze.git
cd binlog-analyze

# Build the project
go build ./cmd/binlog-analyze

# Run the tests
./run-tests.sh
# or
go test ./...
```

## Making Changes

1. Create a new branch for your changes: `git checkout -b feature-or-fix-name`
2. Make your changes
3. Add tests for your changes
4. Ensure all tests pass: `go test ./...`
5. Ensure code is properly formatted: `gofmt -s -w .`
6. Commit your changes with a descriptive commit message

## Pull Requests

1. Push your changes to your fork on GitHub
2. Create a pull request to the main repository
3. Describe your changes in detail
4. Reference any related issues

## Testing

- All new features should include tests
- Run the test suite before submitting a pull request
- Aim for high test coverage

## Code Style

- Follow standard Go conventions
- Use `gofmt` to format your code
- Write clear, concise comments
- Use meaningful variable and function names

## License

By contributing to this project, you agree that your contributions will be licensed under the same [MIT License](LICENSE) that covers the project.

## Questions?

If you have any questions or need help with contributing, please open an issue on GitHub.