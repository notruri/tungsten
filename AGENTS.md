# Agent guidelines

## Building/compiling

- This is a Windows GPUI project, not Linux/macOS.

### WSL environment

- Use the Windows' Rust toolchain using `powershell.exe` to invoke commands like `cargo` instead of local `cargo` binary.
- Check the current working directory within WSL and powershell first before running project specific commands
- Avoid explicitly changing powershell's current working directory (eg; `Set-Location`) unless the current session is in another directory
- Avoid modifying filemodes (eg; `chmod`)
