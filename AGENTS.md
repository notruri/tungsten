# Agent guidelines

### WSL environment

- Use the Windows' Rust toolchain using `powershell.exe` to invoke commands like `cargo` instead of local `cargo` binary when compiling against Windows.
- Avoid explicitly changing powershell's current working directory (eg; `Set-Location`) unless the current session is in another directory
- Avoid modifying filemodes (eg; `chmod`)
