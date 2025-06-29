//
// Copyright 2024 Tabs Data Inc.
//

use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::string::ToString;
use td_common::files::make_executable;

#[derive(Default)]
pub struct ScriptBuilder {
    lines: Vec<String>,
}

impl ScriptBuilder {
    #[cfg(not(windows))]
    pub const SHELL: &'static str = "sh";
    #[cfg(windows)]
    pub const SHELL: &'static str = "cmd";

    #[cfg(not(windows))]
    pub const SHELL_OPTIONS: [&'static str; 0] = [];
    #[cfg(windows)]
    pub const SHELL_OPTIONS: [&'static str; 1] = ["/C"];

    #[cfg(not(target_os = "windows"))]
    const EXTENSION: &'static str = ".sh";
    #[cfg(target_os = "windows")]
    pub const EXTENSION: &'static str = ".cmd";

    #[cfg(not(windows))]
    const BLANK: [&'static str; 1] = [""];
    #[cfg(windows)]
    const BLANK: [&'static str; 1] = [""];

    #[cfg(not(windows))]
    const SHEBANG: [&'static str; 1] = ["#!/bin/bash"];
    #[cfg(windows)]
    const SHEBANG: [&'static str; 0] = [];

    #[cfg(not(windows))]
    const CONTEXT: [&'static str; 2] = ["set -e +x", "trap \"kill 0\" INT TERM"];
    #[cfg(windows)]
    const CONTEXT: [&'static str; 2] = ["@echo off", "setlocal"];

    #[cfg(not(windows))]
    const COPYRIGHT: [&'static str; 3] = ["#", "# Copyright 2024 Tabs Data Inc.", "#"];
    #[cfg(windows)]
    const COPYRIGHT: [&'static str; 3] = ["@rem", "@rem Copyright 2024 Tabs Data Inc.", "@rem"];

    #[cfg(not(windows))]
    const MAIN_CALL: [&'static str; 0] = [];
    #[cfg(windows)]
    const MAIN_CALL: [&'static str; 1] = ["goto :main"];

    #[cfg(not(windows))]
    const FUNCTIONS: &'static str =
        include_str!("../../../../binaries/td-server/resources/scripts/bash/functions.sh");
    #[cfg(windows)]
    const FUNCTIONS: &'static str =
        include_str!("../../../../binaries/td-server/resources/scripts/cmd/functions.cmd");

    #[cfg(not(windows))]
    const MAIN_LABEL: [&'static str; 0] = [];
    #[cfg(windows)]
    const MAIN_LABEL: [&'static str; 1] = [":main"];

    #[cfg(not(windows))]
    const BACKGROUND_PREFIX: [&'static str; 0] = [];
    #[cfg(windows)]
    const BACKGROUND_PREFIX: [&'static str; 1] = ["call"];

    #[cfg(not(windows))]
    const BACKGROUND_SUFFIX: [&'static str; 0] = [];
    #[cfg(windows)]
    const BACKGROUND_SUFFIX: [&'static str; 0] = [];

    #[cfg(not(windows))]
    const BACKGROUND_WAIT: [&'static str; 0] = [];
    #[cfg(windows)]
    const BACKGROUND_WAIT: [&'static str; 0] = [];

    #[cfg(not(windows))]
    const CHECK: [&'static str; 1] = ["check_error $?"];
    #[cfg(windows)]
    const CHECK: [&'static str; 1] = ["call :check_error"];

    #[cfg(not(windows))]
    const TEARDOWN: &'static str =
        include_str!("../../../../binaries/td-server/resources/scripts/bash/teardown.sh");
    #[cfg(windows)]
    const TEARDOWN: &'static str =
        include_str!("../../../../binaries/td-server/resources/scripts/cmd/teardown.cmd");

    #[cfg(not(windows))]
    const RELEASE: [&'static str; 0] = [];
    #[cfg(windows)]
    const RELEASE: [&'static str; 1] = ["endlocal"];

    #[cfg(not(windows))]
    const EXIT0: [&'static str; 1] = ["exit 0"];
    #[cfg(windows)]
    const EXIT0: [&'static str; 1] = ["exit 0"];

    pub fn new() -> Self {
        ScriptBuilder { lines: Vec::new() }
    }

    pub fn foreground_statement(mut self, line: &str) -> Self {
        self.lines.push(line.to_string());
        for line in Self::CHECK {
            self.lines.push(line.to_string());
        }
        self
    }

    pub fn background_statement(mut self, line: &str) -> Self {
        self.lines.push(
            format!(
                "{} {} {}",
                Self::BACKGROUND_PREFIX.join(" "),
                line,
                Self::BACKGROUND_SUFFIX.join(" ")
            )
            .trim()
            .to_string(),
        );
        for line in Self::BACKGROUND_WAIT {
            self.lines.push(line.to_string());
        }
        for line in Self::CHECK {
            self.lines.push(line.to_string());
        }
        self
    }

    pub fn statement(self, line: &str) -> Self {
        self.foreground_statement(line)
    }

    pub fn statements<I>(mut self, lines: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        for line in lines {
            self.lines.push(line.as_ref().to_string());
        }
        for line in Self::CHECK {
            self.lines.push(line.to_string());
        }
        self
    }

    pub fn comment(mut self, comment: &str) -> Self {
        #[cfg(not(windows))]
        self.lines.push(format!("# {comment}"));
        #[cfg(windows)]
        self.lines.push(format!("rem {}", comment));
        self
    }

    pub fn comments<I>(mut self, comments: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        for comment in comments {
            #[cfg(not(windows))]
            self.lines.push(format!("# {}", comment.as_ref()));
            #[cfg(windows)]
            self.lines.push(format!("rem {}", comment.as_ref()));
        }
        self
    }

    pub fn script_to_platform(script: PathBuf) -> String {
        format!(
            "{}{}",
            script.as_os_str().to_string_lossy(),
            Self::EXTENSION
        )
    }

    pub fn build(self, path: PathBuf) -> io::Result<()> {
        let shellscript = Self::script_to_platform(path);
        let mut file = File::create(&shellscript)?;
        self.shebang(&mut file)?;
        self.echoing(&mut file)?;
        self.blank(&mut file)?;
        self.copyright(&mut file)?;
        self.blank(&mut file)?;
        self.main_call(&mut file)?;
        self.blank(&mut file)?;
        self.functions(&mut file)?;
        self.blank(&mut file)?;
        self.main_label(&mut file)?;
        self.script(&mut file)?;
        self.blank(&mut file)?;
        self.teardown(&mut file)?;
        self.release(&mut file)?;
        self.blank(&mut file)?;
        self.exit0(&mut file)?;

        make_executable(&PathBuf::from(&shellscript))?;

        Ok(())
    }

    fn blank(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::BLANK)?;
        Ok(())
    }

    fn shebang(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::SHEBANG)?;
        Ok(())
    }

    fn echoing(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::CONTEXT)?;
        Ok(())
    }

    fn copyright(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::COPYRIGHT)?;
        Ok(())
    }

    fn main_call(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::MAIN_CALL)?;
        Ok(())
    }

    fn functions(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::FUNCTIONS.lines())?;
        Ok(())
    }

    fn main_label(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::MAIN_LABEL)?;
        Ok(())
    }

    fn script(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, &self.lines)?;
        Ok(())
    }

    fn teardown(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::TEARDOWN.lines())?;
        Ok(())
    }

    fn release(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::RELEASE)?;
        Ok(())
    }

    fn exit0(&self, file: &mut File) -> io::Result<()> {
        self.lines(file, Self::EXIT0)?;
        Ok(())
    }

    fn lines<T>(&self, file: &mut File, lines: T) -> io::Result<()>
    where
        T: IntoIterator,
        T::Item: AsRef<str>,
    {
        for line in lines {
            writeln!(file, "{}", line.as_ref())?;
        }
        Ok(())
    }
}

pub enum ArgumentPrefix {
    None,
    Short,
    Long,
}

#[derive(Default)]
pub struct CommandBuilder {
    tokens: Vec<String>,
}

impl CommandBuilder {
    #[cfg(not(target_os = "windows"))]
    const EXTENSION: &'static str = "";
    #[cfg(target_os = "windows")]
    const EXTENSION: &'static str = ".exe";

    #[cfg(not(windows))]
    const POSITIONAL: &'static str = "--";
    #[cfg(windows)]
    const POSITIONAL: &'static str = "--";

    pub fn new() -> Self {
        CommandBuilder { tokens: Vec::new() }
    }

    pub fn positional(mut self) -> Self {
        self.tokens.push(Self::POSITIONAL.to_string());
        self
    }

    pub fn command(mut self, command: String) -> Self {
        self.tokens.push(command);
        self
    }

    pub fn binary(mut self, binary: String) -> Self {
        self.tokens.push(format!("{}{}", binary, Self::EXTENSION));
        self
    }

    pub fn argument(mut self, prefix: ArgumentPrefix, key: &str, value: Option<&str>) -> Self {
        match prefix {
            ArgumentPrefix::None => {
                self.tokens.push(key.to_string());
            }
            ArgumentPrefix::Short => {
                #[cfg(not(windows))]
                self.tokens.push(format!("-{key}"));
                #[cfg(windows)]
                self.tokens.push(format!("-{}", key));
            }
            ArgumentPrefix::Long => {
                #[cfg(not(windows))]
                self.tokens.push(format!("--{key}"));
                #[cfg(windows)]
                self.tokens.push(format!("--{}", key));
            }
        }
        if let Some(content) = value {
            self.tokens.push(content.to_string());
        }
        self
    }

    pub fn build(self) -> String {
        self.tokens.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource::scripting::ArgumentPrefix::{Long, Short};

    #[test]
    fn test_positional() {
        let command = CommandBuilder::new().positional().build();
        assert_eq!(command, "--");
    }

    #[test]
    fn test_argument_with_value_long() {
        let command = CommandBuilder::new()
            .argument(Long, "key", Some("value"))
            .build();
        assert_eq!(command, "--key value");
    }

    #[test]
    fn test_argument_with_value_short() {
        let command = CommandBuilder::new()
            .argument(Short, "k", Some("value"))
            .build();
        assert_eq!(command, "-k value");
    }

    #[test]
    fn test_argument_without_value_long() {
        let command = CommandBuilder::new().argument(Long, "key", None).build();
        assert_eq!(command, "--key");
    }

    #[test]
    fn test_argument_without_value_short() {
        let command = CommandBuilder::new().argument(Short, "k", None).build();
        assert_eq!(command, "-k");
    }

    #[test]
    fn test_multiple_arguments() {
        let command = CommandBuilder::new()
            .argument(Long, "key1", Some("value1"))
            .argument(Short, "k2", Some("value2"))
            .build();
        assert_eq!(command, "--key1 value1 -k2 value2");
    }

    #[test]
    fn test_positional_and_arguments() {
        let command = CommandBuilder::new()
            .positional()
            .argument(Long, "key", Some("value"))
            .argument(Short, "k", None)
            .build();
        assert_eq!(command, "-- --key value -k");
    }

    #[test]
    fn test_command() {
        let command = CommandBuilder::new()
            .command("/usr/bin/ls".to_string())
            .build();
        assert_eq!(command, "/usr/bin/ls");
    }

    #[test]
    fn test_binary() {
        let command = CommandBuilder::new().binary("program".to_string()).build();

        #[cfg(target_os = "windows")]
        assert_eq!(command, "program.exe");

        #[cfg(not(target_os = "windows"))]
        assert_eq!(command, "program");
    }

    #[test]
    fn test_command_with_argument() {
        let command = CommandBuilder::new()
            .command("/usr/bin/ls".to_string())
            .argument(Short, "l", None)
            .build();
        assert_eq!(command, "/usr/bin/ls -l");
    }

    #[test]
    fn test_binary_with_argument() {
        let command = CommandBuilder::new()
            .binary("program".to_string())
            .argument(Long, "config", Some("settings.yaml"))
            .build();

        #[cfg(target_os = "windows")]
        assert_eq!(command, "program.exe --config settings.yaml");

        #[cfg(not(target_os = "windows"))]
        assert_eq!(command, "program --config settings.yaml");
    }

    #[test]
    fn test_empty_command() {
        let command = CommandBuilder::new().build();
        assert_eq!(command, "");
    }

    #[test]
    fn test_command_binary_and_arguments() {
        let command = CommandBuilder::new()
            .command("/usr/bin/ls".to_string())
            .binary("program".to_string())
            .argument(Long, "config", Some("settings.yaml"))
            .argument(Short, "v", None)
            .build();

        #[cfg(target_os = "windows")]
        assert_eq!(command, "/usr/bin/ls program.exe --config settings.yaml -v");

        #[cfg(not(target_os = "windows"))]
        assert_eq!(command, "/usr/bin/ls program --config settings.yaml -v");
    }
}
