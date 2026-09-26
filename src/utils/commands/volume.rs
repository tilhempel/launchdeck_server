use std::{borrow::Cow, process::Command, ptr::null};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Target {
    String(String),
    ID(u32)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", content = "data")]
pub enum VolumeCommand {
    ChangeSinkVolume { target: Target, delta: i32 },
    SetSinkVolume { target: Target, value: u8 },
    MuteSink { target: Target },
    UnmuteSink { target: Target },
    ToggleMuteSink { target: Target },
    ChangeSourceVolume { target: Target, delta: i32 },
    SetSourceVolume { target: Target, value: u8 },
    MuteSource { target: Target },
    UnmuteSource { target: Target },
    ToggleMuteSource { target: Target },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    pub name: String,
    pub description: String,
    pub id: u32,
    pub mute: Option<bool>,
    pub channel_volumes: Option<Vec<f32>>
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Application {
    pub binary: String,
    pub app_name: String,
    pub node_name: String,
    pub media_name: String,
    pub id: u32,
    pub mute: Option<bool>,
    pub channel_volumes: Option<Vec<f32>>
}
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum CombType {
    Device(Device),
    Application(Application),
}

// relative change
fn change_sink_volume(value: i32, target: Target) {
    let t: String;
    match target {
        Target::String(s) => {
            if s == "default" {
                t = "@DEFAULT_SINK@".to_string();
            } else {
                t = s;
            }
        }
        Target::ID(id) => t = id.to_string()
    }
    let _ = Command::new("pactl")
        .arg("set-sink-volume")
        .arg(t)
        .arg(format!("{:+.2}%", value)).status();
}

// absolute
fn set_sink_volume(value: u8, target: Target) {
    let t: String;
    match target {
        Target::String(s) => {
            if s == "default" {
                t = "@DEFAULT_SINK@".to_string();
            } else {
                t = s;
            }
        }
        Target::ID(id) => t = id.to_string()
    }
    let _ = Command::new("pactl")
        .arg("set-sink-volume")
        .arg(t)
        .arg(format!("{}%", value)).status();
}

fn mute_sink(mode: String, target: Target) {
    let t: String;
    match target {
        Target::String(s) => {
            if s == "default" {
                t = "@DEFAULT_SINK@".to_string();
            } else {
                t = s;
            }
        }
        Target::ID(id) => t = id.to_string()
    }
    let _ = Command::new("pactl")
        .arg("set-sink-mute")
        .arg(t)
        .arg(mode).status();
}

// relative change
fn change_source_volume(value: i32, target: Target) {
    let t: String;
    match target {
        Target::String(s) => {
            if s == "default" {
                t = "@DEFAULT_SOURCE@".to_string();
            } else {
                t = s;
            }
        }
        Target::ID(id) => t = id.to_string()
    }
    let _ = Command::new("pactl")
        .arg("set-source-volume")
        .arg(t)
        .arg(format!("{:+.2}%", value)).status();
}

// absolute
fn set_source_volume(value: u8, target: Target) {
    let t: String;
    match target {
        Target::String(s) => {
            if s == "default" {
                t = "@DEFAULT_SOURCE@".to_string();
            } else {
                t = s;
            }
        }
        Target::ID(id) => t = id.to_string()
    }
    let _ = Command::new("pactl")
        .arg("set-source-volume")
        .arg(t)
        .arg(format!("{}%", value)).status();
}

fn mute_source(mode: String, target: Target) {
    let t: String;
    match target {
        Target::String(s) => {
            if s == "default" {
                t = "@DEFAULT_SOURCE@".to_string();
            } else {
                t = s;
            }
        }
        Target::ID(id) => t = id.to_string()
    }
    let _ = Command::new("pactl")
        .arg("set-source-mute")
        .arg(t)
        .arg(mode).status();
}

pub fn get_default_sink() -> String {
    // first get the default sink
    let out = Command::new("pactl")
        .arg("get-default-sink")
        .output();

    match out {
        Ok(sink) => {
            if !sink.status.success() {
                eprintln!("Error getting default sink: {:#?}", String::from_utf8_lossy(&sink.stderr).to_string());
                return String::new();
            }
            return String::from_utf8_lossy(&sink.stdout).to_string();
        },
        Err(e) => {
            eprintln!("Error getting default sink: {}", e);
            return String::new();
        }
    };
}

pub fn get_default_source() -> String {
    let out = Command::new("pactl")
        .arg("get-default-source")
        .output();

    match out {
        Ok(source) => {
            if !source.status.success() {
                eprintln!("Error getting default source: {:#?}", String::from_utf8_lossy(&source.stderr).to_string());
                return String::new();
            }
            return String::from_utf8_lossy(&source.stdout).to_string();
        },
        Err(e) => {
            eprintln!("Error getting default source: {}", e);
            return String::new();
        }
    }
}

pub fn handle_volume(p: VolumeCommand) {
    match p {
        VolumeCommand::ChangeSinkVolume { delta, target } => change_sink_volume(delta, target),
        VolumeCommand::SetSinkVolume { value, target } => set_sink_volume(value, target),
        VolumeCommand::ToggleMuteSink { target } => mute_sink("toggle".to_string(), target),
        VolumeCommand::MuteSink { target } => mute_sink("true".to_string(), target),
        VolumeCommand::UnmuteSink { target } => mute_sink("false".to_string(), target),
        VolumeCommand::ChangeSourceVolume { delta , target} => change_source_volume(delta, target),
        VolumeCommand::SetSourceVolume { value , target } => set_source_volume(value, target),
        VolumeCommand::MuteSource { target } => mute_source("true".to_string(), target),
        VolumeCommand::UnmuteSource { target } => mute_source("false".to_string(), target),
        VolumeCommand::ToggleMuteSource { target } => mute_sink("toggle".to_string(), target),
    }
}