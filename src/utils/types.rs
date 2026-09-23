use serde::{self, Deserialize, Serialize};

use crate::volume;


#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "lowercase")]
pub enum Message {
    Volume(volume::VolumeCommand),
    Other(OtherPayload),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OtherPayload {
    pub data: String,
}