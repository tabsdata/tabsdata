//
// Copyright 2024 Tabs Data Inc.
//

use crate::resource::state::StateError::{
    ErrorReadingFile, InvalidYaml, InvalidYamlEntry, InvalidYamlStructure, MisplacedEndTag,
    MissingEndTag, MissingStartTag,
};
use crate::services::supervisor::SetState;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use getset::{Getters, MutGetters};
use serde::{Deserialize, Serialize};
use serde_yaml::{self, Value};
use std::collections::HashMap;
use std::convert::{From, Into};
use std::path::PathBuf;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::{fmt, fs};
use strum::{AsRefStr, EnumString};
use td_error::{TdError, td_error};
use tokio::sync::RwLock;

const START_TAG: &str = "<message><i>";
const END_TAG: &str = "<message><f>";

#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumString, AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum StateDataKind {
    Blob,
    #[default]
    Map,
}

#[derive(Debug, Clone)]
pub enum StateDataValue {
    Blob(EncodedBlob),
    Map(EncodedMap),
}

// Blob

#[derive(Clone, Debug)]
pub struct EncodedBlob {
    encoded: String,
}

impl EncodedBlob {
    pub fn new(blob: &str) -> Self {
        let encoded = Self::encode(blob);
        EncodedBlob { encoded }
    }

    pub fn encode(blob: &str) -> String {
        blob.to_string()
        /*
        encode64(blob)
        */
    }

    pub fn decode(&self) -> Result<String, StateError> {
        /*
        Ok(decode64(&self.encoded))
        */
        Ok(self.encoded.clone())
    }
}

impl From<String> for EncodedBlob {
    fn from(blob: String) -> Self {
        EncodedBlob::new(&blob)
    }
}

impl From<&str> for EncodedBlob {
    fn from(blob: &str) -> Self {
        EncodedBlob::new(blob)
    }
}

impl From<EncodedBlob> for StateDataValue {
    fn from(encoded: EncodedBlob) -> Self {
        StateDataValue::Blob(encoded)
    }
}

impl TryFrom<EncodedBlob> for String {
    type Error = StateError;

    fn try_from(encoded: EncodedBlob) -> Result<Self, Self::Error> {
        EncodedBlob::decode(&encoded)
    }
}

impl fmt::Display for EncodedBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let decoded = (self.clone().try_into() as Result<String, _>)
            .unwrap_or_else(|_| "<invalid base64 data>".to_string());
        write!(f, "Encoded: '{}' - Decoded: '{}'", self.encoded, decoded)
    }
}

// Map

#[derive(Clone, Debug)]
pub struct EncodedMap {
    encoded: HashMap<String, String>,
}

impl EncodedMap {
    pub fn new(plain: &HashMap<String, String>) -> Self {
        let encoded = Self::encode(plain);
        Self { encoded }
    }

    pub fn encode(map: &HashMap<String, String>) -> HashMap<String, String> {
        map.clone()
    }

    pub fn decode(&self) -> Result<HashMap<String, String>, StateError> {
        Ok(self.encoded.clone())
    }

    pub fn serialize(map: &HashMap<String, String>) -> Result<String, StateError> {
        match serde_yaml::to_string(map) {
            Ok(yaml) => Ok(yaml),
            Err(error) => Err(InvalidYaml(error)),
        }
    }

    pub fn deserialize(string: String) -> Result<HashMap<String, String>, StateError> {
        match serde_yaml::from_str(&string) {
            Ok(map) => Ok(map),
            Err(e) => Err(InvalidYaml(e)),
        }
    }
}

impl From<HashMap<String, String>> for EncodedMap {
    fn from(map: HashMap<String, String>) -> Self {
        EncodedMap::new(&map)
    }
}

impl From<&HashMap<String, String>> for EncodedMap {
    fn from(map: &HashMap<String, String>) -> Self {
        EncodedMap::new(map)
    }
}

impl From<EncodedMap> for StateDataValue {
    fn from(encoded: EncodedMap) -> Self {
        StateDataValue::Map(encoded)
    }
}

impl TryFrom<EncodedMap> for HashMap<String, String> {
    type Error = StateError;

    fn try_from(encoded: EncodedMap) -> Result<Self, Self::Error> {
        EncodedMap::decode(&encoded)
    }
}

impl TryFrom<EncodedMap> for String {
    type Error = StateError;

    fn try_from(encoded: EncodedMap) -> Result<Self, Self::Error> {
        let map = EncodedMap::decode(&encoded)?;
        EncodedMap::serialize(&map)
    }
}

// State

#[derive(Debug, Getters, MutGetters)]
pub struct State {
    #[getset(get = "pub", get_mut = "pub")]
    data: HashMap<SetState, StateDataValue>,
}

pub type SupervisorState = Arc<RwLock<State>>;

impl State {
    pub fn new() -> SupervisorState {
        Arc::new(RwLock::new(State {
            data: HashMap::new(),
        }))
    }
}

pub fn extract_state_data_from_file(
    path: PathBuf,
    kind: StateDataKind,
) -> Result<StateDataValue, TdError> {
    let content = fs::read_to_string(&path).map_err(ErrorReadingFile)?;
    extract_state_data_from_string(content, kind)
}

pub fn extract_state_data_from_string(
    content: String,
    kind: StateDataKind,
) -> Result<StateDataValue, TdError> {
    let start_index = content.find(START_TAG).ok_or(MissingStartTag)? + START_TAG.len();
    let end_index = content.find(END_TAG).ok_or(MissingEndTag)?;
    if start_index > end_index {
        return Err(MisplacedEndTag.into());
    }
    let raw = content[start_index..end_index].trim();

    match kind {
        StateDataKind::Blob => Ok(StateDataValue::Blob(raw.to_string().into())),
        StateDataKind::Map => {
            let yaml: Value = serde_yaml::from_str(raw).map_err(InvalidYaml)?;
            if let Value::Mapping(mapping) = yaml {
                let mut map = HashMap::new();
                for (key, value) in mapping {
                    let key_str = match key {
                        Value::String(ref s) => s.clone(),
                        Value::Number(ref n) => n.to_string(),
                        Value::Bool(ref b) => b.to_string(),
                        _ => return Err(InvalidYamlEntry.into()),
                    };
                    let value_str = match value {
                        Value::String(ref s) => s.clone(),
                        Value::Number(ref n) => n.to_string(),
                        Value::Bool(ref b) => b.to_string(),
                        Value::Null => "".to_string(),
                        Value::Mapping(_) | Value::Sequence(_) => {
                            return Err(InvalidYamlStructure.into());
                        }
                        _ => return Err(InvalidYamlEntry.into()),
                    };
                    map.insert(key_str, value_str);
                }
                Ok(StateDataValue::Map(map.into()))
            } else {
                Err(InvalidYamlStructure.into())
            }
        }
    }
}

pub fn encode64(string: &str) -> String {
    URL_SAFE_NO_PAD.encode(string)
}

pub fn decode64(string: &str) -> Result<String, StateError> {
    let decoded_bytes = URL_SAFE_NO_PAD.decode(string)?;
    let decoded_string = String::from_utf8(decoded_bytes)?;
    Ok(decoded_string)
}

#[td_error]
pub enum StateError {
    #[error("Error reading State file : {0}")]
    ErrorReadingFile(#[source] std::io::Error) = 5001,
    #[error("State contents do not contain the initial tag: '{}'", START_TAG)]
    MissingStartTag = 5002,
    #[error("State contents do not contain the final tag: '{}'", START_TAG)]
    MissingEndTag = 5003,
    #[error("End tag begins before start tag end")]
    MisplacedEndTag = 5004,
    #[error("Error reading yaml contents: {0}")]
    InvalidYaml(#[source] serde_yaml::Error) = 5005,
    #[error("The YAML content contains non string key: value entries")]
    InvalidYamlEntry = 5006,
    #[error("The YAML content is not in strict string key: value format")]
    InvalidYamlStructure = 5007,
    #[error("Base64 decode failed: {0}")]
    DecodeError(#[from] base64::DecodeError) = 5008,
    #[error("UTF-8 decode failed: {0}")]
    Utf8Error(#[from] FromUtf8Error) = 5009,
    #[error("Missing key in state: {:?}", key)]
    MissingStateKey { key: SetState } = 5010,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_success() {
        let content = "something before<message><i>δῶς μοι πᾶ στῶ καὶ τὰν γᾶν κινάσω<message><f>something after";
        let result =
            extract_state_data_from_string(content.to_string(), StateDataKind::Blob).unwrap();
        match result {
            StateDataValue::Blob(s) => {
                assert_eq!(s.decode().unwrap(), "δῶς μοι πᾶ στῶ καὶ τὰν γᾶν κινάσω")
            }
            _ => panic!("Expected Blob"),
        }
    }

    #[test]
    fn test_map_success() {
        let content = "<message><i>\nkey1: value1\nkey2: value2\n<message><f>";
        let result =
            extract_state_data_from_string(content.to_string(), StateDataKind::Map).unwrap();
        match result {
            StateDataValue::Map(map) => {
                let mapping = map.decode().unwrap();
                assert_eq!(mapping.get("key1"), Some(&"value1".to_string()));
                assert_eq!(mapping.get("key2"), Some(&"value2".to_string()));
            }
            _ => panic!("Expected Map"),
        }
    }

    #[test]
    fn test_missing_start_tag() {
        let content = "<message><f>δῶς μοι πᾶ στῶ καὶ τὰν γᾶν κινάσω";
        let err =
            extract_state_data_from_string(content.to_string(), StateDataKind::Blob).unwrap_err();
        let err = err.domain_err::<StateError>();
        assert!(
            matches!(err, MissingStartTag),
            "Expected StateError::MissingStartTag; got: {err:?} instead."
        );
    }

    #[test]
    fn test_missing_end_tag() {
        let content = "<message><i>δῶς μοι πᾶ στῶ καὶ τὰν γᾶν κινάσω";
        let err =
            extract_state_data_from_string(content.to_string(), StateDataKind::Blob).unwrap_err();
        let err = err.domain_err::<StateError>();
        assert!(
            matches!(err, MissingEndTag),
            "Expected StateError::MissingEndTag; got: {err:?} instead."
        );
    }

    #[test]
    fn test_misplaced_tags() {
        let content = "<message><f>δῶς μοι πᾶ στῶ καὶ τὰν γᾶν κινάσω<message><i>";
        let err =
            extract_state_data_from_string(content.to_string(), StateDataKind::Blob).unwrap_err();
        let err = err.domain_err::<StateError>();
        assert!(
            matches!(err, MisplacedEndTag),
            "Expected StateError::MisplacedEndTag; got: {err:?} instead."
        );
    }

    #[test]
    fn test_invalid_yaml_structure() {
        let content = "<message><i>- not: a map<message><f>";
        let err =
            extract_state_data_from_string(content.to_string(), StateDataKind::Map).unwrap_err();
        let err = err.domain_err::<StateError>();
        assert!(
            matches!(err, InvalidYamlStructure),
            "Expected StateError::InvalidYamlStructure; got: {err:?} instead."
        );
    }

    #[test]
    fn test_invalid_yaml_entry_type() {
        let content = "<message><i>\n123:\n   abc: def <message><f>";
        let err =
            extract_state_data_from_string(content.to_string(), StateDataKind::Map).unwrap_err();
        let err = err.domain_err::<StateError>();
        assert!(
            matches!(err, InvalidYamlStructure),
            "Expected StateError::InvalidYamlStructure; got: {err:?} instead."
        );
    }
}
