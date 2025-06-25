use std::{collections::HashMap, fmt};

#[derive(Clone)]
pub(crate) struct DataStore(HashMap<String, String>);

impl fmt::Debug for DataStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl DataStore {
    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }

    pub(crate) fn set(&mut self, key: String, value: String) {
        let _ = self.0.insert(key, value);
    }

    pub(crate) fn delete(&mut self, key: &str) {
        let _ = self.0.remove(key);
    }
}
