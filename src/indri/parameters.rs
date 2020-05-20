//! Indri's XML Parameters format
use crate::Error;
use crate::{HashMap, HashSet};
use roxmltree::*;
use std::fs;

#[derive(Debug)]
pub enum Parameters {
    Value(String),
    List(String, Vec<Parameters>),
    Dict(HashMap<String, Parameters>),
}

impl Parameters {
    pub fn load(path: &str) -> Result<Parameters, Error> {
        let text = fs::read_to_string(path)?;
        let document = Document::parse(&text)?;
        let elem = document.root_element();
        if elem.tag_name().name() != "parameters" {
            return Err(Error::BadParameters.with_context(format!(
                "Indri Parameter XML should start with <parameters> root. found <{}> instead.",
                elem.tag_name().name()
            )));
        }
        Ok(parse(elem)?)
    }
    pub fn value(&self) -> Option<&str> {
        match self {
            Parameters::Value(it) => Some(it.as_str()),
            Parameters::List(_, _) => None,
            Parameters::Dict(_) => None,
        }
    }
    pub fn get(&self, key: &str) -> Option<&Parameters> {
        match self {
            Parameters::Value(_) => None,
            Parameters::List(_, _) => None,
            Parameters::Dict(items) => items.get(key),
        }
    }
}

/// Recursively interpret XML dom as Indri's parameters.
fn parse<'xml, 'input>(elem: Node<'xml, 'input>) -> Result<Parameters, Error> {
    let mut value = String::new();
    let mut children: Vec<(&'xml str, Parameters)> = Vec::new();

    for child in elem.children() {
        match child.node_type() {
            NodeType::Root => panic!("Child of something is root. {:?}", child),
            NodeType::Element => {
                let name = child.tag_name().name();
                let value = parse(child)?;
                children.push((name, value));
            }
            NodeType::PI | NodeType::Comment => continue,
            NodeType::Text => {
                if let Some(text) = child.text() {
                    value.push_str(text)
                }
            }
        }
    }

    if children.len() == 0 {
        return Ok(Parameters::Value(value.trim().to_string()));
    }

    let keys = children
        .iter()
        .map(|(name, _)| name)
        .cloned()
        .collect::<HashSet<&str>>();
    if keys.len() == 1 {
        // this is an array; repeated XML children with the same name:
        let key = keys.into_iter().nth(0).unwrap();
        return Ok(Parameters::List(
            key.to_string(),
            children.into_iter().map(|(_, val)| val).collect(),
        ));
    }
    // This should be a dictionary, but that means no repeated children.
    if keys.len() != children.len() {
        let mut seen = HashSet::default();
        let mut repeated = String::new();
        for key in children.iter().map(|(k, _)| k) {
            if seen.contains(key) {
                repeated = key.to_string();
                break;
            }
            seen.insert(key);
        }
        return Err(
            Error::BadParameters.with_context(format!("Repeated Children in XML: {}", repeated))
        );
    }
    let mut dict = HashMap::default();
    for (key, val) in children {
        dict.insert(key.to_string(), val);
    }
    Ok(Parameters::Dict(dict))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifests() {
        let manifest = Parameters::load("data/index.indri/manifest").unwrap();
        println!("{:#?}", manifest);
        assert_eq!(
            Some("1"),
            manifest.get("indexCount").and_then(|it| it.value())
        );
    }
}
