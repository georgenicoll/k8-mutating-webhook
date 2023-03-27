use log::warn;
use serde::{Serialize, Deserialize};
use std::fmt;
use std::collections::BTreeMap;

type MapType = BTreeMap<String, String>;

#[serde_with::skip_serializing_none]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    name: Option<String>,
    pub namespace: Option<String>,
    pub labels: Option<MapType>,
    pub annotations: Option<MapType>,
}

fn write_string_thing(f: &mut fmt::Formatter<'_>, opt: &Option<String>) -> fmt::Result {
    opt.as_ref().map(|n| write!(f, "{},", n)).unwrap_or_else(|| write!(f, ""))
}

impl fmt::Display for ObjectMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "M(")
            .and_then(|_| write_string_thing(f, &self.name))
            .and_then(|_| write_string_thing(f, &self.namespace))
            .and_then(|_| write!(f, "{:?},{:?})", self.labels, self.annotations))
    }
}

#[serde_with::skip_serializing_none]
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Resource<T> {
    pub api_version: String,
    pub kind: String,
    pub metadata: Option<ObjectMeta>,
    spec: Option<T>,
}

impl <T: fmt::Debug> fmt::Display for Resource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "R(apVersion:{},kind:{},metadata:{:?},spec:{:?})", self.api_version, self.kind, self.metadata, self.spec)
    }
}

impl <T: Clone> Resource<T> {

    pub fn new(api_version: &str, kind: &str) -> Resource<T> {
        Resource {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            metadata: None,
            spec: None,
        }
    }

    fn internal_merge(&self, other: &Resource<T>, merge_values: fn (&T, &T) -> T) -> Resource<T> {
        Resource {
            api_version: self.api_version.clone(),
            kind: self.kind.clone(),
            metadata: merge_meta(&self.metadata, &other.metadata),
            spec: merge_opt_values(&self.spec, &other.spec, merge_values),
        }
    }

}

impl Resource<serde_json::Value> {

    pub fn from_json(rep: &str) -> serde_json::Result<Resource<serde_json::Value>> {
        serde_json::from_str(rep)
    }

    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    pub fn merge(&self, other: &Resource<serde_json::Value>) -> Resource<serde_json::Value> {
        self.internal_merge(other, Self::merge_values)
    }

    fn merge_values(first: &serde_json::Value, second: &serde_json::Value) -> serde_json::Value {
        match (first, second) {
            (serde_json::Value::Bool(b1), serde_json::Value::Bool(_)) =>
                serde_json::Value::Bool(b1.clone()),
            (serde_json::Value::Number(n1), serde_json::Value::Number(_)) =>
                serde_json::Value::Number(n1.clone()),
            (serde_json::Value::String(s1), serde_json::Value::String(_)) =>
                serde_json::Value::String(s1.clone()),
            (serde_json::Value::Array(vec1), serde_json::Value::Array(vec2)) =>
                merge_arrays(vec1, vec2, Self::construct_array_wrapper),
            (serde_json::Value::Array(vec), _) =>
                merge_value_into_array(vec, second, Self::merge_values, Self::construct_array_wrapper),
            (serde_json::Value::Object(map1), serde_json::Value::Object(map2)) =>
                Self::merge_object_maps(map1, map2),
            (&serde_json::Value::Null, _) =>
                second.clone(),
            (_, &serde_json::Value::Null) =>
                first.clone(),
            (_, _) => {
                log::warn!("Different object types encountered - dropping second: {} != {}", first, second);
                first.clone()
            }
        }
    }

    fn construct_array_wrapper(array: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::Value::Array(array)
    }

    fn merge_object_maps(first: &serde_json::Map<String, serde_json::Value>, second: &serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
        let mut new_map = serde_json::Map::with_capacity(first.len() + second.len());
        //Add all in the first
        for (key, v1) in first.iter() {
            let new_value = second.get(key).map(|v2| Self::merge_values(v1, v2)).unwrap_or(v1.clone());
            new_map.insert(key.clone(), new_value);
        }
        //Add any in the second not already added
        for (key, v2) in second.iter() {
            if !new_map.contains_key(key) {
                new_map.insert(key.clone(), v2.clone());
            }
        }
        serde_json::Value::Object(new_map)
    }

}

impl Resource<serde_yaml::Value> {

    pub fn from_yaml(rep: &str) -> serde_yaml::Result<Resource<serde_yaml::Value>> {
        serde_yaml::from_str(rep)
    }

    pub fn to_yaml(&self) -> serde_yaml::Result<String> {
        serde_yaml::to_string(self)
    }

    pub fn merge(&self, other: &Resource<serde_yaml::Value>) -> Resource<serde_yaml::Value> {
        self.internal_merge(other, Self::merge_values)
    }

    fn merge_values(first: &serde_yaml::Value, second: &serde_yaml::Value) -> serde_yaml::Value {
        match (first, second) {
            (serde_yaml::Value::Bool(b1), serde_yaml::Value::Bool(_)) =>
                serde_yaml::Value::Bool(b1.clone()),
            (serde_yaml::Value::Number(n1), serde_yaml::Value::Number(_)) =>
                serde_yaml::Value::Number(n1.clone()),
            (serde_yaml::Value::String(s1), serde_yaml::Value::String(_)) =>
                serde_yaml::Value::String(s1.clone()),
            (serde_yaml::Value::Sequence(seq1), serde_yaml::Value::Sequence(seq2)) =>
                merge_arrays(seq1, seq2, Self::construct_array_wrapper),
            (serde_yaml::Value::Mapping(map1), serde_yaml::Value::Mapping(map2)) =>
                Self::merge_mappings(map1, map2),
            (serde_yaml::Value::Tagged(box1), serde_yaml::Value::Tagged(_)) =>
                serde_yaml::Value::Tagged(box1.clone()),
            (&serde_yaml::Value::Null, _) => second.clone(),
            (_, &serde_yaml::Value::Null) => first.clone(),
            (_, _) => {
                 log::warn!("Different object types encountered - dropping second: {:?} != {:?}", first, second);
                 first.clone()
           }
        }
    }

    fn construct_array_wrapper(array: Vec<serde_yaml::Value>) -> serde_yaml::Value {
        serde_yaml::Value::Sequence(array)
    }

    fn merge_mappings(first: &serde_yaml::Mapping, second: &serde_yaml::Mapping) -> serde_yaml::Value {
        let mut new_map = serde_yaml::Mapping::with_capacity(first.len() + second.len());
        //Add all in the first
        for (key, v1) in first.iter() {
            let new_value = second.get(key).map(|v2| Self::merge_values(v1, v2)).unwrap_or(v1.clone());
            new_map.insert(key.clone(), new_value);
        }
        //Add any in the second not already added
        for (key, v2) in second.iter() {
            if !new_map.contains_key(key) {
                new_map.insert(key.clone(), v2.clone());
            }
        }
        serde_yaml::Value::Mapping(new_map)
    }

    pub fn convert_to_json(&self) -> Resource<serde_json::Value> {
        Resource {
            api_version: self.api_version.clone(),
            kind: self.kind.clone(),
            metadata: self.metadata.clone(),
            spec: self.spec.as_ref().map(Self::convert_value_to_json).flatten()
        }
    }

    fn convert_value_to_json(yaml: &serde_yaml::Value) -> Option<serde_json::Value> {
        match yaml {
            serde_yaml::Value::Bool(b) =>
                Some(serde_json::Value::Bool(b.clone())),
            serde_yaml::Value::Number(n) =>
                Self::convert_number_to_json(n).map(|n| serde_json::Value::Number(n)),
            serde_yaml::Value::String(s) =>
                Some(serde_json::Value::String(s.clone())),
            serde_yaml::Value::Sequence(seq) => {
                let items: Vec<serde_json::Value> = seq.iter()
                    .filter_map(|v| Self::convert_value_to_json(v))
                    .collect();
                Some(serde_json::Value::Array(items))
            },
            serde_yaml::Value::Mapping(map) => {
                let new_map = map.iter().fold(serde_json::Map::new(), |mut acc, (key, value)| {
                    if let Some((k, v)) = Self::convert_value_to_string(key)
                        .map(|k| Self::convert_value_to_json(value).map(|v| (k, v)))
                        .flatten() {
                            acc.insert(k, v);
                        }
                    acc
                });
                Some(serde_json::Value::Object(new_map))
            },
            serde_yaml::Value::Tagged(_) => {
                warn!("Dropping {:?} - Tagged is not supported in json", yaml);
                None
            },
            serde_yaml::Value::Null =>
                Some(serde_json::Value::Null),
        }

    }

    fn convert_number_to_json(yaml: &serde_yaml::Number) -> Option<serde_json::Number> {
        None.or_else(|| yaml.as_u64().map(|u| serde_json::Number::from(u)))
            .or_else(|| yaml.as_i64().map(|i| serde_json::Number::from(i)))
            .or_else(|| yaml.as_f64().map(|f| serde_json::Number::from_f64(f)).flatten())
    }

    fn convert_value_to_string(yaml: &serde_yaml::Value) -> Option<String> {
        match yaml {
            serde_yaml::Value::Bool(b) => Some(b.to_string()),
            serde_yaml::Value::Number(n) => Some(n.to_string()),
            serde_yaml::Value::String(s) => Some(s.clone()),
            _ => {
                warn!("Converting {:?} to String not supported", yaml);
                None
            },
        }
    }

}

fn merge_meta(first: &Option<ObjectMeta>, second: &Option<ObjectMeta>) -> Option<ObjectMeta> {
    first.as_ref().map(|f| {
        second.as_ref().map(|s| {
            ObjectMeta {
                name: f.name.clone(),
                namespace: f.namespace.clone(),
                labels: merge_maps(&f.labels, &s.labels),
                annotations: merge_maps(&f.annotations, &s.annotations),
            }
        }).unwrap_or(f.clone())
    }).or(second.clone())
}

fn merge_maps(first: &Option<MapType>, second: &Option<MapType>) -> Option<MapType> {
    first.as_ref().map(|f| {
        second.as_ref().map(|s| {
            f.iter().fold(s.clone(), |mut acc, (k, v)| {
                acc.insert(k.clone(), v.clone());
                acc
            })
        }).unwrap_or(f.clone())
    }).or(second.clone())
}

fn merge_opt_values<T: Clone>(first: &Option<T>, second: &Option<T>, merge_values: fn (&T, &T) -> T) -> Option<T> {
    first.as_ref().map(|v1| {
        second.as_ref().map(|v2| {
            merge_values(v1, v2)
        }).unwrap_or(v1.clone())
    }).or(second.clone())
}

fn merge_arrays<T: Clone>(first: &Vec<T>, second: &Vec<T>, construct_array_wrapper: fn (Vec<T>) -> T) -> T {
    let first_copies = first.iter()
        .fold(Vec::with_capacity(first.len() + second.len()), |mut acc, v| {
            acc.push(v.clone());
            acc
        });
    let new_vec = second.iter()
        .fold(first_copies, |mut acc, v| {
            acc.push(v.clone());
            acc
        });
    construct_array_wrapper(new_vec)
}

fn merge_value_into_array<T: Clone>(vec: &Vec<T>, value: &T,
    merge_values: fn (&T, &T) -> T, construct_array_wrapper: fn (Vec<T>) -> T) -> T {

    let merged_vec: Vec<T> = vec.iter().map(|item| merge_values(item, value)).collect();
    construct_array_wrapper(merged_vec)
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use crate::resource::ObjectMeta;

    use super::{MapType, Resource};

    static V1: Lazy<String> = Lazy::new(|| String::from("v1"));
    static POD: Lazy<String> = Lazy::new(|| String::from("Pod"));

    #[test]
    fn simple_json_from_string() {
        let json = r#"{
          "apiVersion": "v1beta1",
          "kind": "Service"
        }"#;
        let resource = Resource::from_json(json).expect("was error");
        let expected = Resource::new("v1beta1", "Service");
        println!("Simple: {}", resource);
        assert_eq!(expected, resource);
    }

    #[test]
    fn simple_to_json() {
        let resource = Resource::new("v9", "Delia");
        let json = resource.to_json().expect("Failed to output to json");
        println!("Simple: {}", json);
        assert_eq!(r#"{"apiVersion":"v9","kind":"Delia"}"#, json);
    }

    #[test]
    fn json_with_metadata_serialize_and_deserialize() {
        let expected = bob_resource();
        let json = expected.to_json().unwrap();
        println!("Bob: {}", json);
        let resource = Resource::from_json(&json).unwrap();
        println!("Bob: {}", resource);
        assert_eq!(expected, resource);
    }

    #[test]
    fn json_with_metadata_and_spec_serialize_and_deserialize() {
        let expected = real_json_resource();
        let json = expected.to_json().unwrap();
        println!("Pod: {}", json);
        let resource = Resource::from_json(&json).unwrap();
        println!("Pod: {}", resource);
        assert_eq!(expected, resource);
    }

    #[test]
    fn merge_combines_both_no_labels_in_first_no_annotations_in_second_no_spec() {
        let first: Resource<serde_json::Value> = Resource{
            api_version: V1.clone(), kind: POD.clone(), spec: None,
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("my-pod")),
                namespace: Some(String::from("my-namespace")),
                labels: None,
                annotations: Some(MapType::from([
                    (String::from("annot_1"), String::from("1_annot")),
                    (String::from("annot_1_other"), String::from("1_other_annot"))
                ])),
            }),
        };
        let second = Resource {
            api_version: V1.clone(), kind: POD.clone(), spec: None,
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("my-pod")),
                namespace: Some(String::from("my-namespace")),
                labels: Some(MapType::from([
                    (String::from("in_2"), String::from("2_value")),
                    (String::from("in_2_other"), String::from("other_in_2"))
                ])),
                annotations: None,
            }),
        };
        println!("First A: {}", first);
        println!("Second A: {}", second);
        let merged = first.merge(&second);
        println!("Merged A: {}", merged);
        let expected = Resource {
            api_version: V1.clone(), kind: POD.clone(), spec: None,
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("my-pod")),
                namespace: Some(String::from("my-namespace")),
                labels: Some(MapType::from([
                    (String::from("in_2"), String::from("2_value")),
                    (String::from("in_2_other"), String::from("other_in_2"))
                ])),
                annotations: Some(MapType::from([
                    (String::from("annot_1"), String::from("1_annot")),
                    (String::from("annot_1_other"), String::from("1_other_annot"))
                ])),
            }),
        };
        assert_eq!(expected, merged);
    }

    #[test]
    fn merge_combines_both_meta_and_annotations_in_both_no_spec() {
        let first: Resource<serde_json::Value> = Resource {
            api_version: V1.clone(), kind: POD.clone(), spec: None,
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("my-pod")),
                namespace: Some(String::from("my-namespace")),
                labels: Some(MapType::from([
                    (String::from("in_1"), String::from("1_value")),
                    (String::from("in_both"), String::from("1_both_value"))
                ])),
                annotations: Some(MapType::from([
                    (String::from("annot_1"), String::from("1_annot")),
                    (String::from("annot_in_both"), String::from("1_both_annot"))
                ]))
            }),
        };
        let second = Resource {
            api_version: V1.clone(), kind: POD.clone(), spec: None,
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("my-pod")),
                namespace: Some(String::from("my-namespace")),
                labels: Some(MapType::from([
                    (String::from("in_2"), String::from("2_value")),
                    (String::from("in_both"), String::from("2_both_value"))
                ])),
                annotations: Some(MapType::from([
                    (String::from("annot_2"), String::from("2_annot")),
                    (String::from("annot_in_both"), String::from("2_both_annot"))
                ]))
            }),
        };
        println!("First B: {}", first);
        println!("Second B: {}", second);
        let merged = first.merge(&second);
        println!("Merged B: {}", merged);
        let expected = Resource {
            api_version: V1.clone(), kind: POD.clone(), spec: None,
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("my-pod")),
                namespace: Some(String::from("my-namespace")),
                labels: Some(MapType::from([
                    (String::from("in_1"), String::from("1_value")),
                    (String::from("in_both"), String::from("1_both_value")),
                    (String::from("in_2"), String::from("2_value")),
                ])),
                annotations: Some(MapType::from([
                    (String::from("annot_1"), String::from("1_annot")),
                    (String::from("annot_in_both"), String::from("1_both_annot")),
                    (String::from("annot_2"), String::from("2_annot")),
                ]))
            }),
        };
        assert_eq!(expected, merged);
    }

    #[test]
    fn merge_combine_json_both_specs() {
        let first = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(serde_json::json!({
                "first": "firstValue",
                "both-list": [ "one", "two", "three" ],
                "both-obj": {
                    "first-attr": "attr1",
                    "both-attr": "first-both-attr"
                }
            }))
        };
        println!("First with Spec: {}", first);
        let second = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(serde_json::json!({
                "second": "secondValue",
                "both-list": [ "four", "five" ],
                "both-obj": {
                    "second-attr": "attr2",
                    "both-attr": "second-both-attr"
                }
            }))
        };
        println!("Second with Spec: {}", second);
        let merged = first.merge(&second);
        println!("Merged with Spec: {}", merged);
        let expected = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(serde_json::json!({
                "first": "firstValue",
                "second": "secondValue",
                "both-list": [ "one", "two", "three", "four", "five" ],
                "both-obj": {
                    "first-attr": "attr1",
                    "second-attr": "attr2",
                    "both-attr": "first-both-attr"
                }
            }))
        };
        assert_eq!(expected, merged);
    }

    #[test]
    fn merge_single_to_multiple_json() {
        let first = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(serde_json::json!({
                "containers": [
                    {
                        "name": "zippy"
                    },
                    {
                        "name": "bungle"
                    }
                ]
            }))
        };
        let second = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(serde_json::json!({
                "containers": {
                    "smells": "terrible"
                }
            }))
        };
        let actual = first.merge(&second);
        let expected = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(serde_json::json!({
                "containers": [
                    {
                        "name": "zippy",
                        "smells": "terrible"
                    },
                    {
                        "name": "bungle",
                        "smells": "terrible"
                    }
                ]
            }))
        };
        assert_eq!(expected, actual);
    }

    fn bob_resource<T: Clone>() -> Resource<T> {
        Resource {
            api_version: String::from("v1"),
            kind: String::from("Pod"),
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("bob")),
                namespace: Some(String::from("hoskins")),
                labels: Some(MapType::from([
                    (String::from("job"), String::from("actor")),
                    (String::from("role"), String::from("robber")),
                ])),
                annotations: Some(MapType::from([
                    (String::from("height"), String::from("short")),
                    (String::from("shape"), String::from("round")),
                ]))
            }),
            spec: None,
        }
    }

    fn real_json_resource() -> Resource<serde_json::Value> {
        let spec = serde_json::json!({
            "containers": [
                {
                    "args": [
                        "/wasi_example_main.wasm",
                        "50000000"
                    ],
                    "image": "wasmedge/example-wasi:latest",
                    "name": "wasi-demo"
                }
            ],
            "restartPolicy": "Never",
            "nodeSelector": {
                "kwasm.sh/kwasm-provisioned": "kind-worker2"
            },
            "runtimeClassName": "crun"
        });
        Resource {
            api_version: V1.clone(),
            kind: POD.clone(),
            metadata: Some(super::ObjectMeta {
                name: Some(String::from("wasi-demo")),
                namespace: None,
                labels: Some(MapType::from([(String::from("run"), String::from("wasi-demo"))])),
                annotations: Some(MapType::from([(String::from("module.wasm.image/variant"), String::from("compat-smart"))])),
            }),
            spec: Some(spec),
        }
    }

    #[test]
    fn serialize_deserialize_yaml() {
        let start: Resource<serde_yaml::Value> = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(
                serde_yaml::from_str(r#"
                first: firstValue
                both-list:
                - one
                - two
                - three
                both-obj:
                  first-attr: attr1
                  both-attr: first-both-attr
                "#).unwrap()
            ),
        };
        let yaml = start.to_yaml().unwrap();
        println!("<-- yaml -->\n{}", yaml);
        let end = Resource::from_yaml(&yaml).unwrap();
        assert_eq!(start, end);
    }


    #[test]
    fn merge_combine_yaml_both_specs() {
        let first: Resource<serde_yaml::Value> = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(
                serde_yaml::from_str(r#"
                first: firstValue
                both-list:
                - one
                - two
                - three
                both-obj:
                  first-attr: attr1
                  both-attr: first-both-attr
                "#).unwrap()
            ),
        };
        println!("First with Spec: {}", first);
        let second = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(
                serde_yaml::from_str(r#"
                second: secondValue
                both-list:
                - four
                - five
                both-obj:
                  second-attr: attr2
                  both-attr: second-both-attr
                "#).unwrap()
            ),
        };
        println!("Second with Spec: {}", second);
        let merged = first.merge(&second);
        println!("Merged with Spec: {}", merged);
        let expected = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: None,
            spec: Some(
                serde_yaml::from_str(r#"
                first: firstValue
                second: secondValue
                both-list:
                - one
                - two
                - three
                - four
                - five
                both-obj:
                  first-attr: attr1
                  second-attr: attr2
                  both-attr: first-both-attr
                "#).unwrap()
            ),
        };
        assert_eq!(expected, merged);
    }


    #[test]
    fn convert_yaml_to_json() {
        let create_meta = || ObjectMeta {
                name: Some(String::from("Bob")),
                namespace: Some(String::from("Home")),
                annotations: Some(MapType::from([ (String::from("annot1"), String::from("value1")) ])),
                labels: Some(MapType::from([ (String::from("label1"), String::from("labelvalue1")) ])),
        };
        let yaml_based: Resource<serde_yaml::Value> = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: Some(create_meta()),
            spec: Some(
                serde_yaml::from_str(r#"
                first: firstValue
                second: secondValue
                both-list:
                - one
                - two
                - three
                - four
                - five
                both-obj:
                  first-attr: attr1
                  second-attr: attr2
                  both-attr: first-both-attr
                "#).unwrap()
            ),
        };
        let expected = Resource {
            api_version: V1.clone(), kind: POD.clone(), metadata: Some(create_meta()),
            spec: Some(serde_json::json!({
                "first": "firstValue",
                "second": "secondValue",
                "both-list": [ "one", "two", "three", "four", "five" ],
                "both-obj": {
                    "first-attr": "attr1",
                    "second-attr": "attr2",
                    "both-attr": "first-both-attr"
                }
            }))
        };
        let actual = yaml_based.convert_to_json();
        assert_eq!(expected, actual);
    }

}