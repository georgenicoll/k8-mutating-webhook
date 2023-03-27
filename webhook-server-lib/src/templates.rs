use serde::Deserialize;

use crate::resource::Resource;

#[derive(Clone)]
pub struct Template {
    pub resource: Resource<serde_json::Value>,
}

impl Template {

    fn apply_to(&self, resource: &Resource<serde_json::Value>) -> Option<Resource<serde_json::Value>> {
        if self.matches(resource) {
            return Some(resource.merge(&self.resource));
        };
        None
    }

    fn matches(&self, resource: &Resource<serde_json::Value>) -> bool {
        self.resource.api_version == resource.api_version &&
        self.resource.kind == resource.kind &&
        self.resource.metadata.as_ref().map(|meta| {
            meta.namespace.as_ref().map(|template_ns| {
                resource.metadata.as_ref().map(|rmeta| rmeta.namespace.as_ref()).flatten().map(|rns| {
                    template_ns == rns
                }).unwrap_or(false)
            }).unwrap_or(true)
            &&
            meta.labels.as_ref().map(|template_labels| {
                resource.metadata.as_ref().map(|rmeta| rmeta.labels.as_ref()).flatten().map(|rlabels| {
                    template_labels.iter().all(|(k, v)| rlabels.get(k) == Some(v))
                }).unwrap_or(false)
            }).unwrap_or(true)
            &&
            meta.annotations.as_ref().map(|template_annotations| {
                resource.metadata.as_ref().map(|rmeta| rmeta.annotations.as_ref()).flatten().map(|rannotations| {
                    template_annotations.iter().all(|(k, v)| rannotations.get(k) == Some(v))
                }).unwrap_or(false)
            }).unwrap_or(true)
        }).unwrap_or(true)
    }

}

#[derive(Clone)]
pub struct Templates {
    pub templates: Vec<Template>,
}

#[derive(Deserialize)]
struct ConfigTemplates {
    templates: Vec<Resource<serde_yaml::Value>>
}

impl Templates {
    pub fn len(&self) -> usize {
        self.templates.len()
    }

    pub fn apply_to(&self, target: &Resource<serde_json::Value>) -> Option<Resource<serde_json::Value>> {
        self.templates.iter()
            .filter_map(|template| template.apply_to(target))
            .next()
    }

    fn construct_templates(yaml: &str) -> Result<Templates, String> {
        let templates_result = serde_yaml::from_str(yaml)
            .map(|config_templates: ConfigTemplates| Templates {
                    templates: config_templates.templates.iter()
                        .map(|resource| Template { resource: resource.convert_to_json() })
                        .collect()
            });
        match templates_result {
            Ok(templates) => Ok(templates),
            Err(err) => Err(err.to_string()),
        }
    }

    pub fn from_file(file_name: &str) -> Result<Templates, String> {
      std::fs::read_to_string(file_name)
        .map_err(|err| err.to_string())
        .and_then(|s| Self::construct_templates(&s))
    }

}

#[cfg(test)]
mod tests {

    use crate::resource::Resource;
    use crate::templates::Templates;

    #[test]
    fn load_and_apply_config() {
        let templates = create_test_templates();
        assert_eq!(2, templates.len(), "length was unexpected");
        let pod = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        spec:
          containers:
          - name: BOB
            image: docker.hub/bob
          - name: TOM
            image: docker.hub/tom
        "#).unwrap().convert_to_json();
        let expected = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        spec:
          containers:
          - name: BOB
            image: docker.hub/bob
            env:
            - name: BOB
              value: A_JOB
          - name: TOM
            image: docker.hub/tom
            env:
            - name: BOB
              value: A_JOB
        "#).unwrap().convert_to_json();
        let actual = templates.apply_to(&pod);
        assert_eq!(Some(expected), actual, "actual didn't match expected");
    }

    #[test]
    fn no_matches() {
        let templates = create_test_templates();
        assert_eq!(2, templates.len());
        let pod = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Deployment
        spec:
          containers:
          - name: BOB
            image: docker.hub/bob
          - name: TOM
            image: docker.hub/tom
        "#).unwrap().convert_to_json();
        let actual = templates.apply_to(&pod);
        assert_eq!(None, actual);
    }

    #[test]
    fn filters_by_namespace() {
        let yaml = r#"
        templates:
        - apiVersion: v1
          kind: Pod
          metadata:
            namespace: tv
        "#;
        let templates = Templates::construct_templates(yaml).unwrap();
        let matching = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          name: my-pod
          namespace: tv
        "#).unwrap().convert_to_json();
        let non_matching = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          name: my-pod
          namespace: radio
        "#).unwrap().convert_to_json();
        assert_eq!(true, templates.apply_to(&matching).is_some());
        assert_eq!(true, templates.apply_to(&non_matching).is_none());
    }

    #[test]
    fn filters_by_annotation() {
        let yaml = r#"
        templates:
        - apiVersion: v1
          kind: Pod
          metadata:
            annotations:
              type: server
        "#;
        let templates = Templates::construct_templates(yaml).unwrap();
        let matching = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          annotations:
            type: server
        "#).unwrap().convert_to_json();
        let non_matching1 = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          annotations:
            type: client
        "#).unwrap().convert_to_json();
        let non_matching2 = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          annotations:
            something_else: thing
        "#).unwrap().convert_to_json();
        assert_eq!(true, templates.apply_to(&matching).is_some());
        assert_eq!(true, templates.apply_to(&non_matching1).is_none());
        assert_eq!(true, templates.apply_to(&non_matching2).is_none());
    }

    #[test]
    fn filters_by_label() {
        let yaml = r#"
        templates:
        - apiVersion: v1
          kind: Pod
          metadata:
            labels:
              app: web-server
        "#;
        let templates = Templates::construct_templates(yaml).unwrap();
        let matching = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          labels:
            app: web-server
        "#).unwrap().convert_to_json();
        let non_matching1 = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          labels:
            app: database
        "#).unwrap().convert_to_json();
        let non_matching2 = Resource::from_yaml(r#"
        apiVersion: v1
        kind: Pod
        metadata:
          labels:
            application: messaging
        "#).unwrap().convert_to_json();
        assert_eq!(true, templates.apply_to(&matching).is_some());
        assert_eq!(true, templates.apply_to(&non_matching1).is_none());
        assert_eq!(true, templates.apply_to(&non_matching2).is_none());
    }

    fn create_test_templates() -> Templates {
        let yaml = r#"
        templates:
        - apiVersion: v1
          kind: Pod
          spec:
            containers:
              env:
              - name: BOB
                value: A_JOB
        - apiVersion: v1
          kind: Service
          spec:
            ports:
            - port: 19999
              targetPort: 9999
              protocl: TCP
              name: admin
        "#;
        Templates::construct_templates(yaml).unwrap()
    }

    #[test]
    fn load_from_file() {
      let path = std::env::current_dir().unwrap();
      println!("Current Directory: {}", path.display());
      let templates = Templates::from_file("example-templates.yaml").unwrap();
      assert_eq!(2, templates.len());
      let pob = Resource::from_yaml(r#"
      apiVersion: v2
      kind: Pob
      metadata:
        name: Pobbly
        namespace: not-default
        labels:
          sex: male
          ages: 22
        annotations:
          io.kube.label1: silly
      "#).unwrap().convert_to_json();
      let barb = Resource::from_yaml(r#"
      apiVersion: v2
      kind: Barb
      metadata:
        name: Wire
        namespace: fences
        labels:
          material: steel
        annotations:
          io.kube.label1: scratchy
      "#).unwrap().convert_to_json();
      assert_eq!(templates.templates[0].resource, pob);
      assert_eq!(templates.templates[1].resource, barb);
    }

}