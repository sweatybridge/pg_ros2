//! Read-only snapshots of remote parameter services. No PostgreSQL calls while spinning.
use pgrx::prelude::*;
use rclrs::vendor::rcl_interfaces::{msg::ParameterValue, srv::*};
use rclrs::{Client, Node, Promise};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Parameter {
    node: String,
    namespace: String,
    name: String,
    kind: &'static str,
    value: Value,
}

fn value(parameter: ParameterValue) -> Result<(&'static str, Value), String> {
    Ok(match parameter.type_ {
        0 => ("not_set", Value::Null),
        1 => ("bool", json!(parameter.bool_value)),
        2 => ("integer", json!(parameter.integer_value)),
        3 => ("double", json!(parameter.double_value)),
        4 => ("string", json!(parameter.string_value)),
        5 => ("byte_array", json!(parameter.byte_array_value)),
        6 => ("bool_array", json!(parameter.bool_array_value)),
        7 => ("integer_array", json!(parameter.integer_array_value)),
        8 => ("double_array", json!(parameter.double_array_value)),
        9 => ("string_array", json!(parameter.string_array_value)),
        other => return Err(format!("unknown ROS parameter type {other}")),
    })
}

type Peer = (
    String,
    String,
    Client<ListParameters>,
    Client<GetParameters>,
);

enum Stage {
    Ready,
    Listing(
        Vec<Promise<ListParameters_Response>>,
        Vec<Option<ListParameters_Response>>,
    ),
    Getting(
        Vec<Promise<GetParameters_Response>>,
        Vec<Option<GetParameters_Response>>,
        Vec<Vec<String>>,
    ),
}

/// One poll advanced by the observer loop. No method spins or waits for a peer.
pub(crate) struct Poll {
    clients: Vec<Peer>,
    stage: Stage,
    deadline: Instant,
}

fn collect<T>(pending: &mut [Promise<T>], results: &mut [Option<T>]) -> Result<bool, String> {
    for (promise, result) in pending.iter_mut().zip(results.iter_mut()) {
        if result.is_none() {
            *result = promise.try_recv().map_err(|error| error.to_string())?;
        }
    }
    Ok(results.iter().all(Option::is_some))
}

impl Poll {
    pub(crate) fn start(node: &Node, nodes: &[(String, String)]) -> Result<Self, String> {
        let mut peers = nodes.to_vec();
        peers.sort();
        peers.dedup();
        let mut clients = Vec::new();
        for (name, namespace) in peers {
            let prefix = format!("{}/{name}", namespace.trim_end_matches('/'));
            let services = node
                .get_service_names_and_types_by_node(&name, &namespace)
                .map_err(|error| error.to_string())?;
            let list = format!("{prefix}/list_parameters");
            let get = format!("{prefix}/get_parameters");
            if !services.get(&list).is_some_and(|types| {
                types
                    .iter()
                    .any(|kind| kind == "rcl_interfaces/srv/ListParameters")
            }) {
                continue;
            }
            clients.push((
                name,
                namespace,
                node.create_client::<ListParameters>(list.as_str())
                    .map_err(|e| e.to_string())?,
                node.create_client::<GetParameters>(get.as_str())
                    .map_err(|e| e.to_string())?,
            ));
        }
        Ok(Self {
            clients,
            stage: Stage::Ready,
            deadline: Instant::now() + Duration::from_secs(3),
        })
    }

    pub(crate) fn tick(&mut self) -> Result<Option<Vec<Parameter>>, String> {
        if Instant::now() >= self.deadline {
            let stage = match self.stage {
                Stage::Ready => "service discovery",
                Stage::Listing(..) => "list",
                Stage::Getting(..) => "get",
            };
            return Err(format!("parameter {stage} timed out after 3 seconds"));
        }
        match &mut self.stage {
            Stage::Ready => {
                for (_, _, list, get) in &self.clients {
                    if !list.service_is_ready().map_err(|e| e.to_string())?
                        || !get.service_is_ready().map_err(|e| e.to_string())?
                    {
                        return Ok(None);
                    }
                }
                let pending = self
                    .clients
                    .iter()
                    .map(|(_, _, list, _)| {
                        list.call(&ListParameters_Request {
                            prefixes: vec![],
                            depth: 0,
                        })
                        .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.stage =
                    Stage::Listing(pending, (0..self.clients.len()).map(|_| None).collect());
                self.deadline = Instant::now() + Duration::from_secs(3);
            }
            Stage::Listing(pending, results) => {
                if !collect(pending, results)? {
                    return Ok(None);
                }
                let names: Vec<Vec<String>> = results
                    .iter_mut()
                    .map(|result| {
                        let mut names = result.take().unwrap().result.names;
                        names.sort();
                        names.dedup();
                        names
                    })
                    .collect();
                let pending = self
                    .clients
                    .iter()
                    .zip(&names)
                    .map(|((_, _, _, get), names)| {
                        get.call(&GetParameters_Request {
                            names: names.clone(),
                        })
                        .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.stage = Stage::Getting(
                    pending,
                    (0..self.clients.len()).map(|_| None).collect(),
                    names,
                );
                self.deadline = Instant::now() + Duration::from_secs(3);
            }
            Stage::Getting(pending, results, names) => {
                if !collect(pending, results)? {
                    return Ok(None);
                }
                let mut snapshot = Vec::new();
                for (((name, namespace, _, _), names), response) in
                    self.clients.iter().zip(names).zip(results)
                {
                    let response = response.take().unwrap();
                    if names.len() != response.values.len() {
                        return Err(format!(
                            "{namespace}/{name}: parameter response length mismatch"
                        ));
                    }
                    for (parameter_name, parameter_value) in names.iter().zip(response.values) {
                        let (kind, value) = value(parameter_value)?;
                        snapshot.push(Parameter {
                            node: name.clone(),
                            namespace: namespace.clone(),
                            name: parameter_name.clone(),
                            kind,
                            value,
                        });
                    }
                }
                return Ok(Some(snapshot));
            }
        }
        Ok(None)
    }
}
pub(crate) fn persist(
    snapshot: Result<&[Parameter], &str>,
    previous: Option<&[Parameter]>,
    previous_extension: Option<pg_sys::Oid>,
) -> Option<pg_sys::Oid> {
    let (oid, schema) = Spi::get_two::<pg_sys::Oid, String>(
        "SELECT e.oid, pg_catalog.quote_ident(n.nspname) \
         FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace \
         WHERE e.extname = 'pg_ros2'",
    )
    .unwrap();
    let (Some(oid), Some(schema)) = (oid, schema) else {
        return None;
    };
    let changed =
        snapshot.is_ok_and(|value| previous != Some(value) || previous_extension != Some(oid));
    if let Ok(parameters) = snapshot {
        if changed {
            Spi::run(&format!("LOCK TABLE {schema}.parameters IN SHARE ROW EXCLUSIVE MODE; DELETE FROM {schema}.parameters")).unwrap();
            for parameter in parameters {
                Spi::run_with_args(&format!(
                    "INSERT INTO {schema}.parameters VALUES ($1, $2, $3, $4, $5, statement_timestamp())"),
                    &[parameter.node.as_str().into(), parameter.namespace.as_str().into(),
                      parameter.name.as_str().into(), parameter.kind.into(),
                      pgrx::JsonB(parameter.value.clone()).into()]).unwrap();
            }
        }
    }
    Spi::run_with_args(&format!(
        "INSERT INTO {schema}.parameter_status AS s (singleton, worker_pid, last_checked, last_refreshed, last_error) \
         VALUES (true, pg_backend_pid(), statement_timestamp(), CASE WHEN $1 THEN statement_timestamp() END, $2) \
         ON CONFLICT (singleton) DO UPDATE SET worker_pid = EXCLUDED.worker_pid, \
         last_checked = EXCLUDED.last_checked, \
         last_refreshed = CASE WHEN $1 THEN EXCLUDED.last_refreshed ELSE s.last_refreshed END, \
         last_error = EXCLUDED.last_error"), &[changed.into(), snapshot.err().into()]).unwrap();
    Some(oid)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_parameter_types() {
        let expected = [
            Value::Null,
            json!(true),
            json!(i64::MAX),
            json!(1.5),
            json!("quote'\\\""),
            json!([0, 255]),
            json!([true, false]),
            json!([i64::MIN]),
            json!([1.5]),
            json!(["a", "b"]),
        ];
        for (kind, expected) in expected.into_iter().enumerate() {
            let parameter = ParameterValue {
                type_: kind as u8,
                bool_value: true,
                integer_value: i64::MAX,
                double_value: 1.5,
                string_value: "quote'\\\"".into(),
                byte_array_value: vec![0, 255],
                bool_array_value: vec![true, false],
                integer_array_value: vec![i64::MIN],
                double_array_value: vec![1.5],
                string_array_value: vec!["a".into(), "b".into()],
            };
            assert_eq!(value(parameter).unwrap().1, expected);
        }
        assert_eq!(
            value(ParameterValue {
                type_: 3,
                double_value: f64::NAN,
                ..Default::default()
            })
            .unwrap()
            .1,
            Value::Null
        );
        assert!(value(ParameterValue {
            type_: 255,
            ..Default::default()
        })
        .is_err());
    }

    #[pg_test]
    fn test_parameter_reconciliation() {
        let mut rows = vec![Parameter {
            node: "node'".into(),
            namespace: "/ns".into(),
            name: "nested.greeting".into(),
            kind: "string",
            value: json!("hello'\\\""),
        }];
        let oid = persist(Ok(&rows), None, None);
        assert!(oid.is_some());
        assert_eq!(
            Spi::get_one::<String>("SELECT value #>> '{}' FROM parameters").unwrap(),
            Some("hello'\\\"".into())
        );
        Spi::run("CREATE TEMP TABLE parameter_ctids AS SELECT ctid AS tid FROM parameters")
            .unwrap();
        persist(Ok(&rows), Some(&rows), oid);
        assert_eq!(Spi::get_one::<bool>("SELECT NOT EXISTS (SELECT ctid FROM parameters EXCEPT SELECT tid FROM parameter_ctids)").unwrap(), Some(true));
        persist(Err("unreachable peer"), Some(&rows), oid);
        assert_eq!(
            Spi::get_one::<String>("SELECT last_error FROM parameter_status").unwrap(),
            Some("unreachable peer".into())
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM parameters").unwrap(),
            Some(1)
        );
        let old = rows.clone();
        rows[0].value = json!("changed");
        persist(Ok(&rows), Some(&old), oid);
        assert_eq!(
            Spi::get_one::<String>("SELECT value #>> '{}' FROM parameters").unwrap(),
            Some("changed".into())
        );
        persist(Ok(&[]), Some(&rows), oid);
        assert_eq!(Spi::get_one::<bool>("SELECT NOT EXISTS (SELECT FROM parameters) AND (SELECT last_error IS NULL FROM parameter_status)").unwrap(), Some(true));
    }
}
