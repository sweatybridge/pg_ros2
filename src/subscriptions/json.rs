//! Bounded JSON encoding for runtime ROS message types.
use rclrs::{
    ArrayValue, BoundedSequenceValue, DynamicMessageView, SequenceValue, SimpleValue, Value,
};
use std::fmt::{self, Write};

const LIMIT: usize = 7999;
type Result<T> = std::result::Result<T, &'static str>;

#[derive(Default)]
struct Output(String);

impl Write for Output {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.0.len() + value.len() > LIMIT {
            return Err(fmt::Error);
        }
        self.0.push_str(value);
        Ok(())
    }
}

impl Output {
    fn push(&mut self, value: &str) -> Result<()> {
        self.write_str(value)
            .map_err(|_| "notification payload exceeds 7999 bytes")
    }

    fn string(&mut self, value: impl fmt::Display) -> Result<()> {
        let mut raw = Output::default();
        write!(raw, "{value}").map_err(|_| "notification payload exceeds 7999 bytes")?;
        self.push(&serde_json::to_string(&raw.0).expect("string serialization"))
    }

    fn number(&mut self, value: impl fmt::Display) -> Result<()> {
        write!(self, "{value}").map_err(|_| "notification payload exceeds 7999 bytes")
    }

    fn message(&mut self, message: &DynamicMessageView<'_>, depth: usize) -> Result<()> {
        if depth > 64 {
            return Err("message nesting exceeds 64 levels");
        }
        self.push("{")?;
        for (index, (name, value)) in message.iter().enumerate() {
            if index > 0 {
                self.push(",")?;
            }
            self.string(name)?;
            self.push(":")?;
            self.value(value, depth + 1)?;
        }
        self.push("}")
    }

    fn array<T>(
        &mut self,
        values: &[T],
        mut encode: impl FnMut(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        self.push("[")?;
        for (index, value) in values.iter().enumerate() {
            if index > 0 {
                self.push(",")?;
            }
            encode(self, value)?;
        }
        self.push("]")
    }

    fn value(&mut self, value: Value<'_>, depth: usize) -> Result<()> {
        match value {
            Value::Simple(value) => match value {
                SimpleValue::Float(v) => {
                    if v.is_finite() {
                        self.number(v)
                    } else {
                        self.push("null")
                    }
                }
                SimpleValue::Double(v) => {
                    if v.is_finite() {
                        self.number(v)
                    } else {
                        self.push("null")
                    }
                }
                SimpleValue::Char(v) => self.number(v),
                SimpleValue::WChar(v) => self.number(v),
                SimpleValue::Boolean(v) => self.number(v),
                SimpleValue::Octet(v) => self.number(v),
                SimpleValue::Uint8(v) => self.number(v),
                SimpleValue::Int8(v) => self.number(v),
                SimpleValue::Uint16(v) => self.number(v),
                SimpleValue::Int16(v) => self.number(v),
                SimpleValue::Uint32(v) => self.number(v),
                SimpleValue::Int32(v) => self.number(v),
                SimpleValue::Uint64(v) => self.number(v),
                SimpleValue::Int64(v) => self.number(v),
                SimpleValue::String(v) => self.string(v),
                SimpleValue::BoundedString(v) => self.string(v),
                SimpleValue::WString(v) => self.string(v),
                SimpleValue::BoundedWString(v) => self.string(v),
                SimpleValue::Message(v) => self.message(&v, depth),
                SimpleValue::LongDouble(_) => Err("long double fields are unsupported"),
            },
            Value::Array(value) => match value {
                ArrayValue::FloatArray(v) => self.array(v, |out, v| {
                    if v.is_finite() {
                        out.number(v)
                    } else {
                        out.push("null")
                    }
                }),
                ArrayValue::DoubleArray(v) => self.array(v, |out, v| {
                    if v.is_finite() {
                        out.number(v)
                    } else {
                        out.push("null")
                    }
                }),
                ArrayValue::CharArray(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::WCharArray(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::BooleanArray(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::OctetArray(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Uint8Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Int8Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Uint16Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Int16Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Uint32Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Int32Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Uint64Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::Int64Array(v) => self.array(v, |out, v| out.number(v)),
                ArrayValue::StringArray(v) => self.array(v, |out, v| out.string(v)),
                ArrayValue::BoundedStringArray(v) => self.array(&v, |out, v| out.string(v)),
                ArrayValue::WStringArray(v) => self.array(v, |out, v| out.string(v)),
                ArrayValue::BoundedWStringArray(v) => self.array(&v, |out, v| out.string(v)),
                ArrayValue::MessageArray(v) => self.array(&v, |out, v| out.message(v, depth)),
                ArrayValue::LongDoubleArray(_, _) => Err("long double fields are unsupported"),
            },
            Value::Sequence(value) => match value {
                SequenceValue::FloatSequence(v) => self.array(v, |out, v| {
                    if v.is_finite() {
                        out.number(v)
                    } else {
                        out.push("null")
                    }
                }),
                SequenceValue::DoubleSequence(v) => self.array(v, |out, v| {
                    if v.is_finite() {
                        out.number(v)
                    } else {
                        out.push("null")
                    }
                }),
                SequenceValue::CharSequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::WCharSequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::BooleanSequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::OctetSequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Uint8Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Int8Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Uint16Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Int16Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Uint32Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Int32Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Uint64Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::Int64Sequence(v) => self.array(v, |out, v| out.number(v)),
                SequenceValue::StringSequence(v) => self.array(v, |out, v| out.string(v)),
                SequenceValue::BoundedStringSequence(v) => self.array(&v, |out, v| out.string(v)),
                SequenceValue::WStringSequence(v) => self.array(v, |out, v| out.string(v)),
                SequenceValue::BoundedWStringSequence(v) => self.array(&v, |out, v| out.string(v)),
                SequenceValue::MessageSequence(v) => self.array(&v, |out, v| out.message(v, depth)),
                SequenceValue::LongDoubleSequence(_) => Err("long double fields are unsupported"),
            },
            Value::BoundedSequence(value) => match value {
                BoundedSequenceValue::FloatBoundedSequence(v) => self.array(&v, |out, v| {
                    if v.is_finite() {
                        out.number(v)
                    } else {
                        out.push("null")
                    }
                }),
                BoundedSequenceValue::DoubleBoundedSequence(v) => self.array(&v, |out, v| {
                    if v.is_finite() {
                        out.number(v)
                    } else {
                        out.push("null")
                    }
                }),
                BoundedSequenceValue::CharBoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::WCharBoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::BooleanBoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::OctetBoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Uint8BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Int8BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Uint16BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Int16BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Uint32BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Int32BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Uint64BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::Int64BoundedSequence(v) => {
                    self.array(&v, |out, v| out.number(v))
                }
                BoundedSequenceValue::StringBoundedSequence(v) => {
                    self.array(&v, |out, v| out.string(v))
                }
                BoundedSequenceValue::BoundedStringBoundedSequence(v) => {
                    self.array(&v, |out, v| out.string(v))
                }
                BoundedSequenceValue::WStringBoundedSequence(v) => {
                    self.array(&v, |out, v| out.string(v))
                }
                BoundedSequenceValue::BoundedWStringBoundedSequence(v) => {
                    self.array(&v, |out, v| out.string(v))
                }
                BoundedSequenceValue::MessageBoundedSequence(v) => {
                    self.array(&v, |out, v| out.message(v, depth))
                }
                BoundedSequenceValue::LongDoubleBoundedSequence(_, _) => {
                    Err("long double fields are unsupported")
                }
            },
        }
    }
}

pub(super) fn payload(
    topic: &str,
    message_type: &str,
    sequence: u64,
    message: &DynamicMessageView<'_>,
) -> Result<String> {
    let mut output = Output::default();
    output.push("{\"topic\":")?;
    output.string(topic)?;
    output.push(",\"message_type\":")?;
    output.string(message_type)?;
    output.push(",\"sequence\":")?;
    output.number(sequence)?;
    output.push(",\"message\":")?;
    output.message(message, 0)?;
    output.push("}")?;
    Ok(output.0)
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use rclrs::{DynamicMessage, SimpleValueMut, ValueMut};

    #[pgrx::pg_test]
    fn test_json_string_and_payload_limit() {
        let mut message = DynamicMessage::new("std_msgs/msg/String".try_into().unwrap()).unwrap();
        if let Some(ValueMut::Simple(SimpleValueMut::String(data))) = message.get_mut("data") {
            *data = "hello \"ROS\"\n世界".into();
        } else {
            panic!("missing string field");
        }
        let encoded = payload("/test", "std_msgs/msg/String", 7, &message.view()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(parsed["message"]["data"], "hello \"ROS\"\n世界");
        assert_eq!(parsed["sequence"], 7);
        if let Some(ValueMut::Simple(SimpleValueMut::String(data))) = message.get_mut("data") {
            *data = "x".repeat(8000).as_str().into();
        }
        assert_eq!(
            payload("/test", "std_msgs/msg/String", 8, &message.view()),
            Err("notification payload exceeds 7999 bytes")
        );
    }

    #[pgrx::pg_test]
    fn test_json_nested_arrays_and_sequences() {
        for kind in [
            "test_msgs/msg/Arrays",
            "test_msgs/msg/UnboundedSequences",
            "test_msgs/msg/BoundedSequences",
            "test_msgs/msg/Nested",
        ] {
            let message = DynamicMessage::new(kind.try_into().unwrap()).unwrap();
            let encoded = payload("/test", kind, 0, &message.view()).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            assert!(parsed["message"].is_object());
            if kind.ends_with("Sequences") {
                assert_eq!(
                    parsed["message"]["int32_values_default"],
                    serde_json::json!([0, 2147483647_i32, -2147483648_i32])
                );
                assert_eq!(
                    parsed["message"]["uint64_values_default"][2],
                    serde_json::json!(u64::MAX)
                );
            }
        }
        let mut output = Output::default();
        output
            .value(Value::Array(ArrayValue::Int32Array(&[1, -2, 3])), 0)
            .unwrap();
        assert_eq!(output.0, "[1,-2,3]");
        let mut output = Output::default();
        output
            .value(Value::Simple(SimpleValue::Double(&f64::NAN)), 0)
            .unwrap();
        assert_eq!(output.0, "null");
    }

    #[pgrx::pg_test]
    fn test_output_byte_limit() {
        let mut output = Output::default();
        output.push(&"x".repeat(LIMIT)).unwrap();
        assert!(output.push("x").is_err());
        assert_eq!(output.0.len(), LIMIT);
        let mut output = Output::default();
        assert!(output.string("\n".repeat(4000)).is_err());
    }
}
