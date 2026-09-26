//! Decode a JSON object into a runtime ROS message.
//!
//! This is the inverse of the subscription encoder. Only fields present in the
//! object are written, so absent fields keep the message type's defaults. Field
//! names and JSON shapes must match the message, and unknown fields are
//! rejected rather than silently ignored.
use rclrs::{
    ArrayValueMut, BoundedSequenceValueMut, DynamicBoundedStringMut, DynamicBoundedWStringMut,
    DynamicMessage, DynamicMessageViewMut, SequenceValueMut, SimpleValueMut, ValueMut,
};
use serde_json::Value;

type Result<T> = std::result::Result<T, String>;

/// Fill a message from a JSON object.
pub(super) fn decode(message: &mut DynamicMessage, value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| "message must be a JSON object".to_owned())?;
    for field in object.keys() {
        if message.get(field).is_none() {
            return Err(format!("unknown message field {field:?}"));
        }
    }
    for (field, item) in object {
        let slot = message.get_mut(field).expect("field checked above");
        value_mut(slot, item, field)?;
    }
    Ok(())
}

fn message_view(view: &mut DynamicMessageViewMut<'_>, value: &Value, name: &str) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{name}: expected a JSON object"))?;
    for field in object.keys() {
        if view.get(field).is_none() {
            return Err(format!("{name}: unknown message field {field:?}"));
        }
    }
    for (field, item) in object {
        let slot = view.get_mut(field).expect("field checked above");
        value_mut(slot, item, &format!("{name}.{field}"))?;
    }
    Ok(())
}

fn value_mut(slot: ValueMut<'_>, value: &Value, name: &str) -> Result<()> {
    match slot {
        ValueMut::Simple(slot) => simple(slot, value, name),
        ValueMut::Array(slot) => array(slot, value, name),
        ValueMut::Sequence(slot) => sequence(slot, value, name),
        ValueMut::BoundedSequence(slot) => bounded_sequence(slot, value, name),
    }
}

fn simple(slot: SimpleValueMut<'_>, value: &Value, name: &str) -> Result<()> {
    match slot {
        SimpleValueMut::Float(element) => *element = float64(value, name)? as f32,
        SimpleValueMut::Double(element) => *element = float64(value, name)?,
        SimpleValueMut::LongDouble(_) => return Err(unsupported(name)),
        SimpleValueMut::Char(element) => *element = integer(value, name)?,
        SimpleValueMut::WChar(element) => *element = integer(value, name)?,
        SimpleValueMut::Boolean(element) => *element = boolean(value, name)?,
        SimpleValueMut::Octet(element) => *element = integer(value, name)?,
        SimpleValueMut::Uint8(element) => *element = integer(value, name)?,
        SimpleValueMut::Int8(element) => *element = integer(value, name)?,
        SimpleValueMut::Uint16(element) => *element = integer(value, name)?,
        SimpleValueMut::Int16(element) => *element = integer(value, name)?,
        SimpleValueMut::Uint32(element) => *element = integer(value, name)?,
        SimpleValueMut::Int32(element) => *element = integer(value, name)?,
        SimpleValueMut::Uint64(element) => *element = integer(value, name)?,
        SimpleValueMut::Int64(element) => *element = integer(value, name)?,
        SimpleValueMut::String(element) => *element = string(value, name)?.into(),
        SimpleValueMut::BoundedString(mut element) => element
            .try_assign(string(value, name)?)
            .map_err(|_| format!("{name}: string exceeds its ROS bound"))?,
        SimpleValueMut::WString(element) => *element = string(value, name)?.into(),
        SimpleValueMut::BoundedWString(mut element) => element
            .try_assign(string(value, name)?)
            .map_err(|_| format!("{name}: string exceeds its ROS bound"))?,
        SimpleValueMut::Message(mut element) => message_view(&mut element, value, name)?,
    }
    Ok(())
}

fn array(slot: ArrayValueMut<'_>, value: &Value, name: &str) -> Result<()> {
    let items = json_array(value, name)?;
    match slot {
        ArrayValueMut::FloatArray(slot) => fill_f32(slot, items, name),
        ArrayValueMut::DoubleArray(slot) => fill_f64(slot, items, name),
        ArrayValueMut::LongDoubleArray(_, _) => Err(unsupported(name)),
        ArrayValueMut::CharArray(slot) => fill_integer(slot, items, name),
        ArrayValueMut::WCharArray(slot) => fill_integer(slot, items, name),
        ArrayValueMut::BooleanArray(slot) => fill_boolean(slot, items, name),
        ArrayValueMut::OctetArray(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Uint8Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Int8Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Uint16Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Int16Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Uint32Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Int32Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Uint64Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::Int64Array(slot) => fill_integer(slot, items, name),
        ArrayValueMut::StringArray(slot) => fill_string(slot, items, name),
        ArrayValueMut::BoundedStringArray(mut slot) => {
            fill_bounded_string(&mut slot[..], items, name)
        }
        ArrayValueMut::WStringArray(slot) => fill_string(slot, items, name),
        ArrayValueMut::BoundedWStringArray(mut slot) => {
            fill_bounded_wstring(&mut slot[..], items, name)
        }
        ArrayValueMut::MessageArray(mut slot) => fill_message(&mut slot[..], items, name),
    }
}

fn sequence(slot: SequenceValueMut<'_>, value: &Value, name: &str) -> Result<()> {
    let items = json_array(value, name)?;
    match slot {
        SequenceValueMut::FloatSequence(slot) => *slot = collect_f32(items, name)?.into(),
        SequenceValueMut::DoubleSequence(slot) => *slot = collect_f64(items, name)?.into(),
        SequenceValueMut::LongDoubleSequence(_) => return Err(unsupported(name)),
        SequenceValueMut::CharSequence(slot) => *slot = collect_integer::<u8>(items, name)?.into(),
        SequenceValueMut::WCharSequence(slot) => {
            *slot = collect_integer::<u16>(items, name)?.into()
        }
        SequenceValueMut::BooleanSequence(slot) => *slot = collect_boolean(items, name)?.into(),
        SequenceValueMut::OctetSequence(slot) => *slot = collect_integer::<u8>(items, name)?.into(),
        SequenceValueMut::Uint8Sequence(slot) => *slot = collect_integer::<u8>(items, name)?.into(),
        SequenceValueMut::Int8Sequence(slot) => *slot = collect_integer::<i8>(items, name)?.into(),
        SequenceValueMut::Uint16Sequence(slot) => {
            *slot = collect_integer::<u16>(items, name)?.into()
        }
        SequenceValueMut::Int16Sequence(slot) => {
            *slot = collect_integer::<i16>(items, name)?.into()
        }
        SequenceValueMut::Uint32Sequence(slot) => {
            *slot = collect_integer::<u32>(items, name)?.into()
        }
        SequenceValueMut::Int32Sequence(slot) => {
            *slot = collect_integer::<i32>(items, name)?.into()
        }
        SequenceValueMut::Uint64Sequence(slot) => {
            *slot = collect_integer::<u64>(items, name)?.into()
        }
        SequenceValueMut::Int64Sequence(slot) => {
            *slot = collect_integer::<i64>(items, name)?.into()
        }
        SequenceValueMut::StringSequence(slot) => *slot = collect_string(items, name)?.into(),
        SequenceValueMut::BoundedStringSequence(mut slot) => {
            slot.reset(items.len());
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                element
                    .try_assign(string(item, name)?)
                    .map_err(|_| format!("{name}: string exceeds its ROS bound"))?;
            }
        }
        SequenceValueMut::WStringSequence(slot) => *slot = collect_string(items, name)?.into(),
        SequenceValueMut::BoundedWStringSequence(mut slot) => {
            slot.reset(items.len());
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                element
                    .try_assign(string(item, name)?)
                    .map_err(|_| format!("{name}: string exceeds its ROS bound"))?;
            }
        }
        SequenceValueMut::MessageSequence(mut slot) => {
            slot.reset(items.len());
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                message_view(element, item, name)?;
            }
        }
    }
    Ok(())
}

fn bounded_sequence(slot: BoundedSequenceValueMut<'_>, value: &Value, name: &str) -> Result<()> {
    let items = json_array(value, name)?;
    match slot {
        BoundedSequenceValueMut::FloatBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = float64(item, name)? as f32;
            }
        }
        BoundedSequenceValueMut::DoubleBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = float64(item, name)?;
            }
        }
        BoundedSequenceValueMut::LongDoubleBoundedSequence(_, _) => return Err(unsupported(name)),
        BoundedSequenceValueMut::CharBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::WCharBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::BooleanBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = boolean(item, name)?;
            }
        }
        BoundedSequenceValueMut::OctetBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Uint8BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Int8BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Uint16BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Int16BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Uint32BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Int32BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Uint64BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::Int64BoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = integer(item, name)?;
            }
        }
        BoundedSequenceValueMut::StringBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = string(item, name)?.into();
            }
        }
        BoundedSequenceValueMut::BoundedStringBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                element
                    .try_assign(string(item, name)?)
                    .map_err(|_| format!("{name}: string exceeds its ROS bound"))?;
            }
        }
        BoundedSequenceValueMut::WStringBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                *element = string(item, name)?.into();
            }
        }
        BoundedSequenceValueMut::BoundedWStringBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                element
                    .try_assign(string(item, name)?)
                    .map_err(|_| format!("{name}: string exceeds its ROS bound"))?;
            }
        }
        BoundedSequenceValueMut::MessageBoundedSequence(mut slot) => {
            resize(&mut slot, items, name)?;
            for (element, item) in slot.as_mut_slice().iter_mut().zip(items) {
                message_view(element, item, name)?;
            }
        }
    }
    Ok(())
}

/// Reset a bounded mutable sequence to the requested length.
fn resize<'msg, T>(
    slot: &mut rclrs::DynamicBoundedSequenceMut<'msg, T>,
    items: &[Value],
    name: &str,
) -> Result<()>
where
    T: rclrs::DynamicSequenceElementMut<'msg>,
{
    let bound = slot.upper_bound();
    if items.len() > bound {
        return Err(format!(
            "{name}: {} sequence elements exceed the ROS bound of {bound}",
            items.len()
        ));
    }
    slot.try_reset(items.len())
        .map_err(|_| format!("{name}: sequence exceeds the ROS bound of {bound}"))?;
    Ok(())
}

fn fill_integer<T>(slot: &mut [T], items: &[Value], name: &str) -> Result<()>
where
    T: TryFrom<i128>,
{
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        *element = integer(item, name)?;
    }
    Ok(())
}

fn fill_f32(slot: &mut [f32], items: &[Value], name: &str) -> Result<()> {
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        *element = float64(item, name)? as f32;
    }
    Ok(())
}

fn fill_f64(slot: &mut [f64], items: &[Value], name: &str) -> Result<()> {
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        *element = float64(item, name)?;
    }
    Ok(())
}

fn fill_boolean(slot: &mut [bool], items: &[Value], name: &str) -> Result<()> {
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        *element = boolean(item, name)?;
    }
    Ok(())
}

fn fill_string<T>(slot: &mut [T], items: &[Value], name: &str) -> Result<()>
where
    T: for<'a> From<&'a str>,
{
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        *element = string(item, name)?.into();
    }
    Ok(())
}

fn fill_bounded_string(
    slot: &mut [DynamicBoundedStringMut<'_>],
    items: &[Value],
    name: &str,
) -> Result<()> {
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        element
            .try_assign(string(item, name)?)
            .map_err(|_| format!("{name}: string exceeds its ROS bound"))?;
    }
    Ok(())
}

fn fill_bounded_wstring(
    slot: &mut [DynamicBoundedWStringMut<'_>],
    items: &[Value],
    name: &str,
) -> Result<()> {
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        element
            .try_assign(string(item, name)?)
            .map_err(|_| format!("{name}: string exceeds its ROS bound"))?;
    }
    Ok(())
}

fn fill_message(slot: &mut [DynamicMessageViewMut<'_>], items: &[Value], name: &str) -> Result<()> {
    check_length(slot.len(), items.len(), name)?;
    for (element, item) in slot.iter_mut().zip(items) {
        message_view(element, item, name)?;
    }
    Ok(())
}

fn collect_integer<T>(items: &[Value], name: &str) -> Result<Vec<T>>
where
    T: TryFrom<i128>,
{
    items.iter().map(|item| integer(item, name)).collect()
}

fn collect_f32(items: &[Value], name: &str) -> Result<Vec<f32>> {
    items
        .iter()
        .map(|item| float64(item, name).map(|number| number as f32))
        .collect()
}

fn collect_f64(items: &[Value], name: &str) -> Result<Vec<f64>> {
    items.iter().map(|item| float64(item, name)).collect()
}

fn collect_boolean(items: &[Value], name: &str) -> Result<Vec<bool>> {
    items.iter().map(|item| boolean(item, name)).collect()
}

fn collect_string<T>(items: &[Value], name: &str) -> Result<Vec<T>>
where
    T: for<'a> From<&'a str>,
{
    items
        .iter()
        .map(|item| string(item, name).map(Into::into))
        .collect()
}

fn check_length(expected: usize, actual: usize, name: &str) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(format!(
            "{name}: expected {expected} array elements, got {actual}"
        ))
    }
}

fn json_array<'a>(value: &'a Value, name: &str) -> Result<&'a Vec<Value>> {
    value
        .as_array()
        .ok_or_else(|| format!("{name}: expected a JSON array"))
}

fn integer<T>(value: &Value, name: &str) -> Result<T>
where
    T: TryFrom<i128>,
{
    let number = value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
        .ok_or_else(|| format!("{name}: expected a JSON integer"))?;
    T::try_from(number).map_err(|_| format!("{name}: integer {number} is out of range"))
}

/// Read a JSON number, mapping null to NaN.
///
/// The subscription encoder writes null for non-finite values, so accepting
/// null here keeps the two directions consistent and round-trippable.
fn float64(value: &Value, name: &str) -> Result<f64> {
    if value.is_null() {
        return Ok(f64::NAN);
    }
    value
        .as_f64()
        .ok_or_else(|| format!("{name}: expected a JSON number or null"))
}

fn boolean(value: &Value, name: &str) -> Result<bool> {
    value
        .as_bool()
        .ok_or_else(|| format!("{name}: expected a JSON boolean"))
}

fn string<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value
        .as_str()
        .ok_or_else(|| format!("{name}: expected a JSON string"))
}

fn unsupported(name: &str) -> String {
    format!("{name}: long double fields are unsupported")
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use rclrs::{ArrayValue, SimpleValue, Value};

    fn decoded(kind: &str, json: &str) -> Result<DynamicMessage> {
        let mut message = DynamicMessage::new(kind.try_into().unwrap()).unwrap();
        decode(&mut message, &serde_json::from_str(json).unwrap())?;
        Ok(message)
    }

    // Encoding a decoded default message must reproduce the original JSON. The
    // encoder is the subscription payload builder, so this pins the decoder to
    // its exact field semantics for scalars, arrays, sequences, and nesting.
    fn round_trip(kind: &str) {
        pgrx::log!("pg_ros2 round_trip kind={kind} phase=new");
        let original = DynamicMessage::new(kind.try_into().unwrap()).unwrap();
        pgrx::log!("pg_ros2 round_trip kind={kind} phase=encode");
        let encoded =
            crate::subscriptions::json::payload("/test", kind, 0, &original.view()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        pgrx::log!("pg_ros2 round_trip kind={kind} phase=decode");
        let mut decoded = DynamicMessage::new(kind.try_into().unwrap()).unwrap();
        decode(&mut decoded, &parsed["message"]).unwrap();
        pgrx::log!("pg_ros2 round_trip kind={kind} phase=reencode");
        let reencoded =
            crate::subscriptions::json::payload("/test", kind, 0, &decoded.view()).unwrap();
        assert_eq!(encoded, reencoded, "{kind}");
    }

    #[pgrx::pg_test]
    fn test_round_trip_string() {
        round_trip("std_msgs/msg/String");
    }

    #[pgrx::pg_test]
    fn test_round_trip_arrays() {
        round_trip("test_msgs/msg/Arrays");
    }

    #[pgrx::pg_test]
    fn test_round_trip_unbounded_sequences() {
        round_trip("test_msgs/msg/UnboundedSequences");
    }

    #[pgrx::pg_test]
    fn test_round_trip_bounded_sequences() {
        round_trip("test_msgs/msg/BoundedSequences");
    }

    #[pgrx::pg_test]
    fn test_round_trip_nested() {
        round_trip("test_msgs/msg/Nested");
    }

    #[pgrx::pg_test]
    fn test_decode_scalars_and_rejections() {
        let message = decoded("std_msgs/msg/String", r#"{"data":"hello \"ROS\"\n世界"}"#).unwrap();
        let Some(Value::Simple(SimpleValue::String(data))) = message.get("data") else {
            panic!("missing string field");
        };
        assert_eq!(data.to_string(), "hello \"ROS\"\n世界");
        for json in [r#"{"missing":1}"#, r#"{"data":1}"#, r#"[1,2]"#, "null"] {
            assert!(decoded("std_msgs/msg/String", json).is_err(), "{json}");
        }
    }

    #[pgrx::pg_test]
    fn test_decode_arrays_and_sequences() {
        let message = decoded(
            "test_msgs/msg/Arrays",
            r#"{"int32_values_default":[1,-2,3],"uint64_values_default":[0,1,18446744073709551615],"string_values_default":["a","b","c"]}"#,
        )
        .unwrap();
        let Some(Value::Array(ArrayValue::Uint64Array(values))) =
            message.get("uint64_values_default")
        else {
            panic!("missing uint64 array");
        };
        assert_eq!(values, &[0_u64, 1, u64::MAX][..]);
        let Some(Value::Array(ArrayValue::StringArray(values))) =
            message.get("string_values_default")
        else {
            panic!("missing string array");
        };
        assert_eq!(
            values.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert!(decoded("test_msgs/msg/Arrays", r#"{"int32_values_default":[1,2]}"#).is_err());
        assert!(decoded(
            "test_msgs/msg/Arrays",
            r#"{"int32_values_default":[1,2,9999999999]}"#
        )
        .is_err());
    }

    #[pgrx::pg_test]
    fn test_decode_sequences() {
        let message = decoded(
            "test_msgs/msg/UnboundedSequences",
            r#"{"int32_values_default":[1,-2,3],"float64_values_default":[0.5,1.5]}"#,
        )
        .unwrap();
        let Some(Value::Sequence(rclrs::SequenceValue::Int32Sequence(values))) =
            message.get("int32_values_default")
        else {
            panic!("missing int32 sequence");
        };
        assert_eq!(values.as_slice(), &[1, -2, 3][..]);
    }

    #[pgrx::pg_test]
    fn test_decode_null_float_is_nan() {
        let message = decoded(
            "test_msgs/msg/Arrays",
            r#"{"float32_values_default":[1.5,null,2.5]}"#,
        )
        .unwrap();
        let Some(Value::Array(ArrayValue::FloatArray(values))) =
            message.get("float32_values_default")
        else {
            panic!("missing float array");
        };
        assert_eq!(values[0], 1.5);
        assert!(values[1].is_nan());
        assert_eq!(values[2], 2.5);
    }
}
