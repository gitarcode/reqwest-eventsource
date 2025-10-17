use eventsource_stream::Event as MessageEvent;
use nom::{
    bytes::complete::take,
    multi::many0,
    number::complete::{be_u16, be_u32, be_u8},
    sequence::tuple,
    IResult,
};
use std::collections::HashMap;
use std::convert::TryInto;
use tracing::debug;

/// Parses Amazon EventStream binary format
/// Format: [total_length][headers_length][prelude_crc][headers][payload][message_crc]
pub fn parse_eventstream_message(input: &[u8]) -> IResult<&[u8], MessageEvent> {
    debug!(
        "(eventsource) parse_eventstream_message called with {} bytes",
        input.len()
    );

    let (input, (total_length, headers_length, _prelude_crc)) =
        tuple((be_u32, be_u32, be_u32))(input)?;

    debug!(
        "(eventsource) total_length={}, headers_length={}",
        total_length, headers_length
    );

    // Headers section
    let (input, headers_data) = take(headers_length)(input)?;
    let (_, headers) = parse_headers(headers_data)?;

    debug!("(eventsource) parsed headers: {:?}", headers);

    // Payload section (remaining data minus the final CRC)
    let fixed_overhead = 12 + 4; // prelude (12 bytes) + message_crc (4 bytes)
    let payload_length = if total_length >= fixed_overhead + headers_length {
        total_length - fixed_overhead - headers_length
    } else {
        // Invalid message format, but don't panic
        debug!("(eventsource) Invalid message format - total_length={}, fixed_overhead={}, headers_length={}", 
            total_length, fixed_overhead, headers_length);
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    };
    let (input, payload_data) = take(payload_length)(input)?;

    debug!(
        "(eventsource) payload_length={}, payload_data len={}",
        payload_length,
        payload_data.len()
    );
    debug!(
        "(eventsource) raw payload: {:?}",
        std::str::from_utf8(payload_data).unwrap_or("Invalid UTF-8")
    );

    // Skip message CRC
    let (input, _message_crc) = be_u32(input)?;

    // Extract event data from headers and payload
    let event = build_event_from_eventstream(headers, payload_data)?;

    debug!(
        "(eventsource) built event: id='{}', event='{}', data='{}'",
        event.id, event.event, event.data
    );

    Ok((input, event))
}

fn parse_headers(input: &[u8]) -> IResult<&[u8], HashMap<String, HeaderValue>> {
    many0(parse_header)(input).map(|(input, headers)| {
        let header_map = headers.into_iter().collect();
        (input, header_map)
    })
}

fn parse_header(input: &[u8]) -> IResult<&[u8], (String, HeaderValue)> {
    let (input, header_name_length) = be_u8(input)?;
    let (input, header_name) = take(header_name_length)(input)?;
    let (input, header_value_type) = be_u8(input)?;
    let (input, header_value_length) = be_u16(input)?;
    let (input, header_value_data) = take(header_value_length)(input)?;

    let header_name = String::from_utf8_lossy(header_name).to_string();
    let header_value = match header_value_type {
        0 => {
            if header_value_data.len() >= 1 {
                HeaderValue::Bool(header_value_data[0] != 0)
            } else {
                HeaderValue::Bool(false)
            }
        }
        1 => {
            if header_value_data.len() >= 1 {
                HeaderValue::Byte(header_value_data[0] as i8)
            } else {
                HeaderValue::Byte(0)
            }
        }
        2 => {
            if let Ok(bytes) = header_value_data.try_into() {
                HeaderValue::Short(i16::from_be_bytes(bytes))
            } else {
                HeaderValue::Short(0)
            }
        }
        3 => {
            if let Ok(bytes) = header_value_data.try_into() {
                HeaderValue::Int(i32::from_be_bytes(bytes))
            } else {
                HeaderValue::Int(0)
            }
        }
        4 => {
            if let Ok(bytes) = header_value_data.try_into() {
                HeaderValue::Long(i64::from_be_bytes(bytes))
            } else {
                HeaderValue::Long(0)
            }
        }
        5 => HeaderValue::ByteArray(header_value_data.to_vec()),
        6 => HeaderValue::String(String::from_utf8_lossy(header_value_data).to_string()),
        7 => {
            if let Ok(bytes) = header_value_data.try_into() {
                HeaderValue::Timestamp(i64::from_be_bytes(bytes))
            } else {
                HeaderValue::Timestamp(0)
            }
        }
        8 => HeaderValue::Uuid(header_value_data.to_vec()),
        _ => HeaderValue::ByteArray(header_value_data.to_vec()),
    };

    Ok((input, (header_name, header_value)))
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum HeaderValue {
    Bool(bool),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    ByteArray(Vec<u8>),
    String(String),
    Timestamp(i64),
    Uuid(Vec<u8>),
}

fn build_event_from_eventstream(
    _headers: HashMap<String, HeaderValue>,
    payload: &[u8],
) -> Result<MessageEvent, nom::Err<nom::error::Error<&[u8]>>> {
    debug!("(eventsource) build_event_from_eventstream called");

    let payload_str = String::from_utf8_lossy(payload).to_string();
    debug!("(eventsource) payload: '{}'", payload_str);

    // Try to parse as JSON and extract base64 bytes
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&payload_str) {
        debug!("(eventsource) parsed JSON payload: {:?}", json_value);

        if let Some(bytes_str) = json_value.get("bytes").and_then(|v| v.as_str()) {
            debug!("(eventsource) found base64 bytes field: '{}'", bytes_str);

            if let Ok(decoded_bytes) = base64::decode(bytes_str) {
                if let Ok(decoded_str) = std::str::from_utf8(&decoded_bytes) {
                    debug!("(eventsource) decoded base64 to: '{}'", decoded_str);
                    return Ok(MessageEvent {
                        id: String::new(),
                        event: "message".to_string(),
                        retry: None,
                        data: decoded_str.to_string(),
                    });
                } else {
                    debug!("(eventsource) failed to convert decoded bytes to UTF-8");
                }
            } else {
                debug!("(eventsource) failed to decode base64");
            }
        } else {
            debug!("(eventsource) no 'bytes' field found in JSON");
        }
    } else {
        debug!("(eventsource) failed to parse payload as JSON");
    }

    // Fallback to raw payload
    debug!("(eventsource) using raw payload as fallback");
    Ok(MessageEvent {
        id: String::new(),
        event: "message".to_string(),
        retry: None,
        data: payload_str,
    })
}

/// Parses multiple EventStream messages from a buffer
pub fn parse_eventstream_messages(mut input: &[u8]) -> Vec<MessageEvent> {
    debug!(
        "(eventsource) parse_eventstream_messages called with {} bytes",
        input.len()
    );

    let mut events = Vec::new();
    let mut previous_len = input.len();

    while !input.is_empty() {
        debug!(
            "(eventsource) parsing message from buffer with {} bytes remaining",
            input.len()
        );

        match parse_eventstream_message(input) {
            Ok((remaining, event)) => {
                debug!(
                    "(eventsource) successfully parsed event, {} bytes remaining",
                    remaining.len()
                );
                events.push(event);
                input = remaining;

                // Prevent infinite loops - ensure we're making progress
                if input.len() >= previous_len {
                    debug!("(eventsource) no progress made, breaking to prevent infinite loop");
                    break;
                }
                previous_len = input.len();
            }
            Err(e) => {
                debug!("(eventsource) failed to parse message: {:?}", e);
                break;
            }
        }
    }

    debug!(
        "(eventsource) parse_eventstream_messages returning {} events",
        events.len()
    );
    events
}
