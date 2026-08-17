//! GAX codec round-trip 集成测试

use voice_providers::codec::{decode, decode_frame, encode, encode_frame, GaxFrame};

#[test]
fn encode_decode_roundtrip_short_payload() {
    let frame = GaxFrame::new(0x12, b"hello".to_vec());
    let bytes = encode_frame(&frame);
    let (decoded, consumed) = decode_frame(&bytes).unwrap();
    assert_eq!(decoded.cmd, 0x12);
    assert_eq!(decoded.payload, b"hello");
    assert_eq!(consumed, bytes.len());
}

#[test]
fn encode_decode_roundtrip_empty_payload() {
    let frame = GaxFrame::new(0x03, Vec::new());
    let bytes = encode_frame(&frame);
    let (decoded, _) = decode_frame(&bytes).unwrap();
    assert_eq!(decoded.cmd, 0x03);
    assert!(decoded.payload.is_empty());
}

#[test]
fn encode_decode_large_payload() {
    let payload: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    let frame = GaxFrame::new(0x22, payload.clone());
    let bytes = encode_frame(&frame);
    let (decoded, _) = decode_frame(&bytes).unwrap();
    assert_eq!(decoded.cmd, 0x22);
    assert_eq!(decoded.payload, payload);
}

#[test]
fn encode_length_field_correct() {
    let payload = vec![0u8; 50];
    let bytes = encode(0x11, &payload);
    // 长度 = 1（cmd）+ 50（payload）= 51
    let declared = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!(declared, 51);
    assert_eq!(bytes.len(), 4 + 51);
}

#[test]
fn decode_too_short_returns_err() {
    let r = decode(&[1, 2, 3]);
    assert!(r.is_err());
}

#[test]
fn decode_partial_frame_returns_err() {
    // 声称长度 200 但只给 5 字节
    let bytes = vec![0, 0, 0, 200, 0x12];
    let r = decode(&bytes);
    assert!(r.is_err());
}

#[test]
fn decode_can_be_replayed_after_consuming_full_frame() {
    // 把两个 frame 拼起来，逐个 decode
    let f1 = GaxFrame::new(0x01, b"first".to_vec());
    let f2 = GaxFrame::new(0x02, b"second".to_vec());
    let mut buf = encode_frame(&f1);
    buf.extend_from_slice(&encode_frame(&f2));

    let (d1, n1) = decode_frame(&buf).unwrap();
    assert_eq!(d1.cmd, 0x01);
    assert_eq!(d1.payload, b"first");
    buf.drain(..n1);

    let (d2, n2) = decode_frame(&buf).unwrap();
    assert_eq!(d2.cmd, 0x02);
    assert_eq!(d2.payload, b"second");
    buf.drain(..n2);

    assert!(buf.is_empty());
}